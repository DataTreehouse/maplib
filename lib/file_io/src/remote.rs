//! Files in object storage: Google Cloud Storage (`gs://`), Azure Blob Storage (`az://`,
//! `azure://`, `abfs://`, `abfss://`, `adl://` and `https://<account>.blob.core.windows.net/..`),
//! Amazon S3 and compatible stores (`s3://`, `s3a://`), plain HTTP(S) and `file://`.
//!
//! Credentials are read from the environment as the cloud SDKs do (e.g.
//! `GOOGLE_APPLICATION_CREDENTIALS`, the metadata server on Google Cloud, `AZURE_STORAGE_*`,
//! managed and workload identities on Azure, `AWS_*`), and can be given or overridden with
//! storage options, e.g. `{"azure_storage_account_name": ..., "azure_storage_account_key": ...}`.
//! The option names are those of the `object_store` crate (and of Polars' `storage_options`).
//!
//! Calls block: object storage is reached through a tokio runtime shared by the process, so
//! these functions must not be called from inside a tokio runtime.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
pub use object_store::UpdateVersion;
use object_store::{
    ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutMode, PutOptions, PutPayload,
    WriteMultipart,
};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

/// Options for reaching object storage, e.g. credentials, see the module documentation.
pub type RemoteOptions = HashMap<String, String>;

/// Objects larger than this are downloaded with concurrent range requests.
const RANGED_DOWNLOAD_THRESHOLD: u64 = 32 << 20;
const DOWNLOAD_CHUNK: u64 = 16 << 20;
const DOWNLOAD_CONCURRENCY: usize = 8;
/// Multipart uploads use parts of this size, uploaded concurrently.
const UPLOAD_CHUNK: usize = 16 << 20;
const UPLOAD_CONCURRENCY: usize = 8;
/// Objects smaller than this are uploaded with a single put.
const SINGLE_PUT_LIMIT: u64 = 16 << 20;

#[derive(Error, Debug)]
pub enum RemoteError {
    #[error("Invalid object storage URL {0}: {1}")]
    InvalidUrl(String, String),
    #[error("Object storage error for {0}: {1}")]
    ObjectStore(String, object_store::Error),
    #[error("IO error for {0}: {1}")]
    Io(String, io::Error),
}

impl From<RemoteError> for io::Error {
    fn from(e: RemoteError) -> Self {
        match e {
            RemoteError::Io(_, e) => e,
            RemoteError::ObjectStore(url, object_store::Error::NotFound { .. }) => {
                io::Error::new(io::ErrorKind::NotFound, format!("{url} not found"))
            }
            e => io::Error::other(e.to_string()),
        }
    }
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("maplib-object-store")
        .enable_all()
        .build()
        .expect("Could not start the object storage runtime")
});

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    RUNTIME.block_on(f)
}

const REMOTE_SCHEMES: &[&str] = &[
    "gs", "az", "azure", "abfs", "abfss", "adl", "s3", "s3a", "http", "https", "file",
];

/// Whether `s` is a URL of object storage (or `file://`) rather than a local path.
pub fn is_remote(s: &str) -> bool {
    let Some((scheme, rest)) = s.split_once("://") else {
        return false;
    };
    // Windows paths such as C:\x or C:/x have a single letter "scheme" and no "//".
    !rest.is_empty() && REMOTE_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
}

/// An object, or a prefix of objects (a "folder"), in object storage.
#[derive(Clone)]
pub struct RemoteLocation {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    /// The URL of the store root, e.g. `gs://bucket`, used in messages.
    root: String,
}

impl std::fmt::Debug for RemoteLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteLocation({self})")
    }
}

impl Display for RemoteLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.path.as_ref().is_empty() {
            write!(f, "{}", self.root)
        } else {
            write!(f, "{}/{}", self.root, self.path)
        }
    }
}

macro_rules! with_options {
    ($builder:expr, $url:expr, $options:expr) => {{
        let mut builder = $builder.with_url($url.as_str());
        for (k, v) in $options {
            match k.to_ascii_lowercase().parse() {
                Ok(key) => builder = builder.with_config(key, v.clone()),
                Err(_) => {
                    return Err(RemoteError::InvalidUrl(
                        $url.to_string(),
                        format!("unknown storage option {k}"),
                    ))
                }
            }
        }
        Arc::new(
            builder
                .build()
                .map_err(|e| RemoteError::ObjectStore($url.to_string(), e))?,
        ) as Arc<dyn ObjectStore>
    }};
}

impl RemoteLocation {
    /// Parses an object storage URL, see the module documentation.
    pub fn parse(url: &str, options: &RemoteOptions) -> Result<Self, RemoteError> {
        let invalid = |m: String| RemoteError::InvalidUrl(url.to_string(), m);
        let parsed = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
        let (scheme, path) = ObjectStoreScheme::parse(&parsed).map_err(|e| invalid(e.to_string()))?;
        let store: Arc<dyn ObjectStore> = match scheme {
            ObjectStoreScheme::GoogleCloudStorage => with_options!(
                object_store::gcp::GoogleCloudStorageBuilder::from_env(),
                parsed,
                options
            ),
            ObjectStoreScheme::MicrosoftAzure => with_options!(
                object_store::azure::MicrosoftAzureBuilder::from_env(),
                parsed,
                options
            ),
            ObjectStoreScheme::AmazonS3 => with_options!(
                object_store::aws::AmazonS3Builder::from_env(),
                parsed,
                options
            ),
            ObjectStoreScheme::Http => {
                let base = &parsed[..url::Position::BeforePath];
                let mut builder = object_store::http::HttpBuilder::new().with_url(base);
                for (k, v) in options {
                    match k.to_ascii_lowercase().parse() {
                        Ok(key) => builder = builder.with_config(key, v.clone()),
                        Err(_) => return Err(invalid(format!("unknown storage option {k}"))),
                    }
                }
                Arc::new(
                    builder
                        .build()
                        .map_err(|e| RemoteError::ObjectStore(url.to_string(), e))?,
                )
            }
            ObjectStoreScheme::Local => Arc::new(object_store::local::LocalFileSystem::new()),
            s => return Err(invalid(format!("unsupported scheme {s:?}"))),
        };
        let root = match scheme {
            ObjectStoreScheme::Local => "file://".to_string(),
            _ => {
                // The URL up to where the object path starts, e.g. gs://bucket or
                // https://account.blob.core.windows.net/container.
                let full = url.trim_end_matches('/');
                let p = path.as_ref();
                if !p.is_empty() && full.ends_with(p) {
                    full[..full.len() - p.len()].trim_end_matches('/').to_string()
                } else {
                    full.to_string()
                }
            }
        };
        Ok(RemoteLocation { store, path, root })
    }

    /// The root of a new, empty store in memory, for tests.
    pub fn in_memory() -> RemoteLocation {
        RemoteLocation {
            store: Arc::new(object_store::memory::InMemory::new()),
            path: ObjectPath::default(),
            root: "memory://".to_string(),
        }
    }

    /// The object at `rel`, a path relative to this location with `/` separators.
    pub fn child(&self, rel: &str) -> RemoteLocation {
        let mut path = self.path.clone();
        for part in rel.split('/').filter(|p| !p.is_empty()) {
            path = path.join(part);
        }
        RemoteLocation {
            store: self.store.clone(),
            path,
            root: self.root.clone(),
        }
    }

    /// The last segment of the path, e.g. `data.ttl` for `gs://bucket/folder/data.ttl`.
    pub fn file_name(&self) -> Option<&str> {
        self.path.filename()
    }

    fn err(&self, e: object_store::Error) -> RemoteError {
        RemoteError::ObjectStore(self.to_string(), e)
    }

    fn io_err(&self, e: io::Error) -> RemoteError {
        RemoteError::Io(self.to_string(), e)
    }

    /// The size of the object, or None if there is no object here.
    pub fn size(&self) -> Result<Option<u64>, RemoteError> {
        match block_on(self.store.head(&self.path)) {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(self.err(e)),
        }
    }

    pub fn exists(&self) -> Result<bool, RemoteError> {
        Ok(self.size()?.is_some())
    }

    pub fn get(&self) -> Result<Bytes, RemoteError> {
        block_on(async {
            self.store
                .get(&self.path)
                .await
                .map_err(|e| self.err(e))?
                .bytes()
                .await
                .map_err(|e| self.err(e))
        })
    }

    /// Stores `bytes` as the object, replacing it if it exists. The object appears whole.
    pub fn put(&self, bytes: Bytes) -> Result<(), RemoteError> {
        block_on(self.store.put(&self.path, PutPayload::from_bytes(bytes)))
            .map(|_| ())
            .map_err(|e| self.err(e))
    }

    /// Stores `bytes` as the object like `put`, with a multipart upload when they are large
    /// (single puts are limited to a few GB by some stores).
    pub fn put_large(&self, bytes: Vec<u8>) -> Result<(), RemoteError> {
        if bytes.len() as u64 <= SINGLE_PUT_LIMIT {
            return self.put(Bytes::from(bytes));
        }
        let mut w = self.writer()?;
        w.write_all(&bytes).map_err(|e| self.io_err(e))?;
        w.finish()
    }

    /// The current version of the object, None if there is none, for `put_if`.
    pub fn version(&self) -> Result<Option<UpdateVersion>, RemoteError> {
        match block_on(self.store.head(&self.path)) {
            Ok(meta) => Ok(Some(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(self.err(e)),
        }
    }

    /// Stores `bytes` as the object only if it is still at `expected` (None: if it does not
    /// exist), so that two writers can not both replace the same version. Returns false if the
    /// object was changed by someone else. Stores without conditional puts (the local file
    /// system for updates) replace the object unconditionally.
    pub fn put_if(
        &self,
        bytes: Bytes,
        expected: Option<UpdateVersion>,
    ) -> Result<bool, RemoteError> {
        let mode = match expected {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        match block_on(
            self.store
                .put_opts(&self.path, PutPayload::from_bytes(bytes.clone()), opts),
        ) {
            Ok(_) => Ok(true),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(object_store::Error::NotImplemented { .. }) => {
                self.put(bytes)?;
                Ok(true)
            }
            Err(e) => Err(self.err(e)),
        }
    }

    /// Downloads the object to the local file `dest`, which is replaced. Large objects are
    /// downloaded with concurrent range requests.
    pub fn download_to(&self, dest: &Path) -> Result<u64, RemoteError> {
        let size = match self.size()? {
            Some(s) => s,
            None => {
                return Err(self.err(object_store::Error::NotFound {
                    path: self.path.to_string(),
                    source: "no such object".into(),
                }))
            }
        };
        let file = File::create(dest).map_err(|e| self.io_err(e))?;
        if size <= RANGED_DOWNLOAD_THRESHOLD {
            let bytes = self.get()?;
            (&file).write_all(&bytes).map_err(|e| self.io_err(e))?;
        } else {
            file.set_len(size).map_err(|e| self.io_err(e))?;
            let ranges: Vec<_> = (0..size)
                .step_by(DOWNLOAD_CHUNK as usize)
                .map(|start| start..(start + DOWNLOAD_CHUNK).min(size))
                .collect();
            let file = &file;
            block_on(async {
                futures::stream::iter(ranges)
                    .map(|r| async move {
                        let start = r.start;
                        let bytes = self
                            .store
                            .get_range(&self.path, r)
                            .await
                            .map_err(|e| self.err(e))?;
                        Ok::<_, RemoteError>((start, bytes))
                    })
                    .buffer_unordered(DOWNLOAD_CONCURRENCY)
                    .try_for_each(|(start, bytes)| async move {
                        write_all_at(file, &bytes, start).map_err(|e| self.io_err(e))
                    })
                    .await
            })?;
        }
        file.sync_all().map_err(|e| self.io_err(e))?;
        Ok(size)
    }

    /// Uploads the local file `src` as the object, replacing it if it exists. The object
    /// appears whole when the upload is complete, never partially written.
    pub fn upload_from(&self, src: &Path) -> Result<(), RemoteError> {
        let size = std::fs::metadata(src).map_err(|e| self.io_err(e))?.len();
        if size <= SINGLE_PUT_LIMIT {
            let bytes = std::fs::read(src).map_err(|e| self.io_err(e))?;
            return self.put(Bytes::from(bytes));
        }
        let mut w = self.writer()?;
        let mut f = File::open(src).map_err(|e| self.io_err(e))?;
        io::copy(&mut f, &mut w).map_err(|e| self.io_err(e))?;
        w.finish()
    }

    /// A writer that uploads to the object as it is written, in concurrent parts. Call
    /// `finish` to complete the object; dropping the writer before aborts the upload.
    pub fn writer(&self) -> Result<RemoteWriter, RemoteError> {
        Ok(RemoteWriter {
            location: self.clone(),
            buffer: Vec::new(),
            upload: None,
            finished: false,
        })
    }

    /// The objects under this location (as a prefix), as paths relative to it.
    pub fn list(&self) -> Result<Vec<String>, RemoteError> {
        let prefix = if self.path.as_ref().is_empty() {
            None
        } else {
            Some(&self.path)
        };
        let metas: Vec<_> = block_on(self.store.list(prefix).try_collect::<Vec<_>>())
            .map_err(|e| self.err(e))?;
        let base = self.path.as_ref();
        Ok(metas
            .into_iter()
            .filter_map(|m| {
                let p = m.location.as_ref();
                let rel = if base.is_empty() {
                    Some(p)
                } else {
                    p.strip_prefix(base).and_then(|r| r.strip_prefix('/'))
                };
                rel.map(|r| r.to_string())
            })
            .collect())
    }

    /// The names of the "folders" directly under this location.
    pub fn list_folders(&self) -> Result<Vec<String>, RemoteError> {
        let prefix = if self.path.as_ref().is_empty() {
            None
        } else {
            Some(&self.path)
        };
        let res = block_on(self.store.list_with_delimiter(prefix)).map_err(|e| self.err(e))?;
        Ok(res
            .common_prefixes
            .into_iter()
            .filter_map(|p| p.filename().map(|s| s.to_string()))
            .collect())
    }

    /// Deletes the object, if it exists.
    pub fn delete(&self) -> Result<(), RemoteError> {
        match block_on(self.store.delete(&self.path)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(self.err(e)),
        }
    }

    /// Deletes all objects under this location (as a prefix).
    pub fn delete_all(&self) -> Result<(), RemoteError> {
        for rel in self.list()? {
            self.child(&rel).delete()?;
        }
        if self.root == "file://" && !self.path.as_ref().is_empty() {
            // The local file system keeps the emptied folders, which object stores do not have.
            let folder = if cfg!(windows) {
                PathBuf::from(self.path.as_ref())
            } else {
                PathBuf::from(format!("/{}", self.path))
            };
            if folder.is_dir() {
                let _ = std::fs::remove_dir_all(folder);
            }
        }
        Ok(())
    }
}

/// Uploads to an object as it is written, see [`RemoteLocation::writer`]. The first
/// `SINGLE_PUT_LIMIT` bytes are buffered, so that small objects are stored with a single put.
/// Larger objects are uploaded in concurrent parts as they are written.
pub struct RemoteWriter {
    location: RemoteLocation,
    buffer: Vec<u8>,
    upload: Option<WriteMultipart>,
    finished: bool,
}

impl RemoteWriter {
    /// Completes the upload, the object then appears whole.
    pub fn finish(mut self) -> Result<(), RemoteError> {
        self.finished = true;
        match self.upload.take() {
            None => self.location.put(Bytes::from(std::mem::take(&mut self.buffer))),
            Some(upload) => block_on(upload.finish())
                .map(|_| ())
                .map_err(|e| self.location.err(e)),
        }
    }

    fn start_multipart(&mut self) -> Result<(), RemoteError> {
        let upload = block_on(self.location.store.put_multipart(&self.location.path))
            .map_err(|e| self.location.err(e))?;
        let mut upload = WriteMultipart::new_with_chunk_size(upload, UPLOAD_CHUNK);
        let buffered = std::mem::take(&mut self.buffer);
        let _guard = RUNTIME.enter();
        upload.write(&buffered);
        self.upload = Some(upload);
        Ok(())
    }
}

impl Write for RemoteWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        assert!(!self.finished, "Written after finish");
        if self.upload.is_none() {
            self.buffer.extend_from_slice(buf);
            if self.buffer.len() as u64 > SINGLE_PUT_LIMIT {
                self.start_multipart()?;
            }
            return Ok(buf.len());
        }
        let upload = self.upload.as_mut().unwrap();
        // Parts are uploaded by tasks spawned on the runtime, at most UPLOAD_CONCURRENCY at a
        // time, so memory use is bounded by UPLOAD_CONCURRENCY parts.
        block_on(upload.wait_for_capacity(UPLOAD_CONCURRENCY))
            .map_err(|e| io::Error::from(self.location.err(e)))?;
        // Writing a full part spawns its upload, which needs the runtime's context.
        let _guard = RUNTIME.enter();
        upload.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for RemoteWriter {
    fn drop(&mut self) {
        if let Some(upload) = self.upload.take() {
            let _ = block_on(upload.abort());
        }
    }
}

/// A local file that is removed when dropped.
pub struct TempFile {
    dir: PathBuf,
    path: PathBuf,
}

impl TempFile {
    /// A path for a new file named `name` in a new folder in `parent` (by default the system
    /// temporary folder, see `std::env::temp_dir`).
    pub fn new(parent: Option<&Path>, name: &str) -> io::Result<TempFile> {
        let parent = parent
            .map(|p| p.to_owned())
            .unwrap_or_else(std::env::temp_dir);
        let dir = parent.join(format!(".maplib-download-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(name);
        Ok(TempFile { dir, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Downloads the object at `url` to a temporary file with the same file name, so that formats
/// are still recognised by their extension.
pub fn download_to_temp(
    url: &str,
    options: &RemoteOptions,
    parent: Option<&Path>,
) -> Result<TempFile, RemoteError> {
    let loc = RemoteLocation::parse(url, options)?;
    let name = loc.file_name().unwrap_or("download").to_string();
    let tmp = TempFile::new(parent, &name).map_err(|e| loc.io_err(e))?;
    loc.download_to(tmp.path())?;
    Ok(tmp)
}

/// Opens `path_or_url` for writing: a local file, or an upload to object storage.
pub enum Output {
    Local(File),
    Remote(RemoteWriter),
}

impl Output {
    pub fn create(path_or_url: &str, options: &RemoteOptions) -> io::Result<Output> {
        if is_remote(path_or_url) {
            Ok(Output::Remote(
                RemoteLocation::parse(path_or_url, options)?.writer()?,
            ))
        } else {
            Ok(Output::Local(File::create(path_or_url)?))
        }
    }

    /// Completes the file. For object storage the object only appears when this succeeds.
    pub fn finish(self) -> io::Result<()> {
        match self {
            Output::Local(mut f) => f.flush(),
            Output::Remote(w) => Ok(w.finish()?),
        }
    }
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Output::Local(f) => f.write(buf),
            Output::Remote(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Output::Local(f) => f.flush(),
            Output::Remote(w) => w.flush(),
        }
    }
}

/// A local path to read `path_or_url` from: the path itself, or a downloaded temporary copy
/// that is removed when the returned value is dropped.
pub enum Input {
    Local(PathBuf),
    Downloaded(TempFile),
}

impl Input {
    pub fn open(path_or_url: &str, options: &RemoteOptions) -> io::Result<Input> {
        if is_remote(path_or_url) {
            Ok(Input::Downloaded(download_to_temp(path_or_url, options, None)?))
        } else {
            Ok(Input::Local(PathBuf::from(path_or_url)))
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            Input::Local(p) => p,
            Input::Downloaded(t) => t.path(),
        }
    }
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_write(buf, offset)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write"));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_urls() {
        assert!(is_remote("gs://bucket/a.ttl"));
        assert!(is_remote("az://container/a.ttl"));
        assert!(is_remote("abfss://c@acc.dfs.core.windows.net/a"));
        assert!(is_remote("s3://b/k"));
        assert!(is_remote("file:///tmp/x"));
        assert!(!is_remote("/tmp/x"));
        assert!(!is_remote("C:\\x\\y.ttl"));
        assert!(!is_remote("C:/x/y.ttl"));
        assert!(!is_remote("relative/gs://x"));
        assert!(!is_remote("gs://"));
    }

    #[test]
    fn file_urls_round_trip() {
        let dir = std::env::temp_dir().join(format!("maplib-remote-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("file://{}/folder/x.nt", dir.display());
        let opts = RemoteOptions::new();
        let mut out = Output::create(&url, &opts).unwrap();
        let data = vec![7u8; (40 << 20) + 3];
        out.write_all(&data).unwrap();
        out.finish().unwrap();
        let loc = RemoteLocation::parse(&url, &opts).unwrap();
        assert_eq!(loc.size().unwrap(), Some(data.len() as u64));
        assert_eq!(loc.file_name(), Some("x.nt"));
        let input = Input::open(&url, &opts).unwrap();
        assert_eq!(std::fs::read(input.path()).unwrap(), data);
        let folder = RemoteLocation::parse(&format!("file://{}/folder", dir.display()), &opts)
            .unwrap();
        assert_eq!(folder.list().unwrap(), vec!["x.nt".to_string()]);
        folder.delete_all().unwrap();
        assert!(!loc.exists().unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
