//! Single file bundles of a serialized model.
//!
//! A bundle holds the files of a serialization folder (see `Triplestore::serialize_triples`)
//! concatenated, followed by an index of where each file is:
//!
//! ```text
//! BUNDLE_MAGIC | file bytes ... | index (JSON) | index length (u64 LE) | INDEX_MAGIC
//! ```
//!
//! The index maps the path of each file relative to the folder, with `/` separators, to its
//! offset and length in the bundle. Files are stored as they are, so parquet files are read
//! directly from their byte ranges and DuckDB databases are copied out of the bundle. Parquet
//! files are encoded in memory and appended as they are finished, so they are written once.

use file_io::remote::{RemoteLocation, RemoteOptions, TempFile, UpdateVersion};
use polars::prelude::{
    DataFrame, ParquetCompression, ParquetReader, ParquetWriter, PolarsResult, SerReader,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{create_dir_all, File};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const BUNDLE_MAGIC: &[u8; 8] = b"MAPLIB\x00\x01";
pub const INDEX_MAGIC: &[u8; 8] = b"MAPLIBIX";
const TRAILER_LEN: u64 = 16;
const BUNDLE_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct BundleIndex {
    version: u32,
    /// Relative path -> (offset, length).
    files: BTreeMap<String, (u64, u64)>,
}

/// An opened bundle: its path and index.
pub struct Bundle {
    path: PathBuf,
    files: BTreeMap<String, (u64, u64)>,
    /// A bundle downloaded from object storage, removed when the bundle is dropped.
    _download: Option<TempFile>,
}

struct BundleWriterState {
    w: BufWriter<File>,
    offset: u64,
    files: BTreeMap<String, (u64, u64)>,
}

/// Writes a bundle one file at a time. Files can be added from several threads: each is
/// appended whole under a lock, in the order they are added.
pub struct BundleWriter {
    state: Mutex<BundleWriterState>,
    /// Where files that must exist on disk before they are added (DuckDB databases) are made.
    scratch_dir: PathBuf,
}

impl BundleWriter {
    pub fn create(path: &Path, scratch_dir: PathBuf) -> io::Result<Self> {
        let mut w = BufWriter::with_capacity(1 << 20, File::create(path)?);
        w.write_all(BUNDLE_MAGIC)?;
        Ok(Self {
            state: Mutex::new(BundleWriterState {
                w,
                offset: BUNDLE_MAGIC.len() as u64,
                files: BTreeMap::new(),
            }),
            scratch_dir,
        })
    }

    fn add(
        &self,
        name: &str,
        write: impl FnOnce(&mut BufWriter<File>) -> io::Result<u64>,
    ) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.files.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{name} was added twice"),
            ));
        }
        let len = write(&mut state.w)?;
        let offset = state.offset;
        state.files.insert(name.to_string(), (offset, len));
        state.offset += len;
        Ok(())
    }

    pub fn add_bytes(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        self.add(name, |w| {
            w.write_all(bytes)?;
            Ok(bytes.len() as u64)
        })
    }

    pub fn add_file(&self, name: &str, path: &Path) -> io::Result<()> {
        let mut src = File::open(path)?;
        self.add(name, |w| io::copy(&mut src, w))
    }

    /// Writes the index and syncs the file, which is then complete.
    pub fn finish(self) -> io::Result<()> {
        let BundleWriterState { mut w, files, .. } = self.state.into_inner().unwrap();
        let index = serde_json::to_vec(&BundleIndex {
            version: BUNDLE_VERSION,
            files,
        })
        .map_err(io::Error::other)?;
        w.write_all(&index)?;
        w.write_all(&(index.len() as u64).to_le_bytes())?;
        w.write_all(INDEX_MAGIC)?;
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok(())
    }
}

/// Where a model is serialized to: a folder, or a single file bundle.
pub enum SerializationTarget {
    Folder(PathBuf),
    Bundle(BundleWriter),
    Remote(RemoteFolderWriter),
}

/// The object holding the name of the current generation of a serialization folder in object
/// storage, see [`RemoteFolderWriter`].
pub const LATEST_FILE: &str = "LATEST";
const GENERATION_PREFIX: &str = "gen-";
/// Generations kept in object storage: the current one and the one before, which readers that
/// opened it before it was replaced may still be reading lazily.
const GENERATIONS_KEPT: usize = 2;

/// Writes a serialization folder to object storage. The files go to a new generation folder,
/// `<location>/gen-<time>-<id>/`, and when all are written the generation is published by
/// replacing `<location>/LATEST`, which holds its name. Readers that open the location read
/// `LATEST` first, so they see either the previous or the new serialization whole, never a mix.
/// `LATEST` is replaced with a conditional put, so if another writer published a generation
/// since this one started, `finish` fails instead of overwriting it. Generations older than the
/// last two are then deleted.
pub struct RemoteFolderWriter {
    root: RemoteLocation,
    generation: String,
    folder: RemoteLocation,
    latest_version: Option<UpdateVersion>,
    /// Where files that must exist on disk before they are uploaded (DuckDB databases) are made.
    scratch_dir: PathBuf,
}

impl RemoteFolderWriter {
    pub fn create(root: RemoteLocation, scratch_parent: Option<&Path>) -> io::Result<Self> {
        let latest_version = root.child(LATEST_FILE).version()?;
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let id = uuid::Uuid::new_v4().simple().to_string();
        let generation = format!("{GENERATION_PREFIX}{millis:016}-{}", &id[..8]);
        let folder = root.child(&generation);
        let scratch_dir = scratch_parent
            .map(|p| p.to_owned())
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(".maplib-upload-{id}"));
        Ok(Self {
            root,
            generation,
            folder,
            latest_version,
            scratch_dir,
        })
    }

    /// The folder of the generation being written.
    pub fn folder(&self) -> &RemoteLocation {
        &self.folder
    }

    fn finish(self) -> io::Result<()> {
        let _ = std::fs::remove_dir_all(&self.scratch_dir);
        let latest = self.root.child(LATEST_FILE);
        let published = latest.put_if(
            self.generation.clone().into_bytes().into(),
            self.latest_version.clone(),
        )?;
        if !published {
            let _ = self.folder.delete_all();
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} was changed by another writer while it was serialized, the new \
                     serialization was not published",
                    self.root
                ),
            ));
        }
        let mut older: Vec<String> = self
            .root
            .list_folders()?
            .into_iter()
            .filter(|g| g.starts_with(GENERATION_PREFIX) && *g < self.generation)
            .collect();
        older.sort();
        let keep_old = GENERATIONS_KEPT - 1;
        if older.len() > keep_old {
            for g in &older[..older.len() - keep_old] {
                self.root.child(g).delete_all()?;
            }
        }
        Ok(())
    }

    fn abort(&self) {
        let _ = std::fs::remove_dir_all(&self.scratch_dir);
        let _ = self.folder.delete_all();
    }
}

impl SerializationTarget {
    fn folder_path(folder: &Path, rel: &str) -> io::Result<PathBuf> {
        let mut p = folder.to_owned();
        for part in rel.split('/') {
            p.push(part);
        }
        if let Some(parent) = p.parent() {
            create_dir_all(parent)?;
        }
        Ok(p)
    }

    pub fn write_bytes(&self, rel: &str, bytes: &[u8]) -> io::Result<()> {
        match self {
            SerializationTarget::Folder(folder) => {
                std::fs::write(Self::folder_path(folder, rel)?, bytes)
            }
            SerializationTarget::Bundle(w) => w.add_bytes(rel, bytes),
            SerializationTarget::Remote(r) => Ok(r.folder.child(rel).put_large(bytes.to_vec())?),
        }
    }

    /// Writes `df` as a parquet file. In a bundle, it is encoded in memory and appended whole.
    pub fn write_parquet(
        &self,
        rel: &str,
        df: &mut DataFrame,
        compression: ParquetCompression,
        row_group_size: Option<usize>,
    ) -> PolarsResult<()> {
        fn writer<W: Write>(
            w: W,
            compression: ParquetCompression,
            row_group_size: Option<usize>,
        ) -> ParquetWriter<W> {
            ParquetWriter::new(w)
                .with_compression(compression)
                .with_row_group_size(row_group_size)
                .set_parallel(true)
        }
        match self {
            SerializationTarget::Folder(folder) => {
                let file = BufWriter::new(File::create(Self::folder_path(folder, rel)?)?);
                writer(file, compression, row_group_size).finish(df)?;
            }
            SerializationTarget::Bundle(w) => {
                let mut buf = Vec::new();
                writer(&mut buf, compression, row_group_size).finish(df)?;
                w.add_bytes(rel, &buf)?;
            }
            SerializationTarget::Remote(r) => {
                let mut buf = Vec::new();
                writer(&mut buf, compression, row_group_size).finish(df)?;
                r.folder.child(rel).put_large(buf).map_err(io::Error::from)?;
            }
        }
        Ok(())
    }

    /// The path to create a database file at, which is passed to `finish_db` once written.
    pub fn db_path(&self, rel: &str) -> io::Result<PathBuf> {
        match self {
            SerializationTarget::Folder(folder) => Self::folder_path(folder, rel),
            SerializationTarget::Bundle(w) => {
                create_dir_all(&w.scratch_dir)?;
                Ok(w.scratch_dir.join(rel.replace('/', "_")))
            }
            SerializationTarget::Remote(r) => {
                create_dir_all(&r.scratch_dir)?;
                Ok(r.scratch_dir.join(rel.replace('/', "_")))
            }
        }
    }

    /// Adds a database written at `db_path(rel)`, with its write-ahead log if there is one.
    pub fn finish_db(&self, rel: &str, path: &Path) -> io::Result<()> {
        let mut wal = path.as_os_str().to_owned();
        wal.push(".wal");
        let wal = PathBuf::from(wal);
        match self {
            SerializationTarget::Folder(_) => return Ok(()),
            SerializationTarget::Bundle(w) => {
                w.add_file(rel, path)?;
                if wal.exists() {
                    w.add_file(&format!("{rel}.wal"), &wal)?;
                }
            }
            SerializationTarget::Remote(r) => {
                r.folder.child(rel).upload_from(path)?;
                if wal.exists() {
                    r.folder.child(&format!("{rel}.wal")).upload_from(&wal)?;
                }
            }
        }
        if wal.exists() {
            std::fs::remove_file(&wal)?;
        }
        std::fs::remove_file(path)
    }

    /// Completes a bundle, or publishes a folder in object storage. A local folder needs
    /// nothing more.
    pub fn finish(self) -> io::Result<()> {
        match self {
            SerializationTarget::Folder(_) => Ok(()),
            SerializationTarget::Bundle(w) => w.finish(),
            SerializationTarget::Remote(r) => r.finish(),
        }
    }

    /// Removes what was written of a serialization that failed, for object storage.
    pub fn abort(&self) {
        if let SerializationTarget::Remote(r) = self {
            r.abort()
        }
    }
}

impl Bundle {
    pub fn open(path: &Path) -> io::Result<Bundle> {
        let mut f = File::open(path)?;
        let size = f.metadata()?.len();
        let malformed = |msg: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a maplib file: {msg}", path.display()),
            )
        };
        if size < BUNDLE_MAGIC.len() as u64 + TRAILER_LEN {
            return Err(malformed("too short"));
        }
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != BUNDLE_MAGIC {
            return Err(malformed("wrong header"));
        }
        f.seek(SeekFrom::Start(size - TRAILER_LEN))?;
        let mut trailer = [0u8; TRAILER_LEN as usize];
        f.read_exact(&mut trailer)?;
        if &trailer[8..] != INDEX_MAGIC {
            return Err(malformed("missing index, the file may be truncated"));
        }
        let index_len = u64::from_le_bytes(trailer[..8].try_into().unwrap());
        let Some(index_start) = (size - TRAILER_LEN).checked_sub(index_len) else {
            return Err(malformed("bad index length"));
        };
        f.seek(SeekFrom::Start(index_start))?;
        let mut index = vec![0u8; index_len as usize];
        f.read_exact(&mut index)?;
        let index: BundleIndex =
            serde_json::from_slice(&index).map_err(|e| malformed(&e.to_string()))?;
        if index.version > BUNDLE_VERSION {
            return Err(malformed(&format!(
                "written by a newer version of maplib (format version {})",
                index.version
            )));
        }
        if index
            .files
            .values()
            .any(|(o, l)| o.checked_add(*l).is_none_or(|end| end > index_start))
        {
            return Err(malformed("file ranges outside the data section"));
        }
        Ok(Bundle {
            path: path.to_owned(),
            files: index.files,
            _download: None,
        })
    }

    /// Opens a bundle downloaded to `download`, which is removed when the bundle is dropped.
    pub fn open_downloaded(download: TempFile) -> io::Result<Bundle> {
        let mut b = Bundle::open(download.path())?;
        b._download = Some(download);
        Ok(b)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn contains(&self, name: &str) -> bool {
        self.files.contains_key(name)
    }

    /// The names of the files directly in the folder `dir` of the bundle.
    pub fn files_in(&self, dir: &str) -> Vec<&str> {
        let prefix = format!("{dir}/");
        self.files
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix))
            .filter(|k| !k.contains('/'))
            .collect()
    }

    fn range(&self, name: &str) -> io::Result<(u64, u64)> {
        self.files.get(name).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{name} not found in {}", self.path.display()),
            )
        })
    }

    pub fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let (offset, len) = self.range(name)?;
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Copies the file `name` out of the bundle to `dest`.
    pub fn extract(&self, name: &str, dest: &Path) -> io::Result<()> {
        let (offset, len) = self.range(name)?;
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut w = BufWriter::new(File::create(dest)?);
        let copied = io::copy(&mut f.take(len), &mut w)?;
        if copied != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{} is truncated", self.path.display()),
            ));
        }
        w.flush()?;
        Ok(())
    }

    pub fn read_parquet(&self, name: &str) -> PolarsResult<DataFrame> {
        let bytes = self.read(name)?;
        ParquetReader::new(Cursor::new(bytes)).finish()
    }
}

/// Where a serialized model is read from: a folder, or a single file bundle of one.
pub enum SerializationSource {
    Folder(PathBuf),
    Bundle(Bundle),
    /// A serialization folder in object storage, read file by file.
    Remote(RemoteLocation),
}

impl SerializationSource {
    /// A folder is read as a serialization folder, a file as a bundle.
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.is_dir() {
            Ok(SerializationSource::Folder(path.to_owned()))
        } else {
            Ok(SerializationSource::Bundle(Bundle::open(path)?))
        }
    }

    /// The file at `rel`, a path relative to the serialization folder with `/` separators.
    pub fn file(&self, rel: &str) -> SerializedFile<'_> {
        match self {
            SerializationSource::Folder(folder) => {
                let mut p = folder.clone();
                for part in rel.split('/') {
                    p.push(part);
                }
                SerializedFile::Path(p)
            }
            SerializationSource::Bundle(b) => SerializedFile::InBundle(b, rel.to_string()),
            SerializationSource::Remote(r) => SerializedFile::Remote(r.child(rel)),
        }
    }

    /// Opens a serialization in object storage at `url`: a single file bundle, which is
    /// downloaded to a temporary file in `scratch_parent` (by default the system temporary
    /// folder), or a folder, read file by file. A folder written by [`RemoteFolderWriter`] is
    /// read at the generation named by its `LATEST` object.
    pub fn open_remote(
        url: &str,
        options: &RemoteOptions,
        scratch_parent: Option<&Path>,
    ) -> io::Result<Self> {
        Self::open_remote_location(RemoteLocation::parse(url, options)?, scratch_parent)
    }

    /// Opens a serialization in object storage at `loc`, see `open_remote`.
    pub fn open_remote_location(loc: RemoteLocation, scratch_parent: Option<&Path>) -> io::Result<Self> {
        if loc.file_name().is_some() && loc.exists()? {
            let name = loc.file_name().unwrap_or("model.maplib").to_string();
            let tmp = TempFile::new(scratch_parent, &name)?;
            loc.download_to(tmp.path())?;
            return Ok(SerializationSource::Bundle(Bundle::open_downloaded(tmp)?));
        }
        let latest = loc.child(LATEST_FILE);
        if latest.exists()? {
            let generation = String::from_utf8_lossy(&latest.get()?).trim().to_string();
            return Ok(SerializationSource::Remote(loc.child(&generation)));
        }
        if loc.child("triples/metadata.json").exists()? {
            return Ok(SerializationSource::Remote(loc));
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("No serialized model found at {loc}"),
        ))
    }
}

/// A file of a serialization, either in a folder or in a bundle.
pub enum SerializedFile<'a> {
    Path(PathBuf),
    InBundle(&'a Bundle, String),
    Remote(RemoteLocation),
}

impl SerializedFile<'_> {
    pub fn exists(&self) -> bool {
        match self {
            SerializedFile::Path(p) => p.exists(),
            SerializedFile::InBundle(b, name) => b.contains(name),
            SerializedFile::Remote(r) => r.exists().unwrap_or(false),
        }
    }

    pub fn read(&self) -> io::Result<Vec<u8>> {
        match self {
            SerializedFile::Path(p) => std::fs::read(p),
            SerializedFile::InBundle(b, name) => b.read(name),
            SerializedFile::Remote(r) => Ok(r.get()?.to_vec()),
        }
    }

    pub fn read_parquet(&self) -> PolarsResult<DataFrame> {
        match self {
            SerializedFile::Path(p) => ParquetReader::new(File::open(p)?).finish(),
            SerializedFile::InBundle(b, name) => b.read_parquet(name),
            SerializedFile::Remote(r) => {
                let bytes = r.get().map_err(io::Error::from)?;
                ParquetReader::new(Cursor::new(bytes)).finish()
            }
        }
    }

    /// Copies the file to `dest`.
    pub fn copy_to(&self, dest: &Path) -> io::Result<()> {
        match self {
            SerializedFile::Path(p) => std::fs::copy(p, dest).map(|_| ()),
            SerializedFile::InBundle(b, name) => b.extract(name, dest),
            SerializedFile::Remote(r) => r.download_to(dest).map(|_| ()).map_err(io::Error::from),
        }
    }

    pub fn display(&self) -> String {
        match self {
            SerializedFile::Path(p) => p.display().to_string(),
            SerializedFile::InBundle(b, name) => format!("{}!{name}", b.path().display()),
            SerializedFile::Remote(r) => r.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_generation(root: &RemoteLocation, content: &str) -> io::Result<RemoteFolderWriter> {
        let w = RemoteFolderWriter::create(root.clone(), None)?;
        let t = SerializationTarget::Remote(w);
        t.write_bytes("triples/metadata.json", content.as_bytes())?;
        match t {
            SerializationTarget::Remote(w) => Ok(w),
            _ => unreachable!(),
        }
    }

    fn read_latest(root: &RemoteLocation) -> String {
        let source = SerializationSource::open_remote_location(root.clone(), None).unwrap();
        String::from_utf8(source.file("triples/metadata.json").read().unwrap()).unwrap()
    }

    #[test]
    fn remote_folder_generations_are_published_and_pruned() {
        let root = RemoteLocation::in_memory().child("models/m");
        for i in 0..4 {
            write_generation(&root, &format!("v{i}"))
                .unwrap()
                .finish()
                .unwrap();
            assert_eq!(read_latest(&root), format!("v{i}"));
            // Generation names sort by time, make sure the next one is later.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let generations: Vec<_> = root
            .list_folders()
            .unwrap()
            .into_iter()
            .filter(|g| g.starts_with(GENERATION_PREFIX))
            .collect();
        assert_eq!(generations.len(), GENERATIONS_KEPT);
    }

    #[test]
    fn concurrent_publish_is_refused() {
        let root = RemoteLocation::in_memory().child("m");
        write_generation(&root, "first").unwrap().finish().unwrap();
        // Both writers start from the same published generation.
        let a = write_generation(&root, "a").unwrap();
        let b = write_generation(&root, "b").unwrap();
        let a_folder = a.folder().clone();
        b.finish().unwrap();
        let err = a.finish().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(read_latest(&root), "b");
        // The refused generation was removed.
        assert!(a_folder.list().unwrap().is_empty());
    }

    #[test]
    fn missing_remote_serialization_is_not_found() {
        let root = RemoteLocation::in_memory().child("nothing/here");
        let err = SerializationSource::open_remote_location(root, None)
            .err()
            .unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
