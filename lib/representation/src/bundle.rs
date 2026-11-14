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
        }
    }

    /// Adds a database written at `db_path(rel)`, with its write-ahead log if there is one.
    pub fn finish_db(&self, rel: &str, path: &Path) -> io::Result<()> {
        let SerializationTarget::Bundle(w) = self else {
            return Ok(());
        };
        w.add_file(rel, path)?;
        let mut wal = path.as_os_str().to_owned();
        wal.push(".wal");
        let wal = PathBuf::from(wal);
        if wal.exists() {
            w.add_file(&format!("{rel}.wal"), &wal)?;
            std::fs::remove_file(&wal)?;
        }
        std::fs::remove_file(path)
    }

    /// Completes a bundle, a folder needs nothing more.
    pub fn finish(self) -> io::Result<()> {
        match self {
            SerializationTarget::Folder(_) => Ok(()),
            SerializationTarget::Bundle(w) => w.finish(),
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
        })
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
        }
    }
}

/// A file of a serialization, either in a folder or in a bundle.
pub enum SerializedFile<'a> {
    Path(PathBuf),
    InBundle(&'a Bundle, String),
}

impl SerializedFile<'_> {
    pub fn exists(&self) -> bool {
        match self {
            SerializedFile::Path(p) => p.exists(),
            SerializedFile::InBundle(b, name) => b.contains(name),
        }
    }

    pub fn read(&self) -> io::Result<Vec<u8>> {
        match self {
            SerializedFile::Path(p) => std::fs::read(p),
            SerializedFile::InBundle(b, name) => b.read(name),
        }
    }

    pub fn read_parquet(&self) -> PolarsResult<DataFrame> {
        match self {
            SerializedFile::Path(p) => ParquetReader::new(File::open(p)?).finish(),
            SerializedFile::InBundle(b, name) => b.read_parquet(name),
        }
    }

    /// Copies the file to `dest`.
    pub fn copy_to(&self, dest: &Path) -> io::Result<()> {
        match self {
            SerializedFile::Path(p) => std::fs::copy(p, dest).map(|_| ()),
            SerializedFile::InBundle(b, name) => b.extract(name, dest),
        }
    }

    pub fn display(&self) -> String {
        match self {
            SerializedFile::Path(p) => p.display().to_string(),
            SerializedFile::InBundle(b, name) => format!("{}!{name}", b.path().display()),
        }
    }
}
