use file_io::FileIOError;
use fts::FtsError;
use oxrdfio::RdfSyntaxError;
use std::io;
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TriplestoreError {
    #[error("Error writing NTriples {0}")]
    WriteNTriplesError(String),
    #[error("Path {0} does not exist")]
    PathDoesNotExist(String),
    #[error("Error removing file {0}")]
    RemoveFileError(io::Error),
    #[error("Creating folder resulted in an error: {0}")]
    FolderCreateIOError(io::Error),
    #[error("Read caching directory error {0}")]
    ReadCachingDirectoryError(io::Error),
    #[error("Read caching directory entry error {0}")]
    ReadCachingDirectoryEntryError(io::Error),
    #[error("{0}")]
    RDFSyntaxError(RdfSyntaxError),
    #[error("Read triples file error {0}")]
    ReadTriplesFileError(io::Error),
    #[error("Invalid base iri {0}")]
    InvalidBaseIri(String),
    #[error("Error subtracting from transient triples {0}")]
    SubtractTransientTriplesError(String),
    #[error("RDFS Class inheritance error {0}")]
    RDFSClassInheritanceError(String),
    #[error("Indexing error {0}")]
    IndexingError(String),
    #[error("IPC IO error: {0}")]
    IPCIOError(String),
    #[error("Parquet IO error: {0}")]
    FileIOError(FileIOError),
    #[error("Full text search error {0}")]
    FtsError(FtsError),
    #[error("Graph does not exist: {0}")]
    GraphDoesNotExist(String),
    #[error("A lock was open when a thread crashed, cannot guarantee data consistency")]
    PoisonedLockError,
    #[error("Error writing Turtle {0}")]
    WriteTurtleError(String),
}

impl<T> From<PoisonError<T>> for TriplestoreError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLockError
    }
}
