use parquet_io::ParquetIOError;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TriplestoreError {
    WriteNTriplesError(io::Error),
    PathDoesNotExist(String),
    ParquetIOError(ParquetIOError),
    RemoveParquetFileError(io::Error),
    FolderCreateIOError(io::Error),
    ReadCachingDirectoryError(io::Error),
    ReadCachingDirectoryEntryError(io::Error),
}

impl Display for TriplestoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TriplestoreError::WriteNTriplesError(e) => {
                write!(f, "Error writing NTriples {}", e)
            }
            TriplestoreError::PathDoesNotExist(p) => {
                write!(f, "Path {} does not exist", p)
            }
            TriplestoreError::RemoveParquetFileError(e) => {
                write!(f, "Error removing parquet file {}", e)
            }
            TriplestoreError::ParquetIOError(e) => {
                write!(f, "Parquet IO error: {}", e)
            }
            TriplestoreError::FolderCreateIOError(e) => {
                write!(f, "Creating folder resulted in an error: {}", e)
            }
            TriplestoreError::ReadCachingDirectoryError(e) => {
                write!(f, "Read caching directory error {}", e)
            }
            TriplestoreError::ReadCachingDirectoryEntryError(e) => {
                write!(f, "Read caching directory entry error {}", e)
            }
        }
    }
}
