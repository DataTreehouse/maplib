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
    TurtleParsingError(String),
    XMLParsingError(String),
    ReadTriplesFileError(io::Error),
    InvalidBaseIri(String),
    SubtractTransientTriplesError(String),
    RDFSClassInheritanceError(String),
    NTriplesParsingError(String),
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
            TriplestoreError::TurtleParsingError(tp) => {
                write!(f, "Turtle parsing error {}", tp)
            }
            TriplestoreError::ReadTriplesFileError(rt) => {
                write!(f, "Read triples file error {}", rt)
            }
            TriplestoreError::InvalidBaseIri(x) => {
                write!(f, "Invalid base iri {x}")
            }
            TriplestoreError::SubtractTransientTriplesError(x) => {
                write!(f, "Error subtracting from transient triples {x}")
            }
            TriplestoreError::RDFSClassInheritanceError(x) => {
                write!(f, "RDFS Class inheritance error {x}")
            }
            TriplestoreError::XMLParsingError(xp) => {
                write!(f, "RDF XML parsing error {}", xp)
            }
            TriplestoreError::NTriplesParsingError(xp) => {
                write!(f, "NTriples parsing error {}", xp)
            }
        }
    }
}
