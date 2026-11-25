use file_io::FileIOError;
use fts::FtsError;
use geo::GeoError;
use vector::VectorError;
use oxrdfio::RdfSyntaxError;
use polars::prelude::PolarsError;
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
    #[error("GeoSPARQL error: {0}")]
    GeoError(GeoError),
    #[error("Vector search error: {0}")]
    VectorError(VectorError),
    #[error("Graph does not exist: {0}")]
    GraphDoesNotExist(String),
    #[error("A lock was open when a thread crashed, cannot guarantee data consistency")]
    PoisonedLockError,
    #[error("Error writing Turtle {0}")]
    WriteTurtleError(String),
    #[error("Writing {0} is not supported")]
    UnsupportedWriteFormat(String),
    #[error("Error writing quads {0}")]
    WriteQuadsError(String),
    #[error("Sparql query error {0}")]
    SparqlQueryError(String),
    #[error("Found bad list: {0}")]
    BadListError(String),
    #[error("Error reading JSON file: {0}")]
    ReadJSONFileError(String),
    #[error("XML error: {0}")]
    XMLError(String),
    #[error("Prefix IRI is invalid: {0}")]
    InvalidPrefixIRI(String),
    #[error("Please add the document string corresponding to the url to known_contexts: {0}")]
    MissingContext(String),
    #[error("Error decoding categorical encoding {0}")]
    DecodeError(PolarsError),
    #[error("Error collecting lazy triples {0}")]
    LazyLoadError(PolarsError),
    #[error("HDT error: {0}")]
    HDTError(String),
    #[error("Storage folder error {0}")]
    StorageFolderError(String),
    #[error("Could not parse {0}. Read with strict=False to log and drop such triples instead")]
    UnparseableValue(String),
    #[error("File format of {0} is not supported, supply the format explicitly")]
    UnsupportedReadFormat(String),
    /// An RDF 1.2 feature that maplib does not support yet, see representation::rdf12.
    #[error("{0}")]
    Rdf12NotSupported(String),
    #[error("Prefix {0} has an invalid IRI: {1}")]
    InvalidPrefix(String, String),
    #[error("Could not memory map file: {0}")]
    MemoryMapError(io::Error),
    #[error("Column {0} cannot be mapped: {1}")]
    UnsupportedColumn(String, String),
}

impl<T> From<PoisonError<T>> for TriplestoreError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLockError
    }
}
