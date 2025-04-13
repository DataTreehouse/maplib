use crate::mapping::errors::MappingError;
use cimxml::CIMXMLError;
use datalog::inference::DatalogError;
use oxiri::IriParseError;
use polars::error::PolarsError;
use shacl::errors::ShaclError;
use std::io;
use templates::dataset::errors::TemplateError;
use thiserror::Error;
use triplestore::errors::TriplestoreError;
use triplestore::sparql::errors::SparqlError;

#[derive(Error, Debug)]
pub enum MaplibError {
    #[error(transparent)]
    TemplateError(#[from] TemplateError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
    #[error("Datalog syntax error: `{0}`")]
    DatalogSyntaxError(String),
    #[error(transparent)]
    DatalogError(DatalogError),
    #[error(transparent)]
    CIMXMLError(CIMXMLError),
    #[error("Error creating file: `{0}`")]
    FileCreateIOError(io::Error),
    #[error("Error writing parquet: `{0}`")]
    WriteParquetError(PolarsError),
    #[error("Error reading parquet: `{0}`")]
    ReadParquetError(PolarsError),
    #[error("Path does not exist `{0}`")]
    PathDoesNotExist(String),
    #[error("Error writing NTriples: `{0}`")]
    WriteNTriplesError(io::Error),
    #[error("Error removing parquet file: `{0}`")]
    RemoveParquetFileError(io::Error),
    #[error(transparent)]
    TriplestoreError(#[from] TriplestoreError),
    #[error(transparent)]
    SparqlError(#[from] SparqlError),
    #[error(transparent)]
    ShaclError(#[from] ShaclError),
    #[error(transparent)]
    IRIParseError(#[from] IriParseError),
}
