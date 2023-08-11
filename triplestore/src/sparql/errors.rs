use crate::errors::TriplestoreError;
use representation::RDFNodeType;
use spargebra::ParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SQL Parsersing Error: {0}")]
    ParseError(ParseError),
    #[error("Query type not supported")]
    QueryTypeNotSupported,
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeType, RDFNodeType, String),
    #[error("Variable ?{} not found in context {}",.0, .1)]
    VariableNotFound(String, String),
    #[error("Error deduplicating triples {}", .0)]
    DeduplicationError(TriplestoreError),
    #[error("Read dataframe error {}", .0)]
    TripleTableReadError(TriplestoreError),
    #[error("Error storing triples {}", .0)]
    StoreTriplesError(TriplestoreError),
}
