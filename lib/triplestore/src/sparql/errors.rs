use crate::errors::TriplestoreError;
use fts::FtsError;
use query_processing::errors::QueryProcessingError;
use representation::RDFNodeType;
use spargebra::SparqlSyntaxError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SPARQL parsing error: {0}")]
    ParseError(SparqlSyntaxError),
    #[error("Query type not supported")]
    QueryTypeNotSupported,
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeType, RDFNodeType, String),
    #[error(transparent)]
    QueryProcessingError(#[from] QueryProcessingError),
    #[error("Error indexing triples {}", .0)]
    IndexingError(TriplestoreError),
    #[error("Read dataframe error {}", .0)]
    TripleTableReadError(TriplestoreError),
    #[error("Error storing triples {}", .0)]
    StoreTriplesError(TriplestoreError),
    #[error("Construct query with undefined variable {}", .0)]
    ConstructWithUndefinedVariable(String),
    #[error("Full text search lookup error: {}", .0)]
    FtsLookupError(#[from] FtsError),
    #[error("Query interrupted via signal")]
    InterruptSignal,
}
