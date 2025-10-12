use std::sync::PoisonError;

use crate::errors::TriplestoreError;
use fts::FtsError;
use query_processing::errors::QueryProcessingError;
use representation::RDFNodeState;
use spargebra::SparqlSyntaxError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SPARQL parsing error: {0}")]
    ParseError(SparqlSyntaxError),
    #[error("Query type not supported")]
    QueryTypeNotSupported,
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeState, RDFNodeState, String),
    #[error(transparent)]
    QueryProcessingError(#[from] QueryProcessingError),
    #[error(transparent)]
    TriplestoreError(#[from] TriplestoreError),
    #[error("Construct query with undefined variable {}", .0)]
    ConstructWithUndefinedVariable(String),
    #[error("Full text search lookup error: {}", .0)]
    FtsLookupError(#[from] FtsError),
    #[error("Query interrupted via signal")]
    InterruptSignal,
    #[error("A lock was open when a thread crashed, cannot guarantee data constitency")]
    PoisonedLockError,
}

impl<T> From<PoisonError<T>> for SparqlError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLockError
    }
}
