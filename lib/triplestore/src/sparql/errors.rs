use crate::errors::TriplestoreError;
use fts::FtsError;
use geo::GeoError;
use vector::VectorError;
use oxrdf::Variable;
use query_processing::errors::QueryProcessingError;
use representation::errors::RepresentationError;
use representation::RDFNodeState;
use spargebra::algebra::GraphPattern;
use spargebra::SparqlSyntaxError;
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SPARQL parsing error: {0}")]
    ParseError(SparqlSyntaxError),
    #[error("Query type not supported")]
    QueryTypeNotSupported,
    #[error(transparent)]
    ServiceError(#[from] external_sparql::federation::ServiceError),
    #[error("SERVICE error: {0}")]
    ServiceEvaluationError(String),
    #[error("LOAD error: {0}")]
    LoadError(String),
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeState, RDFNodeState, String),
    #[error(transparent)]
    QueryProcessingError(#[from] QueryProcessingError),
    #[error(transparent)]
    TriplestoreError(#[from] TriplestoreError),
    #[error("Full text search lookup error: {}", .0)]
    FtsLookupError(#[from] FtsError),
    #[error("GeoSPARQL error: {}", .0)]
    GeoError(#[from] GeoError),
    #[error("Vector search error: {}", .0)]
    VectorError(#[from] VectorError),
    #[error("Query interrupted via signal")]
    InterruptSignal,
    #[error("{0}")]
    Cancelled(#[from] utils::cancel::Cancelled),
    #[error("A lock was open when a thread crashed, cannot guarantee data constitency")]
    PoisonedLockError,
    #[error("Tried grouping on a variable `{0}` that is not defined in the inner query: `{1}`")]
    GroupByWithUndefinedVariable(Variable, GraphPattern),
    #[error("Functionality not supported: `{0}`")]
    NotSupportedYet(String),
    #[error("SPARQL execution error: `{0}`")]
    QueryExecutionError(String),
    #[error("PValues error: `{0}`")]
    PValuesError(String),
    #[error("Error processing Values pattern `{0}`")]
    ValuesError(String),
    #[error("Error parsing literal {0}")]
    ParseLiteralError(RepresentationError),
    #[error(transparent)]
    ReplaceBindingsError(RepresentationError),
    /// An error in the use of maplib:path or maplib:shortestPath.
    #[error("Path error: {0}")]
    PathError(String),
    #[error("Paging error: {0}")]
    PagingError(String),
    #[error("Error constructing quads from quad patterns `{0}`")]
    QuadPatternsError(String),
    #[error("{0}")]
    EvaluationHookError(Box<dyn std::error::Error + Send + Sync>),
}

impl<T> From<PoisonError<T>> for SparqlError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLockError
    }
}
