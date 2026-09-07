use std::sync::PoisonError;
use thiserror::*;

#[derive(Debug, Error)]
pub enum RepresentationError {
    #[error("Datatype error `{0}`")]
    DatatypeError(String),
    #[error("Invalid literal `{0}`")]
    InvalidLiteralError(String),
    #[error("Literal parse error `{0}`")]
    LiteralParseError(String),
    #[error("Construct query with undefined variable {}", .0)]
    ConstructWithUndefinedVariable(String),
    #[error("Construct query with undefined blank node {}", .0)]
    ConstructWithUndefinedBlankNode(String),
    #[error("A lock was open when a thread crashed, cannot guarantee data constitency")]
    PoisonedLockError,
}

impl<T> From<PoisonError<T>> for RepresentationError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLockError
    }
}
