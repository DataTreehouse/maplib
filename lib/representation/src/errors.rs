use thiserror::*;

#[derive(Debug, Error)]
pub enum RepresentationError {
    #[error("Datatype error `{0}`")]
    DatatypeError(String),
    #[error("Invalid literal `{0}`")]
    InvalidLiteralError(String),
}
