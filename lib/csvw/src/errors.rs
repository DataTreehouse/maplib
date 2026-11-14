use thiserror::Error;
use triplestore::errors::TriplestoreError;

#[derive(Error, Debug)]
pub enum CsvwError {
    #[error("Error in CSVW metadata: {0}")]
    MetadataError(String),
    #[error("Could not read `{0}`: {1}")]
    ReadError(String, String),
    #[error("Could not parse JSON in `{0}`: {1}")]
    JsonError(String, String),
    #[error("Error parsing tabular data `{0}`: {1}")]
    TabularDataError(String, String),
    #[error("Invalid IRI `{0}`: {1}")]
    IriError(String, String),
    #[error("Invalid URI template `{0}`: {1}")]
    UriTemplateError(String, String),
    #[error(transparent)]
    TriplestoreError(#[from] TriplestoreError),
    #[error("Error running the SPARQL Update generated from the CSVW metadata: {0}")]
    UpdateError(String),
}

pub(crate) fn meta_err<T>(s: impl Into<String>) -> Result<T, CsvwError> {
    Err(CsvwError::MetadataError(s.into()))
}
