use thiserror::Error;

#[derive(Error, Debug)]
pub enum R2RMLError {
    #[error("Could not parse the R2RML mapping: {0}")]
    ParseError(String),
    #[error("Invalid R2RML mapping: {0}")]
    MappingError(String),
    #[error("The column {0} referenced in {1} is not in the logical table, which has the columns {2}")]
    UndefinedColumn(String, String, String),
    #[error("Missing the result of the logical table query {0}")]
    MissingTable(String),
    #[error("R2RML data error in {0}: {1}")]
    DataError(String, String),
    #[error("Polars error: {0}")]
    PolarsError(String),
}

impl From<polars::error::PolarsError> for R2RMLError {
    fn from(e: polars::error::PolarsError) -> Self {
        R2RMLError::PolarsError(e.to_string())
    }
}

pub(crate) fn mapping_err<T>(s: impl Into<String>) -> Result<T, R2RMLError> {
    Err(R2RMLError::MappingError(s.into()))
}
