use thiserror::Error;
use triplestore::errors::TriplestoreError;

#[derive(Error, Debug)]
pub enum ShaclError {
    #[error("Contact DataTreehouse for SHACL support! Triplestore: {0}")]
    TriplestoreError(#[from] TriplestoreError),
}
