use crate::mapping::errors::MappingError;
use templates::dataset::errors::TemplateError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MaplibError {
    #[error(transparent)]
    TemplateError(#[from] TemplateError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
}
