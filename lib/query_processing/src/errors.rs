use representation::{BaseRDFNodeType, RDFNodeType};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryProcessingError {
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeType, RDFNodeType, String),
    #[error("Variable ?{} not found in context {}",.0, .1)]
    VariableNotFound(String, String),
    #[error("Inconsistent datatypes when casting {} to {:?}, got {:?}. Try filtering first.", .0, .1, .2)]
    BadCastDatatype(String, BaseRDFNodeType, BaseRDFNodeType),
}
