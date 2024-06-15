use representation::RDFNodeType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryProcessingError {
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeType, RDFNodeType, String),
    #[error("Variable ?{} not found in context {}",.0, .1)]
    VariableNotFound(String, String),
}
