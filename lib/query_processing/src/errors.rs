use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Function;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryProcessingError {
    #[error("Inconsistent datatypes for {}, {:?}, {:?} in context {}", .0, .1, .2, .3)]
    InconsistentDatatypes(String, RDFNodeState, RDFNodeState, String),
    #[error("Variable ?{} not found in context {}",.0, .1)]
    VariableNotFound(String, String),
    #[error("Inconsistent datatypes when casting {} to {:?}, got {:?}. Try filtering first.", .0, .1, .2)]
    BadCastDatatype(String, BaseRDFNodeType, BaseRDFNodeType),
    #[error("Function {} got wrong number of arguments {}, expected {}", .0, .1, .2)]
    BadNumberOfFunctionArguments(Function, usize, String),
    #[error("Maximum estimated rows `{}` in result exceeds configured maximum `{}`. You may have a cross join in your query, please double check. Alternatively, try setting the max_rows parameter to a higher value. Left columns: `{}` Right columns: `{}`", .0, .1, .2, .3)]
    MaxRowsReached(usize, usize, String, String),
}
