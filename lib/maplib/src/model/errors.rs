use oxrdf::IriParseError;
use polars::prelude::{DataFrame, DataType};
use representation::errors::RepresentationError;
use templates::ast::{ConstantTermOrList, PType};
use templates::dataset::errors::TemplateError;
use templates::functions::FunctionError;
use templates::MappingColumnType;
use thiserror::Error;
use triplestore::errors::TriplestoreError;

#[derive(Error, Debug)]
pub enum MappingError {
    #[error("Could not map vector column: {}", .0)]
    VectorColumnError(String),
    #[error("Invalid template name {}", .0)]
    InvalidTemplateNameError(IriParseError),
    #[error("Could not find template: {}", .0)]
    TemplateNotFound(String),
    #[error("Column {} which is non-optional has null values for keys: {:?}", .0, .1)]
    NonOptionalColumnHasNull(String, DataFrame),
    #[error("Expected column {} is missing", .0)]
    MissingParameterColumn(String),
    #[error("Unexpected columns: {:?}", .0)]
    ContainsIrrelevantColumns(Vec<String>),
    #[error("Could not infer OTTR type for column {} with polars datatype {:?}", .0, .1)]
    CouldNotInferOTTRDatatypeForColumn(String, DataType),
    #[error("Column {} had datatype {:?} which was incompatible with the OTTR datatype {:?}, which expects {:?}", .0, .1, .2, .3)]
    ColumnDataTypeMismatch(String, DataType, PType, Option<DataType>),
    #[error("Column {} had datatype {:?} which was incompatible with the OTTR datatype {:?}", .0, .1, .2)]
    IncompatibleColumnDataType(String, DataType, PType),
    #[error("Expected datatype: {:?}, but got: {:?}", .0, .1)]
    DefaultDataTypeMismatch(MappingColumnType, MappingColumnType),
    #[error("Predicate constant {} is not valid, must be an IRI, e.g. prefix:predicate", .0)]
    InvalidPredicateConstant(ConstantTermOrList),
    #[error("Found value {} with unsupported OTTR datatype {}", .0, .1)]
    PTypeNotSupported(String, PType),
    #[error("Unknown time zone {}", .0)]
    UnknownTimeZoneError(String),
    #[error("Could not find variable {}, is the OTTR template invalid?", .0)]
    UnknownVariableError(String),
    #[error("Expected constant term {:?} to have data type {} but was {}", .0, .1, .2)]
    ConstantDoesNotMatchDataType(ConstantTermOrList, PType, PType),
    #[error("Constant term {:?} has inconsistent data types {} and {}", .0, .1, .2)]
    ConstantListHasInconsistentPType(ConstantTermOrList, PType, PType),
    #[error("Template name {} inferred from prefix could not be found", .0)]
    NoTemplateForTemplateNameFromPrefix(String),
    #[error("Missing DataFrame argument, but signature is not empty")]
    MissingDataFrameForNonEmptySignature,
    #[error("{}", .0)]
    TooDeeplyNestedError(String),
    #[error("{}", .0)]
    DatatypeInferenceError(RepresentationError),
    #[error("Column {} has at least {} invalid IRIs. Examples: {:?}", .0, .1, .2)]
    InvalidIRIError(String, usize, String),
    #[error("Template error: {}", .0)]
    TemplateError(#[from] TemplateError),
    #[error("Error storing model results in triplestore: {}", .0)]
    TriplestoreError(#[from] TriplestoreError),
    #[error("IRI parse error: {}", .0)]
    IriParseError(IriParseError),
    #[error("Reached maximum templates recursion limit {} ({})", .0, .1)]
    MaximumRecursionLimit(usize, String),
    #[error("Literal parse error: {}", .0)]
    LiteralParseError(String),
    #[error("Column {} has nulls, but filling them with a default value containing blank nodes is not supported", .0)]
    BlankNodeDefaultNotSupported(String),
    #[error("Empty constant list {:?} is not supported, use rdf:nil instead", .0)]
    EmptyConstantListNotSupported(ConstantTermOrList),
    #[error("Argument {} is list expanded, but has the non-list type {:?}", .0, .1)]
    ListExpandNonList(String, MappingColumnType),
    #[error("Function call error: {}", .0)]
    FunctionError(#[from] FunctionError),
    #[error("Variable {} is a list, but lists can not be used as arguments of the function call {}", .0, .1)]
    FunctionListArgument(String, String),
    #[error("Could not evaluate the function call {}: {}", .0, .1)]
    FunctionEvaluationError(String, String),
}
