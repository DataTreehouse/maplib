use oxrdf::IriParseError;
use polars::prelude::{DataFrame, DataType};
use representation::errors::RepresentationError;
use std::fmt::{Display, Formatter};
use templates::ast::{ConstantTermOrList, PType};
use templates::dataset::errors::TemplateError;
use templates::MappingColumnType;
use thiserror::Error;
use triplestore::errors::TriplestoreError;

#[derive(Error, Debug)]
pub enum MappingError {
    InvalidTemplateNameError(IriParseError),
    TemplateNotFound(String),
    NonOptionalColumnHasNull(String, DataFrame),
    MissingParameterColumn(String),
    ContainsIrrelevantColumns(Vec<String>),
    CouldNotInferOTTRDatatypeForColumn(String, DataType),
    ColumnDataTypeMismatch(String, DataType, PType, Option<DataType>),
    DefaultDataTypeMismatch(MappingColumnType, MappingColumnType),
    InvalidPredicateConstant(ConstantTermOrList),
    PTypeNotSupported(String, PType),
    UnknownTimeZoneError(String),
    UnknownVariableError(String),
    ConstantDoesNotMatchDataType(ConstantTermOrList, PType, PType),
    ConstantListHasInconsistentPType(ConstantTermOrList, PType, PType),
    NoTemplateForTemplateNameFromPrefix(String),
    MissingDataFrameForNonEmptySignature,
    TooDeeplyNestedError(String),
    DatatypeInferenceError(RepresentationError),
    InvalidIRIError(String, usize, String),
    TemplateError(#[from] TemplateError),
    TriplestoreError(#[from] TriplestoreError),
    IriParseError(IriParseError),
}

impl Display for MappingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingError::TemplateNotFound(t) => {
                write!(f, "Could not find template: {}", t)
            }
            MappingError::NonOptionalColumnHasNull(col, nullkey) => {
                write!(
                    f,
                    "Column {} which is non-optional has null values for keys: {}",
                    col, nullkey
                )
            }
            MappingError::MissingParameterColumn(c) => {
                write!(f, "Expected column {} is missing", c)
            }
            MappingError::ContainsIrrelevantColumns(irr) => {
                write!(f, "Unexpected columns: {}", irr.join(","))
            }
            MappingError::CouldNotInferOTTRDatatypeForColumn(col, dt) => {
                write!(
                    f,
                    "Could not infer OTTR type for column {} with polars datatype {}",
                    col, dt
                )
            }
            MappingError::ColumnDataTypeMismatch(col, dt, ptype, expected) => {
                if let Some(expected) = expected {
                    write!(
                        f,
                        "Column {} had datatype {} which was incompatible with the OTTR datatype {}, which expects {}",
                        col, dt, ptype, expected
                    )
                } else {
                    write!(
                        f,
                        "Column {} had datatype {} which was incompatible with the OTTR datatype {}",
                        col, dt, ptype
                    )
                }
            }
            MappingError::PTypeNotSupported(name, ptype) => {
                write!(
                    f,
                    "Found value {} with unsupported OTTR datatype {}",
                    name, ptype
                )
            }
            MappingError::UnknownTimeZoneError(tz) => {
                write!(f, "Unknown time zone {}", tz)
            }
            MappingError::UnknownVariableError(v) => {
                write!(
                    f,
                    "Could not find variable {}, is the OTTR template invalid?",
                    v
                )
            }
            MappingError::ConstantDoesNotMatchDataType(constant_term, expected, actual) => {
                write!(
                    f,
                    "Expected constant term {:?} to have data type {} but was {}",
                    constant_term, expected, actual
                )
            }
            MappingError::ConstantListHasInconsistentPType(constant_term, prev, next) => {
                write!(
                    f,
                    "Constant term {:?} has inconsistent data types {} and {}",
                    constant_term, prev, next
                )
            }
            MappingError::InvalidTemplateNameError(t) => {
                write!(f, "Invalid template name {}", t)
            }
            MappingError::NoTemplateForTemplateNameFromPrefix(prefix) => {
                write!(
                    f,
                    "Template name {} inferred from prefix could not be found",
                    prefix
                )
            }
            MappingError::InvalidPredicateConstant(constant_term) => {
                write!(
                    f,
                    "Predicate constant {} is not valid, must be an IRI, e.g. prefix:predicate",
                    constant_term,
                )
            }
            MappingError::MissingDataFrameForNonEmptySignature => {
                write!(f, "Missing DataFrame argument, but signature is not empty")
            }
            MappingError::TooDeeplyNestedError(s) => {
                write!(f, "{s}")
            }
            MappingError::DatatypeInferenceError(d) => {
                write!(f, "{d}")
            }
            MappingError::DefaultDataTypeMismatch(expected, actual) => {
                write!(f, "Default value data type {actual:?} does not correspond to data type provided: {expected:?}")
            }
            MappingError::InvalidIRIError(colname, n_errors, examples) => {
                write!(f, "Found at least {n_errors} invalid IRIs for column {colname}, examples: {examples}")
            }
            MappingError::TemplateError(x) => {
                write!(f, "Template error: {}", x)
            }
            MappingError::TriplestoreError(x) => {
                write!(f, "Error storing mapping results in triplestore: {}", x)
            }
            MappingError::IriParseError(x) => {
                write!(f, "IRI parse error: {}", x)
            }
        }
    }
}
