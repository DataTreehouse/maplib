use crate::ast::DatalogRuleset;
use thiserror::*;

pub fn parse_datalog_ruleset(
    _datalog_ruleset: &str,
    _base_iri: Option<&str>,
) -> Result<DatalogRuleset, DatalogSyntaxError> {
    unimplemented!("Contact Data Treehouse to try")
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct DatalogSyntaxError(#[from] DatalogParseErrorKind);

#[derive(Debug, Error)]
pub(crate) enum DatalogParseErrorKind {}
