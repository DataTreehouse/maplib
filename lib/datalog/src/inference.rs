use crate::ast::DatalogRuleset;
use oxrdf::NamedNode;
use representation::solution_mapping::EagerSolutionMappings;
use std::collections::HashMap;
use thiserror::*;
use triplestore::sparql::errors::SparqlError;
use triplestore::Triplestore;

#[cfg(feature = "pyo3")]
use pyo3::Python;

#[derive(Debug, Error)]
pub enum DatalogError {
    #[error(transparent)]
    SparqlError(SparqlError),
}

pub fn infer(
    _triplestore: &mut Triplestore,
    _ruleset: &DatalogRuleset,
    _insert: bool,
    _max_iterations: Option<usize>,
) -> Result<Option<HashMap<NamedNode, EagerSolutionMappings>>, DatalogError> {
    unimplemented!("Contact data treehouse to try")
}
