use crate::ast::DatalogRuleset;
use oxrdf::NamedNode;
use representation::dataset::NamedGraph;
use representation::solution_mapping::EagerSolutionMappings;
use std::collections::HashMap;
use thiserror::*;
use triplestore::sparql::errors::SparqlError;
use triplestore::Triplestore;

#[derive(Debug, Error)]
pub enum DatalogError {
    #[error(transparent)]
    SparqlError(SparqlError),
}

pub fn infer(
    _triplestore: &mut Triplestore,
    _graph: Option<&NamedGraph>,
    _ruleset: &DatalogRuleset,
    _max_iterations: Option<usize>,
) -> Result<Option<HashMap<NamedNode, EagerSolutionMappings>>, DatalogError> {
    unimplemented!("Contact data treehouse to try")
}
