use crate::ast::DatalogRuleset;
use representation::dataset::NamedGraph;
use thiserror::*;
use triplestore::sparql::errors::SparqlError;
use triplestore::Triplestore;

#[derive(Debug, Error)]
pub enum DatalogError {
    #[error(transparent)]
    SparqlError(SparqlError),
}

#[derive(Clone)]
pub struct InferenceResult {
}

pub fn infer(
    _triplestore: &mut Triplestore,
    _graph: Option<&NamedGraph>,
    _ruleset: &DatalogRuleset,
    _max_iterations: Option<usize>,
    _max_results: Option<usize>,
    _include_transient: bool,
    _max_rows: Option<usize>,
    _debug_no_results: bool,
) -> Result<InferenceResult, DatalogError> {
    unimplemented!("Contact data treehouse to try")
}
