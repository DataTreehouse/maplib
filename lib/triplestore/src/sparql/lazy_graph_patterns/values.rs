use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{join, values_pattern};
use query_processing::pushdowns::Pushdowns;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::term::GroundTerm;

impl Triplestore {
    pub(crate) fn lazy_values(
        &mut self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
        _context: &Context,
        _pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        let sm = values_pattern(variables, bindings);
        if let Some(mut mappings) = solution_mappings {
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}
