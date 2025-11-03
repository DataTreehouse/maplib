use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use oxrdf::Variable;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{join, values_pattern};
use query_processing::pushdowns::Pushdowns;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::term::GroundTerm;

impl Triplestore {
    pub(crate) fn lazy_values(
        &self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
        _context: &Context,
        _pushdowns: Pushdowns,
        query_settings: &QuerySettings,
    ) -> Result<SolutionMappings, SparqlError> {
        let sm = values_pattern(variables, bindings);
        let (sm, _) = {
            let cats = self.global_cats.read()?;
            cats.encode_solution_mappings(sm, None)
        };
        if let Some(mut mappings) = solution_mappings {
            mappings = join(
                mappings,
                sm.as_lazy(),
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
            Ok(mappings)
        } else {
            Ok(sm.as_lazy())
        }
    }
}
