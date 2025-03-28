use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::trace;

use query_processing::graph_patterns::union;
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing union graph pattern");
        let left_context = context.extension_with(PathEntry::UnionLeftSide);
        let right_context = context.extension_with(PathEntry::UnionRightSide);
        let mut left_pushdowns = pushdowns.clone();
        left_pushdowns.add_graph_pattern_pushdowns(left);
        let left_solution_mappings = self.lazy_graph_pattern(
            left,
            solution_mappings.clone(),
            &left_context,
            parameters,
            left_pushdowns,
            include_transient,
        )?;
        pushdowns.add_graph_pattern_pushdowns(right);
        let right_solution_mappings = self.lazy_graph_pattern(
            right,
            solution_mappings,
            &right_context,
            parameters,
            pushdowns,
            include_transient,
        )?;

        Ok(union(
            vec![left_solution_mappings, right_solution_mappings],
            true,
        )?)
    }
}
