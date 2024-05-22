use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;

use query_processing::graph_patterns::union;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing union graph pattern");
        let left_context = context.extension_with(PathEntry::UnionLeftSide);
        let right_context = context.extension_with(PathEntry::UnionRightSide);

        let left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings.clone(), &left_context, parameters)?;
        let right_solution_mappings =
            self.lazy_graph_pattern(right, solution_mappings, &right_context, parameters)?;

        Ok(union(
            vec![left_solution_mappings, right_solution_mappings],
            true,
        )?)
    }
}
