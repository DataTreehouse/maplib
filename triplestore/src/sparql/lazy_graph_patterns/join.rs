use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;

use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use std::collections::HashMap;

use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub fn lazy_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let mut output_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings, &left_context, parameters)?;
        output_solution_mappings = self.lazy_graph_pattern(
            right,
            Some(output_solution_mappings),
            &right_context,
            parameters,
        )?;
        Ok(output_solution_mappings)
    }
}
