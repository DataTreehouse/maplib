use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;

use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub fn lazy_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);
        let mut output_solution_mappings = self.lazy_graph_pattern(
            left,
            solution_mappings,
            &left_context,
            parameters,
            pushdowns.clone(),
        )?;
        output_solution_mappings = self.lazy_graph_pattern(
            right,
            Some(output_solution_mappings),
            &right_context,
            parameters,
            pushdowns,
        )?;

        Ok(output_solution_mappings)
    }
}
