use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::trace;

use query_processing::graph_patterns::minus;
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing minus graph pattern");
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let left_solution_mappings = self.lazy_graph_pattern(
            left,
            solution_mappings.clone(),
            &left_context,
            parameters,
            pushdowns,
            include_transient,
        )?;

        let right_solution_mappings = self.lazy_graph_pattern(
            right,
            solution_mappings,
            &right_context,
            parameters,
            Pushdowns::new(),
            include_transient,
        )?;

        Ok(minus(left_solution_mappings, right_solution_mappings)?)
    }
}
