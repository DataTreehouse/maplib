use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::graph_patterns::minus;
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{GraphPattern};
use std::collections::HashMap;
use representation::dataset::QueryGraph;

impl Triplestore {
    #[instrument(skip_all)]
    pub(crate) fn lazy_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
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
            query_settings,
            dataset,
        )?;

        let right_solution_mappings = self.lazy_graph_pattern(
            right,
            solution_mappings,
            &right_context,
            parameters,
            Pushdowns::new(),
            query_settings,
            dataset,
        )?;

        Ok(minus(left_solution_mappings, right_solution_mappings)?)
    }
}
