use super::join::maybe_unbound_variables;
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::graph_patterns::minus;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use utils::fast_path::{self, FastPath};

impl Triplestore {
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
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

        fast_path::hit(FastPath::MinusAntiJoin);
        Ok(minus(
            left_solution_mappings,
            &maybe_unbound_variables(left),
            right_solution_mappings,
            &maybe_unbound_variables(right),
            self.global_cats.clone(),
        )?)
    }
}
