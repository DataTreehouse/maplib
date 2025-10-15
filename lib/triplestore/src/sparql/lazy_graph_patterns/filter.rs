use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::expressions::contains_graph_pattern;
use query_processing::graph_patterns::filter;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    pub(crate) fn lazy_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing filter graph pattern");
        let inner_context = context.extension_with(PathEntry::FilterInner);
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        let expression_pushdowns = if contains_graph_pattern(expression) {
            Some(pushdowns.clone())
        } else {
            None
        };
        let mut output_solution_mappings = self.lazy_graph_pattern(
            inner,
            input_solution_mappings,
            &inner_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        output_solution_mappings = self.lazy_expression(
            expression,
            output_solution_mappings,
            &expression_context,
            parameters,
            expression_pushdowns.as_ref(),
            query_settings,
            dataset,
        )?;

        output_solution_mappings = filter(output_solution_mappings, &expression_context)?;

        Ok(output_solution_mappings)
    }
}
