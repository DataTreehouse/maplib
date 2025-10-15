use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::by_name;
use query_processing::graph_patterns::order_by;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{GraphPattern, OrderExpression};
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    pub(crate) fn lazy_order_by(
        &self,
        inner: &GraphPattern,
        expression: &[OrderExpression],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing order by graph pattern");
        let mut output_solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::OrderByInner),
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;

        let order_expression_contexts: Vec<Context> = (0..expression.len())
            .map(|i| context.extension_with(PathEntry::OrderByExpression(i as u16)))
            .collect();
        let mut asc_ordering = vec![];
        let mut inner_contexts = vec![];
        for i in 0..expression.len() {
            let (ordering_solution_mappings, reverse, inner_context) = self.lazy_order_expression(
                expression.get(i).unwrap(),
                output_solution_mappings,
                order_expression_contexts.get(i).unwrap(),
                parameters,
                query_settings,
                dataset,
            )?;
            output_solution_mappings = ordering_solution_mappings;
            inner_contexts.push(inner_context);
            asc_ordering.push(reverse);
        }
        let sort_columns: Vec<_> = inner_contexts
            .iter()
            .map(|x| x.as_str().to_string())
            .collect();
        output_solution_mappings = order_by(
            output_solution_mappings,
            &sort_columns,
            asc_ordering,
            self.global_cats.clone(),
        )?;
        output_solution_mappings.mappings = output_solution_mappings
            .mappings
            .drop(by_name(sort_columns, true));
        Ok(output_solution_mappings)
    }
}
