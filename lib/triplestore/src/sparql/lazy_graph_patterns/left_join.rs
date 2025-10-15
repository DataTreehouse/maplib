use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::{col, JoinType};
use query_processing::expressions::contains_graph_pattern;
use query_processing::graph_patterns::{filter, join};
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;
use representation::dataset::QueryGraph;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn lazy_left_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing left join graph pattern");
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
        let left_solution_mappings = self.lazy_graph_pattern(
            left,
            solution_mappings,
            &left_context,
            parameters,
            pushdowns.clone(),
            query_settings,
            dataset,
        )?;

        pushdowns.add_graph_pattern_pushdowns(right);

        let expression_pushdowns = if let Some(expr) = expression {
            pushdowns.add_filter_variable_pushdowns(expr, None);
            if contains_graph_pattern(expr) {
                Some(pushdowns.clone())
            } else {
                None
            }
        } else {
            None
        };

        let mut right_solution_mappings = self.lazy_graph_pattern(
            right,
            Some(left_solution_mappings.clone()),
            &right_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;

        if let Some(expr) = expression {
            right_solution_mappings = self.lazy_expression(
                expr,
                right_solution_mappings,
                &expression_context,
                parameters,
                expression_pushdowns.as_ref(),
                query_settings,
                dataset,
            )?;
            right_solution_mappings = filter(right_solution_mappings, &expression_context)?;
            //The following is a workaround:
            let keep_cols: Vec<_> = right_solution_mappings
                .rdf_node_types
                .keys()
                .filter(|x| x.as_str() != expression_context.as_str())
                .map(|x| col(x))
                .collect();
            right_solution_mappings.mappings = right_solution_mappings.mappings.select(keep_cols);
            right_solution_mappings
                .rdf_node_types
                .remove(expression_context.as_str());
            //right_solution_mappings =
            //    drop_inner_contexts(right_solution_mappings, &vec![&expression_context]);
        }
        let left_solution_mappings = join(
            left_solution_mappings,
            right_solution_mappings,
            JoinType::Left,
            self.global_cats.clone(),
        )?;
        Ok(left_solution_mappings)
    }
}
