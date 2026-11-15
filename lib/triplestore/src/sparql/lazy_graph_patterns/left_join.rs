use super::service::{contains_service, in_scope_variables};
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::{col, JoinType};
use query_processing::expressions::contains_graph_pattern;
use query_processing::expressions::effective_boolean_value;
use query_processing::graph_patterns::{filter, join};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;

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
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
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
        // Constraints on variables that only the optional side binds must not be pushed into
        // it, since a filtered out optional match gives an unbound variable instead.
        let mut left_variables = vec![];
        left.on_in_scope_variable(|v| left_variables.push(v.clone()));
        pushdowns.limit_to_variables(&left_variables);
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
        // With a SERVICE in the optional part, the distinct values of the variables it shares
        // with the left side are evaluated with it, so that they are sent to the endpoint (a bind
        // join) instead of fetching all its solutions. The right side then only has solutions
        // compatible with these values, extended with them, which gives the same left join.
        let (left_solution_mappings, right_incoming) = if contains_service(right) {
            let left_eager = left_solution_mappings.as_eager_interruptable().map_err(|e| {
                match e {
                    utils::polars::InterruptableCollectError::Interrupted => {
                        SparqlError::InterruptSignal
                    }
                    e => SparqlError::QueryExecutionError(e.to_string()),
                }
            })?;
            let keys = self.bind_keys(&left_eager, &in_scope_variables(right))?;
            (left_eager.as_lazy(), keys.map(|k| k.as_lazy()))
        } else {
            (left_solution_mappings, None)
        };
        let mut right_solution_mappings = self.lazy_graph_pattern(
            right,
            right_incoming,
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
            right_solution_mappings = effective_boolean_value(
                right_solution_mappings,
                &expression_context,
                self.global_cats.clone(),
            );
            right_solution_mappings = filter(right_solution_mappings, &expression_context)?;
            //The following is a workaround:
            let keep_cols: Vec<_> = right_solution_mappings
                .rdf_node_types
                .keys()
                .filter(|x| x.as_str() != expression_context.as_str())
                .map(col)
                .collect();
            right_solution_mappings.mappings = right_solution_mappings.mappings.select(keep_cols);
            right_solution_mappings
                .rdf_node_types
                .remove(expression_context.as_str());
        }
        let left_solution_mappings = join(
            left_solution_mappings,
            right_solution_mappings,
            JoinType::Left,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        Ok(left_solution_mappings)
    }
}
