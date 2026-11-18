use super::service::{contains_service, in_scope_variables};
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use super::filter::{pushdowns_for_expression, uses_variables_out_of_scope};
use super::join::maybe_unbound_variables;
use crate::sparql::QuerySettings;
use oxrdf::Variable;
use polars::prelude::{col, JoinType};
use query_processing::expressions::contains_graph_pattern;
use query_processing::expressions::effective_boolean_value;
use query_processing::graph_patterns::{extend, filter, join, join_with_unbound};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use oxrdf::vocab::xsd;
use polars::prelude::{NamedFrom, Series};
use polars_core::prelude::IntoColumn;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::BaseRDFNodeType;
use std::collections::{HashMap, HashSet};

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
        // The expression of an OPTIONAL sees only the variables of the two sides: a variable
        // that only the incoming solutions bind is unbound for it (as for FILTER).
        if let (Some(expr), Some(sm)) = (expression, &solution_mappings) {
            if uses_variables_out_of_scope(expr, &[left, right], sm, context, parameters) {
                let left_joined = self.lazy_left_join(
                    left,
                    right,
                    expression,
                    None,
                    context,
                    parameters,
                    Pushdowns::new(),
                    query_settings,
                    dataset,
                )?;
                return Ok(join(
                    solution_mappings.unwrap(),
                    left_joined,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?);
            }
        }
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
        // The incoming variables, for what the EXISTS of the expression may see unbound.
        let expression_incoming = if expression.as_ref().is_some_and(contains_graph_pattern) {
            solution_mappings.clone()
        } else {
            None
        };
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
                // The condition is evaluated on the right side's solutions or on the merged
                // ones; which variables may be unbound in them is set below.
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
            let left_eager =
                left_solution_mappings
                    .as_eager_interruptable()
                    .map_err(|e| match e {
                        utils::polars::InterruptableCollectError::Interrupted => {
                            SparqlError::InterruptSignal
                        }
                        e => SparqlError::QueryExecutionError(e.to_string()),
                    })?;
            let keys = self.bind_keys(&left_eager, &in_scope_variables(right))?;
            (left_eager.as_lazy(), keys.map(|k| k.as_lazy()))
        } else {
            (left_solution_mappings, None)
        };
        let right_expression_pushdowns = expression_pushdowns
            .as_ref()
            .map(|p| pushdowns_for_expression(p, right, right_incoming.as_ref(), parameters));
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
            // The expression is evaluated on the merged solutions. When it uses a variable that
            // only the left side binds, it is evaluated after joining the two sides.
            let mut expression_variables = HashSet::new();
            find_all_used_variables_in_expression(expr, &mut expression_variables, true, true);
            if expression_variables.iter().any(|v| {
                left_solution_mappings
                    .rdf_node_types
                    .contains_key(v.as_str())
                    && !right_solution_mappings
                        .rdf_node_types
                        .contains_key(v.as_str())
            }) {
                let merged = GraphPattern::Join {
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                };
                let merged_expression_pushdowns = expression_pushdowns.as_ref().map(|p| {
                    pushdowns_for_expression(p, &merged, expression_incoming.as_ref(), parameters)
                });
                return self.left_join_filtering_merged_solutions(
                    left_solution_mappings,
                    right_solution_mappings,
                    expr,
                    &expression_context,
                    parameters,
                    merged_expression_pushdowns.as_ref(),
                    query_settings,
                    dataset,
                );
            }
            // Without these, every variable the EXISTS patterns share with the solutions would
            // count as maybe unbound, and each pattern would be evaluated once for the bound and
            // once for the unbound values, at the same context (which chrontext prepares for one
            // evaluation).
            right_solution_mappings = self.filter_on_expression(
                right_solution_mappings,
                expr,
                &expression_context,
                parameters,
                right_expression_pushdowns.as_ref(),
                query_settings,
                dataset,
            )?;
        }
        // Rows of the left side that leave a shared variable unbound are compatible with every
        // value of it on the right side.
        let left_solution_mappings = join_with_unbound(
            left_solution_mappings,
            &maybe_unbound_variables(left),
            right_solution_mappings,
            &HashSet::new(),
            JoinType::Left,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        Ok(left_solution_mappings)
    }

    /// Keeps the solutions where the expression is true, without the expression column.
    #[allow(clippy::too_many_arguments)]
    fn filter_on_expression(
        &self,
        solution_mappings: SolutionMappings,
        expr: &Expression,
        expression_context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        expression_pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut solution_mappings = self.lazy_expression(
            expr,
            solution_mappings,
            expression_context,
            parameters,
            expression_pushdowns,
            query_settings,
            dataset,
        )?;
        solution_mappings = effective_boolean_value(
            solution_mappings,
            expression_context,
            self.global_cats.clone(),
        );
        solution_mappings = filter(solution_mappings, expression_context)?;
        Ok(drop_columns(solution_mappings, &[expression_context.as_str()]))
    }

    /// The left join where the expression is evaluated on the merged solutions: the left
    /// solutions are numbered, joined with the right ones and filtered, and the matches that
    /// remain are joined back to the left solutions by their number.
    #[allow(clippy::too_many_arguments)]
    fn left_join_filtering_merged_solutions(
        &self,
        left: SolutionMappings,
        right: SolutionMappings,
        expr: &Expression,
        expression_context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        expression_pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let row = format!("__maplib_left_join_row_{}", expression_context.as_str());
        // Collected once, so that the rows are numbered the same in both uses below.
        let mut left = left.as_eager_interruptable().map_err(|e| match e {
            utils::polars::InterruptableCollectError::Interrupted => SparqlError::InterruptSignal,
            e => SparqlError::QueryExecutionError(e.to_string()),
        })?;
        let height = left.mappings.height();
        left.mappings
            .with_column(
                Series::new(row.as_str().into(), (0..height as i64).collect::<Vec<_>>())
                    .into_column(),
            )
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        left.rdf_node_types.insert(
            row.clone(),
            BaseRDFNodeType::Literal(xsd::LONG.into_owned(), None)
                .into_default_input_rdf_node_state(),
        );
        let left = left.as_lazy();
        let left_columns: Vec<String> = left.rdf_node_types.keys().cloned().collect();
        let matches = join(
            left.clone(),
            right,
            JoinType::Inner,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        let matches = self.filter_on_expression(
            matches,
            expr,
            expression_context,
            parameters,
            expression_pushdowns,
            query_settings,
            dataset,
        )?;
        let shared: Vec<&str> = left_columns
            .iter()
            .filter(|c| **c != row)
            .map(|c| c.as_str())
            .collect();
        let matches = drop_columns(matches, &shared);
        let joined = join(
            left,
            matches,
            JoinType::Left,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        Ok(drop_columns(joined, &[row.as_str()]))
    }

    /// Used by joins that treat unbound values as compatible (SERVICE results).
    /// Keeps the rows where `v` and `outer` are compatible (one is unbound, or they are the same
    /// term) and binds `v` to whichever is bound.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn merge_outer_binding(
        &self,
        sm: SolutionMappings,
        v: &Variable,
        outer: &Variable,
        i: usize,
        context: &Context,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let var = |x: &Variable| Box::new(Expression::Variable(x.clone()));
        let not_bound =
            |x: &Variable| Box::new(Expression::Not(Box::new(Expression::Bound(x.clone()))));
        let compatible = Expression::Or(
            Box::new(Expression::Or(not_bound(v), not_bound(outer))),
            Box::new(Expression::SameTerm(var(v), var(outer))),
        );
        let filter_context = context.extension_with(PathEntry::Coalesce(u16::MAX - 2 * i as u16));
        let mut sm = self.lazy_expression(
            &compatible,
            sm,
            &filter_context,
            None,
            None,
            query_settings,
            dataset,
        )?;
        sm = effective_boolean_value(sm, &filter_context, self.global_cats.clone());
        sm = filter(sm, &filter_context)?;
        let coalesce_context =
            context.extension_with(PathEntry::Coalesce(u16::MAX - 2 * i as u16 - 1));
        sm = self.lazy_expression(
            &Expression::Coalesce(vec![
                Expression::Variable(v.clone()),
                Expression::Variable(outer.clone()),
            ]),
            sm,
            &coalesce_context,
            None,
            None,
            query_settings,
            dataset,
        )?;
        sm = drop_columns(sm, &[v.as_str(), outer.as_str()]);
        Ok(extend(sm, &coalesce_context, v, self.global_cats.clone())?)
    }
}

fn drop_columns(mut sm: SolutionMappings, names: &[&str]) -> SolutionMappings {
    let keep: Vec<_> = sm
        .rdf_node_types
        .keys()
        .filter(|x| !names.contains(&x.as_str()))
        .map(col)
        .collect();
    sm.mappings = sm.mappings.select(keep);
    for n in names {
        sm.rdf_node_types.remove(*n);
    }
    sm
}
