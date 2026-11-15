use super::service::{contains_service, in_scope_variables};
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use oxrdf::Variable;
use polars::prelude::{col, JoinType};
use query_processing::expressions::contains_graph_pattern;
use query_processing::expressions::effective_boolean_value;
use query_processing::graph_patterns::{extend, filter, join};
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
        // Incoming solutions that bind a variable which only the optional side binds are renamed
        // before they are passed to the left side, and made compatible after the left join:
        // otherwise a left row whose optional match differs on that variable would be kept
        // with the incoming value instead of dropped.
        let mut left_scope = vec![];
        left.on_in_scope_variable(|v| left_scope.push(v.as_str().to_string()));
        let mut outer_only = vec![];
        let solution_mappings = solution_mappings.map(|mut sm| {
            right.on_in_scope_variable(|v| {
                let v = v.as_str();
                if !left_scope.iter().any(|l| l == v)
                    && !outer_only
                        .iter()
                        .any(|(o, _): &(Variable, Variable)| o.as_str() == v)
                {
                    if let Some(state) = sm.rdf_node_types.remove(v) {
                        let renamed = format!("__maplib_outer_{v}");
                        sm.mappings = sm.mappings.clone().rename([v], [renamed.as_str()], true);
                        sm.rdf_node_types.insert(renamed.clone(), state);
                        outer_only
                            .push((Variable::new_unchecked(v), Variable::new_unchecked(renamed)));
                    }
                }
            });
            sm
        });
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
        let mut sm = left_solution_mappings;
        for (i, (v, outer)) in outer_only.iter().enumerate() {
            sm = self.merge_outer_binding(sm, v, outer, i, context, query_settings, dataset)?;
        }
        Ok(sm)
    }

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
