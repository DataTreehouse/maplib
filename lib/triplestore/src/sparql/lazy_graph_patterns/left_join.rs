use super::service::{contains_service, in_scope_variables};
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use super::filter::{pushdowns_for_expression, uses_variables_out_of_scope};
use super::join::{certainly_bound_variables, maybe_unbound_variables};
use crate::sparql::QuerySettings;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::{col, IntoLazy, JoinType};
use polars::prelude::{NamedFrom, Series};
use polars_core::prelude::IntoColumn;
use query_processing::expressions::contains_graph_pattern;
use query_processing::expressions::effective_boolean_value;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use query_processing::graph_patterns::{distinct, extend, filter, join, join_with_unbound, union};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::{HashMap, HashSet};

/// The left side of OPTIONAL is collected to evaluate the right side with its values when it is
/// estimated to have at most this many solutions, and they are used when it has.
const OPTIONAL_KEYS_HEIGHT: usize = 10_000;

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
            self.optional_keys(left_solution_mappings, left, right)?
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
                return self.left_join_numbered(
                    left_solution_mappings,
                    &maybe_unbound_variables(left),
                    right_solution_mappings,
                    &maybe_unbound_variables(right),
                    Some((expr, merged_expression_pushdowns.as_ref())),
                    &expression_context,
                    parameters,
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
        // value of it on the right side. So are rows of the right side (`{ ?s :b ?y } UNION
        // { ?z :h ?y }` for a left side binding ?s), which a left join cannot split by: a left
        // row matching only some of the parts would also be kept unmatched by the others.
        let left_maybe_unbound = maybe_unbound_variables(left);
        let right_maybe_unbound = maybe_unbound_variables(right);
        if right_maybe_unbound.iter().any(|v| {
            left_solution_mappings.rdf_node_types.contains_key(v)
                && right_solution_mappings.rdf_node_types.contains_key(v)
        }) {
            return self.left_join_numbered(
                left_solution_mappings,
                &left_maybe_unbound,
                right_solution_mappings,
                &right_maybe_unbound,
                None,
                &expression_context,
                parameters,
                query_settings,
                dataset,
            );
        }
        let left_solution_mappings = join_with_unbound(
            left_solution_mappings,
            &left_maybe_unbound,
            right_solution_mappings,
            &HashSet::new(),
            JoinType::Left,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        Ok(left_solution_mappings)
    }

    /// For a small left side of OPTIONAL: the left solutions, collected, and the distinct values
    /// of the variables both sides certainly bind, which the right side is then evaluated with.
    /// Its patterns then read only the triples that can match a left solution, instead of whole
    /// tables (`?review :rating ?r` for a few reviews). The right side then only has solutions
    /// compatible with these values, which gives the same left join.
    fn optional_keys(
        &self,
        left_solution_mappings: SolutionMappings,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> Result<(SolutionMappings, Option<SolutionMappings>), SparqlError> {
        // A constant subject or object already makes the right side selective on its own.
        // Patterns that see their incoming solutions (LATERAL, subqueries with LIMIT or
        // aggregates) would give other solutions when evaluated with the left side's values.
        if left_solution_mappings.height_estimate > OPTIONAL_KEYS_HEIGHT
            || has_constant_term(right)
            || !keys_keep_solutions(right)
        {
            return Ok((left_solution_mappings, None));
        }
        let left_certain = certainly_bound_variables(left);
        let right_certain = certainly_bound_variables(right);
        let shared: Vec<String> = left_solution_mappings
            .rdf_node_types
            .iter()
            .filter(|(v, t)| {
                left_certain.contains(*v)
                    && right_certain.contains(*v)
                    && !t.is_multi()
                    && !t.is_none()
            })
            .map(|(v, _)| v.clone())
            .collect();
        if shared.is_empty() {
            return Ok((left_solution_mappings, None));
        }
        // Small, so collected with the in-memory engine.
        let SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate,
        } = left_solution_mappings;
        let df = utils::polars::pl_interruptable_collect_estimated(mappings, height_estimate)
            .map_err(|e| match e {
                utils::polars::InterruptableCollectError::Interrupted => {
                    SparqlError::InterruptSignal
                }
                e => SparqlError::QueryExecutionError(e.to_string()),
            })?;
        let left_eager = EagerSolutionMappings::new(df, rdf_node_types);
        let height = left_eager.mappings.height();
        // Values bound in every left solution only: an unbound one is compatible with anything.
        let shared: Vec<String> = shared
            .into_iter()
            .filter(|v| left_eager.mappings.column(v).unwrap().null_count() == 0)
            .collect();
        if shared.is_empty() || height > OPTIONAL_KEYS_HEIGHT {
            return Ok((left_eager.as_lazy(), None));
        }
        let keys = SolutionMappings::new(
            left_eager
                .mappings
                .select(shared.iter().map(|x| x.as_str()))
                .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?
                .lazy(),
            shared
                .iter()
                .map(|v| (v.clone(), left_eager.rdf_node_types.get(v).unwrap().clone()))
                .collect(),
            height,
        );
        let keys = distinct(keys)?;
        Ok((left_eager.as_lazy(), Some(keys)))
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
        Ok(drop_columns(
            solution_mappings,
            &[expression_context.as_str()],
        ))
    }

    /// A left join by numbering the left solutions: they are joined with the right ones (and
    /// the matches filtered on the expression, when it is evaluated on the merged solutions),
    /// and the left solutions whose number has no match are added unmatched. Either side may
    /// leave shared variables unbound in some solutions, which are then compatible with any
    /// value on the other side.
    #[allow(clippy::too_many_arguments)]
    fn left_join_numbered(
        &self,
        left: SolutionMappings,
        left_maybe_unbound: &HashSet<String>,
        right: SolutionMappings,
        right_maybe_unbound: &HashSet<String>,
        expression: Option<(&Expression, Option<&Pushdowns>)>,
        expression_context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
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
        let mut matches = join_with_unbound(
            left.clone(),
            left_maybe_unbound,
            right,
            right_maybe_unbound,
            JoinType::Inner,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        if let Some((expr, expression_pushdowns)) = expression {
            matches = self.filter_on_expression(
                matches,
                expr,
                expression_context,
                parameters,
                expression_pushdowns,
                query_settings,
                dataset,
            )?;
        }
        // Collected once, since it is used twice below.
        let matches = matches
            .as_eager_interruptable()
            .map_err(|e| match e {
                utils::polars::InterruptableCollectError::Interrupted => {
                    SparqlError::InterruptSignal
                }
                e => SparqlError::QueryExecutionError(e.to_string()),
            })?
            .as_lazy();
        let not_row: Vec<String> = matches
            .rdf_node_types
            .keys()
            .filter(|c| **c != row)
            .cloned()
            .collect();
        let not_row: Vec<&str> = not_row.iter().map(|c| c.as_str()).collect();
        let matched_rows = drop_columns(matches.clone(), &not_row);
        let unmatched = join(
            left,
            matched_rows,
            JoinType::Anti,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        let joined = union(vec![matches, unmatched], false, self.global_cats.clone())?;
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

/// Whether a triple pattern the pattern evaluates first (outside OPTIONAL right sides, UNION and
/// subqueries) has a constant subject or object.
fn has_constant_term(gp: &GraphPattern) -> bool {
    use spargebra::term::TermPattern;
    match gp {
        GraphPattern::Bgp { patterns } => patterns.iter().any(|tp| {
            [&tp.subject, &tp.object]
                .iter()
                .any(|t| matches!(t, TermPattern::NamedNode(_) | TermPattern::Literal(_)))
        }),
        GraphPattern::Join { left, right } => has_constant_term(left) || has_constant_term(right),
        GraphPattern::LeftJoin { left, .. }
        | GraphPattern::Minus { left, .. }
        | GraphPattern::Lateral { left, .. } => has_constant_term(left),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner } => has_constant_term(inner),
        _ => false,
    }
}

/// Whether evaluating the pattern with incoming solutions gives the join of its own solutions
/// with them: true for patterns built from triple patterns, paths, joins, OPTIONAL, UNION,
/// MINUS, FILTER, BIND, VALUES and GRAPH.
fn keys_keep_solutions(gp: &GraphPattern) -> bool {
    match gp {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => true,
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            keys_keep_solutions(left) && keys_keep_solutions(right)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Graph { inner, .. } => keys_keep_solutions(inner),
        _ => false,
    }
}
