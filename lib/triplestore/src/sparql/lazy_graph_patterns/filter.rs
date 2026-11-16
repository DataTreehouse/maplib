use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::lazy_graph_patterns::geo_filter::{
    binds_variable_in_triple, conjunction, conjuncts, split_for_spatial_join,
};
use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::expressions::{contains_graph_pattern, effective_boolean_value};
use query_processing::graph_patterns::{extend, filter, join};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use crate::sparql::lazy_window::is_rank_window;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::window::{contains_window_function, WindowCall};
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing filter graph pattern");
        // A window function is evaluated over the solutions of the inner pattern alone, so
        // the incoming solutions and pushdowns must not restrict them.
        let mut pushdowns = pushdowns;
        if contains_window_function(expression) {
            pushdowns = Pushdowns::new();
            if let Some(sm) = input_solution_mappings {
                let filtered = self.lazy_filter(
                    inner,
                    expression,
                    None,
                    context,
                    parameters,
                    Pushdowns::new(),
                    query_settings,
                    dataset,
                )?;
                return Ok(join(
                    sm,
                    filtered,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?);
            }
        }
        let inner_context = context.extension_with(PathEntry::FilterInner);
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        if let Some((rank_inner, variable, window)) = rank_one_filter(inner, expression) {
            // FILTER(?r = 1) over BIND(RANK() OVER (...) AS ?r): the rows with rank 1 are found
            // without ranking. The BIND is evaluated as lazy_extend evaluates a window.
            let bind_context = inner_context.extension_with(PathEntry::ExtendExpression);
            let sm = self.lazy_graph_pattern(
                rank_inner,
                None,
                &inner_context.extension_with(PathEntry::ExtendInner),
                parameters,
                Pushdowns::new(),
                query_settings,
                dataset,
            )?;
            let (sm, filtered) = self.lazy_window_rank_one(
                &window,
                sm,
                &bind_context,
                parameters,
                query_settings,
                dataset,
            )?;
            let mut sm = extend(sm, &bind_context, variable, self.global_cats.clone())?;
            if !filtered {
                sm = self.lazy_expression(
                    expression,
                    sm,
                    &expression_context,
                    parameters,
                    None,
                    query_settings,
                    dataset,
                )?;
                sm = effective_boolean_value(sm, &expression_context, self.global_cats.clone());
                sm = filter(sm, &expression_context)?;
            }
            if let Some(input) = input_solution_mappings {
                sm = join(
                    input,
                    sm,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
            }
            return Ok(sm);
        }
        let expression_pushdowns = if contains_graph_pattern(expression) {
            Some(pushdowns.clone())
        } else {
            None
        };

        // GeoSPARQL conjuncts that the geo index can answer are taken out of the filter.
        let mut input_solution_mappings = input_solution_mappings;
        let mut residual: Option<Expression> = None;
        let mut geo_joined: Option<SolutionMappings> = None;
        let mut used_geo_index = false;
        if let Some(geo_index) = &self.geo_index {
            let mut remaining = conjuncts(expression);

            // Conjuncts comparing a variable with a constant geometry restrict the variable
            // before the inner pattern is evaluated.
            let candidates =
                geo::filter_candidates(geo_index, &remaining, self.global_cats.clone())?;
            let mut used = vec![];
            for c in candidates {
                if !binds_variable_in_triple(inner, c.variable.as_str()) {
                    continue;
                }
                used.push(c.conjunct);
                let mappings = c.mappings.as_lazy();
                input_solution_mappings = Some(if let Some(sm) = input_solution_mappings {
                    join(
                        sm,
                        mappings,
                        JoinType::Inner,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?
                } else {
                    mappings
                });
            }
            if !used.is_empty() {
                used_geo_index = true;
                remaining = remaining
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !used.contains(i))
                    .map(|(_, c)| c)
                    .collect();
            }

            // A conjunct relating geometry variables from two independent parts of the inner
            // pattern is evaluated as a spatial join of the two parts.
            if input_solution_mappings.is_none() {
                let mut join_conjunct = None;
                for (i, c) in remaining.iter().enumerate() {
                    if let Some((a, b)) = geo::spatial_join_variables(c) {
                        if let Some(sides) = split_for_spatial_join(inner, a.as_str(), b.as_str()) {
                            join_conjunct = Some((i, sides));
                            break;
                        }
                    }
                }
                if let Some((i, (left, right))) = join_conjunct {
                    let left_sm = self.lazy_graph_pattern(
                        &left,
                        None,
                        &inner_context.extension_with(PathEntry::JoinLeftSide),
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        dataset,
                    )?;
                    let right_sm = self.lazy_graph_pattern(
                        &right,
                        None,
                        &inner_context.extension_with(PathEntry::JoinRightSide),
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        dataset,
                    )?;
                    match geo::spatial_join(
                        geo_index,
                        remaining[i],
                        left_sm,
                        right_sm,
                        self.global_cats.clone(),
                    )? {
                        Ok(joined) => {
                            geo_joined = Some(joined);
                            remaining.remove(i);
                        }
                        // The sides are already evaluated, so they are joined as the inner
                        // pattern would be, and the conjunct stays in the filter.
                        Err((left_sm, right_sm)) => {
                            geo_joined = Some(join(
                                left_sm,
                                right_sm,
                                JoinType::Inner,
                                self.global_cats.clone(),
                                query_settings.max_rows,
                            )?);
                        }
                    }
                    used_geo_index = true;
                }
            }
            if used_geo_index {
                residual = conjunction(remaining);
            }
        }
        let expression = if used_geo_index {
            match &residual {
                Some(residual) => residual,
                None => {
                    // The geo index answered the whole filter.
                    return if let Some(joined) = geo_joined {
                        Ok(joined)
                    } else {
                        self.lazy_graph_pattern(
                            inner,
                            input_solution_mappings,
                            &inner_context,
                            parameters,
                            pushdowns,
                            query_settings,
                            dataset,
                        )
                    };
                }
            }
        } else {
            expression
        };

        let mut output_solution_mappings = if let Some(joined) = geo_joined {
            joined
        } else {
            self.lazy_graph_pattern(
                inner,
                input_solution_mappings,
                &inner_context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            )?
        };
        output_solution_mappings = self.lazy_expression(
            expression,
            output_solution_mappings,
            &expression_context,
            parameters,
            expression_pushdowns.as_ref(),
            query_settings,
            dataset,
        )?;
        output_solution_mappings = effective_boolean_value(
            output_solution_mappings,
            &expression_context,
            self.global_cats.clone(),
        );
        output_solution_mappings = filter(output_solution_mappings, &expression_context)?;
        Ok(output_solution_mappings)
    }
}

/// `FILTER(?r = 1)` (or `?r <= 1`, `?r < 2`, `1 = ?r`, `1 >= ?r`, `2 > ?r`) directly over
/// `BIND(RANK() OVER (... ORDER BY one key) AS ?r)` or DENSE_RANK: the inner pattern of the
/// BIND, the variable and the window.
fn rank_one_filter<'a>(
    inner: &'a GraphPattern,
    expression: &Expression,
) -> Option<(&'a GraphPattern, &'a Variable, WindowCall)> {
    let GraphPattern::Extend {
        inner: rank_inner,
        variable,
        expression: bound,
    } = inner
    else {
        return None;
    };
    let window = WindowCall::from_expression(bound)?;
    if !is_rank_window(&window) {
        return None;
    }
    let integer = |e: &Expression| match e {
        Expression::Literal(l) if l.datatype() == xsd::INTEGER => l.value().parse::<i64>().ok(),
        _ => None,
    };
    let is_var = |e: &Expression| matches!(e, Expression::Variable(v) if v == variable);
    let rank_one = match expression {
        Expression::Equal(a, b) => {
            (is_var(a) && integer(b) == Some(1)) || (is_var(b) && integer(a) == Some(1))
        }
        Expression::LessOrEqual(a, b) => is_var(a) && integer(b) == Some(1),
        Expression::GreaterOrEqual(a, b) => is_var(b) && integer(a) == Some(1),
        Expression::Less(a, b) => is_var(a) && integer(b) == Some(2),
        Expression::Greater(a, b) => is_var(b) && integer(a) == Some(2),
        _ => false,
    };
    rank_one.then_some((rank_inner.as_ref(), variable, window))
}
