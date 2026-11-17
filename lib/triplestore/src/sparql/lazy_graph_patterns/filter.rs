use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::lazy_graph_patterns::geo_filter::{
    binds_variable_in_triple, conjunction, conjuncts, split_for_spatial_join,
};
use crate::sparql::lazy_expressions::mentioned_variable_names;
use crate::sparql::lazy_graph_patterns::join::certainly_bound_variables;
use crate::sparql::lazy_graph_patterns::lateral::lateral_seed;
use crate::sparql::QuerySettings;
use polars::prelude::{col, Expr, IntoLazy, JoinArgs, JoinType, UniqueKeepStrategy};
use query_processing::expressions::{contains_graph_pattern, effective_boolean_value};
use query_processing::find_query_variables::find_all_used_variables_in_expression;
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
use std::collections::{HashMap, HashSet};

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
        // A FILTER sees only the variables of its own group: a variable that only the incoming
        // solutions bind (e.g. ?v in { :x :p ?v . { FILTER(?v = 1) } }) is unbound for it. The
        // filter is then evaluated without them, and joined with them afterwards.
        if let Some(sm) = &input_solution_mappings {
            if uses_variables_out_of_scope(expression, &[inner], sm, context, parameters) {
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
                    input_solution_mappings.unwrap(),
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
        // FILTER EXISTS and FILTER NOT EXISTS conjuncts become semi and anti joins on the
        // variables the pattern shares with the solutions (see lazy_exists_join).
        let certain = certainly_bound_variables(inner);
        let mut residual_conjuncts = vec![];
        let mut exists_joins = vec![];
        for c in conjuncts(expression) {
            if let Some((pattern, negated)) = exists_conjunct(c) {
                let mentioned = mentioned_variable_names(pattern);
                let mut keys: Vec<String> = output_solution_mappings
                    .rdf_node_types
                    .keys()
                    .filter(|k| mentioned.contains(k.as_str()))
                    .cloned()
                    .collect();
                if !keys.is_empty() && keys.iter().all(|k| certain.contains(k)) {
                    keys.sort();
                    exists_joins.push((pattern, negated, keys));
                    continue;
                }
            }
            residual_conjuncts.push(c);
        }
        for (pattern, negated, keys) in exists_joins {
            output_solution_mappings = self.lazy_exists_join(
                output_solution_mappings,
                inner,
                pattern,
                negated,
                &keys,
                &expression_context,
                parameters,
                expression_pushdowns.as_ref(),
                query_settings,
                dataset,
            )?;
        }
        let Some(expression) = conjunction(residual_conjuncts) else {
            return Ok(output_solution_mappings);
        };
        output_solution_mappings = self.lazy_expression(
            &expression,
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

    /// Keeps the solutions for which the pattern has a solution (EXISTS), or has none (NOT
    /// EXISTS, `negated`), with the solutions substituted into it.
    ///
    /// Whether it has one depends only on the values of `keys`, the variables of the solutions
    /// that the pattern mentions, which the caller has checked are bound in every solution. The
    /// pattern is evaluated once for each distinct combination of their values, and the
    /// solutions are then semi or anti joined with the combinations that have one. EXISTS as an
    /// expression instead evaluates the pattern once for each solution, carrying all of its
    /// columns through the pattern, and then joins a boolean column back and filters on it. BSBM
    /// BI Q4's FILTER NOT EXISTS { ?product bsbm:productFeature ?feature } over a million
    /// feature × offer solutions asks about 52 thousand feature × product pairs.
    #[allow(clippy::too_many_arguments)]
    fn lazy_exists_join(
        &self,
        solution_mappings: SolutionMappings,
        inner: &GraphPattern,
        pattern: &GraphPattern,
        negated: bool,
        keys: &[String],
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate,
        } = solution_mappings;
        // The solutions are read twice, for their keys and for the join. Collecting them first
        // would keep a copy of them alive during the join, which doubled the peak memory of
        // BSBM BI Q4, so they are evaluated twice instead, unless that may give other values.
        let mappings = if may_change_between_evaluations(inner) {
            mappings.collect().unwrap().lazy()
        } else {
            mappings
        };
        let key_columns: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        let key_types: HashMap<_, _> = keys
            .iter()
            .map(|k| (k.clone(), rdf_node_types.get(k).unwrap().clone()))
            .collect();
        let distinct_keys = SolutionMappings::new(
            mappings
                .clone()
                .select(key_columns.clone())
                .unique(None, UniqueKeepStrategy::Any),
            key_types,
            height_estimate,
        );
        let answers = self.lazy_exists(
            pattern,
            distinct_keys,
            &context.extension_with(PathEntry::Exists),
            context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        let matching_keys = answers
            .mappings
            .filter(col(context.as_str()))
            .select(key_columns.clone());
        let how = if negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        };
        let mappings = mappings
            .join(
                matching_keys,
                key_columns.as_slice(),
                key_columns.as_slice(),
                JoinArgs::new(how),
            )
            .unwrap();
        Ok(SolutionMappings::new(
            mappings,
            rdf_node_types,
            height_estimate,
        ))
    }
}

/// Whether evaluating the pattern twice may give different solutions: it calls RAND(), UUID(),
/// STRUUID(), BNODE(), NOW() or a custom function, or queries a SERVICE. The pattern's text is
/// scanned, as in [mentioned_variable_names], so no kind of pattern is missed; a false match only
/// costs memory.
fn may_change_between_evaluations(gp: &GraphPattern) -> bool {
    let text = gp.to_string().to_lowercase();
    ["rand(", "uuid(", "bnode(", "now(", "service", ">("]
        .iter()
        .any(|s| text.contains(s))
}

/// The pattern of `EXISTS { pattern }` (false) or `NOT EXISTS { pattern }` (true).
fn exists_conjunct(expression: &Expression) -> Option<(&GraphPattern, bool)> {
    match expression {
        Expression::Exists(pattern) => Some((pattern.as_ref(), false)),
        Expression::Not(inner) => match inner.as_ref() {
            Expression::Exists(pattern) => Some((pattern.as_ref(), true)),
            _ => None,
        },
        _ => None,
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

/// True if the expression uses a variable that the incoming solutions bind but that is not in
/// scope in the patterns the expression belongs to. Such a variable is unbound for the
/// expression (SPARQL 1.1 18.2.2: a filter applies to the group it is in).
///
/// Inside EXISTS and LATERAL the incoming solutions are substituted into the whole pattern, so
/// their variables are in scope everywhere in it, and this is always false.
pub(crate) fn uses_variables_out_of_scope(
    expression: &Expression,
    patterns: &[&GraphPattern],
    incoming: &SolutionMappings,
    context: &Context,
    parameters: Option<&HashMap<String, EagerSolutionMappings>>,
) -> bool {
    if context.contains(&PathEntry::Exists) || lateral_seed(parameters).is_some() {
        return false;
    }
    let mut used = HashSet::new();
    find_all_used_variables_in_expression(expression, &mut used, true, true);
    used.iter().any(|v| {
        incoming.rdf_node_types.contains_key(v.as_str()) && {
            let mut in_scope = false;
            for p in patterns {
                p.on_in_scope_variable(|w| in_scope |= w == v);
            }
            !in_scope
        }
    })
}
