//! LATERAL (SEP-0006): the right side is evaluated once for every solution of the left side,
//! with the variables of that solution substituted into it.
//!
//! The substitution is evaluated set-based. The distinct bindings of the left variables that the
//! right side uses (the correlated variables) are numbered with a key column, and this table, the
//! seed, is passed down the evaluation of the right side as a reserved parameter. The seed is
//! joined wherever the right side starts from no solutions, so a substituted variable is bound
//! everywhere in the group, including in FILTER and BIND. The operators that start a new scope
//! or work on whole solution sequences take the key into account:
//! - a sub-select only keeps the correlated variables it projects (others are different variables
//!   inside the sub-select), and keeps the key,
//! - GROUP BY groups by the key as well, and without GROUP BY gives one group per key,
//! - LIMIT/OFFSET slice each key on its own, so `ORDER BY .. LIMIT n` gives the top n per left
//!   binding,
//! - DISTINCT includes the key.
//!
//! The result of the right side is joined with the left side on the key. Setting the
//! environment variable `MAPLIB_LATERAL_PER_ROW` evaluates the right side separately for every
//! seed row instead, which is used to compare the two strategies.

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::{by_name, col, lit, IntoLazy, JoinType, NamedFrom, Series};
use polars_core::prelude::{IntoColumn, UniqueKeepStrategy};
use query_processing::find_query_variables::{
    find_all_used_variables_in_aggregate_expression, find_all_used_variables_in_expression,
    find_all_used_variables_in_order_expression,
};
use query_processing::graph_patterns::{join, union};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::collections::{HashMap, HashSet};
use tracing::{instrument, trace};

/// The parameter holding the seed of the innermost LATERAL being evaluated.
pub(crate) const LATERAL_SEED_PARAMETER: &str = "__maplib_lateral_seed";
/// Prefix of the key columns of seeds. Nested LATERALs each add a key column.
pub(crate) const LATERAL_KEY_PREFIX: &str = "__maplib_lateral_key_";
const PER_ROW_ENV: &str = "MAPLIB_LATERAL_PER_ROW";

pub(crate) fn lateral_seed(
    parameters: Option<&HashMap<String, EagerSolutionMappings>>,
) -> Option<&EagerSolutionMappings> {
    parameters.and_then(|p| p.get(LATERAL_SEED_PARAMETER))
}

pub(crate) fn is_lateral_key(c: &str) -> bool {
    c.starts_with(LATERAL_KEY_PREFIX)
}

/// The key columns of the seed in the parameters, if any.
pub(crate) fn lateral_key_columns(
    parameters: Option<&HashMap<String, EagerSolutionMappings>>,
) -> Vec<String> {
    if let Some(seed) = lateral_seed(parameters) {
        let mut keys: Vec<_> = seed
            .rdf_node_types
            .keys()
            .filter(|c| is_lateral_key(c))
            .cloned()
            .collect();
        keys.sort();
        keys
    } else {
        vec![]
    }
}

/// The seed as starting solution mappings, when no solutions flow into a pattern.
pub(crate) fn seed_solution_mappings(
    parameters: Option<&HashMap<String, EagerSolutionMappings>>,
) -> Option<SolutionMappings> {
    lateral_seed(parameters).map(|s| s.clone().as_lazy())
}

/// The parameters with the seed replaced, or removed when `seed` is None.
pub(crate) fn with_seed(
    parameters: Option<&HashMap<String, EagerSolutionMappings>>,
    seed: Option<EagerSolutionMappings>,
) -> Option<HashMap<String, EagerSolutionMappings>> {
    let mut new = parameters.cloned().unwrap_or_default();
    if let Some(seed) = seed {
        new.insert(LATERAL_SEED_PARAMETER.to_string(), seed);
    } else {
        new.remove(LATERAL_SEED_PARAMETER);
    }
    if new.is_empty() {
        None
    } else {
        Some(new)
    }
}

/// The seed for the inner pattern of a sub-select projecting `variables`: correlated variables
/// that are not projected are other variables inside the sub-select. None if no correlated
/// variable is projected, then the sub-select does not depend on the left side.
pub(crate) fn project_seed(
    seed: &EagerSolutionMappings,
    variables: &[Variable],
) -> Option<EagerSolutionMappings> {
    let projected: HashSet<_> = variables.iter().map(|v| v.as_str()).collect();
    let keep: Vec<String> = seed
        .rdf_node_types
        .keys()
        .filter(|c| is_lateral_key(c) || projected.contains(c.as_str()))
        .cloned()
        .collect();
    if keep.iter().all(|c| is_lateral_key(c)) {
        return None;
    }
    if keep.len() == seed.rdf_node_types.len() {
        return Some(seed.clone());
    }
    let mappings = seed.mappings.select(keep.iter().map(|x| x.as_str())).unwrap();
    let rdf_node_types = keep
        .iter()
        .map(|c| (c.clone(), seed.rdf_node_types.get(c).unwrap().clone()))
        .collect();
    Some(EagerSolutionMappings::new(mappings, rdf_node_types))
}

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(crate) fn lazy_lateral(
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
        trace!("Processing lateral graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);
        let left_sm = self.lazy_graph_pattern(
            left,
            solution_mappings,
            &left_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;

        // The left variables the right side may use, and the columns of an outer seed, which
        // stay substituted in the right side.
        let mut right_variables = HashSet::new();
        variables_in_graph_pattern(right, &mut right_variables);
        let outer_seed_columns: HashSet<String> = lateral_seed(parameters)
            .map(|s| s.rdf_node_types.keys().cloned().collect())
            .unwrap_or_default();
        let mut correlated: Vec<String> = left_sm
            .rdf_node_types
            .keys()
            .filter(|c| right_variables.contains(c.as_str()))
            .cloned()
            .collect();
        correlated.sort();
        if correlated.is_empty() {
            // The right side does not depend on the left side: a join.
            let right_sm = self.lazy_graph_pattern(
                right,
                None,
                &right_context,
                parameters,
                Pushdowns::new(),
                query_settings,
                dataset,
            )?;
            return Ok(join(
                left_sm,
                right_sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?);
        }
        let mut seed_columns = correlated.clone();
        let mut outer: Vec<_> = left_sm
            .rdf_node_types
            .keys()
            .filter(|c| outer_seed_columns.contains(*c) && !correlated.contains(*c))
            .cloned()
            .collect();
        outer.sort();
        seed_columns.extend(outer);
        let n_keys = seed_columns.iter().filter(|c| is_lateral_key(c)).count();
        let key = format!("{LATERAL_KEY_PREFIX}{n_keys}");

        let left_sm = left_sm.as_eager();
        let seed = make_seed(&left_sm, &seed_columns, &key);
        let left_sm = left_sm.as_lazy();
        // The key of each left solution.
        let keyed_left = join(
            left_sm,
            seed.clone().as_lazy(),
            JoinType::Inner,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;

        // Correlated variables that are unbound in some left solutions are not substituted
        // there, so the seed is split by which of them are bound.
        let mut nullable = vec![];
        for c in &seed_columns {
            if seed.mappings.column(c).unwrap().null_count() > 0 {
                nullable.push(c.clone());
            }
        }
        let parts = if nullable.is_empty() {
            vec![(seed, vec![])]
        } else {
            split_seed_by_unbound(seed, &nullable)
        };

        let n_parts = parts.len();
        let per_row = std::env::var(PER_ROW_ENV).is_ok();
        let mut outputs = vec![];
        for (part_seed, unbound) in parts {
            let right_sm = if per_row && part_seed.mappings.height() > 0 {
                let mut rows = vec![];
                for i in 0..part_seed.mappings.height() {
                    let row_seed = EagerSolutionMappings::new(
                        part_seed.mappings.slice(i as i64, 1),
                        part_seed.rdf_node_types.clone(),
                    );
                    rows.push(
                        self.lazy_lateral_right(
                            right,
                            row_seed,
                            &right_context,
                            parameters,
                            query_settings,
                            dataset,
                        )?
                        .as_eager()
                        .as_lazy(),
                    );
                }
                if rows.is_empty() {
                    None
                } else {
                    Some(union(rows, false, self.global_cats.clone())?)
                }
            } else {
                Some(self.lazy_lateral_right(
                    right,
                    part_seed.clone(),
                    &right_context,
                    parameters,
                    query_settings,
                    dataset,
                )?)
            };
            let Some(mut right_sm) = right_sm else {
                continue;
            };
            // Only the key and the variables the right side binds itself are needed. The
            // substituted variables equal those of the left solution.
            let drop: Vec<String> = right_sm
                .rdf_node_types
                .keys()
                .filter(|c| {
                    *c != &key
                        && (part_seed.rdf_node_types.contains_key(c.as_str()) || is_lateral_key(c))
                })
                .cloned()
                .collect();
            right_sm = drop_columns(right_sm, &drop);
            let mut left_part = if n_parts == 1 {
                keyed_left.clone()
            } else {
                let mut lf = keyed_left.clone();
                let keys = part_seed
                    .mappings
                    .column(&key)
                    .unwrap()
                    .as_materialized_series()
                    .clone();
                lf.mappings = lf
                    .mappings
                    .filter(col(&key).is_in(lit(keys).implode(false), false));
                lf
            };
            // The right side may bind the correlated variables that were unbound.
            let rebound: Vec<String> = unbound
                .iter()
                .filter(|c| right_sm.rdf_node_types.contains_key(c.as_str()))
                .cloned()
                .collect();
            left_part = drop_columns(left_part, &rebound);
            outputs.push(join(
                left_part,
                right_sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?);
        }
        let out = if outputs.is_empty() {
            // No left solutions.
            let mut keyed_left = keyed_left;
            keyed_left.mappings = keyed_left.mappings.limit(0);
            keyed_left
        } else {
            union(outputs, false, self.global_cats.clone())?
        };
        Ok(drop_columns(out, &[key]))
    }

    /// Evaluates the right side of a LATERAL with the given seed.
    fn lazy_lateral_right(
        &self,
        right: &GraphPattern,
        seed: EagerSolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let seed_sm = seed.clone().as_lazy();
        let parameters = with_seed(parameters, Some(seed));
        self.lazy_graph_pattern(
            right,
            Some(seed_sm),
            context,
            parameters.as_ref(),
            Pushdowns::new(),
            query_settings,
            dataset,
        )
    }
}

fn drop_columns(mut sm: SolutionMappings, columns: &[String]) -> SolutionMappings {
    let columns: Vec<_> = columns
        .iter()
        .filter(|c| sm.rdf_node_types.contains_key(c.as_str()))
        .cloned()
        .collect();
    if columns.is_empty() {
        return sm;
    }
    for c in &columns {
        sm.rdf_node_types.remove(c);
    }
    sm.mappings = sm.mappings.drop(by_name(columns, true, false));
    sm
}

/// The distinct bindings of the seed columns in the left solutions, numbered by the key column.
fn make_seed(left: &EagerSolutionMappings, seed_columns: &[String], key: &str) -> EagerSolutionMappings {
    let mut rdf_node_types = HashMap::new();
    for c in seed_columns {
        rdf_node_types.insert(c.clone(), left.rdf_node_types.get(c).unwrap().clone());
    }
    let distinct = query_processing::graph_patterns::distinct(SolutionMappings::new(
        left.mappings
            .clone()
            .lazy()
            .select(seed_columns.iter().map(|c| col(c)).collect::<Vec<_>>()),
        rdf_node_types,
        left.mappings.height(),
    ))
    .unwrap();
    let mut df = distinct.mappings.collect().unwrap();
    let height = df.height();
    let key_column = Series::new(key.into(), (0..height as i64).collect::<Vec<_>>())
        .into_column();
    df.with_column(key_column).unwrap();
    let mut rdf_node_types = distinct.rdf_node_types;
    rdf_node_types.insert(
        key.to_string(),
        BaseRDFNodeType::Literal(xsd::LONG.into_owned(), None).into_default_input_rdf_node_state(),
    );
    EagerSolutionMappings::new(df, rdf_node_types)
}

/// Splits the seed by which of the nullable columns are unbound, dropping those columns from
/// each part.
fn split_seed_by_unbound(
    seed: EagerSolutionMappings,
    nullable: &[String],
) -> Vec<(EagerSolutionMappings, Vec<String>)> {
    let patterns = seed
        .mappings
        .clone()
        .lazy()
        .select(
            nullable
                .iter()
                .map(|c| col(c).is_null().alias(c))
                .collect::<Vec<_>>(),
        )
        .unique(None, UniqueKeepStrategy::Any)
        .collect()
        .unwrap();
    let mut parts = vec![];
    for i in 0..patterns.height() {
        let mut filter = lit(true);
        let mut unbound = vec![];
        for c in nullable {
            let is_null = patterns
                .column(c)
                .unwrap()
                .bool()
                .unwrap()
                .get(i)
                .unwrap();
            if is_null {
                unbound.push(c.clone());
                filter = filter.and(col(c).is_null());
            } else {
                filter = filter.and(col(c).is_not_null());
            }
        }
        let mut lf = seed.mappings.clone().lazy().filter(filter);
        let mut rdf_node_types = seed.rdf_node_types.clone();
        if !unbound.is_empty() {
            lf = lf.drop(by_name(unbound.clone(), true, false));
            for c in &unbound {
                rdf_node_types.remove(c);
            }
        }
        parts.push((
            EagerSolutionMappings::new(lf.collect().unwrap(), rdf_node_types),
            unbound,
        ));
    }
    parts
}

/// All variables used anywhere in the pattern (a superset of those a substitution can affect).
pub(crate) fn variables_in_graph_pattern(gp: &GraphPattern, vars: &mut HashSet<String>) {
    fn add_term(t: &TermPattern, vars: &mut HashSet<String>) {
        if let TermPattern::Variable(v) = t {
            vars.insert(v.as_str().to_string());
        }
    }
    let mut expr_vars = HashSet::new();
    match gp {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                add_term(&p.subject, vars);
                if let NamedNodePattern::Variable(v) = &p.predicate {
                    vars.insert(v.as_str().to_string());
                }
                add_term(&p.object, vars);
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            add_term(subject, vars);
            add_term(object, vars);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            variables_in_graph_pattern(left, vars);
            variables_in_graph_pattern(right, vars);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            variables_in_graph_pattern(left, vars);
            variables_in_graph_pattern(right, vars);
            if let Some(e) = expression {
                find_all_used_variables_in_expression(e, &mut expr_vars, true, true);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            find_all_used_variables_in_expression(expr, &mut expr_vars, true, true);
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Graph { name, inner } => {
            if let NamedNodePattern::Variable(v) = name {
                vars.insert(v.as_str().to_string());
            }
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            vars.insert(variable.as_str().to_string());
            find_all_used_variables_in_expression(expression, &mut expr_vars, true, true);
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Values { variables, .. } | GraphPattern::PValues { variables, .. } => {
            for v in variables {
                vars.insert(v.as_str().to_string());
            }
        }
        GraphPattern::OrderBy { inner, expression } => {
            for e in expression {
                find_all_used_variables_in_order_expression(e, &mut expr_vars, true, true);
            }
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Project { inner, variables } => {
            for v in variables {
                vars.insert(v.as_str().to_string());
            }
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => variables_in_graph_pattern(inner, vars),
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            for v in variables {
                vars.insert(v.as_str().to_string());
            }
            for (v, a) in aggregates {
                vars.insert(v.as_str().to_string());
                find_all_used_variables_in_aggregate_expression(a, &mut expr_vars, true, true);
            }
            variables_in_graph_pattern(inner, vars);
        }
        GraphPattern::Service { name, inner, .. } => {
            if let NamedNodePattern::Variable(v) = name {
                vars.insert(v.as_str().to_string());
            }
            variables_in_graph_pattern(inner, vars);
        }
    }
    for v in expr_vars {
        vars.insert(v.as_str().to_string());
    }
}
