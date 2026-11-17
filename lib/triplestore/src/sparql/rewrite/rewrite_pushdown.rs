use crate::sparql::lazy_graph_patterns::join::certainly_bound_variables;
use oxrdf::Variable;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::NamedNodePattern;
use spargebra::window::contains_window_function;
use std::collections::HashSet;

/// Moves the conjuncts of filters above patterns with a variable predicate down towards the
/// patterns that bind their variables.
///
/// A filter is moved into a pattern only when that pattern binds every variable of the filter
/// in all of its solutions. A sibling in a join is not enough: the patterns of a join are
/// reordered when it is evaluated, so a filter moved next to a sibling's variables could be
/// evaluated before they are bound. Every filter that reaches a pattern it cannot be moved into
/// is kept above it.
pub fn rewrite_gp_pushdown(q: GraphPattern, mut filters: Vec<Expression>) -> GraphPattern {
    match q {
        GraphPattern::Bgp { patterns } => {
            let n_variable_predicates = patterns
                .iter()
                .filter(|p| matches!(p.predicate, NamedNodePattern::Variable(..)))
                .count();
            if filters.is_empty()
                || n_variable_predicates == 0
                || n_variable_predicates == patterns.len()
            {
                return create_filter_gps(GraphPattern::Bgp { patterns }, filters);
            }
            let mut left_patterns = vec![];
            let mut right_patterns = vec![];
            for p in patterns {
                if matches!(p.predicate, NamedNodePattern::Variable(..)) {
                    right_patterns.push(p);
                } else {
                    left_patterns.push(p);
                }
            }
            let mut left_gp = GraphPattern::Bgp {
                patterns: left_patterns,
            };
            let left_vars = get_gp_vars(&left_gp);
            let mut outer_filters = vec![];
            for f in filters {
                if get_expr_vars(&f).is_subset(&left_vars) {
                    left_gp = GraphPattern::Filter {
                        inner: Box::new(left_gp),
                        expr: f,
                    }
                } else {
                    outer_filters.push(f);
                }
            }
            let outer = GraphPattern::Join {
                left: Box::new(left_gp),
                right: Box::new(GraphPattern::Bgp {
                    patterns: right_patterns,
                }),
            };
            create_filter_gps(outer, outer_filters)
        }
        GraphPattern::Filter { inner, expr } if contains_window_function(&expr) => {
            // A window function depends on all solutions of the inner pattern, so no filters
            // are moved below or out of it.
            let outer = GraphPattern::Filter {
                inner: Box::new(rewrite_gp_pushdown(*inner, vec![])),
                expr,
            };
            create_filter_gps(outer, filters)
        }
        GraphPattern::Filter { inner, expr } => {
            if contains_variable_predicate(&inner) {
                let exprs = rewrite_expr_pushdown(expr);
                filters.extend(exprs);
                rewrite_gp_pushdown(*inner, filters)
            } else {
                GraphPattern::Filter {
                    inner: Box::new(rewrite_gp_pushdown(*inner, filters)),
                    expr,
                }
            }
        }
        GraphPattern::Join { left, right } => {
            let left_vars = get_certain_vars(&left);
            let right_vars = get_certain_vars(&right);
            let mut left_filters = vec![];
            let mut right_filters = vec![];
            let mut outer_filters = vec![];
            for f in filters {
                let expr_vars = get_expr_vars(&f);
                if expr_vars.is_subset(&left_vars) {
                    left_filters.push(f);
                } else if expr_vars.is_subset(&right_vars) {
                    right_filters.push(f);
                } else {
                    outer_filters.push(f);
                }
            }
            let left = rewrite_gp_pushdown(*left, left_filters);
            let right = rewrite_gp_pushdown(*right, right_filters);
            let outer = GraphPattern::Join {
                left: Box::new(left),
                right: Box::new(right),
            };
            create_filter_gps(outer, outer_filters)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left_vars = get_certain_vars(&left);
            let mut left_filters = vec![];
            let mut outer_filters = vec![];
            for f in filters {
                let expr_vars = get_expr_vars(&f);
                if expr_vars.is_subset(&left_vars) {
                    left_filters.push(f);
                } else {
                    outer_filters.push(f);
                }
            }
            let left = rewrite_gp_pushdown(*left, left_filters);
            let right = rewrite_gp_pushdown(*right, vec![]);
            let mut outer = GraphPattern::LeftJoin {
                left: Box::new(left),
                right: Box::new(right),
                expression,
            };
            outer = create_filter_gps(outer, outer_filters);
            outer
        }
        GraphPattern::Lateral { left, right } => {
            // Filters on the left variables can be applied before the right side is evaluated.
            let left_vars = get_certain_vars(&left);
            let mut left_filters = vec![];
            let mut outer_filters = vec![];
            for f in filters {
                let expr_vars = get_expr_vars(&f);
                if expr_vars.is_subset(&left_vars) {
                    left_filters.push(f);
                } else {
                    outer_filters.push(f);
                }
            }
            let left = rewrite_gp_pushdown(*left, left_filters);
            let right = rewrite_gp_pushdown(*right, vec![]);
            let mut outer = GraphPattern::Lateral {
                left: Box::new(left),
                right: Box::new(right),
            };
            outer = create_filter_gps(outer, outer_filters);
            outer
        }
        GraphPattern::Union { left, right } => {
            let left = rewrite_gp_pushdown(*left, vec![]);
            let right = rewrite_gp_pushdown(*right, vec![]);
            let mut outer = GraphPattern::Union {
                left: Box::new(left),
                right: Box::new(right),
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Extend {
            expression,
            variable,
            inner,
        } => {
            let inner = rewrite_gp_pushdown(*inner, vec![]);
            let mut outer = GraphPattern::Extend {
                inner: Box::new(inner),
                variable,
                expression,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Minus { left, right } => {
            let left = rewrite_gp_pushdown(*left, vec![]);
            let right = rewrite_gp_pushdown(*right, vec![]);
            let mut outer = GraphPattern::Minus {
                left: Box::new(left),
                right: Box::new(right),
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::OrderBy { inner, expression } => {
            let inner = rewrite_gp_pushdown(*inner, filters);
            GraphPattern::OrderBy {
                inner: Box::new(inner),
                expression,
            }
        }
        GraphPattern::Project { inner, variables } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![]);
            let mut outer = GraphPattern::Project {
                inner: Box::new(inner),
                variables,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Distinct { inner } => {
            let inner = rewrite_gp_pushdown(*inner, filters);
            GraphPattern::Distinct {
                inner: Box::new(inner),
            }
        }
        GraphPattern::Reduced { inner } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![]);
            let mut outer = GraphPattern::Reduced {
                inner: Box::new(inner),
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![]);
            let mut outer = GraphPattern::Slice {
                inner: Box::new(inner),
                start,
                length,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![]);
            let mut outer = GraphPattern::Group {
                inner: Box::new(inner),
                variables,
                aggregates,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        // VALUES, property paths, GRAPH, SERVICE and the other patterns the filters are not moved
        // into keep them above.
        gp => create_filter_gps(gp, filters),
    }
}

pub fn rewrite_expr_pushdown(e: Expression) -> Vec<Expression> {
    let mut out_exprs = vec![];
    match e {
        Expression::Exists(gp) => {
            out_exprs.push(Expression::Exists(Box::new(rewrite_gp_pushdown(
                *gp,
                vec![],
            ))));
        }
        Expression::And(left, right) => {
            out_exprs.extend(rewrite_expr_pushdown(*left));
            out_exprs.extend(rewrite_expr_pushdown(*right));
        }
        e => out_exprs.push(e),
    }
    out_exprs
}

fn create_filter_gps(mut gp: GraphPattern, mut filters: Vec<Expression>) -> GraphPattern {
    while !filters.is_empty() {
        gp = GraphPattern::Filter {
            inner: Box::new(gp),
            expr: filters.pop().unwrap(),
        }
    }
    gp
}

fn get_gp_vars(graph_pattern: &GraphPattern) -> HashSet<Variable> {
    let mut vars = HashSet::new();
    graph_pattern.on_in_scope_variable(|x| {
        vars.insert(x.clone());
    });
    vars
}

/// The variables bound in every solution of the pattern.
fn get_certain_vars(graph_pattern: &GraphPattern) -> HashSet<Variable> {
    certainly_bound_variables(graph_pattern)
        .into_iter()
        .map(Variable::new_unchecked)
        .collect()
}

fn get_expr_vars(expression: &Expression) -> HashSet<Variable> {
    let mut vars = HashSet::new();
    // Variables inside EXISTS / NOT EXISTS count: the filter is correlated with them, so it must
    // stay above the patterns that bind them. Without them a NOT EXISTS looked variable-free
    // and was pushed below the pattern binding its variables, where it filtered nothing.
    find_all_used_variables_in_expression(expression, &mut vars, true, true);
    vars
}

fn contains_variable_predicate(graph_pattern: &GraphPattern) -> bool {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                if matches!(p.predicate, NamedNodePattern::Variable(..)) {
                    return true;
                }
            }
            false
        }
        GraphPattern::Path { .. }
        | GraphPattern::Values { .. }
        | GraphPattern::Group { .. }
        | GraphPattern::Service { .. }
        | GraphPattern::Slice { .. }
        | GraphPattern::Reduced { .. }
        | GraphPattern::PValues { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right } => {
            contains_variable_predicate(left) || contains_variable_predicate(right)
        }
        GraphPattern::Graph { .. } => false, //todo
        GraphPattern::Minus { left: inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Filter { inner, .. } => contains_variable_predicate(inner),
        GraphPattern::Project { .. } => false, //todo
    }
}
