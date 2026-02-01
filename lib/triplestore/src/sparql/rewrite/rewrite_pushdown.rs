use oxrdf::Variable;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::NamedNodePattern;
use std::collections::HashSet;

pub fn rewrite_gp_pushdown(
    q: GraphPattern,
    mut filters: Vec<Expression>,
    seen_vars: HashSet<Variable>,
) -> GraphPattern {
    match q {
        GraphPattern::Bgp { patterns } => {
            let mut outer_filter = vec![];

            if !filters.is_empty() {
                let mut left_patterns = vec![];
                let mut right_patterns = vec![];
                for p in patterns {
                    if matches!(p.predicate, NamedNodePattern::Variable(..)) {
                        right_patterns.push(p);
                    } else {
                        left_patterns.push(p);
                    }
                }
                let mut outer = if !right_patterns.is_empty() {
                    let mut left_gp = GraphPattern::Bgp {
                        patterns: left_patterns,
                    };
                    let left_vars = get_gp_vars(&left_gp);
                    let all_seen_vars: HashSet<_> = left_vars.union(&seen_vars).cloned().collect();
                    while !filters.is_empty() {
                        let candidate = filters.pop().unwrap();
                        let candidate_vars = get_expr_vars(&candidate);
                        if candidate_vars.is_subset(&all_seen_vars) {
                            left_gp = GraphPattern::Filter {
                                inner: Box::new(left_gp),
                                expr: candidate,
                            }
                        } else {
                            outer_filter.push(candidate);
                        }
                    }

                    GraphPattern::Join {
                        left: Box::new(left_gp),
                        right: Box::new(GraphPattern::Bgp {
                            patterns: right_patterns,
                        }),
                    }
                } else {
                    GraphPattern::Bgp {
                        patterns: left_patterns,
                    }
                };
                for f in outer_filter {
                    outer = GraphPattern::Filter {
                        inner: Box::new(outer),
                        expr: f,
                    };
                }
                outer
            } else {
                GraphPattern::Bgp { patterns }
            }
        }
        GraphPattern::Filter { inner, expr } => {
            if contains_variable_predicate(&inner) {
                let exprs = rewrite_expr_pushdown(expr);
                filters.extend(exprs);
                rewrite_gp_pushdown(*inner, filters, seen_vars)
            } else {
                GraphPattern::Filter {
                    inner: Box::new(rewrite_gp_pushdown(*inner, filters, seen_vars)),
                    expr,
                }
            }
        }
        GraphPattern::Join { left, right } => {
            let left_vars = get_gp_vars(&left);
            let right_vars = get_gp_vars(&right);
            let left_and_right_vars: HashSet<_> = left_vars.union(&right_vars).cloned().collect();
            let mut left_filters = vec![];
            let mut right_filters = vec![];
            let mut outer_filters = vec![];
            for f in filters {
                let expr_vars = get_expr_vars(&f);
                if expr_vars.is_subset(&left_vars) {
                    left_filters.push(f);
                } else if expr_vars.is_subset(&left_and_right_vars) {
                    right_filters.push(f);
                } else {
                    outer_filters.push(f);
                }
            }
            let all_seen_vars: HashSet<_> = seen_vars.union(&left_vars).cloned().collect();
            let left = rewrite_gp_pushdown(*left, left_filters, seen_vars);
            let right = rewrite_gp_pushdown(*right, right_filters, all_seen_vars);
            let mut outer = GraphPattern::Join {
                left: Box::new(left),
                right: Box::new(right),
            };
            outer = create_filter_gps(outer, outer_filters);
            outer
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left_vars = get_gp_vars(&left);
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
            let all_seen_vars: HashSet<_> = seen_vars.union(&left_vars).cloned().collect();
            let left = rewrite_gp_pushdown(*left, left_filters, seen_vars);
            let right = rewrite_gp_pushdown(*right, vec![], all_seen_vars);
            let mut outer = GraphPattern::LeftJoin {
                left: Box::new(left),
                right: Box::new(right),
                expression,
            };
            outer = create_filter_gps(outer, outer_filters);
            outer
        }
        GraphPattern::Union { left, right } => {
            let left = rewrite_gp_pushdown(*left, vec![], seen_vars.clone());
            let right = rewrite_gp_pushdown(*right, vec![], seen_vars);
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
            let inner = rewrite_gp_pushdown(*inner, vec![], seen_vars);
            let mut outer = GraphPattern::Extend {
                inner: Box::new(inner),
                variable,
                expression,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Minus { left, right } => {
            let left = rewrite_gp_pushdown(*left, vec![], seen_vars.clone());
            let right = rewrite_gp_pushdown(*right, vec![], seen_vars);
            let mut outer = GraphPattern::Minus {
                left: Box::new(left),
                right: Box::new(right),
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::OrderBy { inner, expression } => {
            let inner = rewrite_gp_pushdown(*inner, filters, seen_vars);
            let outer = GraphPattern::OrderBy {
                inner: Box::new(inner),
                expression,
            };
            outer
        }
        GraphPattern::Project { inner, variables } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![], seen_vars);
            let mut outer = GraphPattern::Project {
                inner: Box::new(inner),
                variables,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        GraphPattern::Distinct { inner } => {
            let inner = rewrite_gp_pushdown(*inner, filters, seen_vars);
            let outer = GraphPattern::Distinct {
                inner: Box::new(inner),
            };
            outer
        }
        GraphPattern::Reduced { inner } => {
            let mut inner = *inner;
            inner = rewrite_gp_pushdown(inner, vec![], seen_vars);
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
            inner = rewrite_gp_pushdown(inner, vec![], seen_vars);
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
            inner = rewrite_gp_pushdown(inner, vec![], seen_vars);
            let mut outer = GraphPattern::Group {
                inner: Box::new(inner),
                variables,
                aggregates,
            };
            outer = create_filter_gps(outer, filters);
            outer
        }
        gp => gp,
    }
}

pub fn rewrite_expr_pushdown(e: Expression) -> Vec<Expression> {
    let mut out_exprs = vec![];
    match e {
        Expression::Exists(gp) => {
            out_exprs.push(Expression::Exists(Box::new(rewrite_gp_pushdown(
                *gp,
                vec![],
                HashSet::new(),
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

fn get_expr_vars(expression: &Expression) -> HashSet<Variable> {
    let mut vars = HashSet::new();
    find_all_used_variables_in_expression(&expression, &mut vars, false, true);
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
