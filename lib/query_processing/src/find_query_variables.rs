use oxrdf::Variable;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::TermPattern;
use std::collections::HashSet;

pub fn find_all_used_variables_in_graph_pattern(
    graph_pattern: &GraphPattern,
    used_vars: &mut HashSet<Variable>,
    include_exists: bool,
    include_bound: bool,
) {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                if let TermPattern::Variable(v) = &p.subject {
                    used_vars.insert(v.clone());
                }
                if let TermPattern::Variable(v) = &p.object {
                    used_vars.insert(v.clone());
                }
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            if let TermPattern::Variable(v) = subject {
                used_vars.insert(v.clone());
            }
            if let TermPattern::Variable(v) = object {
                used_vars.insert(v.clone());
            }
        }
        GraphPattern::Join { left, right } => {
            find_all_used_variables_in_graph_pattern(
                left,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_graph_pattern(
                right,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            find_all_used_variables_in_graph_pattern(
                left,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_graph_pattern(
                right,
                used_vars,
                include_exists,
                include_bound,
            );
            if let Some(e) = expression {
                find_all_used_variables_in_expression(e, used_vars, include_exists, include_bound);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_expression(expr, used_vars, include_exists, include_bound);
        }
        GraphPattern::Union { left, right } => {
            find_all_used_variables_in_graph_pattern(
                left,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_graph_pattern(
                right,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Graph { inner, .. } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_expression(
                expression,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Minus { left, right } => {
            find_all_used_variables_in_graph_pattern(
                left,
                used_vars,
                include_exists,
                include_bound,
            );
            find_all_used_variables_in_graph_pattern(
                right,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::OrderBy { inner, .. } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Project { inner, .. } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Distinct { inner } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Reduced { inner } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Slice { inner, .. } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        GraphPattern::Group { inner, .. } => {
            find_all_used_variables_in_graph_pattern(
                inner,
                used_vars,
                include_exists,
                include_bound,
            );
        }
        _ => {}
    }
}

pub fn find_all_used_variables_in_aggregate_expression(
    aggregate_expression: &AggregateExpression,
    used_vars: &mut HashSet<Variable>,
    include_exists: bool,
    include_bound: bool,
) {
    match aggregate_expression {
        AggregateExpression::CountSolutions { .. } => {
            //No action
        }
        AggregateExpression::FunctionCall { expr, .. } => {
            find_all_used_variables_in_expression(expr, used_vars, include_exists, include_bound);
        }
    }
}

pub fn find_all_used_variables_in_order_expression(
    order_expression: &OrderExpression,
    used_vars: &mut HashSet<Variable>,
    include_exists: bool,
    include_bound: bool,
) {
    match order_expression {
        OrderExpression::Asc(e) | OrderExpression::Desc(e) => {
            find_all_used_variables_in_expression(e, used_vars, include_exists, include_bound);
        }
    }
}

pub fn find_all_used_variables_in_expression(
    expression: &Expression,
    used_vars: &mut HashSet<Variable>,
    include_exists: bool,
    include_bound: bool,
) {
    match expression {
        Expression::Variable(v) => {
            used_vars.insert(v.clone());
        }
        Expression::Or(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::And(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Equal(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::SameTerm(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Greater(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::GreaterOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Less(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::LessOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::In(left, rights) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            for e in rights {
                find_all_used_variables_in_expression(e, used_vars, include_exists, include_bound);
            }
        }
        Expression::Add(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Subtract(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Multiply(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Divide(left, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::UnaryPlus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars, include_exists, include_bound);
        }
        Expression::UnaryMinus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars, include_exists, include_bound);
        }
        Expression::Not(inner) => {
            find_all_used_variables_in_expression(inner, used_vars, include_exists, include_bound);
        }
        Expression::Exists(graph_pattern) => {
            if include_exists {
                find_all_used_variables_in_graph_pattern(
                    graph_pattern,
                    used_vars,
                    include_exists,
                    include_bound,
                );
            }
        }
        Expression::Bound(inner) => {
            if include_bound {
                used_vars.insert(inner.clone());
            }
        }
        Expression::If(left, middle, right) => {
            find_all_used_variables_in_expression(left, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(middle, used_vars, include_exists, include_bound);
            find_all_used_variables_in_expression(right, used_vars, include_exists, include_bound);
        }
        Expression::Coalesce(inner) => {
            for e in inner {
                find_all_used_variables_in_expression(e, used_vars, include_exists, include_bound);
            }
        }
        Expression::FunctionCall(_, args) => {
            for e in args {
                find_all_used_variables_in_expression(e, used_vars, include_exists, include_bound);
            }
        }
        _ => {}
    }
}

pub fn solution_mappings_has_all_expression_variables(
    solution_mappings: &SolutionMappings,
    expression: &Expression,
) -> bool {
    let mut used_vars = HashSet::new();
    find_all_used_variables_in_expression(expression, &mut used_vars, false, false);
    for v in used_vars {
        if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
            return false;
        }
    }
    true
}

pub fn solution_mappings_has_all_aggregate_expression_variables(
    solution_mappings: &SolutionMappings,
    aggregate_expression: &AggregateExpression,
) -> bool {
    let mut used_vars = HashSet::new();
    find_all_used_variables_in_aggregate_expression(
        aggregate_expression,
        &mut used_vars,
        false,
        false,
    );
    for v in used_vars {
        if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
            return false;
        }
    }
    true
}

pub fn solution_mappings_has_all_order_expression_variables(
    solution_mappings: &SolutionMappings,
    order_expression: &OrderExpression,
) -> bool {
    let mut used_vars = HashSet::new();
    find_all_used_variables_in_order_expression(order_expression, &mut used_vars, false, false);
    for v in used_vars {
        if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
            return false;
        }
    }
    true
}
