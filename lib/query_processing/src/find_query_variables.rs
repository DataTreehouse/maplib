use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use spargebra::term::TermPattern;
use std::collections::HashSet;

pub fn find_all_used_variables_in_graph_pattern(
    graph_pattern: &GraphPattern,
    used_vars: &mut HashSet<Variable>,
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
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
            if let Some(e) = expression {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
            find_all_used_variables_in_expression(expr, used_vars);
        }
        GraphPattern::Union { left, right } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::Graph { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
            find_all_used_variables_in_expression(expression, used_vars);
        }
        GraphPattern::Minus { left, right } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::OrderBy { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Project { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Distinct { inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Reduced { inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Slice { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Group { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        _ => {}
    }
}

pub fn find_all_used_variables_in_aggregate_expression(
    aggregate_expression: &AggregateExpression,
    used_vars: &mut HashSet<Variable>,
) {
    match aggregate_expression {
        AggregateExpression::CountSolutions { .. } => {
            //No action
        }
        AggregateExpression::FunctionCall { expr, .. } => {
            find_all_used_variables_in_expression(expr, used_vars);
        }
    }
}

pub fn find_all_used_variables_in_expression(
    expression: &Expression,
    used_vars: &mut HashSet<Variable>,
) {
    match expression {
        Expression::Variable(v) => {
            used_vars.insert(v.clone());
        }
        Expression::Or(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::And(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Equal(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::SameTerm(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Greater(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::GreaterOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Less(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::LessOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::In(left, rights) => {
            find_all_used_variables_in_expression(left, used_vars);
            for e in rights {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        Expression::Add(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Subtract(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Multiply(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Divide(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::UnaryPlus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::UnaryMinus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::Not(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::Exists(graph_pattern) => {
            find_all_used_variables_in_graph_pattern(graph_pattern, used_vars);
        }
        Expression::Bound(inner) => {
            used_vars.insert(inner.clone());
        }
        Expression::If(left, middle, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(middle, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Coalesce(inner) => {
            for e in inner {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        Expression::FunctionCall(_, args) => {
            for e in args {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        _ => {}
    }
}
