use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, GraphPattern};

pub fn rewrite_exists_graph_pattern(
    graph_pattern: &GraphPattern,
    helper_column_name: &str,
) -> GraphPattern {
    match graph_pattern {
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_exists_graph_pattern(left, helper_column_name)),
            right: Box::new(rewrite_exists_graph_pattern(right, helper_column_name)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_exists_graph_pattern(left, helper_column_name)),
            right: Box::new(rewrite_exists_graph_pattern(right, helper_column_name)),
            expression: expression.clone(),
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
            expr: expr.clone(),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_exists_graph_pattern(left, helper_column_name)),
            right: Box::new(rewrite_exists_graph_pattern(right, helper_column_name)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name: name.clone(),
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
            variable: variable.clone(),
            expression: expression.clone(),
        },
        GraphPattern::Minus { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_exists_graph_pattern(left, helper_column_name)),
            right: right.clone(),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
            expression: expression.clone(),
        },
        GraphPattern::Project { inner, variables } => {
            let mut new_variables = variables.clone();
            new_variables.push(Variable::new_unchecked(helper_column_name));
            let new_inner = rewrite_exists_graph_pattern(inner, helper_column_name);
            GraphPattern::Project {
                inner: Box::new(new_inner),
                variables: new_variables,
            }
        }
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_exists_graph_pattern(inner, helper_column_name)),
            start: *start,
            length: *length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let mut new_aggregates = aggregates.clone();
            let new_var = Variable::new_unchecked(helper_column_name);
            let new_inner = rewrite_exists_graph_pattern(inner, helper_column_name);
            new_aggregates.push((
                new_var.clone(),
                AggregateExpression::FunctionCall {
                    name: AggregateFunction::Max,
                    expr: Expression::Variable(new_var),
                    distinct: false,
                },
            ));
            GraphPattern::Group {
                inner: Box::new(new_inner),
                variables: variables.clone(),
                aggregates: new_aggregates,
            }
        }
        _ => graph_pattern.clone(),
    }
}
