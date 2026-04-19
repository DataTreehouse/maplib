use oxrdf::Variable;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

pub fn terminating_lift_extend(gp: GraphPattern) -> GraphPattern {
    let (mut gp, extends) = rewrite_lift_extend(gp);
    for (expression, variable) in extends {
        gp = GraphPattern::Extend {
            inner: Box::new(gp),
            expression,
            variable,
        }
    }
    gp
}

pub fn rewrite_lift_extend(gp: GraphPattern) -> (GraphPattern, Vec<(Expression, Variable)>) {
    match gp {
        GraphPattern::Bgp { patterns } => (GraphPattern::Bgp { patterns }, Vec::new()),
        GraphPattern::Path {
            path,
            subject,
            object,
        } => (
            GraphPattern::Path {
                path,
                subject,
                object,
            },
            Vec::new(),
        ),
        GraphPattern::Join { left, right } => {
            let (left, mut left_extends) = rewrite_lift_extend(*left);
            let (right, right_extends) = rewrite_lift_extend(*right);
            left_extends.extend(right_extends);
            (
                GraphPattern::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                },
                left_extends,
            )
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let (left, mut left_extends) = rewrite_lift_extend(*left);
            let (right, right_extends) = rewrite_lift_extend(*right);
            left_extends.extend(right_extends);
            (
                GraphPattern::LeftJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    expression,
                },
                left_extends,
            )
        }
        GraphPattern::Filter { inner, expr } => {
            let (mut inner, extends) = rewrite_lift_extend(*inner);
            let mut expr_vars = HashSet::new();
            find_all_used_variables_in_expression(&expr, &mut expr_vars, true, true);
            let mut keep_extends = Vec::new();
            for (expression, variable) in extends {
                if expr_vars.contains(&variable) {
                    inner = GraphPattern::Extend {
                        inner: Box::new(inner),
                        expression,
                        variable,
                    }
                } else {
                    keep_extends.push((expression, variable));
                }
            }
            (
                GraphPattern::Filter {
                    inner: Box::new(inner),
                    expr,
                },
                keep_extends,
            )
        }
        GraphPattern::Union { left, right } => (
            GraphPattern::Union {
                left: Box::new(terminating_lift_extend(*left)),
                right: Box::new(terminating_lift_extend(*right)),
            },
            Vec::new(),
        ),
        GraphPattern::Graph { name, inner } => {
            let (inner, extends) = rewrite_lift_extend(*inner);
            (
                GraphPattern::Graph {
                    name,
                    inner: Box::new(inner),
                },
                extends,
            )
        }
        GraphPattern::Extend {
            inner,
            expression,
            variable,
        } => {
            let (inner, mut extends) = rewrite_lift_extend(*inner);
            extends.push((expression, variable));
            (inner, extends)
        }
        GraphPattern::Minus { left, right } => (
            GraphPattern::Minus {
                left: Box::new(terminating_lift_extend(*left)),
                right: Box::new(terminating_lift_extend(*right)),
            },
            Vec::new(),
        ),
        GraphPattern::Values {
            variables,
            bindings,
        } => (
            GraphPattern::Values {
                variables,
                bindings,
            },
            Vec::new(),
        ),
        GraphPattern::PValues {
            variables,
            bindings_parameter,
        } => (
            GraphPattern::PValues {
                variables,
                bindings_parameter,
            },
            Vec::new(),
        ),
        GraphPattern::OrderBy { inner, expression } => {
            let mut inner = terminating_lift_extend(*inner);
            inner = GraphPattern::OrderBy {
                inner: Box::new(inner),
                expression,
            };
            (inner, Vec::new())
        }
        GraphPattern::Project { inner, variables } => {
            let mut inner = terminating_lift_extend(*inner);
            inner = GraphPattern::Project {
                inner: Box::new(inner),
                variables,
            };
            (inner, Vec::new())
        }
        GraphPattern::Distinct { inner } => (
            GraphPattern::Distinct {
                inner: Box::new(terminating_lift_extend(*inner)),
            },
            Vec::new(),
        ),
        GraphPattern::Reduced { inner } => (
            GraphPattern::Reduced {
                inner: Box::new(terminating_lift_extend(*inner)),
            },
            Vec::new(),
        ),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => (
            GraphPattern::Slice {
                inner: Box::new(terminating_lift_extend(*inner)),
                start,
                length,
            },
            Vec::new(),
        ),
        GraphPattern::Group {
            inner,
            aggregates,
            variables,
        } => (
            GraphPattern::Group {
                inner: Box::new(terminating_lift_extend(*inner)),
                aggregates,
                variables,
            },
            Vec::new(),
        ),
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => (
            GraphPattern::Service {
                name,
                inner,
                silent,
            },
            Vec::new(),
        ),
    }
}
