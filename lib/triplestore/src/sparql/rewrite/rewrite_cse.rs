use oxrdf::{NamedNode, Variable};
use query_processing::constants::DECODE;
use spargebra::algebra::{Expression, Function, GraphPattern};
use std::collections::HashMap;

pub fn rewrite_gp_cse(gp: GraphPattern) -> GraphPattern {
    match gp {
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_gp_cse(*left)),
            right: Box::new(rewrite_gp_cse(*right)),
        },
        GraphPattern::LeftJoin {
            left,
            mut right,
            mut expression,
        } => {
            expression = if let Some(mut expression) = expression {
                let (expr, rename_map) = rewrite_expr_cse(expression);
                expression = rename_vars(expr, &rename_map);
                for (src, trg) in rename_map {
                    right = Box::new(GraphPattern::Extend {
                        inner: right,
                        expression: Expression::FunctionCall(
                            Function::Custom(NamedNode::new_unchecked(DECODE)),
                            vec![Expression::Variable(Variable::new_unchecked(src))],
                        ),
                        variable: Variable::new_unchecked(&trg),
                    });
                }
                Some(expression)
            } else {
                None
            };
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            }
        }
        GraphPattern::Filter { inner, expr } => {
            let mut inner = *inner;
            let (expr, rename_map) = rewrite_expr_cse(expr);
            for (src, trg) in rename_map {
                inner = GraphPattern::Extend {
                    inner: Box::new(inner),
                    expression: Expression::FunctionCall(
                        Function::Custom(NamedNode::new_unchecked(DECODE)),
                        vec![Expression::Variable(Variable::new_unchecked(src))],
                    ),
                    variable: Variable::new_unchecked(&trg),
                }
            }
            GraphPattern::Filter {
                inner: Box::new(inner),
                expr,
            }
        }
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_gp_cse(*left)),
            right: Box::new(rewrite_gp_cse(*right)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_gp_cse(*inner)),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_gp_cse(*inner)),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_gp_cse(*left)),
            right: Box::new(rewrite_gp_cse(*right)),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_gp_cse(*inner)),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_gp_cse(*inner)),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_gp_cse(*inner)),
        },

        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_gp_cse(*inner)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_gp_cse(*inner)),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_gp_cse(*inner)),
            variables,
            aggregates,
        },
        gp => gp,
    }
}

fn rename_vars(e: Expression, rename_map: &HashMap<String, String>) -> Expression {
    match e {
        Expression::Variable(v) => {
            if let Some(r) = rename_map.get(v.as_str()) {
                Expression::Variable(Variable::new_unchecked(r))
            } else {
                Expression::Variable(v)
            }
        }
        Expression::Or(left, right) => Expression::Or(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::And(left, right) => Expression::And(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Equal(left, right) => Expression::Equal(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::SameTerm(left, right) => Expression::SameTerm(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Greater(left, right) => Expression::Greater(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::GreaterOrEqual(left, right) => Expression::GreaterOrEqual(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Less(left, right) => Expression::Less(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::LessOrEqual(left, right) => Expression::LessOrEqual(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::In(left, right) => Expression::In(
            Box::new(rename_vars(*left, rename_map)),
            right
                .into_iter()
                .map(|x| rename_vars(x, rename_map))
                .collect(),
        ),
        Expression::Add(left, right) => Expression::Add(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Subtract(left, right) => Expression::Subtract(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Multiply(left, right) => Expression::Multiply(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Divide(left, right) => Expression::Divide(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::UnaryPlus(inner) => {
            Expression::UnaryPlus(Box::new(rename_vars(*inner, rename_map)))
        }
        Expression::UnaryMinus(inner) => {
            Expression::UnaryMinus(Box::new(rename_vars(*inner, rename_map)))
        }
        Expression::Not(inner) => Expression::Not(Box::new(rename_vars(*inner, rename_map))),
        Expression::Bound(v) => {
            if let Some(r) = rename_map.get(v.as_str()) {
                Expression::Bound(Variable::new_unchecked(r))
            } else {
                Expression::Bound(v)
            }
        }
        Expression::Exists(e) => Expression::Exists(e),
        Expression::If(left, mid, right) => Expression::If(
            Box::new(rename_vars(*left, rename_map)),
            Box::new(rename_vars(*mid, rename_map)),
            Box::new(rename_vars(*right, rename_map)),
        ),
        Expression::Coalesce(exprs) => Expression::Coalesce(
            exprs
                .into_iter()
                .map(|x| rename_vars(x, rename_map))
                .collect(),
        ),
        Expression::FunctionCall(f, exprs) => Expression::FunctionCall(
            f,
            exprs
                .into_iter()
                .map(|x| rename_vars(x, rename_map))
                .collect(),
        ),
        Expression::NamedNode(n) => Expression::NamedNode(n),
        Expression::Literal(l) => Expression::Literal(l),
    }
}

fn rewrite_expr_cse(mut e: Expression) -> (Expression, HashMap<String, String>) {
    let mut counts = HashMap::new();
    get_str_counts(&e, &mut counts);
    let mut renames = HashMap::new();
    for (s, c) in counts {
        if c > 1 {
            renames.insert(s, uuid::Uuid::new_v4().to_string());
        }
    }
    if renames.len() > 0 {
        e = rename_vars(e, &renames);
    }
    (e, renames)
}

fn get_str_counts(e: &Expression, str_counts: &mut HashMap<String, usize>) {
    match e {
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            get_str_counts(left, str_counts);
            get_str_counts(right, str_counts);
        }
        Expression::In(left, right) => {
            get_str_counts(left, str_counts);
            for r in right {
                get_str_counts(r, str_counts);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            get_str_counts(inner, str_counts);
        }
        Expression::If(left, mid, right) => {
            get_str_counts(left, str_counts);
        }
        Expression::Coalesce(inners) => {
            for i in inners {
                get_str_counts(i, str_counts);
            }
        }
        Expression::FunctionCall(f, exprs) => {
            match f {
                Function::Str => {
                    //Todo
                }
                Function::StrLen
                | Function::Concat
                | Function::StrStarts
                | Function::StrEnds
                | Function::UCase
                | Function::LCase
                | Function::StrBefore
                | Function::Contains
                | Function::StrAfter => {
                    add_var_counts(exprs.as_slice(), str_counts);
                }
                Function::SubStr | Function::Replace | Function::Regex => {
                    add_var_counts(&exprs[0..1], str_counts);
                }
                _ => {}
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_)
        | Expression::Exists(_) => {}
    }
}

fn add_var_counts(expr: &[Expression], str_counts: &mut HashMap<String, usize>) {
    for e in expr {
        if let Expression::Variable(v) = e {
            let e = str_counts.entry(v.as_str().to_string());
            e.and_modify(|x| *x += 1).or_insert(1);
        }
    }
}
