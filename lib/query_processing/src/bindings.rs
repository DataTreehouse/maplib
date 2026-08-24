use oxrdf::vocab::xsd;
use oxrdf::Literal;
use representation::errors::RepresentationError;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

pub fn maybe_replace_bindings(
    query: Query,
    bindings: Option<&HashMap<String, GroundTerm>>,
) -> Result<Query, RepresentationError> {
    if let Some(bindings) = bindings {
        match query {
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => Ok(Query::Select {
                dataset,
                pattern: replace_bindings_graph_pattern(pattern, bindings)?,
                base_iri,
            }),
            Query::Construct {
                template,
                dataset,
                pattern,
                base_iri,
            } => Ok(Query::Construct {
                template,
                dataset,
                pattern: replace_bindings_graph_pattern(pattern, bindings)?,
                base_iri,
            }),
            Query::Describe {
                dataset,
                pattern,
                base_iri,
            } => Ok(Query::Describe {
                dataset,
                pattern: replace_bindings_graph_pattern(pattern, bindings)?,
                base_iri,
            }),
            Query::Ask {
                dataset,
                pattern,
                base_iri,
            } => Ok(Query::Ask {
                dataset,
                pattern: replace_bindings_graph_pattern(pattern, bindings)?,
                base_iri,
            }),
        }
    } else {
        Ok(query)
    }
}

pub fn replace_bindings_graph_pattern(
    gp: GraphPattern,
    bindings: &HashMap<String, GroundTerm>,
) -> Result<GraphPattern, RepresentationError> {
    match gp {
        GraphPattern::Bgp { patterns } => {
            let mut rep_patterns = Vec::with_capacity(patterns.len());
            for p in patterns {
                let TriplePattern {
                    subject,
                    predicate,
                    object,
                } = p;
                let subject = maybe_replace_term_pattern(subject, bindings);
                let object = maybe_replace_term_pattern(object, bindings);
                let predicate = maybe_replace_named_node_pattern(predicate, bindings)?;
                rep_patterns.push(TriplePattern {
                    subject,
                    predicate,
                    object,
                });
            }

            Ok(GraphPattern::Bgp {
                patterns: rep_patterns,
            })
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            let subject = maybe_replace_term_pattern(subject, bindings);
            let object = maybe_replace_term_pattern(object, bindings);
            Ok(GraphPattern::Path {
                subject,
                path,
                object,
            })
        }
        GraphPattern::Join { left, right } => Ok(GraphPattern::Join {
            left: Box::new(replace_bindings_graph_pattern(*left, bindings)?),
            right: Box::new(replace_bindings_graph_pattern(*right, bindings)?),
        }),
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let expression = if let Some(expression) = expression {
                Some(replace_bindings_expression(expression, bindings)?)
            } else {
                None
            };
            Ok(GraphPattern::LeftJoin {
                left: Box::new(replace_bindings_graph_pattern(*left, bindings)?),
                right: Box::new(replace_bindings_graph_pattern(*right, bindings)?),
                expression,
            })
        }
        GraphPattern::Filter { expr, inner } => Ok(GraphPattern::Filter {
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
            expr: replace_bindings_expression(expr, bindings)?,
        }),
        GraphPattern::Union { left, right } => Ok(GraphPattern::Union {
            left: Box::new(replace_bindings_graph_pattern(*left, bindings)?),
            right: Box::new(replace_bindings_graph_pattern(*right, bindings)?),
        }),
        GraphPattern::Graph { name, inner } => Ok(GraphPattern::Graph {
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
            name: maybe_replace_named_node_pattern(name, bindings)?,
        }),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            if bindings.contains_key(variable.as_str()) {
                return Err(RepresentationError::DatatypeError(
                    format!("Cannot replace binding of variable {variable} with a ground term in the bind position")
                ));
            }
            Ok(GraphPattern::Extend {
                inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
                variable,
                expression: replace_bindings_expression(expression, bindings)?,
            })
        }
        GraphPattern::Minus { left, right } => Ok(GraphPattern::Minus {
            left: Box::new(replace_bindings_graph_pattern(*left, bindings)?),
            right: Box::new(replace_bindings_graph_pattern(*right, bindings)?),
        }),
        GraphPattern::Values {
            variables,
            bindings: values_bindings,
        } => {
            for v in &variables {
                if bindings.contains_key(v.as_str()) {
                    return Err(RepresentationError::DatatypeError(format!(
                        "Cannot replace binding of variable {v} with a ground term in Values"
                    )));
                }
            }
            Ok(GraphPattern::Values {
                variables,
                bindings: values_bindings,
            })
        }
        GraphPattern::PValues {
            variables,
            bindings_parameter,
        } => {
            for v in &variables {
                if bindings.contains_key(v.as_str()) {
                    return Err(RepresentationError::DatatypeError(format!(
                        "Cannot replace binding of variable {v} with a ground term in PValues"
                    )));
                }
            }
            Ok(GraphPattern::PValues {
                variables,
                bindings_parameter,
            })
        }
        GraphPattern::OrderBy { inner, expression } => {
            let mut b_ord = Vec::with_capacity(expression.len());
            for e in expression {
                b_ord.push(replace_bindings_order(e, bindings)?);
            }
            Ok(GraphPattern::OrderBy {
                inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
                expression: b_ord,
            })
        }
        GraphPattern::Project { inner, variables } => {
            for v in &variables {
                if bindings.contains_key(v.as_str()) {
                    return Err(RepresentationError::DatatypeError(format!(
                        "Cannot replace binding of variable {v} with a ground term in Project"
                    )));
                }
            }
            Ok(GraphPattern::Project {
                variables,
                inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
            })
        }
        GraphPattern::Distinct { inner } => Ok(GraphPattern::Distinct {
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
        }),
        GraphPattern::Reduced { inner } => Ok(GraphPattern::Reduced {
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
        }),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => Ok(GraphPattern::Slice {
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
            start,
            length,
        }),
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let mut b_aggs = Vec::with_capacity(aggregates.len());
            for (v, agg) in aggregates {
                if bindings.contains_key(v.as_str()) {
                    return Err(RepresentationError::DatatypeError(format!(
                        "Cannot replace binding of variable {v} with a ground term in Group By"
                    )));
                }
                b_aggs.push((v, replace_bindings_agg(agg, bindings)?));
            }
            Ok(GraphPattern::Group {
                inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
                variables,
                aggregates: b_aggs,
            })
        }
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => Ok(GraphPattern::Service {
            name: maybe_replace_named_node_pattern(name, bindings)?,
            inner: Box::new(replace_bindings_graph_pattern(*inner, bindings)?),
            silent,
        }),
    }
}

pub fn replace_bindings_expression(
    expression: Expression,
    bindings: &HashMap<String, GroundTerm>,
) -> Result<Expression, RepresentationError> {
    match expression {
        Expression::NamedNode(n) => Ok(Expression::NamedNode(n)),
        Expression::Literal(l) => Ok(Expression::Literal(l)),
        Expression::Variable(v) => {
            if let Some(t) = bindings.get(v.as_str()) {
                match t {
                    GroundTerm::NamedNode(n) => Ok(Expression::NamedNode(n.clone())),
                    GroundTerm::Literal(l) => Ok(Expression::Literal(l.clone())),
                }
            } else {
                Ok(Expression::Variable(v))
            }
        }
        Expression::Or(a, b) => Ok(Expression::Or(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::And(a, b) => Ok(Expression::And(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Equal(a, b) => Ok(Expression::Equal(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::SameTerm(a, b) => Ok(Expression::SameTerm(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Greater(a, b) => Ok(Expression::Greater(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::GreaterOrEqual(a, b) => Ok(Expression::GreaterOrEqual(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Less(a, b) => Ok(Expression::Less(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::LessOrEqual(a, b) => Ok(Expression::LessOrEqual(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::In(a, b) => {
            let rep_a = Box::new(replace_bindings_expression(*a, bindings)?);
            let rep_b: Result<Vec<_>, RepresentationError> = b
                .into_iter()
                .map(|x| replace_bindings_expression(x, bindings))
                .collect();
            Ok(Expression::In(rep_a, rep_b?))
        }
        Expression::Add(a, b) => Ok(Expression::Add(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Subtract(a, b) => Ok(Expression::Subtract(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Multiply(a, b) => Ok(Expression::Multiply(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::Divide(a, b) => Ok(Expression::Divide(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
        )),
        Expression::UnaryPlus(a) => Ok(Expression::UnaryPlus(Box::new(
            replace_bindings_expression(*a, bindings)?,
        ))),
        Expression::UnaryMinus(a) => Ok(Expression::UnaryMinus(Box::new(
            replace_bindings_expression(*a, bindings)?,
        ))),
        Expression::Not(a) => Ok(Expression::Not(Box::new(replace_bindings_expression(
            *a, bindings,
        )?))),
        Expression::Exists(gp) => Ok(Expression::Exists(Box::new(
            replace_bindings_graph_pattern(*gp, bindings)?,
        ))),
        Expression::Bound(v) => {
            if bindings.contains_key(v.as_str()) {
                Ok(Expression::Literal(Literal::new_typed_literal(
                    "true",
                    xsd::BOOLEAN.into_owned(),
                )))
            } else {
                Ok(Expression::Bound(v))
            }
        }
        Expression::If(a, b, c) => Ok(Expression::If(
            Box::new(replace_bindings_expression(*a, bindings)?),
            Box::new(replace_bindings_expression(*b, bindings)?),
            Box::new(replace_bindings_expression(*c, bindings)?),
        )),
        Expression::Coalesce(a) => {
            let rep_a: Result<Vec<_>, RepresentationError> = a
                .into_iter()
                .map(|x| replace_bindings_expression(x, bindings))
                .collect();
            Ok(Expression::Coalesce(rep_a?))
        }
        Expression::FunctionCall(func, b) => {
            let rep_b: Result<Vec<_>, RepresentationError> = b
                .into_iter()
                .map(|x| replace_bindings_expression(x, bindings))
                .collect();
            Ok(Expression::FunctionCall(func, rep_b?))
        }
    }
}

pub fn replace_bindings_agg(
    agg: AggregateExpression,
    bindings: &HashMap<String, GroundTerm>,
) -> Result<AggregateExpression, RepresentationError> {
    match agg {
        AggregateExpression::CountSolutions { distinct } => {
            Ok(AggregateExpression::CountSolutions { distinct })
        }
        AggregateExpression::FunctionCall {
            name,
            expr,
            distinct,
        } => Ok(AggregateExpression::FunctionCall {
            name,
            expr: replace_bindings_expression(expr, bindings)?,
            distinct,
        }),
    }
}

pub fn replace_bindings_order(
    order: OrderExpression,
    bindings: &HashMap<String, GroundTerm>,
) -> Result<OrderExpression, RepresentationError> {
    match order {
        OrderExpression::Asc(e) => Ok(OrderExpression::Asc(replace_bindings_expression(
            e, bindings,
        )?)),
        OrderExpression::Desc(e) => Ok(OrderExpression::Desc(replace_bindings_expression(
            e, bindings,
        )?)),
    }
}

fn maybe_replace_term_pattern(
    term_pattern: TermPattern,
    bindings: &HashMap<String, GroundTerm>,
) -> TermPattern {
    if let TermPattern::Variable(v) = term_pattern {
        if let Some(t) = bindings.get(v.as_str()) {
            match t {
                GroundTerm::NamedNode(n) => TermPattern::NamedNode(n.clone()),
                GroundTerm::Literal(l) => TermPattern::Literal(l.clone()),
            }
        } else {
            TermPattern::Variable(v)
        }
    } else {
        term_pattern
    }
}

fn maybe_replace_named_node_pattern(
    named_node_pattern: NamedNodePattern,
    bindings: &HashMap<String, GroundTerm>,
) -> Result<NamedNodePattern, RepresentationError> {
    if let NamedNodePattern::Variable(v) = named_node_pattern {
        if let Some(t) = bindings.get(v.as_str()) {
            match t {
                GroundTerm::NamedNode(n) => Ok(NamedNodePattern::NamedNode(n.clone())),
                GroundTerm::Literal(_) => Err(RepresentationError::DatatypeError(format!(
                    "Cannot replace binding of variable {v} with a literal when an IRI is expected"
                ))),
            }
        } else {
            Ok(NamedNodePattern::Variable(v))
        }
    } else {
        Ok(named_node_pattern)
    }
}
