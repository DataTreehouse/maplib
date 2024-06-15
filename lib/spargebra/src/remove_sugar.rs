pub const HAS_TIMESTAMP: &str = "https://github.com/DataTreehouse/chrontext#hasTimestamp";
pub const HAS_TIMESERIES: &str = "https://github.com/DataTreehouse/chrontext#hasTimeseries";
pub const HAS_DATA_POINT: &str = "https://github.com/DataTreehouse/chrontext#hasDataPoint";
pub const HAS_VALUE: &str = "https://github.com/DataTreehouse/chrontext#hasValue";

const VALUE_SUFFIX: &str = "value";
const DATA_POINT_SUFFIX: &str = "datapoint";
const TIMESTAMP_VARIABLE_NAME: &str = "timestamp";

pub const FLOOR_DATETIME_TO_SECONDS_INTERVAL: &str =
    "https://github.com/DataTreehouse/chrontext#FloorDateTimeToSecondsInterval";

use crate::algebra::{
    AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use crate::query_context::{Context, PathEntry};
use crate::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use crate::treehouse::{
    DataTreehousePattern, SimpleTimestampExpression, TimestampBinaryOperator, TimestampExpression,
};
use crate::Query;
use chrono::{DateTime, Utc};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode};

pub struct SyntacticSugarRemover {}

impl SyntacticSugarRemover {
    pub fn new() -> SyntacticSugarRemover {
        SyntacticSugarRemover {}
    }

    pub fn remove_sugar(&self, query: Query) -> Query {
        match query {
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => {
                let gp = self.remove_sugar_from_graph_pattern(pattern, vec![], &Context::new());
                let new_query = Query::Select {
                    dataset,
                    pattern: gp.gp.unwrap(),
                    base_iri,
                };
                new_query
            }
            q => q,
        }
    }

    fn remove_sugar_from_graph_pattern(
        &self,
        graph_pattern: GraphPattern,
        mut ts_tps_in_scope: Vec<TermPattern>,
        context: &Context,
    ) -> RemoveSugarGraphPatternReturn {
        match graph_pattern {
            GraphPattern::Bgp { .. } | GraphPattern::Path { .. } => {
                RemoveSugarGraphPatternReturn::from_pattern(graph_pattern)
            }
            GraphPattern::Join { left, right } => {
                if contains_sugar(&left) && !contains_sugar(&right) {
                    ts_tps_in_scope.extend(find_ts_term_pattens_in_scope(&right))
                }
                if contains_sugar(&right) && !contains_sugar(&left) {
                    ts_tps_in_scope.extend(find_ts_term_pattens_in_scope(&left))
                }

                let left_context = context.extension_with(PathEntry::JoinLeftSide);
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    ts_tps_in_scope.clone(),
                    &left_context,
                );
                let right_context = context.extension_with(PathEntry::JoinRightSide);
                let mut right =
                    self.remove_sugar_from_graph_pattern(*right, ts_tps_in_scope, &right_context);
                let mut left_gp = left.gp.take().unwrap();
                let mut right_gp = right.gp.take().unwrap();

                if left.has_sugar_context(&left_context) {
                    left_gp = add_filter_and_bindings(
                        left_gp,
                        left.filter_expr.take(),
                        left.binding.take(),
                    );
                }

                if right.has_sugar_context(&right_context) {
                    right_gp = add_filter_and_bindings(
                        right_gp,
                        right.filter_expr.take(),
                        right.binding.take(),
                    );
                }

                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Join {
                    left: Box::new(left_gp),
                    right: Box::new(right_gp),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                if contains_sugar(&left) && !contains_sugar(&right) {
                    ts_tps_in_scope.extend(find_ts_term_pattens_in_scope(&right))
                }
                if contains_sugar(&right) && !contains_sugar(&left) {
                    ts_tps_in_scope.extend(find_ts_term_pattens_in_scope(&left))
                }
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    ts_tps_in_scope.clone(),
                    &context.extension_with(PathEntry::LeftJoinLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::LeftJoinRightSide),
                );
                let preprocessed_expression = if let Some(e) = expression {
                    Some(self.remove_sugar_from_expression(
                        e,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ))
                } else {
                    None
                };
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::LeftJoin {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                    expression: preprocessed_expression,
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Filter { expr, inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::FilterInner),
                );
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Filter {
                    inner: Box::new(inner.gp.take().unwrap()),
                    expr: self.remove_sugar_from_expression(
                        expr,
                        &context.extension_with(PathEntry::FilterExpression),
                    ),
                });
                out.projections_from(&mut inner);
                out
            }
            GraphPattern::Union { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    ts_tps_in_scope.clone(),
                    &context.extension_with(PathEntry::UnionLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::UnionRightSide),
                );
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Union {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Graph { name, inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::GraphInner),
                );
                inner.gp = Some(GraphPattern::Graph {
                    inner: Box::new(inner.gp.unwrap()),
                    name: name.clone(),
                });
                inner
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::ExtendInner),
                );

                inner.gp = Some(GraphPattern::Extend {
                    inner: Box::new(inner.gp.unwrap()),
                    variable: variable.clone(),
                    expression: self.remove_sugar_from_expression(
                        expression,
                        &context.extension_with(PathEntry::ExtendExpression),
                    ),
                });
                inner
            }
            GraphPattern::Minus { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    ts_tps_in_scope.clone(),
                    &context.extension_with(PathEntry::MinusLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::MinusRightSide),
                );

                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Minus {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out.projections_from(&mut right);
                out
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Values {
                variables: variables.clone(),
                bindings: bindings.clone(),
            }),
            GraphPattern::OrderBy { inner, expression } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::OrderByInner),
                );
                inner.gp = Some(GraphPattern::OrderBy {
                    inner: Box::new(inner.gp.unwrap()),
                    expression: expression
                        .into_iter()
                        .enumerate()
                        .map(|(i, oe)| {
                            self.remove_sugar_from_order_expression(
                                oe,
                                &context.extension_with(PathEntry::OrderByExpression(i as u16)),
                            )
                        })
                        .collect(),
                });
                inner
            }
            GraphPattern::Project {
                inner,
                mut variables,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::ProjectInner),
                );
                let mut gp = inner.gp.take().unwrap();
                if !inner.vars_to_group_by.is_empty() {
                    let mut group_by_vars: Vec<_> = inner.vars_to_group_by.drain(..).collect();
                    group_by_vars.extend(variables.clone());
                    let order_expr = if is_order_by(&gp) {
                        if let GraphPattern::OrderBy { inner, expression } = gp {
                            gp = *inner;
                            Some(expression)
                        } else {
                            panic!()
                        }
                    } else {
                        None
                    };

                    gp = GraphPattern::Group {
                        inner: Box::new(gp),
                        variables: group_by_vars,
                        aggregates: inner.aggregations.drain(..).collect(),
                    };
                    if let Some(order_expr) = order_expr {
                        gp = GraphPattern::OrderBy {
                            inner: Box::new(gp),
                            expression: order_expr,
                        };
                    }
                }

                for v in inner.vars_to_project.drain(..) {
                    if !variables.contains(&v) {
                        variables.push(v);
                    }
                }
                inner.gp = Some(GraphPattern::Project {
                    inner: Box::new(gp),
                    variables,
                });
                inner
            }
            GraphPattern::Distinct { inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::DistinctInner),
                );
                let gp = inner.gp.take().unwrap();
                inner.gp = Some(GraphPattern::Distinct {
                    inner: Box::new(gp),
                });
                inner
            }
            GraphPattern::Reduced { inner } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::ReducedInner),
                );
                let gp = inner.gp.take().unwrap();
                inner.gp = Some(GraphPattern::Reduced {
                    inner: Box::new(gp),
                });
                inner
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::SliceInner),
                );
                let gp = inner.gp.take().unwrap();
                inner.gp = Some(GraphPattern::Slice {
                    inner: Box::new(gp),
                    start,
                    length,
                });
                inner
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::GroupInner),
                );
                let mut preprocessed_aggregates = vec![];
                for (i, (var, agg)) in aggregates.into_iter().enumerate() {
                    preprocessed_aggregates.push((
                        var.clone(),
                        self.remove_sugar_from_aggregate_expression(
                            agg,
                            &context.extension_with(PathEntry::GroupAggregation(i as u16)),
                        ),
                    ))
                }
                for (v, a) in inner.aggregations.drain(..) {
                    preprocessed_aggregates.push((v, a))
                }

                let mut variables = variables;
                for v in inner.vars_to_group_by.drain(..) {
                    if !variables.contains(&v) {
                        variables.push(v);
                    }
                }
                inner.gp = Some(GraphPattern::Group {
                    inner: Box::new(inner.gp.unwrap()),
                    variables,
                    aggregates: preprocessed_aggregates,
                });
                inner
            }
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => {
                let mut inner = self.remove_sugar_from_graph_pattern(
                    *inner,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::ServiceInner),
                );
                inner.gp = Some(GraphPattern::Service {
                    inner: Box::new(inner.gp.unwrap()),
                    name: name,
                    silent: silent,
                });
                inner
            }
            GraphPattern::DT { dt } => {
                let ret = dt_to_ret(dt, ts_tps_in_scope, context);
                ret
            }
            GraphPattern::PValues {
                bindings_parameter,
                variables,
            } => RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::PValues {
                bindings_parameter,
                variables,
            }),
            #[cfg(feature = "sep-0006")]
            GraphPattern::Lateral { left, right } => {
                let mut left = self.remove_sugar_from_graph_pattern(
                    *left,
                    ts_tps_in_scope.clone(),
                    &context.extension_with(PathEntry::UnionLeftSide),
                );
                let mut right = self.remove_sugar_from_graph_pattern(
                    *right,
                    ts_tps_in_scope,
                    &context.extension_with(PathEntry::UnionRightSide),
                );
                let mut out = RemoveSugarGraphPatternReturn::from_pattern(GraphPattern::Lateral {
                    left: Box::new(left.gp.take().unwrap()),
                    right: Box::new(right.gp.take().unwrap()),
                });
                out.projections_from(&mut left);
                out
            }
        }
    }

    fn remove_sugar_from_expression(
        &self,
        expression: Expression,
        context: &Context,
    ) -> Expression {
        match expression {
            Expression::Or(left, right) => Expression::Or(
                Box::new(self.remove_sugar_from_expression(
                    *left,
                    &context.extension_with(PathEntry::OrLeft),
                )),
                Box::new(self.remove_sugar_from_expression(
                    *right,
                    &context.extension_with(PathEntry::OrRight),
                )),
            ),
            Expression::And(left, right) => Expression::And(
                Box::new(self.remove_sugar_from_expression(
                    *left,
                    &context.extension_with(PathEntry::AndLeft),
                )),
                Box::new(self.remove_sugar_from_expression(
                    *right,
                    &context.extension_with(PathEntry::AndRight),
                )),
            ),
            Expression::Not(inner) => Expression::Not(Box::new(
                self.remove_sugar_from_expression(*inner, &context.extension_with(PathEntry::Not)),
            )),
            Expression::Exists(graph_pattern) => Expression::Exists(Box::new(
                self.remove_sugar_from_graph_pattern(
                    *graph_pattern,
                    vec![], //TODO generalize
                    &context.extension_with(PathEntry::Exists),
                )
                .gp
                .unwrap(),
            )),
            Expression::If(left, middle, right) => Expression::If(
                Box::new(self.remove_sugar_from_expression(
                    *left,
                    &context.extension_with(PathEntry::IfLeft),
                )),
                Box::new(self.remove_sugar_from_expression(
                    *middle,
                    &context.extension_with(PathEntry::IfMiddle),
                )),
                Box::new(self.remove_sugar_from_expression(
                    *right,
                    &context.extension_with(PathEntry::IfRight),
                )),
            ),
            _ => expression,
        }
    }

    fn remove_sugar_from_aggregate_expression(
        &self,
        aggregate_expression: AggregateExpression,
        context: &Context,
    ) -> AggregateExpression {
        match aggregate_expression {
            AggregateExpression::CountSolutions { distinct } => {
                AggregateExpression::CountSolutions { distinct }
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                let expr = self.remove_sugar_from_expression(
                    expr,
                    &context.extension_with(PathEntry::AggregationOperation),
                );
                AggregateExpression::FunctionCall {
                    name,
                    expr,
                    distinct,
                }
            }
        }
    }
    fn remove_sugar_from_order_expression(
        &self,
        order_expression: OrderExpression,
        context: &Context,
    ) -> OrderExpression {
        match order_expression {
            OrderExpression::Asc(e) => OrderExpression::Asc(self.remove_sugar_from_expression(
                e,
                &context.extension_with(PathEntry::OrderingOperation),
            )),
            OrderExpression::Desc(e) => OrderExpression::Desc(self.remove_sugar_from_expression(
                e,
                &context.extension_with(PathEntry::OrderingOperation),
            )),
        }
    }
}

fn is_order_by(gp: &GraphPattern) -> bool {
    if let GraphPattern::OrderBy { .. } = gp {
        true
    } else {
        false
    }
}

fn add_filter_and_bindings(
    mut gp: GraphPattern,
    expr: Option<Expression>,
    binding: Option<(Variable, Expression)>,
) -> GraphPattern {
    if let Some(expr) = expr {
        gp = GraphPattern::Filter {
            inner: Box::new(gp),
            expr,
        }
    }
    if let Some((variable, expression)) = binding {
        gp = GraphPattern::Extend {
            inner: Box::new(gp),
            variable,
            expression,
        }
    }
    gp
}

fn contains_sugar(gp: &GraphPattern) -> bool {
    match gp {
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right, .. } => contains_sugar(left) || contains_sugar(right),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner, .. }
        | GraphPattern::Reduced { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => contains_sugar(inner),
        GraphPattern::DT { .. } => true,
        _ => false,
    }
}

fn find_ts_term_pattens_in_scope(gp: &GraphPattern) -> Vec<TermPattern> {
    match gp {
        GraphPattern::Bgp { patterns } => {
            let mut timeseries_term_patterns = vec![];
            for p in patterns {
                if let NamedNodePattern::NamedNode(nn) = &p.predicate {
                    if HAS_TIMESERIES == nn.as_str() {
                        timeseries_term_patterns.push(p.object.clone());
                    }
                }
            }
            timeseries_term_patterns
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => find_final_timeseries_tp(Some(subject), path, Some(object)),
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right } => {
            let mut left_term_patterns = find_ts_term_pattens_in_scope(left);
            left_term_patterns.extend(find_ts_term_pattens_in_scope(right));
            left_term_patterns
        }
        GraphPattern::Minus { left: inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => find_ts_term_pattens_in_scope(inner),
        _ => vec![],
    }
}

fn find_final_timeseries_tp(
    subject: Option<&TermPattern>,
    ppe: &PropertyPathExpression,
    object: Option<&TermPattern>,
) -> Vec<TermPattern> {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            if nn.as_str() == HAS_TIMESERIES {
                if let Some(tp) = object {
                    vec![tp.clone()]
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        PropertyPathExpression::Reverse(ppe) => find_final_timeseries_tp(object, ppe, subject),
        PropertyPathExpression::Sequence(_, b) => find_final_timeseries_tp(None, b, object),
        PropertyPathExpression::Alternative(a, b) => {
            let mut v1 = find_final_timeseries_tp(subject, a, object);
            v1.extend(find_final_timeseries_tp(subject, b, object));
            v1
        }
        _ => vec![],
    }
}

fn dt_to_ret(
    dt: DataTreehousePattern,
    ts_tps_in_scope: Vec<TermPattern>,
    context: &Context,
) -> RemoveSugarGraphPatternReturn {
    let DataTreehousePattern {
        mut timeseries,
        values,
        timestamp,
        from,
        to,
        aggregations: aggregation,
        filter,
        interval,
    } = dt;
    if timeseries.is_none() {
        timeseries = Some(
            ts_tps_in_scope
                .into_iter()
                .map(|x| match x {
                    TermPattern::Variable(v) => v,
                    _ => panic!(),
                })
                .collect(),
        );
    }

    let mut patterns = vec![];
    let timestamp = timestamp.unwrap_or(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
    let inner_timestamp = if aggregation.is_some() {
        Variable::new_unchecked(format!("{}_inner", timestamp.as_str()))
    } else {
        timestamp.clone()
    };

    let mut vars_to_project = vec![timestamp.clone()];

    let values = if let Some(values) = values {
        values
    } else {
        let mut values = vec![];
        for t in timeseries.as_ref().unwrap() {
            values.push((
                t.clone(),
                Variable::new_unchecked(format!("{}_{}", t.as_str(), VALUE_SUFFIX)),
            ));
        }
        values
    };

    for (t, v) in &values {
        if aggregation.is_none() {
            vars_to_project.push(v.clone());
        }
        let dp = Variable::new_unchecked(format!("{}_{}", t.as_str(), DATA_POINT_SUFFIX));
        patterns.push(TriplePattern {
            subject: t.to_owned().into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_DATA_POINT)),
            object: dp.clone().into(),
        });
        patterns.push(TriplePattern {
            subject: dp.clone().into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_VALUE)),
            object: v.clone().into(),
        });
        patterns.push(TriplePattern {
            subject: dp.into(),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESTAMP)),
            object: inner_timestamp.clone().into(),
        });
    }

    let gp = GraphPattern::Bgp { patterns };

    let mut from_ts_datetime_opt = None;
    let mut to_ts_datetime_opt = None;

    if let Some(from) = &from {
        from_ts_datetime_opt = eval_timestamp_expression(from, None);
    }
    if let Some(to) = &to {
        to_ts_datetime_opt = eval_timestamp_expression(to, None);
    }
    let from_ts_datetime_opt = if let Some(from) = from_ts_datetime_opt {
        Some(from)
    } else {
        if let Some(from) = &from {
            if let Some(to) = &to_ts_datetime_opt {
                eval_timestamp_expression(from, Some(to))
            } else {
                panic!()
            }
        } else {
            None
        }
    };

    let to_ts_datetime_opt = if let Some(to) = to_ts_datetime_opt {
        Some(to)
    } else {
        if let Some(to) = &to {
            if let Some(from) = &from_ts_datetime_opt {
                eval_timestamp_expression(to, Some(from))
            } else {
                panic!()
            }
        } else {
            None
        }
    };
    let mut expr = None;
    if let Some(to) = to_ts_datetime_opt {
        expr = Some(Expression::LessOrEqual(
            Box::new(Expression::Variable(inner_timestamp.clone())),
            Box::new(datetime_to_literal(&to)),
        ));
    }

    if let Some(from) = from_ts_datetime_opt {
        let new_expr = Expression::GreaterOrEqual(
            Box::new(Expression::Variable(inner_timestamp.clone())),
            Box::new(datetime_to_literal(&from)),
        );
        expr = if let Some(expr) = expr {
            Some(Expression::And(Box::new(expr), Box::new(new_expr)))
        } else {
            Some(new_expr)
        };
    }

    if let Some(mut filter) = filter {
        filter = rewrite_timestamp_variable(filter, &timestamp, &inner_timestamp);
        expr = if let Some(expr) = expr {
            Some(Expression::And(Box::new(expr), Box::new(filter)))
        } else {
            Some(filter)
        };
    }

    let mut vars_to_group_by = vec![];

    let mut binding = None;
    if let Some(i) = interval {
        let seconds = i.num_seconds();
        binding = Some((
            timestamp.clone(),
            Expression::FunctionCall(
                Function::Custom(NamedNode::new_unchecked(FLOOR_DATETIME_TO_SECONDS_INTERVAL)),
                vec![
                    Expression::Variable(inner_timestamp.clone()),
                    Expression::Literal(Literal::from(seconds)),
                ],
            ),
        ));
        vars_to_group_by.push(timestamp.clone());
    }

    let mut aggregations = vec![];
    if let Some(aggs) = aggregation {
        for (_, v) in values {
            for name in &aggs {
                let agg_v = Variable::new_unchecked(format!("{}_{}", v.as_str(), name));
                let a = AggregateExpression::FunctionCall {
                    name: name.clone(),
                    expr: Expression::Variable(v.clone()),
                    distinct: false,
                };
                aggregations.push((agg_v.clone(), a));
                vars_to_project.push(agg_v);
            }
        }
    }
    RemoveSugarGraphPatternReturn {
        gp: Some(gp),
        aggregations,
        filter_expr: expr,
        binding,
        vars_to_group_by,
        vars_to_project,
        context: Some(context.clone()),
    }
}

fn rewrite_timestamp_variable(e: Expression, outer: &Variable, inner: &Variable) -> Expression {
    match e {
        Expression::NamedNode(..) | Expression::Literal(..) => e,
        Expression::Variable(v) => {
            if &v == outer {
                Expression::Variable(inner.clone())
            } else {
                Expression::Variable(v)
            }
        }
        Expression::Or(a, b) => Expression::Or(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::And(a, b) => Expression::And(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Equal(a, b) => Expression::Equal(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::SameTerm(a, b) => Expression::SameTerm(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Greater(a, b) => Expression::Greater(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::GreaterOrEqual(a, b) => Expression::GreaterOrEqual(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Less(a, b) => Expression::Less(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::LessOrEqual(a, b) => Expression::LessOrEqual(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::In(a, bs) => {
            let bs = bs
                .into_iter()
                .map(|b| rewrite_timestamp_variable(b, outer, inner))
                .collect();
            Expression::In(Box::new(rewrite_timestamp_variable(*a, outer, inner)), bs)
        }
        Expression::Add(a, b) => Expression::Add(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Subtract(a, b) => Expression::Subtract(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Multiply(a, b) => Expression::Multiply(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::Divide(a, b) => Expression::Divide(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
        ),
        Expression::UnaryPlus(a) => {
            Expression::UnaryPlus(Box::new(rewrite_timestamp_variable(*a, outer, inner)))
        }
        Expression::UnaryMinus(a) => {
            Expression::UnaryMinus(Box::new(rewrite_timestamp_variable(*a, outer, inner)))
        }
        Expression::Not(a) => {
            Expression::Not(Box::new(rewrite_timestamp_variable(*a, outer, inner)))
        }
        Expression::Exists(b) => {
            Expression::Exists(b)
            //Todo:fix properly
        }
        Expression::Bound(a) => {
            if &a == outer {
                Expression::Bound(inner.clone())
            } else {
                Expression::Bound(a)
            }
        }
        Expression::If(a, b, c) => Expression::If(
            Box::new(rewrite_timestamp_variable(*a, outer, inner)),
            Box::new(rewrite_timestamp_variable(*b, outer, inner)),
            Box::new(rewrite_timestamp_variable(*c, outer, inner)),
        ),
        Expression::Coalesce(a) => {
            let a = a
                .into_iter()
                .map(|a| rewrite_timestamp_variable(a, outer, inner))
                .collect();
            Expression::Coalesce(a)
        }
        Expression::FunctionCall(f, a) => {
            let a = a
                .into_iter()
                .map(|a| rewrite_timestamp_variable(a, outer, inner))
                .collect();
            Expression::FunctionCall(f, a)
        }
    }
}

fn eval_timestamp_expression(
    te: &TimestampExpression,
    from_or_to_expression: Option<&DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match te {
        TimestampExpression::Simple(s) => {
            eval_simple_timestamp_expression(s, from_or_to_expression)
        }
        TimestampExpression::Binary(s, op, d) => {
            if let Some(dt) = eval_simple_timestamp_expression(s, from_or_to_expression) {
                match op {
                    TimestampBinaryOperator::Plus => Some(dt + d.clone()),
                    TimestampBinaryOperator::Minus => Some(dt - d.clone()),
                }
            } else {
                None
            }
        }
    }
}

fn eval_simple_timestamp_expression(
    se: &SimpleTimestampExpression,
    from_or_to_expression: Option<&DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match se {
        SimpleTimestampExpression::Now => Some(Utc::now()),
        SimpleTimestampExpression::From | SimpleTimestampExpression::To => {
            if let Some(from_or_to) = from_or_to_expression {
                Some(from_or_to.clone())
            } else {
                panic!()
            }
        }
        SimpleTimestampExpression::DateTime(dt) => Some(dt.clone()),
    }
}

fn datetime_to_literal(dt: &DateTime<Utc>) -> Expression {
    Expression::Literal(Literal::new_typed_literal(dt.to_rfc3339(), xsd::DATE_TIME))
}

struct RemoveSugarGraphPatternReturn {
    pub gp: Option<GraphPattern>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
    pub filter_expr: Option<Expression>,
    pub binding: Option<(Variable, Expression)>,
    pub vars_to_group_by: Vec<Variable>,
    pub vars_to_project: Vec<Variable>,
    pub context: Option<Context>,
}

impl RemoveSugarGraphPatternReturn {
    pub(crate) fn has_sugar_context(&self, c: &Context) -> bool {
        if let Some(context) = &self.context {
            context == c
        } else {
            false
        }
    }
}

impl RemoveSugarGraphPatternReturn {
    pub fn from_pattern(gp: GraphPattern) -> Self {
        RemoveSugarGraphPatternReturn {
            gp: Some(gp),
            aggregations: vec![],
            filter_expr: None,
            vars_to_project: vec![],
            vars_to_group_by: vec![],
            binding: None,
            context: None,
        }
    }

    pub fn projections_from(&mut self, p: &mut RemoveSugarGraphPatternReturn) {
        self.aggregations.extend(p.aggregations.drain(..));
        self.vars_to_project.extend(p.vars_to_project.drain(..));
        self.vars_to_group_by.extend(p.vars_to_group_by.drain(..));
    }
}
