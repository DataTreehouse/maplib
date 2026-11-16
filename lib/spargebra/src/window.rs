// maplib extension, not part of oxigraph's spargebra.
//
// Window functions (`ROW_NUMBER() OVER (PARTITION BY ?a ORDER BY ?b)`) and ordered aggregates
// (`GROUP_CONCAT(?x; SEPARATOR=", " ORDER BY ?y)`) are represented in the algebra with
// function calls to reserved maplib IRIs instead of new variants of `Expression` and
// `AggregateExpression`, so that code matching on the algebra does not change. Since they are
// ordinary function calls, the variables they use are found, renamed and projected like those of
// other function calls. `WindowCall` and `OrderedAggregate` convert to and from the encoding.
//
// A window call is `<window function IRI>(args..., <partitionBy>(exprs...), <orderBy>(<asc>(e),
// <desc>(e), ...), [<rows>(start, end)])`. An ordered aggregate wraps the aggregated expression:
// `GROUP_CONCAT(<orderedAggregate>(expr, <asc>(e), ...); SEPARATOR=...)`. A custom aggregate
// such as `agg:stdev` is `<windowCustom>(<aggregate IRI>, arg, <partitionBy>(...), ...)`.

use crate::algebra::{AggregateFunction, Expression, Function, OrderExpression};
use oxrdf::{Literal, NamedNode, NamedNodeRef};
use std::fmt;

pub const MAPLIB_VOCAB: &str = "https://datatreehouse.github.io/maplib/vocab#";

const ROW_NUMBER: &str = "https://datatreehouse.github.io/maplib/vocab#windowRowNumber";
const RANK: &str = "https://datatreehouse.github.io/maplib/vocab#windowRank";
const DENSE_RANK: &str = "https://datatreehouse.github.io/maplib/vocab#windowDenseRank";
const COUNT_STAR: &str = "https://datatreehouse.github.io/maplib/vocab#windowCountStar";
const COUNT: &str = "https://datatreehouse.github.io/maplib/vocab#windowCount";
const SUM: &str = "https://datatreehouse.github.io/maplib/vocab#windowSum";
const AVG: &str = "https://datatreehouse.github.io/maplib/vocab#windowAvg";
const MIN: &str = "https://datatreehouse.github.io/maplib/vocab#windowMin";
const MAX: &str = "https://datatreehouse.github.io/maplib/vocab#windowMax";
const SAMPLE: &str = "https://datatreehouse.github.io/maplib/vocab#windowSample";
const GROUP_CONCAT: &str = "https://datatreehouse.github.io/maplib/vocab#windowGroupConcat";
/// A custom aggregate, such as the statistical aggregates. The aggregate IRI is the first argument.
const CUSTOM: &str = "https://datatreehouse.github.io/maplib/vocab#windowCustom";
const DISTINCT_SUFFIX: &str = "Distinct";

const PARTITION_BY: &str = "https://datatreehouse.github.io/maplib/vocab#windowPartitionBy";
const ORDER_BY: &str = "https://datatreehouse.github.io/maplib/vocab#windowOrderBy";
const ROWS: &str = "https://datatreehouse.github.io/maplib/vocab#windowRows";
const UNBOUNDED: &str = "https://datatreehouse.github.io/maplib/vocab#windowUnbounded";
const ASC: &str = "https://datatreehouse.github.io/maplib/vocab#asc";
const DESC: &str = "https://datatreehouse.github.io/maplib/vocab#desc";
const ORDERED_AGGREGATE: &str = "https://datatreehouse.github.io/maplib/vocab#orderedAggregate";

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    /// COUNT(*)
    CountSolutions,
    Aggregate {
        name: AggregateFunction,
        distinct: bool,
    },
}

/// A frame `ROWS BETWEEN start AND end`, as offsets from the current row (negative for
/// PRECEDING), `None` for UNBOUNDED.
#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct WindowFrame {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct WindowCall {
    pub function: WindowFunction,
    /// The argument of an aggregate (none for ROW_NUMBER, RANK, DENSE_RANK and COUNT(*)).
    pub arg: Option<Expression>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderExpression>,
    pub frame: Option<WindowFrame>,
}

fn call(iri: &str, args: Vec<Expression>) -> Expression {
    Expression::FunctionCall(Function::Custom(NamedNode::new_unchecked(iri)), args)
}

fn custom_args<'a>(e: &'a Expression, iri: &str) -> Option<&'a Vec<Expression>> {
    if let Expression::FunctionCall(Function::Custom(nn), args) = e {
        if nn.as_str() == iri {
            return Some(args);
        }
    }
    None
}

fn encode_order(o: &OrderExpression) -> Expression {
    match o {
        OrderExpression::Asc(e) => call(ASC, vec![e.clone()]),
        OrderExpression::Desc(e) => call(DESC, vec![e.clone()]),
    }
}

fn decode_order(e: &Expression) -> Option<OrderExpression> {
    if let Some([a]) = custom_args(e, ASC).map(|x| x.as_slice()) {
        Some(OrderExpression::Asc(a.clone()))
    } else if let Some([a]) = custom_args(e, DESC).map(|x| x.as_slice()) {
        Some(OrderExpression::Desc(a.clone()))
    } else {
        None
    }
}

fn encode_bound(b: Option<i64>) -> Expression {
    match b {
        Some(i) => Expression::Literal(Literal::from(i)),
        None => Expression::NamedNode(NamedNode::new_unchecked(UNBOUNDED)),
    }
}

fn decode_bound(e: &Expression) -> Option<Option<i64>> {
    match e {
        Expression::NamedNode(nn) if nn.as_str() == UNBOUNDED => Some(None),
        Expression::Literal(l) => l.value().parse::<i64>().ok().map(Some),
        _ => None,
    }
}

impl WindowFunction {
    fn iri(&self) -> String {
        let (base, distinct) = match self {
            WindowFunction::RowNumber => (ROW_NUMBER, false),
            WindowFunction::Rank => (RANK, false),
            WindowFunction::DenseRank => (DENSE_RANK, false),
            WindowFunction::CountSolutions => (COUNT_STAR, false),
            WindowFunction::Aggregate { name, distinct } => (
                match name {
                    AggregateFunction::Count => COUNT,
                    AggregateFunction::Sum => SUM,
                    AggregateFunction::Avg => AVG,
                    AggregateFunction::Min => MIN,
                    AggregateFunction::Max => MAX,
                    AggregateFunction::Sample => SAMPLE,
                    AggregateFunction::GroupConcat { .. } => GROUP_CONCAT,
                    AggregateFunction::Custom(_) => CUSTOM,
                },
                *distinct,
            ),
        };
        if distinct {
            format!("{base}{DISTINCT_SUFFIX}")
        } else {
            base.to_string()
        }
    }

    fn from_iri(iri: &str) -> Option<(WindowFunction, bool)> {
        let (base, distinct) = match iri.strip_suffix(DISTINCT_SUFFIX) {
            Some(b) => (b, true),
            None => (iri, false),
        };
        let agg = |name| Some((WindowFunction::Aggregate { name, distinct }, true));
        let f = match base {
            ROW_NUMBER if !distinct => Some((WindowFunction::RowNumber, false)),
            RANK if !distinct => Some((WindowFunction::Rank, false)),
            DENSE_RANK if !distinct => Some((WindowFunction::DenseRank, false)),
            COUNT_STAR if !distinct => Some((WindowFunction::CountSolutions, false)),
            COUNT => agg(AggregateFunction::Count),
            SUM => agg(AggregateFunction::Sum),
            AVG => agg(AggregateFunction::Avg),
            MIN => agg(AggregateFunction::Min),
            MAX => agg(AggregateFunction::Max),
            SAMPLE => agg(AggregateFunction::Sample),
            GROUP_CONCAT => agg(AggregateFunction::GroupConcat { separator: None }),
            // The name is replaced by the first argument in `WindowCall::from_expression`
            CUSTOM => agg(AggregateFunction::Custom(NamedNode::new_unchecked(CUSTOM))),
            _ => None,
        };
        f
    }

    /// Whether the value depends on the order of the rows in the partition (and not only on the
    /// rows of the frame).
    pub fn is_ranking(&self) -> bool {
        matches!(
            self,
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank
        )
    }
}

/// Whether the IRI is that of a window function, so the call must be decoded with
/// `WindowCall::from_expression`.
pub fn is_window_function(nn: NamedNodeRef<'_>) -> bool {
    nn.as_str().starts_with(MAPLIB_VOCAB) && WindowFunction::from_iri(nn.as_str()).is_some()
}

/// Whether the expression contains a window function call. Such expressions depend on all the
/// solutions of the pattern they are evaluated on, so they must not be moved to other patterns.
pub fn contains_window_function(e: &Expression) -> bool {
    match e {
        Expression::FunctionCall(Function::Custom(nn), _) if is_window_function(nn.as_ref()) => {
            true
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_)
        | Expression::Exists(_) => false,
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => contains_window_function(a) || contains_window_function(b),
        Expression::In(a, b) => {
            contains_window_function(a) || b.iter().any(contains_window_function)
        }
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
            contains_window_function(a)
        }
        Expression::If(a, b, c) => {
            contains_window_function(a)
                || contains_window_function(b)
                || contains_window_function(c)
        }
        Expression::Coalesce(args) | Expression::FunctionCall(_, args) => {
            args.iter().any(contains_window_function)
        }
    }
}

impl WindowCall {
    pub fn into_expression(self) -> Expression {
        let mut args = vec![];
        if let WindowFunction::Aggregate {
            name: AggregateFunction::Custom(name),
            ..
        } = &self.function
        {
            args.push(Expression::NamedNode(name.clone()));
        }
        if let Some(arg) = self.arg {
            args.push(arg);
        }
        if let WindowFunction::Aggregate {
            name: AggregateFunction::GroupConcat {
                separator: Some(s),
            },
            ..
        } = &self.function
        {
            args.push(Expression::Literal(Literal::new_simple_literal(s)));
        }
        args.push(call(PARTITION_BY, self.partition_by));
        args.push(call(
            ORDER_BY,
            self.order_by.iter().map(encode_order).collect(),
        ));
        if let Some(frame) = self.frame {
            args.push(call(
                ROWS,
                vec![encode_bound(frame.start), encode_bound(frame.end)],
            ));
        }
        call(&self.function.iri(), args)
    }

    pub fn from_expression(e: &Expression) -> Option<WindowCall> {
        let Expression::FunctionCall(Function::Custom(nn), args) = e else {
            return None;
        };
        let (mut function, has_arg) = WindowFunction::from_iri(nn.as_str())?;
        let mut rest = args.as_slice();
        if let WindowFunction::Aggregate {
            name: AggregateFunction::Custom(name),
            ..
        } = &mut function
        {
            let (Expression::NamedNode(n), r) = rest.split_first()? else {
                return None;
            };
            *name = n.clone();
            rest = r;
        }
        let mut arg = None;
        if has_arg {
            let (first, r) = rest.split_first()?;
            arg = Some(first.clone());
            rest = r;
        }
        if let WindowFunction::Aggregate {
            name: AggregateFunction::GroupConcat { separator },
            ..
        } = &mut function
        {
            if let Some((Expression::Literal(l), r)) = rest.split_first() {
                *separator = Some(l.value().to_string());
                rest = r;
            }
        }
        let (partition, rest) = rest.split_first()?;
        let partition_by = custom_args(partition, PARTITION_BY)?.clone();
        let (order, rest) = rest.split_first()?;
        let order_by = custom_args(order, ORDER_BY)?
            .iter()
            .map(decode_order)
            .collect::<Option<Vec<_>>>()?;
        let frame = match rest {
            [] => None,
            [rows] => match custom_args(rows, ROWS)?.as_slice() {
                [s, e] => Some(WindowFrame {
                    start: decode_bound(s)?,
                    end: decode_bound(e)?,
                }),
                _ => return None,
            },
            _ => return None,
        };
        Some(WindowCall {
            function,
            arg,
            partition_by,
            order_by,
            frame,
        })
    }
}

fn fmt_bound(b: Option<i64>, start: bool, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match b {
        None if start => f.write_str("UNBOUNDED PRECEDING"),
        None => f.write_str("UNBOUNDED FOLLOWING"),
        Some(0) => f.write_str("CURRENT ROW"),
        Some(i) if i < 0 => write!(f, "{} PRECEDING", -i),
        Some(i) => write!(f, "{i} FOLLOWING"),
    }
}

fn fmt_order_by(order_by: &[OrderExpression], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("ORDER BY")?;
    for o in order_by {
        write!(f, " {o}")?;
    }
    Ok(())
}

impl fmt::Display for WindowCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let distinct = |d: bool| if d { "DISTINCT " } else { "" };
        match &self.function {
            WindowFunction::RowNumber => f.write_str("ROW_NUMBER()")?,
            WindowFunction::Rank => f.write_str("RANK()")?,
            WindowFunction::DenseRank => f.write_str("DENSE_RANK()")?,
            WindowFunction::CountSolutions => f.write_str("COUNT(*)")?,
            WindowFunction::Aggregate { name, distinct: d } => {
                let arg = self.arg.as_ref().unwrap();
                if let AggregateFunction::GroupConcat {
                    separator: Some(s),
                } = name
                {
                    write!(
                        f,
                        "{name}({}{arg}; SEPARATOR = {})",
                        distinct(*d),
                        Literal::new_simple_literal(s)
                    )?
                } else {
                    write!(f, "{name}({}{arg})", distinct(*d))?
                }
            }
        }
        f.write_str(" OVER (")?;
        let mut sep = "";
        if !self.partition_by.is_empty() {
            f.write_str("PARTITION BY")?;
            for p in &self.partition_by {
                match p {
                    Expression::Variable(v) => write!(f, " {v}")?,
                    p => write!(f, " ({p})")?,
                }
            }
            sep = " ";
        }
        if !self.order_by.is_empty() {
            f.write_str(sep)?;
            fmt_order_by(&self.order_by, f)?;
            sep = " ";
        }
        if let Some(frame) = &self.frame {
            write!(f, "{sep}ROWS BETWEEN ")?;
            fmt_bound(frame.start, true, f)?;
            f.write_str(" AND ")?;
            fmt_bound(frame.end, false, f)?;
        }
        f.write_str(")")
    }
}

/// The expression of an aggregate with an ORDER BY, e.g. `GROUP_CONCAT(?x ORDER BY ?y)`.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct OrderedAggregate {
    pub expr: Expression,
    pub order_by: Vec<OrderExpression>,
}

impl OrderedAggregate {
    /// The expression to use as the argument of the aggregate.
    pub fn into_expression(self) -> Expression {
        if self.order_by.is_empty() {
            return self.expr;
        }
        let mut args = vec![self.expr];
        args.extend(self.order_by.iter().map(encode_order));
        call(ORDERED_AGGREGATE, args)
    }

    pub fn from_expression(e: &Expression) -> Option<OrderedAggregate> {
        let (expr, keys) = custom_args(e, ORDERED_AGGREGATE)?.split_first()?;
        Some(OrderedAggregate {
            expr: expr.clone(),
            order_by: keys.iter().map(decode_order).collect::<Option<Vec<_>>>()?,
        })
    }

    pub(crate) fn fmt_order_by(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(" ")?;
        fmt_order_by(&self.order_by, f)
    }
}

pub(crate) fn ordered_aggregate_argument(
    order_by: Option<Vec<OrderExpression>>,
    expr: Expression,
) -> Expression {
    OrderedAggregate {
        expr,
        order_by: order_by.unwrap_or_default(),
    }
    .into_expression()
}
