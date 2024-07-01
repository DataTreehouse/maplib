use crate::algebra::{AggregateFunction, Expression};
use chrono::{DateTime, Duration, Utc};
use oxrdf::Variable;
use std::fmt::{Display, Formatter};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Default)]
pub struct DataTreehousePattern {
    pub timeseries: Option<Vec<Variable>>,
    pub values: Option<Vec<(Variable, Variable)>>,
    pub timestamp: Option<Variable>,
    pub from: Option<TimestampExpression>,
    pub to: Option<TimestampExpression>,
    pub aggregations: Option<Vec<AggregateFunction>>,
    pub filter: Option<Expression>,
    pub interval: Option<Duration>,
}

impl DataTreehousePattern {
    pub fn union(mut self, other: Self) -> Self {
        let DataTreehousePattern {
            timeseries,
            values,
            timestamp,
            from,
            to,
            aggregations,
            filter,
            interval,
        } = other;
        if let Some(timeseries) = timeseries {
            self.timeseries = Some(timeseries);
        }
        if let Some(values) = values {
            self.values = Some(values);
        }
        if let Some(timestamp) = timestamp {
            self.timestamp = Some(timestamp);
        }
        if let Some(from) = from {
            self.from = Some(from);
        }
        if let Some(to) = to {
            self.to = Some(to);
        }
        if let Some(aggregations) = aggregations {
            self.aggregations = Some(aggregations);
        }
        if let Some(filter) = filter {
            self.filter = Some(filter);
        }
        if let Some(interval) = interval {
            self.interval = Some(interval);
        }
        self
    }
}

impl DataTreehousePattern {
    pub(crate) fn lookup_in_scope_variables<'a>(&'a self, callback: &mut impl FnMut(&'a Variable)) {
        if let Some(values) = &self.values {
            for (_, v) in values {
                callback(v);
            }
        }
    }
}

impl Display for DataTreehousePattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "DT(").unwrap();
        if let Some(values) = &self.values {
            write!(f, "values=(").unwrap();
            for (ts, val) in values {
                write!(f, "({}={}), ", ts, val).unwrap();
            }
            writeln!(f, "),").unwrap();
        }
        if let Some(timestamp) = &self.timestamp {
            writeln!(f, "timestamp={},", timestamp,).unwrap();
        }
        if let Some(from) = &self.from {
            writeln!(f, "from={},", from).unwrap();
        }
        if let Some(to) = &self.from {
            writeln!(f, "from={},", to).unwrap();
        }
        if let Some(aggregations) = &self.aggregations {
            writeln!(f, "aggregation=[,").unwrap();
            for aggregation in aggregations {
                writeln!(f, "{},", aggregation).unwrap();
            }
            writeln!(f, "]").unwrap();
        }
        if let Some(filter) = &self.filter {
            writeln!(f, "filter={},", filter).unwrap();
        }
        if let Some(window) = &self.interval {
            writeln!(f, "window={}", window).unwrap();
        }
        write!(f, ")")
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum TimestampExpression {
    Simple(SimpleTimestampExpression),
    Binary(SimpleTimestampExpression, TimestampBinaryOperator, Duration),
}

impl Display for TimestampExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimestampExpression::Simple(t) => {
                write!(f, "{t}")
            }
            TimestampExpression::Binary(t, op, d) => {
                write!(f, "{} {} {}", t, op, d)
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum SimpleTimestampExpression {
    Now,
    From,
    To,
    DateTime(DateTime<Utc>),
}

impl Display for SimpleTimestampExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleTimestampExpression::Now => {
                write!(f, "now")
            }
            SimpleTimestampExpression::From => {
                write!(f, "from")
            }
            SimpleTimestampExpression::To => {
                write!(f, "to")
            }
            SimpleTimestampExpression::DateTime(dt) => {
                write!(f, "{}", dt.to_rfc3339())
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum TimestampBinaryOperator {
    Plus,
    Minus,
}

impl Display for TimestampBinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TimestampBinaryOperator::Plus => {
                    "+"
                }
                TimestampBinaryOperator::Minus => {
                    "-"
                }
            }
        )
    }
}
