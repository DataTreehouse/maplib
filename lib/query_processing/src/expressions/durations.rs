//! Arithmetic and comparisons on durations, dates, dateTimes and times
//! (XPath and XQuery Functions and Operators 3.1, sections 8 and 9.7, as used by SPARQL 1.1
//! section 17.3 and SPARQL 1.2).
//!
//! Durations (xsd:duration, xsd:yearMonthDuration and xsd:dayTimeDuration) and xsd:time are
//! stored as their lexical forms, xsd:date as days and xsd:dateTime / xsd:dateTimeStamp as UTC
//! instants with the time zone offset in the type. The operations are evaluated value by value
//! with the datatypes of oxsdatatypes, which implement the XPath semantics.

use chrono::{DateTime as ChronoDateTime, Months, NaiveDateTime};
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef};
use oxsdatatypes::{
    DayTimeDuration, Decimal, Double, Duration, Integer, Time, YearMonthDuration,
};
use polars::prelude::{
    BooleanChunked, Column, DataType, Expr, Field, Int32Chunked, Int64Chunked, IntoColumn,
    LiteralValue, NewChunkedArray, PolarsResult, Schema, StringChunked, lit,
};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::rdf_to_polars::{
    default_decimal_scale, default_decimal_type, default_time_unit, default_time_zone,
};
use representation::solution_mapping::BaseCatState;
use representation::{literal_is_numeric, BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Expression;
use std::str::FromStr;

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;

/// The kinds of values that take part in temporal arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    YearMonth,
    DayTime,
    Duration,
    DateTime,
    Date,
    Time,
    Numeric,
}

impl Kind {
    fn of(dt: NamedNodeRef) -> Option<Kind> {
        Some(match dt {
            xsd::YEAR_MONTH_DURATION => Kind::YearMonth,
            xsd::DAY_TIME_DURATION => Kind::DayTime,
            xsd::DURATION => Kind::Duration,
            xsd::DATE_TIME | xsd::DATE_TIME_STAMP => Kind::DateTime,
            xsd::DATE => Kind::Date,
            xsd::TIME => Kind::Time,
            _ if literal_is_numeric(dt) => Kind::Numeric,
            _ => return None,
        })
    }

    fn is_temporal(self) -> bool {
        !matches!(self, Kind::Numeric)
    }

    fn is_duration(self) -> bool {
        matches!(self, Kind::YearMonth | Kind::DayTime | Kind::Duration)
    }

    /// The physical type the values are converted to before they are read.
    fn input_dtype(self) -> DataType {
        match self {
            Kind::DateTime => DataType::Int64,
            Kind::Date => DataType::Int32,
            _ => DataType::String,
        }
    }
}

/// Whether the datatype is xsd:duration or one of its subtypes.
pub fn is_duration_datatype(dt: NamedNodeRef) -> bool {
    Kind::of(dt).is_some_and(|k| k.is_duration())
}

/// Whether the type is xsd:duration or one of its subtypes.
pub fn is_duration_type(t: &BaseRDFNodeType) -> bool {
    matches!(t, BaseRDFNodeType::Literal(nn, _) if is_duration_datatype(nn.as_ref()))
}

/// A value read from a column.
#[derive(Clone, Copy, Debug)]
enum Val {
    /// A yearMonthDuration, as the number of months.
    Months(i64),
    /// A dayTimeDuration.
    DayTime(DayTimeDuration),
    Duration(Duration),
    /// A dateTime, as microseconds since the epoch in UTC.
    Micros(i64),
    /// A date, as days since the epoch.
    Days(i32),
    Time(Time),
    Number(Decimal),
    /// A double that is infinite or outside the range of xsd:decimal.
    Float(f64),
}

/// A result value.
enum Out {
    Str(String),
    I64(i64),
    I32(i32),
    Bool(bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutKind {
    /// A lexical form: a duration, a time or a decimal.
    Str,
    /// A dateTime, as microseconds since the epoch in UTC.
    Micros,
    /// A date, as days since the epoch.
    Days,
    Bool,
}

impl OutKind {
    fn dtype(self) -> DataType {
        match self {
            OutKind::Str => DataType::String,
            OutKind::Micros => DataType::Int64,
            OutKind::Days => DataType::Int32,
            OutKind::Bool => DataType::Boolean,
        }
    }
}

fn parse_number(s: &str) -> Option<Val> {
    if let Ok(d) = Decimal::from_str(s) {
        return Some(Val::Number(d));
    }
    // Floating point values, e.g. 1e-5, 1.7976931348623157E308, NaN or INF. NaN is an error,
    // values that are infinite or too large for a decimal are kept as doubles.
    let f = f64::from_str(s).ok()?;
    if f.is_nan() {
        return None;
    }
    Some(match Decimal::try_from(Double::from(f)) {
        Ok(d) => Val::Number(d),
        Err(_) => Val::Float(f),
    })
}

/// `a / b` for a non-zero `b`. Decimal::checked_div fails for a zero dividend and a divisor
/// with a fractional part, e.g. 0 / 0.1.
fn checked_div(a: Decimal, b: Decimal) -> Option<Decimal> {
    if a == Decimal::from(0) {
        Some(a)
    } else {
        a.checked_div(b)
    }
}

/// A finite double as a decimal, values too small for a decimal are zero.
fn f64_decimal(f: f64) -> Option<Decimal> {
    if !f.is_finite() {
        return None;
    }
    if f.abs() < 1e-18 {
        return Some(Decimal::from(0));
    }
    Decimal::try_from(Double::from(f)).ok()
}

fn parse_val(kind: Kind, s: &str) -> Option<Val> {
    let s = s.trim();
    Some(match kind {
        Kind::YearMonth => {
            let d = YearMonthDuration::from_str(s).ok()?;
            Val::Months(d.years().checked_mul(12)?.checked_add(d.months())?)
        }
        Kind::DayTime => Val::DayTime(DayTimeDuration::from_str(s).ok()?),
        Kind::Duration => Val::Duration(Duration::from_str(s).ok()?),
        Kind::Time => Val::Time(Time::from_str(s).ok()?),
        Kind::Numeric => parse_number(s)?,
        Kind::DateTime | Kind::Date => unreachable!("Not a lexical form"),
    })
}

fn read_column(c: &Column, kind: Kind) -> PolarsResult<Vec<Option<Val>>> {
    Ok(match kind {
        Kind::DateTime => c
            .i64()?
            .iter()
            .map(|x| x.map(Val::Micros))
            .collect(),
        Kind::Date => c.i32()?.iter().map(|x| x.map(Val::Days)).collect(),
        _ => c
            .str()?
            .iter()
            .map(|x| x.and_then(|s| parse_val(kind, s)))
            .collect(),
    })
}

/// The microseconds of a dayTimeDuration, truncated to whole microseconds.
fn day_time_micros(d: DayTimeDuration) -> Option<i64> {
    let micros = d.as_seconds().checked_mul(MICROS_PER_SECOND)?;
    Integer::try_from(micros).ok().map(i64::from)
}

fn micros_day_time(micros: i64) -> Option<DayTimeDuration> {
    Some(DayTimeDuration::new(
        Decimal::from(micros).checked_div(MICROS_PER_SECOND)?,
    ))
}

/// Adds months to a dateTime given as UTC microseconds, in its local time at the offset:
/// the day is the last day of the month if the month is shorter (XPath 9.7.4).
fn add_months_micros(micros: i64, months: i64, offset_minutes: i64) -> Option<i64> {
    let offset = offset_minutes.checked_mul(60 * MICROS_PER_SECOND)?;
    let local = ChronoDateTime::from_timestamp_micros(micros.checked_add(offset)?)?.naive_utc();
    let shifted = add_months_naive(local, months)?;
    shifted
        .and_utc()
        .timestamp_micros()
        .checked_sub(offset)
}

fn add_months_naive(local: NaiveDateTime, months: i64) -> Option<NaiveDateTime> {
    let m = Months::new(u32::try_from(months.unsigned_abs()).ok()?);
    if months >= 0 {
        local.checked_add_months(m)
    } else {
        local.checked_sub_months(m)
    }
}

fn add_months_days(days: i32, months: i64) -> Option<i32> {
    let micros = i64::from(days).checked_mul(MICROS_PER_DAY)?;
    let shifted = add_months_micros(micros, months, 0)?;
    i32::try_from(shifted.div_euclid(MICROS_PER_DAY)).ok()
}

/// A date plus a dayTimeDuration: the date is the dateTime at the start of the day plus the
/// duration, without the time (XPath 9.7.9).
fn add_micros_days(days: i32, micros: i64) -> Option<i32> {
    let total = i64::from(days)
        .checked_mul(MICROS_PER_DAY)?
        .checked_add(micros)?;
    i32::try_from(total.div_euclid(MICROS_PER_DAY)).ok()
}

/// Rounds to the nearest integer, with halves rounded up, as fn:round.
fn round_half_up(d: Decimal) -> Option<i64> {
    let r = d.checked_add(Decimal::from_str("0.5").ok()?)?.checked_floor()?;
    Integer::try_from(r).ok().map(i64::from)
}

/// The lexical form of a decimal with at most as many fractional digits as the decimals of
/// solution mappings have, so that it can be cast to them.
fn decimal_lexical(d: Decimal) -> String {
    let s = d.to_string();
    match s.split_once('.') {
        Some((int, frac)) if frac.len() > default_decimal_scale() => {
            format!("{int}.{}", &frac[..default_decimal_scale()])
        }
        _ => s,
    }
}

fn months_out(months: Option<i64>) -> Option<Out> {
    Some(Out::Str(YearMonthDuration::new(months?).to_string()))
}

fn day_time_out(d: Option<DayTimeDuration>) -> Option<Out> {
    Some(Out::Str(d?.to_string()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Op {
    fn of(expression: &Expression) -> Option<Op> {
        Some(match expression {
            Expression::Add(..) => Op::Add,
            Expression::Subtract(..) => Op::Subtract,
            Expression::Multiply(..) => Op::Multiply,
            Expression::Divide(..) => Op::Divide,
            Expression::Equal(..) => Op::Equal,
            Expression::Less(..) => Op::Less,
            Expression::LessOrEqual(..) => Op::LessOrEqual,
            Expression::Greater(..) => Op::Greater,
            Expression::GreaterOrEqual(..) => Op::GreaterOrEqual,
            _ => return None,
        })
    }

    fn is_ordering(self) -> bool {
        matches!(
            self,
            Op::Less | Op::LessOrEqual | Op::Greater | Op::GreaterOrEqual
        )
    }

    fn compare<T: PartialOrd>(self, l: T, r: T) -> Option<bool> {
        Some(match self {
            Op::Equal => l == r,
            Op::Less => l < r,
            Op::LessOrEqual => l <= r,
            Op::Greater => l > r,
            Op::GreaterOrEqual => l >= r,
            _ => return None,
        })
    }
}

/// The type of the result of an operation, or None if the operation is not defined on the
/// types, which is a type error.
fn result_type(
    op: Op,
    lk: Kind,
    rk: Kind,
    lt: &BaseRDFNodeType,
    rt: &BaseRDFNodeType,
) -> Option<(OutKind, BaseRDFNodeType)> {
    use Kind::*;
    let lit_type = |nn: NamedNodeRef| BaseRDFNodeType::Literal(nn.into_owned(), None);
    let ym = lit_type(xsd::YEAR_MONTH_DURATION);
    let dt = lit_type(xsd::DAY_TIME_DURATION);
    Some(match (op, lk, rk) {
        (Op::Add | Op::Subtract, YearMonth, YearMonth) => (OutKind::Str, ym),
        (Op::Add | Op::Subtract, DayTime, DayTime) => (OutKind::Str, dt),
        (Op::Add | Op::Subtract, DateTime, YearMonth | DayTime) => (OutKind::Micros, lt.clone()),
        (Op::Add, YearMonth | DayTime, DateTime) => (OutKind::Micros, rt.clone()),
        (Op::Add | Op::Subtract, Date, YearMonth | DayTime) => (OutKind::Days, lt.clone()),
        (Op::Add, YearMonth | DayTime, Date) => (OutKind::Days, rt.clone()),
        (Op::Add | Op::Subtract, Time, DayTime) => (OutKind::Str, lt.clone()),
        (Op::Add, DayTime, Time) => (OutKind::Str, rt.clone()),
        (Op::Subtract, DateTime, DateTime) | (Op::Subtract, Date, Date) => (OutKind::Str, dt),
        (Op::Subtract, Time, Time) => (OutKind::Str, dt),
        (Op::Multiply, YearMonth, Numeric) | (Op::Multiply, Numeric, YearMonth) => {
            (OutKind::Str, ym)
        }
        (Op::Multiply, DayTime, Numeric) | (Op::Multiply, Numeric, DayTime) => {
            (OutKind::Str, dt)
        }
        (Op::Divide, YearMonth, Numeric) => (OutKind::Str, ym),
        (Op::Divide, DayTime, Numeric) => (OutKind::Str, dt),
        (Op::Divide, YearMonth, YearMonth) | (Op::Divide, DayTime, DayTime) => {
            (OutKind::Str, lit_type(xsd::DECIMAL))
        }
        // All durations can be compared for equality, only yearMonthDurations and
        // dayTimeDurations can be ordered.
        (Op::Equal, l, r) if l.is_duration() && r.is_duration() => {
            (OutKind::Bool, lit_type(xsd::BOOLEAN))
        }
        (o, YearMonth, YearMonth) | (o, DayTime, DayTime) if o.is_ordering() => {
            (OutKind::Bool, lit_type(xsd::BOOLEAN))
        }
        (o, Time, Time) if o == Op::Equal || o.is_ordering() => {
            (OutKind::Bool, lit_type(xsd::BOOLEAN))
        }
        _ => return None,
    })
}

/// Evaluates an operation on two values, None is an error.
fn apply(op: Op, l: Val, r: Val, l_offset: i64, r_offset: i64) -> Option<Out> {
    use Val::*;
    match (op, l, r) {
        (Op::Add, Months(a), Months(b)) => months_out(a.checked_add(b)),
        (Op::Subtract, Months(a), Months(b)) => months_out(a.checked_sub(b)),
        (Op::Add, DayTime(a), DayTime(b)) => day_time_out(a.checked_add(b)),
        (Op::Subtract, DayTime(a), DayTime(b)) => day_time_out(a.checked_sub(b)),

        (Op::Add, Micros(a), Months(m)) => Some(Out::I64(add_months_micros(a, m, l_offset)?)),
        (Op::Add, Months(m), Micros(a)) => Some(Out::I64(add_months_micros(a, m, r_offset)?)),
        (Op::Subtract, Micros(a), Months(m)) => Some(Out::I64(add_months_micros(
            a,
            m.checked_neg()?,
            l_offset,
        )?)),
        (Op::Add, Micros(a), DayTime(d)) | (Op::Add, DayTime(d), Micros(a)) => {
            Some(Out::I64(a.checked_add(day_time_micros(d)?)?))
        }
        (Op::Subtract, Micros(a), DayTime(d)) => {
            Some(Out::I64(a.checked_sub(day_time_micros(d)?)?))
        }
        // Values without a time zone are compared as if they were in UTC, the implicit
        // time zone.
        (Op::Subtract, Micros(a), Micros(b)) => day_time_out(micros_day_time(a.checked_sub(b)?)),

        (Op::Add, Days(a), Months(m)) | (Op::Add, Months(m), Days(a)) => {
            Some(Out::I32(add_months_days(a, m)?))
        }
        (Op::Subtract, Days(a), Months(m)) => Some(Out::I32(add_months_days(a, m.checked_neg()?)?)),
        (Op::Add, Days(a), DayTime(d)) | (Op::Add, DayTime(d), Days(a)) => {
            Some(Out::I32(add_micros_days(a, day_time_micros(d)?)?))
        }
        (Op::Subtract, Days(a), DayTime(d)) => {
            Some(Out::I32(add_micros_days(a, day_time_micros(d)?.checked_neg()?)?))
        }
        (Op::Subtract, Days(a), Days(b)) => day_time_out(Some(DayTimeDuration::new(
            i64::from(a).checked_sub(i64::from(b))?.checked_mul(86_400)?,
        ))),

        (Op::Add, Time(t), DayTime(d)) | (Op::Add, DayTime(d), Time(t)) => {
            Some(Out::Str(t.checked_add_day_time_duration(d)?.to_string()))
        }
        (Op::Subtract, Time(t), DayTime(d)) => {
            Some(Out::Str(t.checked_sub_day_time_duration(d)?.to_string()))
        }
        (Op::Subtract, Time(a), Time(b)) => day_time_out(a.checked_sub(b)),

        // op:multiply-yearMonthDuration and op:divide-yearMonthDuration round to the nearest
        // month, op:divide-*-by-* is an error for a zero divisor (XPath 8.4).
        (Op::Multiply, Months(m), Number(n)) | (Op::Multiply, Number(n), Months(m)) => {
            months_out(round_half_up(Decimal::from(m).checked_mul(n)?))
        }
        (Op::Divide, Months(m), Number(n)) => {
            if n == Decimal::from(0) {
                return None;
            }
            months_out(round_half_up(checked_div(Decimal::from(m), n)?))
        }
        (Op::Multiply, DayTime(d), Number(n)) | (Op::Multiply, Number(n), DayTime(d)) => {
            day_time_out(Some(DayTimeDuration::new(d.as_seconds().checked_mul(n)?)))
        }
        (Op::Divide, DayTime(d), Number(n)) => {
            if n == Decimal::from(0) {
                return None;
            }
            day_time_out(Some(DayTimeDuration::new(checked_div(d.as_seconds(), n)?)))
        }
        // Doubles that are infinite or too large for a decimal: a zero duration times a finite
        // double is zero, other products overflow. The quotients are close to zero.
        (Op::Multiply, Months(m), Float(f)) | (Op::Multiply, Float(f), Months(m)) => {
            (m == 0 && f.is_finite()).then(|| months_out(Some(0)))?
        }
        (Op::Multiply, DayTime(d), Float(f)) | (Op::Multiply, Float(f), DayTime(d)) => {
            (d.as_seconds() == Decimal::from(0) && f.is_finite()).then(|| day_time_out(Some(d)))?
        }
        (Op::Divide, Months(m), Float(f)) => {
            months_out(round_half_up(f64_decimal(m as f64 / f)?))
        }
        (Op::Divide, DayTime(d), Float(f)) => {
            let seconds = f64::from(Double::from(d.as_seconds()));
            day_time_out(Some(DayTimeDuration::new(f64_decimal(seconds / f)?)))
        }
        (Op::Divide, Months(a), Months(b)) => {
            if b == 0 {
                return None;
            }
            Some(Out::Str(decimal_lexical(
                checked_div(Decimal::from(a), Decimal::from(b))?,
            )))
        }
        (Op::Divide, DayTime(a), DayTime(b)) => {
            if b.as_seconds() == Decimal::from(0) {
                return None;
            }
            Some(Out::Str(decimal_lexical(
                checked_div(a.as_seconds(), b.as_seconds())?,
            )))
        }

        (op, Months(a), Months(b)) => Some(Out::Bool(op.compare(a, b)?)),
        (op, DayTime(a), DayTime(b)) => Some(Out::Bool(op.compare(a, b)?)),
        (op, Time(a), Time(b)) => Some(Out::Bool(op.compare(a, b)?)),
        // Durations are equal when they have the same months and seconds.
        (Op::Equal, a, b) => Some(Out::Bool(as_duration(a)? == as_duration(b)?)),
        _ => None,
    }
}

fn as_duration(v: Val) -> Option<Duration> {
    Some(match v {
        Val::Months(m) => YearMonthDuration::new(m).into(),
        Val::DayTime(d) => d.into(),
        Val::Duration(d) => d,
        _ => return None,
    })
}

fn build_column(out_kind: OutKind, values: Vec<Option<Out>>) -> Column {
    let name = "temporal".into();
    match out_kind {
        OutKind::Str => StringChunked::from_iter_options(
            name,
            values.into_iter().map(|x| match x {
                Some(Out::Str(s)) => Some(s),
                _ => None,
            }),
        )
        .into_column(),
        OutKind::Micros => Int64Chunked::from_iter_options(
            name,
            values.into_iter().map(|x| match x {
                Some(Out::I64(v)) => Some(v),
                _ => None,
            }),
        )
        .into_column(),
        OutKind::Days => Int32Chunked::from_iter_options(
            name,
            values.into_iter().map(|x| match x {
                Some(Out::I32(v)) => Some(v),
                _ => None,
            }),
        )
        .into_column(),
        OutKind::Bool => BooleanChunked::from_iter_options(
            name,
            values.into_iter().map(|x| match x {
                Some(Out::Bool(v)) => Some(v),
                _ => None,
            }),
        )
        .into_column(),
    }
}

fn null_of(dtype: DataType) -> Expr {
    lit(LiteralValue::untyped_null()).cast(dtype)
}

/// The expression of an operand, decoded if it is categorical, as the physical type it is
/// read as.
fn operand_expr(
    e: Expr,
    kind: Kind,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    global_cats: LockedCats,
) -> Expr {
    let e = maybe_decode_expr(e, t, s, global_cats);
    match kind {
        // UTC instants as microseconds since the epoch, the default time unit.
        Kind::DateTime => e
            .cast(DataType::Datetime(
                default_time_unit(),
                Some(default_time_zone()),
            ))
            .cast(DataType::Int64),
        Kind::Date => e.cast(DataType::Date).cast(DataType::Int32),
        k => e.cast(k.input_dtype()),
    }
}

/// An operation where at least one of the operands is a duration, date, dateTime or time:
/// `+`, `-`, `*` or `/`, or a comparison of two durations or two times with `=` or an ordering
/// operator. None if the operation is not one of these, which is then evaluated as before,
/// otherwise the expression for the result and its type. The result is unbound for operations
/// that are not defined on the types.
#[allow(clippy::too_many_arguments)]
pub fn temporal_operation(
    e_left: Expr,
    e_right: Expr,
    lt: &BaseRDFNodeType,
    ls: &BaseCatState,
    rt: &BaseRDFNodeType,
    rs: &BaseCatState,
    expression: &Expression,
    global_cats: LockedCats,
) -> Option<(Expr, RDFNodeState)> {
    temporal_op(
        e_left,
        e_right,
        lt,
        ls,
        rt,
        rs,
        Op::of(expression)?,
        global_cats,
    )
}

/// Value equality of two durations, which are equal when they have the same months and
/// seconds (op:duration-equal), or None if one of them is not a duration. False if one of the
/// values is missing, and unbound if one of them is invalid.
pub fn duration_equal_expr(
    e_left: Expr,
    e_right: Expr,
    lt: &BaseRDFNodeType,
    ls: &BaseCatState,
    rt: &BaseRDFNodeType,
    rs: &BaseCatState,
    global_cats: LockedCats,
) -> Option<Expr> {
    if !(is_duration_type(lt) && is_duration_type(rt)) {
        return None;
    }
    let present = e_left.clone().is_not_null().and(e_right.clone().is_not_null());
    let (e, _) = temporal_op(e_left, e_right, lt, ls, rt, rs, Op::Equal, global_cats)?;
    Some(present.and(e))
}

#[allow(clippy::too_many_arguments)]
fn temporal_op(
    e_left: Expr,
    e_right: Expr,
    lt: &BaseRDFNodeType,
    ls: &BaseCatState,
    rt: &BaseRDFNodeType,
    rs: &BaseCatState,
    op: Op,
    global_cats: LockedCats,
) -> Option<(Expr, RDFNodeState)> {
    let (BaseRDFNodeType::Literal(l_nn, l_offset), BaseRDFNodeType::Literal(r_nn, r_offset)) =
        (lt, rt)
    else {
        return None;
    };
    let lk = Kind::of(l_nn.as_ref());
    let rk = Kind::of(r_nn.as_ref());
    if !lk.is_some_and(Kind::is_temporal) && !rk.is_some_and(Kind::is_temporal) {
        return None;
    }
    // Comparisons of other values, e.g. of dateTimes, are evaluated as before.
    if !matches!(op, Op::Add | Op::Subtract | Op::Multiply | Op::Divide) {
        let both = |f: fn(Kind) -> bool| lk.is_some_and(f) && rk.is_some_and(f);
        if !(both(Kind::is_duration) || both(|k| k == Kind::Time)) {
            return None;
        }
    }
    let error = || {
        let t = if matches!(op, Op::Add | Op::Subtract | Op::Multiply | Op::Divide) {
            BaseRDFNodeType::None
        } else {
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
        };
        Some((
            null_of(t.default_input_polars_data_type()),
            t.into_default_input_rdf_node_state(),
        ))
    };
    let (Some(lk), Some(rk)) = (lk, rk) else {
        return error();
    };
    let Some((out_kind, out_type)) = result_type(op, lk, rk, lt, rt) else {
        return error();
    };
    let l_offset = i64::from(l_offset.unwrap_or(0));
    let r_offset = i64::from(r_offset.unwrap_or(0));
    let left = operand_expr(e_left, lk, lt, ls, global_cats.clone());
    let right = operand_expr(e_right, rk, rt, rs, global_cats);
    let out_dtype = out_kind.dtype();
    let f = move |cols: &mut [Column]| -> PolarsResult<Column> {
        let lv = read_column(&cols[0], lk)?;
        let rv = read_column(&cols[1], rk)?;
        // Constants are columns of length one, which are broadcast.
        let n = lv.len().max(rv.len());
        let get = |v: &Vec<Option<Val>>, i: usize| -> Option<Val> {
            if v.len() == 1 {
                v[0]
            } else {
                v.get(i).copied().flatten()
            }
        };
        let values: Vec<Option<Out>> = (0..n)
            .map(|i| match (get(&lv, i), get(&rv, i)) {
                (Some(a), Some(b)) => apply(op, a, b, l_offset, r_offset),
                _ => None,
            })
            .collect();
        let mut out = build_column(out_kind, values);
        out.rename(cols[0].name().clone());
        Ok(out)
    };
    let mut e = left.map_many(f, &[right], move |_: &Schema, fields: &[Field]| {
        Ok(Field::new(fields[0].name().clone(), out_dtype.clone()))
    });
    e = match out_kind {
        OutKind::Micros => e.cast(DataType::Datetime(
            default_time_unit(),
            Some(default_time_zone()),
        )),
        OutKind::Days => e.cast(DataType::Date),
        OutKind::Str if out_type.is_lit_type(xsd::DECIMAL) => e.cast(default_decimal_type()),
        _ => e,
    };
    Some((e, out_type.into_default_input_rdf_node_state()))
}

/// The datatypes whose values can be = to a value of the datatype, other than the datatype
/// itself: all durations are compared by their months and seconds.
pub fn equal_duration_datatypes(dt: NamedNodeRef) -> Vec<NamedNode> {
    if is_duration_datatype(dt) {
        [xsd::DURATION, xsd::YEAR_MONTH_DURATION, xsd::DAY_TIME_DURATION]
            .into_iter()
            .filter(|x| *x != dt)
            .map(|x| x.into_owned())
            .collect()
    } else {
        vec![]
    }
}

fn total_months(d: Duration) -> Option<i64> {
    d.years().checked_mul(12)?.checked_add(d.months())
}

fn total_seconds(d: Duration) -> Option<Decimal> {
    let whole = d
        .days()
        .checked_mul(86_400)?
        .checked_add(d.hours().checked_mul(3_600)?)?
        .checked_add(d.minutes().checked_mul(60)?)?;
    Decimal::from(whole).checked_add(d.seconds())
}

/// Casts a lexical form or a duration to a duration type (XPath 19.1.6): the lexical form
/// must be valid for the target type, and when casting between durations, the part of the
/// value that the target type cannot represent is dropped.
pub fn cast_to_duration(value: &str, src: NamedNodeRef, trg: NamedNodeRef) -> Option<String> {
    let value = value.trim();
    let duration = if src == xsd::STRING {
        match trg {
            xsd::YEAR_MONTH_DURATION => Duration::from(YearMonthDuration::from_str(value).ok()?),
            xsd::DAY_TIME_DURATION => Duration::from(DayTimeDuration::from_str(value).ok()?),
            _ => Duration::from_str(value).ok()?,
        }
    } else if is_duration_datatype(src) {
        Duration::from_str(value).ok()?
    } else {
        return None;
    };
    Some(match trg {
        xsd::YEAR_MONTH_DURATION => YearMonthDuration::new(total_months(duration)?).to_string(),
        xsd::DAY_TIME_DURATION => DayTimeDuration::new(total_seconds(duration)?).to_string(),
        _ => duration.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ym(s: &str) -> Val {
        parse_val(Kind::YearMonth, s).unwrap()
    }
    fn dt(s: &str) -> Val {
        parse_val(Kind::DayTime, s).unwrap()
    }
    fn num(s: &str) -> Val {
        parse_val(Kind::Numeric, s).unwrap()
    }
    fn s(o: Option<Out>) -> String {
        match o {
            Some(Out::Str(s)) => s,
            Some(Out::I64(v)) => v.to_string(),
            Some(Out::I32(v)) => v.to_string(),
            Some(Out::Bool(v)) => v.to_string(),
            None => "error".to_string(),
        }
    }

    #[test]
    fn test_duration_arithmetic() {
        assert_eq!(s(apply(Op::Add, ym("P2Y11M"), ym("P3Y3M"), 0, 0)), "P6Y2M");
        assert_eq!(s(apply(Op::Subtract, ym("P2Y11M"), ym("P3Y3M"), 0, 0)), "-P4M");
        assert_eq!(s(apply(Op::Add, dt("P2DT12H5M"), dt("P5DT12H"), 0, 0)), "P8DT5M");
        assert_eq!(s(apply(Op::Multiply, ym("P2Y11M"), num("2.3"), 0, 0)), "P6Y9M");
        assert_eq!(s(apply(Op::Divide, ym("P2Y11M"), num("1.5"), 0, 0)), "P1Y11M");
        assert_eq!(s(apply(Op::Multiply, dt("PT2H10M"), num("2.1"), 0, 0)), "PT4H33M");
        assert_eq!(s(apply(Op::Divide, dt("P1DT2H30M10.5S"), num("1.5"), 0, 0)), "PT17H40M7S");
        assert_eq!(s(apply(Op::Divide, ym("P3Y4M"), ym("-P1Y4M"), 0, 0)), "-2.5");
        assert_eq!(s(apply(Op::Divide, ym("P3Y4M"), num("0"), 0, 0)), "error");
        // NaN is not a number to multiply with
        assert!(parse_val(Kind::Numeric, "NaN").is_none());
        assert_eq!(s(apply(Op::Equal, ym("P1Y"), dt("PT0S"), 0, 0)), "false");
        assert_eq!(s(apply(Op::Equal, ym("P0M"), dt("PT0S"), 0, 0)), "true");
        assert_eq!(s(apply(Op::Less, dt("PT1H"), dt("PT61M"), 0, 0)), "true");
    }

    #[test]
    fn test_date_time_arithmetic() {
        // 2000-10-30T11:12:00 + P1Y2M = 2001-12-30T11:12:00
        let a = NaiveDateTime::parse_from_str("2000-10-30T11:12:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let b = NaiveDateTime::parse_from_str("2001-12-30T11:12:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(s(apply(Op::Add, Val::Micros(a), ym("P1Y2M"), 0, 0)), b.to_string());
        // The last day of the month
        let jan31 = NaiveDateTime::parse_from_str("2001-01-31T00:00:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let feb28 = NaiveDateTime::parse_from_str("2001-02-28T00:00:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(s(apply(Op::Add, Val::Micros(jan31), ym("P1M"), 0, 0)), feb28.to_string());
        assert_eq!(
            s(apply(Op::Subtract, Val::Micros(b), Val::Micros(a), 0, 0)),
            "P426D"
        );
        assert_eq!(s(apply(Op::Subtract, Val::Days(10), Val::Days(3), 0, 0)), "P7D");
        assert_eq!(s(apply(Op::Subtract, Val::Days(10), dt("PT1H"), 0, 0)), "9");
        assert_eq!(
            s(apply(
                Op::Add,
                parse_val(Kind::Time, "11:12:00").unwrap(),
                dt("P3DT1H15M"),
                0,
                0
            )),
            "12:27:00"
        );
    }

    #[test]
    fn test_cast_to_duration() {
        assert_eq!(
            cast_to_duration("P1Y2M3DT4H", xsd::DURATION, xsd::YEAR_MONTH_DURATION).unwrap(),
            "P1Y2M"
        );
        assert_eq!(
            cast_to_duration("P1Y2M3DT4H", xsd::DURATION, xsd::DAY_TIME_DURATION).unwrap(),
            "P3DT4H"
        );
        assert_eq!(
            cast_to_duration("P1D", xsd::STRING, xsd::YEAR_MONTH_DURATION),
            None
        );
        assert_eq!(
            cast_to_duration("PT36H", xsd::STRING, xsd::DAY_TIME_DURATION).unwrap(),
            "P1DT12H"
        );
    }
}
