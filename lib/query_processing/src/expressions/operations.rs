use crate::expressions::coalesce_expressions;
use crate::expressions::durations::temporal_operation;
use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::prelude::{col, lit, when, DataType, Expr, LiteralValue};
use representation::cats::LockedCats;
use representation::solution_mapping::BaseCatState;
use representation::{
    literal_is_boolean, literal_is_date, literal_is_datetime, literal_is_numeric,
    literal_is_string, BaseRDFNodeType, RDFNodeState,
};
use spargebra::algebra::Expression;
use std::cmp;
use std::ops::{Add, Mul, Sub};

/// The literal parts of a column: the expression, type and state of the values of each literal
/// type.
pub(crate) fn literal_parts(
    c: &str,
    t: &RDFNodeState,
) -> Vec<(Expr, BaseRDFNodeType, BaseCatState)> {
    let mut parts = vec![];
    if t.is_multi() {
        for (bt, bs) in &t.map {
            if matches!(bt, BaseRDFNodeType::Literal(..)) {
                parts.push((
                    col(c).struct_().field_by_name(&bt.field_col_name()),
                    bt.clone(),
                    bs.clone(),
                ));
            }
        }
    } else if t.is_literal() {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        parts.push((col(c), bt.clone(), bs.clone()));
    }
    parts
}

pub fn typed_numerical_operation(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    expression: &Expression,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    let mut ops = vec![];
    let right_parts = literal_parts(right_col, right_type);
    for (le, lt, ls) in literal_parts(left_col, left_type) {
        for (re, rt, rs) in &right_parts {
            let (BaseRDFNodeType::Literal(lt_nn, _), BaseRDFNodeType::Literal(rt_nn, _)) =
                (&lt, rt)
            else {
                unreachable!("Should never happen")
            };
            // Durations, dates, dateTimes and times (XPath 8.4 and 9.7)
            if let Some(o) = temporal_operation(
                le.clone(),
                re.clone(),
                &lt,
                &ls,
                rt,
                rs,
                expression,
                global_cats.clone(),
            ) {
                ops.push(o);
            } else {
                ops.push(op(
                    le.clone(),
                    re.clone(),
                    lt_nn.as_ref(),
                    rt_nn.as_ref(),
                    expression,
                ));
            }
        }
    }
    if ops.is_empty() {
        let t = BaseRDFNodeType::None;
        let e = lit(LiteralValue::untyped_null()).cast(t.default_input_polars_data_type());
        (e, t.into_default_input_rdf_node_state())
    } else {
        let (exprs, types): (Vec<_>, Vec<_>) = ops.into_iter().unzip();
        coalesce_expressions(exprs, types, global_cats)
    }
}

fn op(
    e_left: Expr,
    e_right: Expr,
    dt_left: NamedNodeRef,
    dt_right: NamedNodeRef,
    expression: &Expression,
) -> (Expr, RDFNodeState) {
    if matches!(expression, Expression::Divide(..)) {
        return divide(e_left, e_right, dt_left, dt_right);
    }
    let mut e = match expression {
        Expression::Multiply(_, _) => e_left.clone().mul(e_right.clone()),
        Expression::Add(_, _) => e_left.clone().add(e_right.clone()),
        Expression::Subtract(_, _) => e_left.clone().sub(e_right.clone()),
        _ => panic!("Should never happen"),
    };
    let compat = compatible_operation(expression, dt_left, dt_right);
    if compat {
        let t = match expression {
            Expression::Multiply(_, _) => greatest_common_dt(dt_left, dt_right),
            Expression::Add(_, _) => greatest_common_dt(dt_left, dt_right),
            Expression::Subtract(_, _) => greatest_common_dt(dt_left, dt_right),
            _ => unreachable!("Should never happen"),
        };
        e = e.cast(t.default_input_polars_data_type());
        (e, t.into_default_input_rdf_node_state())
    } else {
        let t = BaseRDFNodeType::None;
        (
            lit(LiteralValue::untyped_null()).cast(t.default_input_polars_data_type()),
            t.into_default_input_rdf_node_state(),
        )
    }
}

/// Numeric type promotion levels (SPARQL 1.1 section 17.3, XPath op:numeric-*):
/// integer types < decimal < float < double. None if the type is not numeric.
pub(crate) fn numeric_promotion_level(t: NamedNodeRef) -> Option<u8> {
    if !literal_is_numeric(t) {
        return None;
    }
    Some(match t {
        xsd::DOUBLE => 3,
        xsd::FLOAT => 2,
        xsd::DECIMAL => 1,
        _ => 0,
    })
}

/// op:numeric-divide: the operands are promoted to a common type, and integer / integer is
/// xsd:decimal. Division by zero is an error (unbound) for xsd:decimal, while xsd:float and
/// xsd:double follow IEEE 754 (INF, -INF or NaN).
fn divide(
    e_left: Expr,
    e_right: Expr,
    dt_left: NamedNodeRef,
    dt_right: NamedNodeRef,
) -> (Expr, RDFNodeState) {
    let (Some(level_left), Some(level_right)) = (
        numeric_promotion_level(dt_left),
        numeric_promotion_level(dt_right),
    ) else {
        let t = BaseRDFNodeType::None;
        return (
            lit(LiteralValue::untyped_null()).cast(t.default_input_polars_data_type()),
            t.into_default_input_rdf_node_state(),
        );
    };
    let t = match cmp::max(level_left, level_right) {
        3 => xsd::DOUBLE,
        2 => xsd::FLOAT,
        _ => xsd::DECIMAL,
    };
    let t = BaseRDFNodeType::Literal(t.into_owned(), None);
    let dtype = t.default_input_polars_data_type();
    let e = if dtype == DataType::Float32 {
        e_left.cast(DataType::Float32) / e_right.cast(DataType::Float32)
    } else if dtype == DataType::Float64 {
        e_left.cast(DataType::Float64) / e_right.cast(DataType::Float64)
    } else {
        // Decimal division is computed in floating point and rounded to the decimal scale
        let right = e_right.cast(DataType::Float64);
        when(right.clone().eq(lit(0.0)))
            .then(lit(LiteralValue::untyped_null()).cast(dtype.clone()))
            .otherwise((e_left.cast(DataType::Float64) / right).cast(dtype.clone()))
    };
    (e, t.into_default_input_rdf_node_state())
}

pub fn compatible_operation(expression: &Expression, l1: NamedNodeRef, l2: NamedNodeRef) -> bool {
    match expression {
        Expression::Equal(..)
        | Expression::LessOrEqual(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Greater(..)
        | Expression::Less(..) => {
            (literal_is_numeric(l1) && literal_is_numeric(l2))
                || (literal_is_boolean(l1) && literal_is_boolean(l2))
                || (literal_is_string(l1) && literal_is_string(l2))
                || (literal_is_datetime(l1) && literal_is_datetime(l2))
                || (literal_is_date(l1) && literal_is_date(l2))
        }
        Expression::Or(..) | Expression::And(..) => {
            literal_is_boolean(l1) && literal_is_boolean(l2)
        }
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..) => literal_is_numeric(l1) && literal_is_numeric(l2),
        _ => todo!(),
    }
}

fn greatest_common_dt(dt_left: NamedNodeRef, dt_right: NamedNodeRef) -> BaseRDFNodeType {
    let bits_left = n_bits(dt_left);
    let bits_right = n_bits(dt_right);
    let decimal_left = is_decimal(dt_left);
    let decimal_right = is_decimal(dt_right);

    let use_bits = cmp::max(bits_left, bits_right);
    let decimal = decimal_left || decimal_right;
    let use_type = gen_type(use_bits, decimal, dt_left, dt_right);
    BaseRDFNodeType::Literal(use_type.into_owned(), None)
}

fn gen_type(
    bits: u8,
    decimal: bool,
    left: NamedNodeRef,
    right: NamedNodeRef,
) -> NamedNodeRef<'static> {
    if decimal {
        if bits == 32 {
            xsd::FLOAT
        } else if bits == 64 {
            if left == xsd::DECIMAL || right == xsd::DECIMAL {
                xsd::DECIMAL
            } else {
                xsd::DOUBLE
            }
        } else {
            todo!()
        }
    } else {
        if bits == 1 {
            xsd::BOOLEAN
        } else if bits == 8 {
            xsd::BYTE
        } else if bits == 16 {
            xsd::SHORT
        } else if bits == 32 {
            xsd::INT
        } else if bits == 64 {
            xsd::INTEGER
        } else {
            todo!()
        }
    }
}

fn n_bits(t: NamedNodeRef) -> u8 {
    match t {
        xsd::DOUBLE
        | xsd::DECIMAL
        | xsd::LONG
        | xsd::UNSIGNED_LONG
        | xsd::POSITIVE_INTEGER
        | xsd::NON_NEGATIVE_INTEGER
        | xsd::NEGATIVE_INTEGER
        | xsd::INTEGER => 64,
        xsd::FLOAT | xsd::INT | xsd::UNSIGNED_INT => 32,
        xsd::SHORT | xsd::UNSIGNED_SHORT => 16,
        xsd::BYTE | xsd::UNSIGNED_BYTE => 8,
        xsd::BOOLEAN => 1,
        _ => todo!("nbytes {}", t),
    }
}

fn is_decimal(t: NamedNodeRef) -> bool {
    matches!(t, xsd::FLOAT | xsd::DOUBLE | xsd::DECIMAL)
}
