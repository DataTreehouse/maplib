//! The trigonometric and exponential functions of XPath and XQuery Functions and Operators 3.1,
//! section 4.8 (https://www.w3.org/TR/xpath-functions-31/#trigonometry), in the namespace
//! `http://www.w3.org/2005/xpath-functions/math#`: math:pi, math:exp, math:exp10, math:log,
//! math:log10, math:pow, math:sqrt, math:sin, math:cos, math:tan, math:asin, math:acos,
//! math:atan and math:atan2.
//!
//! As the specification says, every argument is converted to xs:double (the numeric types by
//! casting), the result is an xs:double, an empty argument gives an empty result, and the
//! operations follow IEEE 754-2008 (the special cases of -0, INF and NaN), which Rust's f64
//! functions implement. math:pow also allows an integer exponent for a negative base. An argument
//! that is not numeric is a type error, which leaves the result unbound, as for ABS and the other
//! numeric functions of SPARQL.

use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{
    coalesce, col, lit, ChunkApply, Column, DataType, Expr, Field, IntoColumn, LiteralValue,
    PolarsResult,
    Schema,
};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub const XPATH_MATH: &str = "http://www.w3.org/2005/xpath-functions/math#";

/// The functions of the namespace, with their number of arguments.
const FUNCTIONS: [(&str, usize); 14] = [
    ("pi", 0),
    ("exp", 1),
    ("exp10", 1),
    ("log", 1),
    ("log10", 1),
    ("pow", 2),
    ("sqrt", 1),
    ("sin", 1),
    ("cos", 1),
    ("tan", 1),
    ("asin", 1),
    ("acos", 1),
    ("atan", 1),
    ("atan2", 2),
];

/// The local name and arity of an XPath math function, if `nn` is one.
pub fn xpath_math_function(nn: &NamedNode) -> Option<(&'static str, usize)> {
    let local = nn.as_str().strip_prefix(XPATH_MATH)?;
    FUNCTIONS.iter().find(|(f, _)| *f == local).copied()
}

fn unary(name: &str) -> fn(f64) -> f64 {
    match name {
        "exp" => f64::exp,
        // 10^x, exact for integral x within the range of doubles
        "exp10" => |x| 10f64.powf(x),
        "log" => f64::ln,
        "log10" => f64::log10,
        "sqrt" => f64::sqrt,
        "sin" => f64::sin,
        "cos" => f64::cos,
        "tan" => f64::tan,
        "asin" => f64::asin,
        "acos" => f64::acos,
        "atan" => f64::atan,
        _ => unreachable!("not a unary XPath math function: {name}"),
    }
}

/// math:pow(x, y): IEEE 754-2008 pow, which is what Rust's powf implements (pow(-1, ±INF) = 1,
/// pow(x, 0) = 1 for any x, also NaN, pow(-8, 1/3) = NaN, ...).
fn pow(x: f64, y: f64) -> f64 {
    x.powf(y)
}

/// math:atan2(y, x): the angle in radians, in -π..π, of the point (x, y).
fn atan2(y: f64, x: f64) -> f64 {
    y.atan2(x)
}

/// The argument in `context` as a column of doubles, null where it is not numeric; None when it
/// has no numeric value at all.
fn as_double(solution_mappings: &SolutionMappings, context: &Context) -> Option<Expr> {
    let state = solution_mappings.rdf_node_types.get(context.as_str())?;
    if state.is_multi() {
        let mut fields: Vec<_> = state.map.keys().filter(|t| t.is_numeric()).collect();
        // a stable order of the struct's fields
        fields.sort_by_key(|t| t.field_col_name());
        let parts: Vec<Expr> = fields
            .into_iter()
            .map(|t| {
                col(context.as_str())
                    .struct_()
                    .field_by_name(&t.field_col_name())
                    .cast(DataType::Float64)
            })
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(coalesce(&parts))
        }
    } else if state.is_numeric() {
        Some(col(context.as_str()).cast(DataType::Float64))
    } else {
        None
    }
}

fn float_field(_: &Schema, fields: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(fields[0].name().clone(), DataType::Float64))
}

/// Evaluates the XPath math function `name` (see xpath_math_function) on the arguments.
pub fn xpath_math(
    name: &str,
    arity: usize,
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != arity {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            arity.to_string(),
        ));
    }
    let operands: Option<Vec<Expr>> = (0..arity)
        .map(|i| as_double(&solution_mappings, args_contexts.get(&i).unwrap()))
        .collect();
    let expr = match (name, operands) {
        ("pi", _) => Some(lit(std::f64::consts::PI)),
        (_, None) => None,
        (_, Some(mut ops)) if arity == 1 => {
            let f = unary(name);
            let x = ops.pop().unwrap();
            Some(x.map(
                move |c: Column| {
                    let out = c.f64()?.apply_values(f);
                    Ok(out.into_column())
                },
                |_, field| Ok(Field::new(field.name().clone(), DataType::Float64)),
            ))
        }
        (_, Some(ops)) => {
            let f: fn(f64, f64) -> f64 = if name == "pow" { pow } else { atan2 };
            let (a, b) = (ops[0].clone(), ops[1].clone());
            Some(a.map_many(
                move |cols: &mut [Column]| {
                    let (x, y) = (cols[0].f64()?, cols[1].f64()?);
                    // A constant is a column of length one, which is broadcast.
                    let n = x.len().max(y.len());
                    let at = |c: &polars::prelude::Float64Chunked, i: usize| {
                        if c.len() == 1 {
                            c.get(0)
                        } else {
                            c.get(i)
                        }
                    };
                    let out: polars::prelude::Float64Chunked = (0..n)
                        .map(|i| match (at(x, i), at(y, i)) {
                            (Some(a), Some(b)) => Some(f(a, b)),
                            _ => None,
                        })
                        .collect();
                    Ok(out.with_name(cols[0].name().clone()).into_column())
                },
                &[b],
                float_field,
            ))
        }
    };
    let (expr, t) = match expr {
        Some(e) => (e, BaseRDFNodeType::Literal(xsd::DOUBLE.into_owned(), None)),
        None => (
            lit(LiteralValue::untyped_null()).cast(BaseRDFNodeType::None.default_input_polars_data_type()),
            BaseRDFNodeType::None,
        ),
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    let state: RDFNodeState = t.into_default_input_rdf_node_state();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), state);
    Ok(solution_mappings)
}
