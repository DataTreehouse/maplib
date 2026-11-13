use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{coalesce, col, lit, when, DataType, Expr, LiteralValue};
use representation::query_context::Context;
use representation::rdf_to_polars::{default_time_unit, default_time_zone};
use representation::solution_mapping::SolutionMappings;
use representation::{
    format_time_zone_offset, literal_is_datetime, BaseRDFNodeType, RDFNodeState,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

/// The dateTime types of a column, with the expression for the values of each type.
fn datetime_parts(c: &str, t: &RDFNodeState) -> Vec<(Expr, BaseRDFNodeType)> {
    let is_datetime =
        |bt: &BaseRDFNodeType| matches!(bt, BaseRDFNodeType::Literal(l, _) if literal_is_datetime(l.as_ref()));
    if t.is_multi() {
        let mut types: Vec<_> = t.map.keys().filter(|bt| is_datetime(bt)).collect();
        types.sort();
        types
            .into_iter()
            .map(|bt| {
                (
                    col(c).struct_().field_by_name(&bt.field_col_name()),
                    bt.clone(),
                )
            })
            .collect()
    } else if let Some(bt) = t.get_base_type() {
        if is_datetime(bt) {
            vec![(col(c), bt.clone())]
        } else {
            vec![]
        }
    } else {
        vec![]
    }
}

fn null_of(dtype: DataType) -> Expr {
    lit(LiteralValue::untyped_null()).cast(dtype)
}

/// The dateTime (and xsd:date) values of a column as local times at their time zone offsets.
/// Values are stored as UTC instants, so values with an offset are shifted by it.
/// Values without a time zone are local times already.
pub fn local_datetime_expr(c: &str, t: &RDFNodeState) -> Expr {
    // xsd:date values have no time zone in the type, so they are used as they are.
    let mut parts_with_types = datetime_parts(c, t);
    if t.is_multi() {
        if let Some(bt) = t.map.keys().find(|bt| bt.is_lit_type(xsd::DATE)) {
            parts_with_types.push((col(c).struct_().field_by_name(&bt.field_col_name()), bt.clone()));
        }
    } else if let Some(bt) = t.get_base_type() {
        if bt.is_lit_type(xsd::DATE) {
            parts_with_types.push((col(c), bt.clone()));
        }
    }
    let parts: Vec<_> = parts_with_types
        .into_iter()
        .map(|(e, bt)| match bt.time_zone_offset() {
            Some(offset) if offset != 0 => {
                let datetime_type =
                    DataType::Datetime(default_time_unit(), Some(default_time_zone()));
                // Values have the default time unit, microseconds.
                (e.cast(datetime_type.clone()).cast(DataType::Int64)
                    + lit(offset as i64 * 60 * 1_000_000))
                .cast(datetime_type)
            }
            _ => e,
        })
        .collect();
    match parts.len() {
        // Not a dateTime, which gives a type error and thus an unbound value.
        0 => null_of(BaseRDFNodeType::literal(xsd::DATE_TIME.into_owned()).default_input_polars_data_type()),
        1 => parts.into_iter().next().unwrap(),
        _ => coalesce(&parts),
    }
}

/// The xsd:dayTimeDuration lexical form of an offset in minutes, e.g. -480 is "-PT8H".
fn offset_duration(offset: i16) -> String {
    if offset == 0 {
        return "PT0S".to_string();
    }
    let sign = if offset < 0 { "-" } else { "" };
    let abs = offset.unsigned_abs();
    let (hours, minutes) = (abs / 60, abs % 60);
    let mut s = format!("{sign}PT");
    if hours > 0 {
        s.push_str(&format!("{hours}H"));
    }
    if minutes > 0 {
        s.push_str(&format!("{minutes}M"));
    }
    s
}

#[derive(Clone, Copy)]
pub enum TimeZoneFunction {
    /// TZ: the time zone as a simple literal, "" if there is none.
    Tz,
    /// TIMEZONE: the time zone as an xsd:dayTimeDuration, unbound if there is none.
    Timezone,
}

/// TZ and TIMEZONE (SPARQL 1.1 section 17.4.5). The time zone offset is part of the type,
/// so the result is a constant for each dateTime type of the argument.
pub fn time_zone_function(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    which: TimeZoneFunction,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let c = args_contexts.get(&0).unwrap().as_str();
    let t = solution_mappings.rdf_node_types.get(c).unwrap();
    let parts: Vec<Expr> = datetime_parts(c, t)
        .into_iter()
        .filter_map(|(e, bt)| {
            let value = match (which, bt.time_zone_offset()) {
                (TimeZoneFunction::Tz, Some(offset)) => format_time_zone_offset(offset),
                (TimeZoneFunction::Tz, None) => String::new(),
                (TimeZoneFunction::Timezone, Some(offset)) => offset_duration(offset),
                (TimeZoneFunction::Timezone, None) => return None,
            };
            Some(
                when(e.is_not_null())
                    .then(lit(value))
                    .otherwise(null_of(DataType::String)),
            )
        })
        .collect();
    let expr = match parts.len() {
        0 => null_of(DataType::String),
        1 => parts.into_iter().next().unwrap(),
        _ => coalesce(&parts),
    };
    let datatype = match which {
        TimeZoneFunction::Tz => xsd::STRING,
        TimeZoneFunction::Timezone => xsd::DAY_TIME_DURATION,
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::literal(datatype.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}

#[cfg(test)]
mod tests {
    use super::offset_duration;

    #[test]
    fn test_offset_duration() {
        assert_eq!(offset_duration(0), "PT0S");
        assert_eq!(offset_duration(-480), "-PT8H");
        assert_eq!(offset_duration(330), "PT5H30M");
        assert_eq!(offset_duration(-30), "-PT30M");
    }
}
