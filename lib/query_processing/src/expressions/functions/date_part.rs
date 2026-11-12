use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, lit, DataType, Expr};
use representation::query_context::Context;
use representation::rdf_to_polars::default_decimal_type;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
}

/// YEAR, MONTH, DAY, HOURS and MINUTES return xsd:integer, SECONDS returns xsd:decimal
/// including fractional seconds (SPARQL 1.1 section 17.4.5).
pub fn date_part(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    part: DatePart,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let c = col(args_contexts.get(&0).unwrap().as_str());
    let (expr, t): (Expr, _) = match part {
        DatePart::Year => (c.dt().year().cast(DataType::Int64), xsd::INTEGER),
        DatePart::Month => (c.dt().month().cast(DataType::Int64), xsd::INTEGER),
        DatePart::Day => (c.dt().day().cast(DataType::Int64), xsd::INTEGER),
        DatePart::Hours => (c.dt().hour().cast(DataType::Int64), xsd::INTEGER),
        DatePart::Minutes => (c.dt().minute().cast(DataType::Int64), xsd::INTEGER),
        DatePart::Seconds => {
            // Built as the text "seconds.microseconds" so the decimal is exact
            let micros = c
                .clone()
                .dt()
                .microsecond()
                .cast(DataType::String)
                .str()
                .zfill(lit(6));
            let seconds = c.dt().second().cast(DataType::String) + lit(".") + micros;
            (seconds.cast(default_decimal_type()), xsd::DECIMAL)
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(t.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
