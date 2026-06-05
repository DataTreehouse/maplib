use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::datatypes::TimeUnit;
use polars::prelude::{lit, LiteralValue, Scalar};
use representation::query_context::Context;
use representation::rdf_to_polars::{default_time_unit, default_time_zone};
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_(
    mut solution_mappings: SolutionMappings,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    assert_eq!(
        TimeUnit::Microseconds,
        default_time_unit(),
        "Should never happen"
    );
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        lit(LiteralValue::Scalar(Scalar::new_datetime(
            since_epoch.as_micros() as i64,
            default_time_unit(),
            Some(default_time_zone()),
        )))
        .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
