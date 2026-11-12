use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{int_range, len, lit, DataType, Field, Float64Chunked, IntoColumn};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;

/// RAND(): a pseudo-random xsd:double in [0, 1), drawn separately for each solution.
pub fn rand_(
    mut solution_mappings: SolutionMappings,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let expr = int_range(lit(0), len(), 1, DataType::Int64).map(
        |c| {
            let values: Float64Chunked =
                (0..c.len()).map(|_| Some(rand::random::<f64>())).collect();
            Ok(values.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::Float64)),
    );
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::DOUBLE.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
