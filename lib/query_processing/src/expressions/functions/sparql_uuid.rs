use crate::errors::QueryProcessingError;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::prelude::{by_name, col, lit, IntoColumn, NamedFrom, Series};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
pub fn uuid(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if !args.is_empty() {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "0".to_string(),
        ));
    }
    let tmp_column = uuid::Uuid::new_v4().to_string();
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_row_index(PlSmallStr::from_str(&tmp_column), None);
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        (lit("urn:uuid:")
            + col(&tmp_column).map(
                |c| {
                    let uuids: Vec<_> = (0..c.len())
                        .into_par_iter()
                        .map(|_| uuid::Uuid::new_v4().to_string())
                        .collect();
                    let s = Series::new("uuids".into(), uuids);
                    Ok(s.into_column())
                },
                |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
            ))
        .alias(outer_context.as_str()),
    );
    solution_mappings.mappings =
        solution_mappings
            .mappings
            .drop(by_name([&tmp_column], true, false));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
