use crate::errors::QueryProcessingError;
use polars::prelude::col;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn abs_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        col(first_context.as_str())
            .abs()
            .alias(outer_context.as_str()),
    );
    let existing_type = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    Ok(solution_mappings)
}
