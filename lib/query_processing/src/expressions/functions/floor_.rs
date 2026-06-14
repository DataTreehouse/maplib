use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::col;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn floor_(
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
            .floor()
            .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
