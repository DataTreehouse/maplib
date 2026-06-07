use crate::errors::QueryProcessingError;
use crate::expressions::functions::str_function;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn str_len(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let mut expr = str_function(first_context.as_str(), t, global_cats);
    expr = expr.str().len_chars().cast(DataType::Int64);
    expr = expr.alias(outer_context.as_str());
    solution_mappings.mappings = solution_mappings.mappings.with_column(expr);
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
