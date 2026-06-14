use crate::errors::QueryProcessingError;
use polars::prelude::{as_struct, col, lit, LiteralValue, RoundMode};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn round_(
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
            .round(0, RoundMode::HalfAwayFromZero)
            .alias(outer_context.as_str()),
    );
    let existing_type = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let mut rounded_exprs = vec![];
    let mut rounded_types = vec![];
    if existing_type.is_multi() {
        for t in existing_type.map.keys() {
            if t.is_numeric() {
                rounded_exprs.push(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(&t.field_col_name())
                        .round(0, RoundMode::HalfAwayFromZero)
                        .alias(&t.field_col_name()),
                );
                rounded_types.push(t.clone());
            }
        }
    } else if existing_type.is_numeric() {
        rounded_exprs.push(col(first_context.as_str()).round(0, RoundMode::HalfAwayFromZero));
        rounded_types.push(existing_type.get_base_type().unwrap().clone());
    }
    if rounded_exprs.is_empty() {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        );
    } else if rounded_exprs.len() == 1 {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(rounded_exprs.pop().unwrap().alias(outer_context.as_str()));
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            rounded_types
                .pop()
                .unwrap()
                .into_default_stored_rdf_node_state(),
        );
    } else {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(as_struct(rounded_exprs).alias(outer_context.as_str()));
        let mut types_map = HashMap::new();
        for t in rounded_types {
            let s = t.default_input_cat_state();
            types_map.insert(t, s);
        }
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            RDFNodeState::from_map(types_map),
        );
    }
    Ok(solution_mappings)
}
