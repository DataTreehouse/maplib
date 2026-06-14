use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{as_struct, col};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn str_lang(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 2 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "2".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let first_t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let first_expr = if first_t.is_lit_type(xsd::STRING) {
        let f_bt = first_t.get_base_type().unwrap();
        let f_bs = first_t.get_base_state().unwrap();
        maybe_decode_expr(col(first_context.as_str()), f_bt, f_bs, global_cats.clone())
    } else {
        todo!();
    };
    let second_context = args_contexts.get(&1).unwrap();
    let second_t = solution_mappings
        .rdf_node_types
        .get(second_context.as_str())
        .unwrap();
    let second_expr = if second_t.is_lit_type(xsd::STRING) {
        let f_bt = second_t.get_base_type().unwrap();
        let f_bs = second_t.get_base_state().unwrap();
        maybe_decode_expr(col(second_context.as_str()), f_bt, f_bs, global_cats)
    } else {
        todo!();
    };
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        as_struct(vec![
            first_expr.alias(LANG_STRING_VALUE_FIELD),
            second_expr.alias(LANG_STRING_LANG_FIELD),
        ])
        .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
