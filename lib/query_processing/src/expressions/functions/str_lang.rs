use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{as_struct, col, lit, when, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
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
    let first_expr = simple_string_expr(first_context.as_str(), first_t, global_cats.clone());
    let second_context = args_contexts.get(&1).unwrap();
    let second_t = solution_mappings
        .rdf_node_types
        .get(second_context.as_str())
        .unwrap();
    let second_expr = simple_string_expr(second_context.as_str(), second_t, global_cats);
    // Arguments that are not simple literals give a type error, i.e. an unbound result.
    // A lang string is unbound when its value field is null.
    let both_bound = first_expr
        .clone()
        .is_not_null()
        .and(second_expr.clone().is_not_null());
    let first_expr = when(both_bound.clone())
        .then(first_expr)
        .otherwise(lit(LiteralValue::untyped_null()).cast(DataType::String));
    let second_expr = when(both_bound)
        .then(second_expr)
        .otherwise(lit(LiteralValue::untyped_null()).cast(DataType::String));
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        as_struct(vec![
            first_expr.alias(LANG_STRING_VALUE_FIELD),
            second_expr.alias(LANG_STRING_LANG_FIELD),
        ])
        .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None)
            .into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}

/// The string value of the column where it is an xsd:string literal, otherwise null.
pub(crate) fn simple_string_expr(c: &str, t: &RDFNodeState, global_cats: LockedCats) -> Expr {
    if t.is_multi() {
        for (bt, bs) in &t.map {
            if bt.is_lit_type(xsd::STRING) {
                return maybe_decode_expr(
                    col(c).struct_().field_by_name(&bt.field_col_name()),
                    bt,
                    bs,
                    global_cats,
                );
            }
        }
    } else if t.is_lit_type(xsd::STRING) {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        return maybe_decode_expr(col(c), bt, bs, global_cats);
    }
    lit(LiteralValue::untyped_null()).cast(DataType::String)
}
