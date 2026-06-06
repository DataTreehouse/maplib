use crate::errors::QueryProcessingError;
use crate::expressions::functions::str_starts_ends_contains;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn starts_ends_contains(
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
    let second_context = args_contexts.get(&1).unwrap();

    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let second_t = solution_mappings
        .rdf_node_types
        .get(second_context.as_str())
        .unwrap();
    let second_expr = &args[1];
    if !second_t.is_lit_type(xsd::STRING) {
        return Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
            second_expr.clone(),
        ));
    }
    let second_bt = second_t.get_base_type().unwrap();
    let second_bs = second_t.get_base_state().unwrap();
    let second_decoded = maybe_decode_expr(
        col(second_context.as_str()),
        second_bt,
        second_bs,
        global_cats.clone(),
    );
    if t.is_lit_type(xsd::STRING) {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        let decoded = maybe_decode_expr(col(first_context.as_str()), bt, bs, global_cats.clone());
        let expr =
            str_starts_ends_contains(decoded, second_decoded, func).alias(outer_context.as_str());
        solution_mappings.mappings = solution_mappings.mappings.with_column(expr);
    } else if t.is_lang_string() {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        let decoded = maybe_decode_expr(
            col(first_context.as_str())
                .struct_()
                .field_by_name(LANG_STRING_VALUE_FIELD),
            bt,
            bs,
            global_cats.clone(),
        );
        let expr =
            str_starts_ends_contains(decoded, second_decoded, func).alias(outer_context.as_str());
        solution_mappings.mappings = solution_mappings.mappings.with_column(expr);
    } else if t.is_multi() {
        let mut exprs = vec![];
        for (bt, bs) in &t.map {
            if bt.is_lit_type(xsd::STRING) {
                let decoded = maybe_decode_expr(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(&bt.field_col_name()),
                    bt,
                    bs,
                    global_cats.clone(),
                );
                exprs.push(
                    str_starts_ends_contains(decoded, second_decoded.clone(), func)
                        .alias(outer_context.as_str()),
                );
            } else if bt.is_lang_string() {
                let decoded = maybe_decode_expr(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(LANG_STRING_VALUE_FIELD),
                    bt,
                    bs,
                    global_cats.clone(),
                );
                exprs.push(
                    str_starts_ends_contains(decoded, second_decoded.clone(), func)
                        .alias(outer_context.as_str()),
                );
            }
        }
        if exprs.is_empty() {
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(DataType::Boolean)
                    .alias(outer_context.as_str()),
            );
        } else if exprs.len() == 1 {
            solution_mappings.mappings =
                solution_mappings.mappings.with_column(exprs.pop().unwrap());
        } else {
            solution_mappings.mappings = solution_mappings.mappings.with_column(coalesce(&exprs));
        }
    } else {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(DataType::Boolean)
                .alias(outer_context.as_str()),
        );
    }
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
