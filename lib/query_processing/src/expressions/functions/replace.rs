use crate::errors::QueryProcessingError;
use crate::expressions::functions::create_regex_replace_expr::create_regex_replace_expr;
use crate::expressions::functions::create_regex_string::create_regex_string;

use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{as_struct, coalesce, col, lit, DataType, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn sparql_replace(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 3 && args.len() != 4 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "3 or 4".to_string(),
        ));
    }
    let arg_context = args_contexts.get(&0).unwrap();
    let arg_type = solution_mappings
        .rdf_node_types
        .get(arg_context.as_str())
        .unwrap();

    let replacement_context = args_contexts.get(&2).unwrap();
    let replacement_state = solution_mappings
        .rdf_node_types
        .get(replacement_context.as_str())
        .unwrap();
    let replacement_expr = &args[2];
    if !replacement_state.is_lit_type(xsd::STRING) {
        return Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
            replacement_expr.clone(),
        ));
    }
    let replacement_bt = replacement_state.get_base_type().unwrap();
    let replacement_bs = replacement_state.get_base_state().unwrap();

    let pattern_context = args_contexts.get(&1).unwrap();
    let pattern_sparql_expr = args.get(1).unwrap();
    let pattern_type = solution_mappings
        .rdf_node_types
        .get(pattern_context.as_str())
        .unwrap();
    let flags = if let Some(flags_context) = args_contexts.get(&3) {
        let flags_sparql_expr = args.get(3).unwrap();
        let flags_type = solution_mappings
            .rdf_node_types
            .get(flags_context.as_str())
            .unwrap();
        Some((flags_sparql_expr, flags_type))
    } else {
        None
    };
    let pattern = create_regex_string(pattern_sparql_expr, pattern_type, flags)?;
    let replacement_expr = maybe_decode_expr(
        col(replacement_context.as_str()),
        replacement_bt,
        replacement_bs,
        global_cats.clone(),
    );
    let is_multi = arg_type.is_multi();
    let mut string_exprs = vec![];
    let mut lang_value_exprs = vec![];
    let mut lang_lang_exprs = vec![];
    for (t, s) in &arg_type.map {
        let value_col = if is_multi || t.is_lang_string() {
            col(arg_context.as_str())
                .struct_()
                .field_by_name(&t.field_col_name())
        } else {
            col(arg_context.as_str())
        };
        if t.is_lit_type(xsd::STRING) {
            string_exprs.push(create_regex_replace_expr(
                value_col,
                t,
                s,
                &pattern,
                &replacement_expr,
                global_cats.clone(),
            ));
        } else if t.is_lang_string() {
            lang_value_exprs.push(create_regex_replace_expr(
                value_col,
                t,
                s,
                &pattern,
                &replacement_expr,
                global_cats.clone(),
            ));
            lang_lang_exprs.push(maybe_decode_expr(
                col(arg_context.as_str())
                    .struct_()
                    .field_by_name(LANG_STRING_LANG_FIELD),
                t,
                s,
                global_cats.clone(),
            ));
        }
    }
    let string_type = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
    let lang_type = BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned());
    // REPLACE keeps the language tag of the first argument (SPARQL 1.1 17.4.3.15)
    let (expr, state) = if lang_value_exprs.is_empty() {
        let expr = if string_exprs.is_empty() {
            lit(LiteralValue::untyped_null()).cast(DataType::String)
        } else {
            coalesce(&string_exprs)
        };
        (expr, string_type.into_default_input_rdf_node_state())
    } else {
        let lang_struct = as_struct(vec![
            coalesce(&lang_value_exprs).alias(LANG_STRING_VALUE_FIELD),
            coalesce(&lang_lang_exprs).alias(LANG_STRING_LANG_FIELD),
        ]);
        if string_exprs.is_empty() {
            let state = lang_type.into_default_input_rdf_node_state();
            (set_struct_all_null_to_null_row(lang_struct, &state), state)
        } else {
            let state = RDFNodeState::from_map(HashMap::from([
                (lang_type.clone(), lang_type.default_input_cat_state()),
                (string_type.clone(), string_type.default_input_cat_state()),
            ]));
            let expr = as_struct(vec![
                coalesce(&lang_value_exprs).alias(LANG_STRING_VALUE_FIELD),
                coalesce(&lang_lang_exprs).alias(LANG_STRING_LANG_FIELD),
                coalesce(&string_exprs).alias(string_type.field_col_name()),
            ]);
            (set_struct_all_null_to_null_row(expr, &state), state)
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), state);
    Ok(solution_mappings)
}
