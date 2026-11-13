use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{
    all_horizontal, as_struct, coalesce, col, concat_str, lit, when, DataType, LiteralValue,
};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn concat_(
    mut solution_mappings: SolutionMappings,
    _func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings,
        rdf_node_types: datatypes,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None);
    let lang_string = BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None);
    let all_maybe_lang_string = !args.is_empty()
        && (0..args.len()).all(|i| {
            let c = args_contexts.get(&i).unwrap().as_str();
            datatypes.get(c).unwrap().map.contains_key(&lang_string)
        });

    let mut cols = Vec::with_capacity(args.len());
    let mut lang_cols = Vec::with_capacity(args.len());
    for i in 0..args.len() {
        let c = args_contexts.get(&i).unwrap().as_str();
        let t = datatypes.get(c).unwrap();

        let string_col = if let Some(s) = t.map.get(&base_string) {
            if t.is_multi() {
                Some(maybe_decode_expr(
                    col(c)
                        .struct_()
                        .field_by_name(&base_string.field_col_name()),
                    &base_string,
                    s,
                    global_cats.clone(),
                ))
            } else {
                Some(maybe_decode_expr(
                    col(c),
                    &base_string,
                    s,
                    global_cats.clone(),
                ))
            }
        } else {
            None
        };
        let value_col = if let Some(s) = t.map.get(&lang_string) {
            Some(maybe_decode_expr(
                col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD),
                &lang_string,
                s,
                global_cats.clone(),
            ))
        } else {
            None
        };

        // Arguments that are not string literals (or unbound) are errors, giving an unbound result
        let use_col = match (string_col, value_col) {
            (Some(s), Some(v)) => coalesce(&[s, v]),
            (Some(s), None) => s,
            (None, Some(v)) => v,
            (None, None) => lit(LiteralValue::untyped_null()).cast(DataType::String),
        };
        cols.push(use_col);

        if all_maybe_lang_string {
            let s = t.map.get(&lang_string).unwrap();
            lang_cols.push(maybe_decode_expr(
                col(c).struct_().field_by_name(LANG_STRING_LANG_FIELD),
                &lang_string,
                s,
                global_cats.clone(),
            ));
        }
    }

    // CONCAT() is the empty string
    let concat_values = if cols.is_empty() {
        lit("")
    } else {
        concat_str(cols, "", false)
    };

    // The result has a language tag only if all arguments have the same language tag
    let (output_expr, output_type) = if all_maybe_lang_string {
        let first_lang = lang_cols.first().unwrap().clone();
        let same_lang: Vec<_> = lang_cols
            .iter()
            .map(|l| l.clone().eq(first_lang.clone()).fill_null(lit(false)))
            .collect();
        let is_lang = all_horizontal(same_lang).unwrap();
        let null_str = lit(LiteralValue::untyped_null()).cast(DataType::String);
        let state = RDFNodeState::from_map(HashMap::from([
            (lang_string.clone(), lang_string.default_input_cat_state()),
            (base_string.clone(), base_string.default_input_cat_state()),
        ]));
        let ex = as_struct(vec![
            when(is_lang.clone())
                .then(concat_values.clone())
                .otherwise(null_str.clone())
                .alias(LANG_STRING_VALUE_FIELD),
            when(is_lang.clone())
                .then(first_lang)
                .otherwise(null_str.clone())
                .alias(LANG_STRING_LANG_FIELD),
            when(is_lang)
                .then(null_str)
                .otherwise(concat_values)
                .alias(base_string.field_col_name()),
        ]);
        (set_struct_all_null_to_null_row(ex, &state), state)
    } else {
        (
            concat_values,
            base_string.into_default_input_rdf_node_state(),
        )
    };

    let new_mappings = mappings.with_column(output_expr.alias(outer_context.as_str()));
    solution_mappings = SolutionMappings::new(new_mappings, datatypes, height_upper_bound);
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), output_type);
    Ok(solution_mappings)
}
