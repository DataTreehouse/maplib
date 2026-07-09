use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{as_struct, coalesce, col, concat_str};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn concat_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() < 2 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            ">1".to_string(),
        ));
    }
    let SolutionMappings {
        mappings,
        rdf_node_types: datatypes,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let mut any_string = false;
    let mut any_lang_string = false;
    let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
    let lang_string = BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned());
    for i in 0..args.len() {
        let c = args_contexts.get(&i).unwrap().as_str();
        let t = datatypes.get(c).unwrap();
        if t.map.contains_key(&base_string) {
            any_string = true;
        }
        if t.map.contains_key(&lang_string) {
            any_lang_string = true;
        }
    }
    let output_lang_string = any_lang_string && !any_string;

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

        let use_col = if string_col.is_some() && value_col.is_some() {
            Some(coalesce(&[string_col.unwrap(), value_col.unwrap()]))
        } else if string_col.is_some() {
            string_col
        } else if value_col.is_some() {
            value_col
        } else {
            None
        };
        if let Some(use_col) = use_col {
            cols.push(use_col);
        }

        if output_lang_string {
            if let Some(s) = t.map.get(&lang_string) {
                lang_cols.push(maybe_decode_expr(
                    col(c).struct_().field_by_name(LANG_STRING_LANG_FIELD),
                    &lang_string,
                    s,
                    global_cats.clone(),
                ))
            }
        }
    }

    let concat_values = concat_str(cols, "", true);

    let (output_expr, output_type) = if output_lang_string {
        let ex = as_struct(vec![
            concat_values.alias(LANG_STRING_VALUE_FIELD),
            coalesce(&lang_cols).alias(LANG_STRING_LANG_FIELD),
        ]);
        (ex, lang_string)
    } else {
        (concat_values, base_string)
    };

    let new_mappings = mappings.with_column(output_expr.alias(outer_context.as_str()));
    solution_mappings = SolutionMappings::new(new_mappings, datatypes, height_upper_bound);
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        output_type.into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
