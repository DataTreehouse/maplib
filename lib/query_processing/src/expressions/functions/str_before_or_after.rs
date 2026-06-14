use crate::errors::QueryProcessingError;
use crate::expressions::functions::eval_expression_to_string::eval_expression_to_string;
use crate::expressions::functions::keep_field_::keep_field;
use crate::expressions::functions::str_after::str_after;
use crate::expressions::functions::str_before::str_before;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{as_struct, col, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn str_before_or_after(
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
    let second_string = eval_expression_to_string(args.get(1).unwrap(), true)?;

    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();

    if t.is_lit_type(xsd::STRING) {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        let decoded = maybe_decode_expr(col(first_context.as_str()), bt, bs, global_cats.clone());
        match func {
            Function::StrBefore => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    decoded
                        .apply(move |x| str_before(x, second_string.clone()), keep_field)
                        .alias(outer_context.as_str()),
                );
            }
            Function::StrAfter => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    decoded
                        .apply(move |x| str_after(x, second_string.clone()), keep_field)
                        .alias(outer_context.as_str()),
                );
            }
            _ => panic!("Should never happen"),
        }
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            RDFNodeState::from_bases(bt.clone(), BaseCatState::String),
        );
    } else if t.is_lang_string() {
        let bt = t.get_base_type().unwrap();
        let bs = t.get_base_state().unwrap();
        let str_expr = maybe_decode_expr(
            col(first_context.as_str())
                .struct_()
                .field_by_name(LANG_STRING_VALUE_FIELD),
            bt,
            bs,
            global_cats.clone(),
        );
        let mut exprs = vec![];
        match func {
            Function::StrBefore => {
                exprs.push(
                    str_expr
                        .apply(move |x| str_before(x, second_string.clone()), keep_field)
                        .alias(LANG_STRING_VALUE_FIELD),
                );
            }
            Function::StrAfter => {
                exprs.push(
                    str_expr
                        .apply(move |x| str_after(x, second_string.clone()), keep_field)
                        .alias(LANG_STRING_VALUE_FIELD),
                );
            }
            _ => panic!("Should never happen"),
        }
        exprs.push(maybe_decode_expr(
            col(first_context.as_str())
                .struct_()
                .field_by_name(LANG_STRING_LANG_FIELD),
            bt,
            bs,
            global_cats.clone(),
        ));
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(as_struct(exprs).alias(outer_context.as_str()));
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned())
                .into_default_input_rdf_node_state(),
        );
    } else if t.is_multi() {
        let mut exprs = vec![];
        let mut keep_types = vec![];
        for (bt, bs) in &t.map {
            let decoded = maybe_decode_expr(
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(&bt.field_col_name()),
                bt,
                bs,
                global_cats.clone(),
            );
            if bt.is_lit_type(xsd::STRING) || bt.is_lang_string() {
                match func {
                    Function::StrBefore => {
                        let use_ss = second_string.clone();
                        exprs.push(
                            decoded
                                .apply(move |x| str_before(x, use_ss.clone()), keep_field)
                                .alias(&bt.field_col_name()),
                        );
                    }
                    Function::StrAfter => {
                        let use_ss = second_string.clone();
                        exprs.push(
                            decoded
                                .apply(move |x| str_after(x, use_ss.clone()), keep_field)
                                .alias(&bt.field_col_name()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
                keep_types.push(bt);
            }
            if bt.is_lang_string() {
                exprs.push(maybe_decode_expr(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(LANG_STRING_LANG_FIELD),
                    bt,
                    bs,
                    global_cats.clone(),
                ));
            }
        }
        if keep_types.is_empty() {
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(
                        BaseRDFNodeType::None
                            .into_default_input_rdf_node_state()
                            .polars_data_type(),
                    )
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            );
        } else if keep_types.len() == 1 {
            if exprs.len() > 1 {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(as_struct(exprs).alias(outer_context.as_str()));
            } else {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(exprs.pop().unwrap().alias(outer_context.as_str()));
            }
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                keep_types
                    .pop()
                    .unwrap()
                    .clone()
                    .into_default_input_rdf_node_state(),
            );
        } else {
            let type_map: HashMap<_, _> = keep_types
                .into_iter()
                .map(|bt| (bt.clone(), bt.default_input_cat_state()))
                .collect();
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(as_struct(exprs).alias(outer_context.as_str()));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeState::from_map(type_map),
            );
        }
    } else {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(
                    BaseRDFNodeType::None
                        .into_default_input_rdf_node_state()
                        .polars_data_type(),
                )
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        );
    }
    Ok(solution_mappings)
}
