use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{as_struct, col, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn lower_upper_substr(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if matches!(func, Function::LCase | Function::UCase) {
        if args.len() != 1 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "1".to_string(),
            ));
        }
    } else {
        if args.len() != 2 && args.len() != 3 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "2 or 3".to_string(),
            ));
        }
    }
    let first_context = args_contexts.get(&0).unwrap();
    let starting_loc = if let Some(Expression::Literal(starting_loc_lit)) = args.get(1) {
        let starting_loc: i64 = starting_loc_lit
            .value()
            .parse()
            .map_err(|_x| QueryProcessingError::ExpectedIntegerArgument(func.clone()))?;
        Some(lit(starting_loc))
    } else {
        None
    };
    let length = if let Some(Expression::Literal(length)) = args.get(2) {
        let length: i64 = length.value().parse().unwrap();
        lit(length)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Int64)
    };
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    if !t.is_multi() {
        let b = t.get_base_type().unwrap();
        let s = t.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode | BaseRDFNodeType::None => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    lit(LiteralValue::untyped_null())
                        .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                );
            }
            BaseRDFNodeType::Literal(_) => {
                if t.is_lit_type(xsd::STRING) {
                    let str_expr =
                        maybe_decode_expr(col(first_context.as_str()), b, s, global_cats);
                    match func {
                        Function::LCase => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                str_expr.str().to_lowercase().alias(outer_context.as_str()),
                            );
                        }
                        Function::UCase => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                str_expr.str().to_uppercase().alias(outer_context.as_str()),
                            );
                        }
                        Function::SubStr => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                str_expr
                                    .str()
                                    .slice(starting_loc.unwrap(), length)
                                    .alias(outer_context.as_str()),
                            );
                        }
                        _ => unreachable!("Should never happen"),
                    }
                    solution_mappings.rdf_node_types.insert(
                        outer_context.as_str().to_string(),
                        BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                            .into_default_input_rdf_node_state(),
                    );
                } else if t.is_lang_string() {
                    let str_expr = maybe_decode_expr(
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD),
                        b,
                        s,
                        global_cats.clone(),
                    );
                    match func {
                        Function::LCase => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .struct_()
                                    .with_fields(vec![str_expr
                                        .str()
                                        .to_lowercase()
                                        .alias(LANG_STRING_VALUE_FIELD)])
                                    .alias(outer_context.as_str()),
                            );
                        }
                        Function::UCase => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .struct_()
                                    .with_fields(vec![str_expr
                                        .str()
                                        .to_uppercase()
                                        .alias(LANG_STRING_VALUE_FIELD)])
                                    .alias(outer_context.as_str()),
                            );
                        }
                        Function::SubStr => {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .struct_()
                                    .with_fields(vec![str_expr
                                        .cast(DataType::String)
                                        .str()
                                        .slice(starting_loc.unwrap(), length)
                                        .alias(LANG_STRING_VALUE_FIELD)])
                                    .alias(outer_context.as_str()),
                            );
                        }
                        _ => unreachable!("Should never happen"),
                    }
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        col(outer_context.as_str())
                            .struct_()
                            .with_fields(vec![maybe_decode_expr(
                                col(first_context.as_str())
                                    .struct_()
                                    .field_by_name(LANG_STRING_LANG_FIELD),
                                b,
                                s,
                                global_cats,
                            )
                            .alias(LANG_STRING_LANG_FIELD)]),
                    );
                    solution_mappings.rdf_node_types.insert(
                        outer_context.as_str().to_string(),
                        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned())
                            .into_default_input_rdf_node_state(),
                    );
                } else {
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        lit(LiteralValue::untyped_null())
                            .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                            .alias(outer_context.as_str()),
                    );
                    solution_mappings.rdf_node_types.insert(
                        outer_context.as_str().to_string(),
                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                    );
                }
            }
        }
    } else {
        //is multi
        let mut exprs = vec![];
        let mut keep_types = vec![];
        for (t, s) in &t.map {
            if let BaseRDFNodeType::Literal(l) = t {
                if l.as_ref() == xsd::STRING {
                    let field_name = t.field_col_name();
                    let str_expr = maybe_decode_expr(
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(&field_name),
                        t,
                        s,
                        global_cats.clone(),
                    );
                    match func {
                        Function::LCase => {
                            exprs.push(str_expr.str().to_lowercase().alias(&field_name));
                        }
                        Function::UCase => {
                            exprs.push(str_expr.str().to_uppercase().alias(&field_name));
                        }
                        Function::SubStr => {
                            exprs.push(
                                str_expr
                                    .str()
                                    .slice(starting_loc.as_ref().unwrap().clone(), length.clone())
                                    .alias(&field_name),
                            );
                        }
                        _ => unreachable!("Should never happen"),
                    }
                    keep_types.push(t.clone());
                } else if t.is_lang_string() {
                    let str_expr = maybe_decode_expr(
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD),
                        t,
                        s,
                        global_cats.clone(),
                    );
                    match func {
                        Function::LCase => {
                            exprs
                                .push(str_expr.str().to_lowercase().alias(LANG_STRING_VALUE_FIELD));
                        }
                        Function::UCase => {
                            exprs
                                .push(str_expr.str().to_uppercase().alias(LANG_STRING_VALUE_FIELD));
                        }
                        Function::SubStr => {
                            exprs.push(
                                str_expr
                                    .str()
                                    .slice(starting_loc.as_ref().unwrap().clone(), length.clone())
                                    .alias(LANG_STRING_VALUE_FIELD),
                            );
                        }
                        _ => unreachable!("Should never happen"),
                    }
                    exprs.push(
                        maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD),
                            t,
                            s,
                            global_cats.clone(),
                        )
                        .alias(LANG_STRING_LANG_FIELD),
                    );
                    keep_types.push(t.clone());
                }
            }
        }
        if keep_types.is_empty() {
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            );
        } else if keep_types.len() == 1 {
            let t = keep_types.pop().unwrap();
            if t.is_lang_string() {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(as_struct(exprs).alias(outer_context.as_str()));
            } else {
                if args.len() != 1 {
                    return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                        func.clone(),
                        args.len(),
                        "1".to_string(),
                    ));
                }
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(exprs.pop().unwrap().alias(outer_context.as_str()));
            }
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                t.into_default_input_rdf_node_state(),
            );
        } else {
            let t = RDFNodeState::from_map(
                keep_types
                    .into_iter()
                    .map(|x| {
                        let cat_state = x.default_input_cat_state();
                        (x, cat_state)
                    })
                    .collect(),
            );
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(as_struct(exprs).alias(outer_context.as_str()));
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), t);
        }
    }
    Ok(solution_mappings)
}
