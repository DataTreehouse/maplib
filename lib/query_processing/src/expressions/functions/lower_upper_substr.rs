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
    // SUBSTR(s, start, length) keeps the characters at the 1-based positions p with
    // start <= p < start + length, so positions before 1 are cut from the length.
    let (starting_loc, length) = if matches!(func, Function::SubStr) {
        let start = integer_literal_argument(args.get(1), func)?;
        let length = args
            .get(2)
            .map(|a| integer_literal_argument(Some(a), func))
            .transpose()?;
        let from = start.max(1);
        let length = match length {
            Some(length) => lit(start.saturating_add(length).saturating_sub(from).max(0)),
            None => lit(LiteralValue::untyped_null()).cast(DataType::Int64),
        };
        (Some(lit(from - 1)), length)
    } else {
        (None, lit(LiteralValue::untyped_null()).cast(DataType::Int64))
    };
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    if !t.is_multi() {
        let b = t.get_base_type().unwrap();
        let s = t.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::IRI
            | BaseRDFNodeType::BlankNode
            | BaseRDFNodeType::None
            | BaseRDFNodeType::TripleTerm => {
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
            BaseRDFNodeType::Literal(_, _) => {
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
                        BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None)
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
                        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None)
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
            if let BaseRDFNodeType::Literal(l, _) = t {
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

/// Reads an integer SUBSTR argument, which must be a (possibly signed) literal.
fn integer_literal_argument(
    arg: Option<&Expression>,
    func: &Function,
) -> Result<i64, QueryProcessingError> {
    let err = || QueryProcessingError::ExpectedIntegerArgument(func.clone());
    match arg {
        Some(Expression::Literal(l)) => parse_integer_lexical(l.value()).ok_or_else(err),
        Some(Expression::UnaryMinus(inner)) => {
            Ok(integer_literal_argument(Some(inner), func)?.saturating_neg())
        }
        Some(Expression::UnaryPlus(inner)) => integer_literal_argument(Some(inner), func),
        _ => Err(err()),
    }
}

/// Parses an integer, also accepting decimals and doubles with an integral value such as 2.0,
/// which SUBSTR rounds.
fn parse_integer_lexical(s: &str) -> Option<i64> {
    if let Ok(i) = s.parse::<i64>() {
        return Some(i);
    }
    let f = s.parse::<f64>().ok()?;
    f.is_finite().then(|| f.round() as i64)
}
