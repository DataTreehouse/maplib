use crate::constants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, FLOOR_DATETIME_TO_SECONDS_INTERVAL, MODULUS,
    NANOS_AS_DATETIME, SECONDS_AS_DATETIME,
};
use crate::errors::QueryProcessingError;
use crate::expressions::{cast_lang_string_to_string, drop_inner_contexts};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNodeRef};
use polars::datatypes::{DataType, Field, PlSmallStr, TimeUnit};
use polars::prelude::{
    as_struct, by_name, coalesce, col, concat_str, lit, when, Expr, IntoColumn, LiteralValue,
    NamedFrom, RoundMode, Scalar, Series, StrptimeOptions,
};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use representation::cats::{maybe_decode_expr, Cats};
use representation::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT};
use representation::query_context::Context;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::sync::Arc;

pub fn func_expression(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: Arc<Cats>,
) -> Result<SolutionMappings, QueryProcessingError> {
    match func {
        Function::Year => {
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
                    .dt()
                    .year()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Month => {
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
                    .dt()
                    .month()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Day => {
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
                    .dt()
                    .day()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Hours => {
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
                    .dt()
                    .hour()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Minutes => {
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
                    .dt()
                    .minute()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Seconds => {
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
                    .dt()
                    .second()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Abs => {
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
                    .abs()
                    .alias(outer_context.as_str()),
            );
            let existing_type = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), existing_type.clone());
        }
        Function::Ceil => {
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
                    .ceil()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Floor => {
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
                    .floor()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Concat => {
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
            let mut cols = Vec::with_capacity(args.len());
            let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
            for i in 0..args.len() {
                let c = args_contexts.get(&i).unwrap().as_str();
                let t = datatypes.get(c).unwrap();
                if let Some(s) = t.map.get(&base_string) {
                    if t.is_multi() {
                        cols.push(maybe_decode_expr(
                            col(c)
                                .struct_()
                                .field_by_name(&base_string.field_col_name()),
                            &base_string,
                            s,
                            global_cats.clone(),
                        ))
                    } else {
                        cols.push(maybe_decode_expr(
                            col(c),
                            &base_string,
                            s,
                            global_cats.clone(),
                        ))
                    }
                } else {
                    cols.push(lit(LiteralValue::untyped_null()).cast(DataType::String))
                }
            }
            let new_mappings =
                mappings.with_column(concat_str(cols, "", true).alias(outer_context.as_str()));
            solution_mappings = SolutionMappings::new(new_mappings, datatypes, height_upper_bound);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Round => {
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
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), existing_type.clone());
        }
        Function::Str => {
            if args.len() != 1 {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "1".to_string(),
                ));
            }
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                str_function(
                    first_context.as_str(),
                    solution_mappings
                        .rdf_node_types
                        .get(first_context.as_str())
                        .unwrap(),
                    global_cats,
                )
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Lang => {
            if args.len() != 1 {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "1".to_string(),
                ));
            }
            let first_context = args_contexts.get(&0).unwrap();
            let dt = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            if !dt.is_multi() {
                let b = dt.get_base_type().unwrap();
                match b {
                    BaseRDFNodeType::Literal(l) => {
                        if l.as_ref() == rdf::LANG_STRING {
                            solution_mappings.mappings = solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .struct_()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .cast(DataType::String)
                                    .alias(outer_context.as_str()),
                            )
                        } else {
                            solution_mappings.mappings = solution_mappings
                                .mappings
                                .with_column(lit("").alias(outer_context.as_str()));
                        }
                    }
                    _ => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            lit(LiteralValue::untyped_null())
                                .cast(DataType::String)
                                .alias(outer_context.as_str()),
                        );
                    }
                }
            } else {
                let mut exprs = vec![];
                //Prioritize column that is lang string
                for (t, s) in &dt.map {
                    if t.is_lang_string() {
                        exprs.push(maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD),
                            t,
                            s,
                            global_cats.clone(),
                        ))
                    }
                }
                //Then the rest..
                for (t, _) in &dt.map {
                    if !t.is_lang_string() && matches!(t, BaseRDFNodeType::Literal(_)) {
                        exprs.push(
                            when(
                                col(first_context.as_str())
                                    .struct_()
                                    .field_by_name(&t.field_col_name())
                                    .is_null()
                                    .not(),
                            )
                            .then(lit(""))
                            .otherwise(lit(LiteralValue::untyped_null()).cast(DataType::String)),
                        )
                    } else {
                        exprs.push(lit(LiteralValue::untyped_null()).cast(DataType::String))
                    }
                }
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(coalesce(exprs.as_slice()).alias(outer_context.as_str()));
            }
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::LangMatches => {
            if args.len() != 2 {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "2".to_string(),
                ));
            }
            let lang_expr = col(args_contexts.get(&0).unwrap().as_str()).cast(DataType::String);

            if let Expression::Literal(l) = args.get(1).unwrap() {
                if l.value() == "*" {
                    solution_mappings.mappings = solution_mappings
                        .mappings
                        .with_column(lang_expr.is_null().not().alias(outer_context.as_str()));
                } else {
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        lang_expr
                            .clone()
                            .str()
                            .to_lowercase()
                            .eq(lit(l.value().to_lowercase()))
                            .or(lang_expr
                                .str()
                                .to_lowercase()
                                .str()
                                .starts_with(lit(format!("{}-", l.value().to_lowercase()))))
                            .alias(outer_context.as_str()),
                    );
                }
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else {
                todo!("Handle this error.. ")
            }
        }
        Function::Regex => {
            if args.len() != 2 && args.len() != 3 {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "2 or 3".to_string(),
                ));
            }

            let text_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(text_context.as_str())
                .unwrap();
            if let Expression::Literal(regex_lit) = args.get(1).unwrap() {
                let pattern = create_regex_literal(regex_lit, args.get(2));
                let expr = if t.is_multi() {
                    let mut exprs = vec![];
                    for (t, s) in &t.map {
                        let replace_expr = create_regex_expr(
                            col(text_context.as_str())
                                .struct_()
                                .field_by_name(&t.field_col_name()),
                            t,
                            s,
                            &pattern,
                            global_cats.clone(),
                        );
                        exprs.push(replace_expr);
                    }
                    coalesce(&exprs)
                } else {
                    let b = t.get_base_type().unwrap();
                    let s = t.get_base_state().unwrap();
                    let use_col = if b.is_lang_string() {
                        col(text_context.as_str())
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                    } else {
                        col(text_context.as_str())
                    };

                    create_regex_expr(use_col, b, s, &pattern, global_cats)
                };
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(expr.alias(outer_context.as_str()));
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else {
                unimplemented!("Non literal regex")
            }
        }
        Function::Uuid => {
            if !args.is_empty() {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "0".to_string(),
                ));
            }
            let tmp_column = uuid::Uuid::new_v4().to_string();
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_row_index(PlSmallStr::from_str(&tmp_column), None);
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                (lit("urn:uuid:")
                    + col(&tmp_column).map(
                        |c| {
                            let uuids: Vec<_> = (0..c.len())
                                .into_par_iter()
                                .map(|_| uuid::Uuid::new_v4().to_string())
                                .collect();
                            let s = Series::new("uuids".into(), uuids);
                            Ok(s.into_column())
                        },
                        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
                    ))
                .alias(outer_context.as_str()),
            );
            solution_mappings.mappings = solution_mappings
                .mappings
                .drop(by_name([&tmp_column], true));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
            );
        }
        Function::Iri => {
            if args.len() != 1 {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "1".to_string(),
                ));
            }
            let first_context = args_contexts.get(&0).unwrap();
            let dt = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let (c, s) = if !dt.is_multi() {
                let b = dt.get_base_type().unwrap();
                let bs = dt.get_base_state().unwrap();
                let (c, s) = match b {
                    BaseRDFNodeType::IRI => (col(first_context.as_str()), bs.clone()),
                    BaseRDFNodeType::Literal(l) => {
                        if l.as_ref() == xsd::STRING {
                            (
                                maybe_decode_expr(col(first_context.as_str()), b, bs, global_cats),
                                BaseCatState::String,
                            )
                        } else {
                            (
                                lit(LiteralValue::untyped_null()).cast(DataType::String),
                                BaseCatState::String,
                            )
                        }
                    }
                    BaseRDFNodeType::BlankNode | BaseRDFNodeType::None => (
                        lit(LiteralValue::untyped_null()).cast(DataType::String),
                        BaseCatState::String,
                    ),
                };
                (c, s)
            } else {
                let mut iri_col = None;
                let mut string_col = None;
                let mut iri_state = None;

                for (t, s) in &dt.map {
                    match t {
                        BaseRDFNodeType::Literal(l) => {
                            if l.as_ref() == xsd::STRING {
                                string_col = Some(maybe_decode_expr(
                                    col(first_context.as_str())
                                        .struct_()
                                        .field_by_name(t.field_col_name().as_str()),
                                    t,
                                    s,
                                    global_cats.clone(),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
                for (t, s) in &dt.map {
                    match t {
                        BaseRDFNodeType::IRI => {
                            if string_col.is_some() {
                                iri_col = Some(maybe_decode_expr(
                                    col(first_context.as_str())
                                        .struct_()
                                        .field_by_name(MULTI_IRI_DT),
                                    t,
                                    s,
                                    global_cats.clone(),
                                ));
                                iri_state = Some(BaseCatState::String);
                            } else {
                                iri_col = Some(
                                    col(first_context.as_str())
                                        .struct_()
                                        .field_by_name(MULTI_IRI_DT),
                                );
                                iri_state = Some(s.clone());
                            }
                        }
                        _ => {}
                    }
                }
                if iri_state.is_none() {
                    iri_state = Some(BaseCatState::String);
                }
                let e = if iri_col.is_some() && string_col.is_none() {
                    iri_col.unwrap()
                } else if iri_col.is_none() && string_col.is_some() {
                    string_col.unwrap()
                } else if iri_col.is_some() && string_col.is_some() {
                    coalesce(&[
                        iri_col.unwrap().cast(DataType::String),
                        string_col.unwrap().cast(DataType::String),
                    ])
                } else {
                    lit(LiteralValue::untyped_null()).cast(DataType::String)
                };
                (e, iri_state.unwrap())
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(c.alias(outer_context.as_str()));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeState::from_bases(BaseRDFNodeType::IRI, s),
            );
        }
        Function::StrUuid => {
            if !args.is_empty() {
                return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                    func.clone(),
                    args.len(),
                    "0".to_string(),
                ));
            }
            let tmp_column = uuid::Uuid::new_v4().to_string();
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_row_index(PlSmallStr::from_str(&tmp_column), None);
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(&tmp_column)
                    .map(
                        |c| {
                            let uuids: Vec<_> = (0..c.len())
                                .into_par_iter()
                                .map(|_| uuid::Uuid::new_v4().to_string())
                                .collect();
                            let s = Series::new("uuids".into(), uuids);
                            Ok(s.into_column())
                        },
                        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
                    )
                    .alias(outer_context.as_str()),
            );
            solution_mappings.mappings = solution_mappings
                .mappings
                .drop(by_name([&tmp_column], true));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Replace => {
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
            assert!(replacement_state.is_lit_type(xsd::STRING));
            let replacement_bt = replacement_state.get_base_type().unwrap();
            let replacement_bs = replacement_state.get_base_state().unwrap();

            if let Expression::Literal(regex_lit) = args.get(1).unwrap() {
                let pattern = create_regex_literal(regex_lit, args.get(3));
                let replacement_expr = maybe_decode_expr(
                    col(replacement_context.as_str()),
                    replacement_bt,
                    replacement_bs,
                    global_cats.clone(),
                );
                let expr = if arg_type.is_multi() {
                    let mut exprs = vec![];
                    for (t, s) in &arg_type.map {
                        let replace_expr = create_regex_replace_expr(
                            col(arg_context.as_str())
                                .struct_()
                                .field_by_name(&t.field_col_name()),
                            t,
                            s,
                            &pattern,
                            &replacement_expr,
                            global_cats.clone(),
                        );
                        exprs.push(replace_expr);
                    }
                    coalesce(&exprs)
                } else {
                    let t = arg_type.get_base_type().unwrap();
                    let s = arg_type.get_base_state().unwrap();
                    let use_col = if t.is_lang_string() {
                        col(arg_context.as_str())
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                    } else {
                        col(arg_context.as_str())
                    };

                    create_regex_replace_expr(
                        use_col,
                        t,
                        s,
                        &pattern,
                        &replacement_expr,
                        global_cats,
                    )
                };
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(expr.alias(outer_context.as_str()));
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else {
                todo!("Non literal pattern")
            }
        }
        Function::Custom(nn) => {
            let iri = nn.as_str();
            if matches!(
                nn.as_ref(),
                xsd::INT
                    | xsd::LONG
                    | xsd::INTEGER
                    | xsd::BOOLEAN
                    | xsd::UNSIGNED_LONG
                    | xsd::UNSIGNED_INT
                    | xsd::UNSIGNED_SHORT
                    | xsd::UNSIGNED_BYTE
                    | xsd::DECIMAL
                    | xsd::DOUBLE
                    | xsd::FLOAT
                    | xsd::STRING
                    | xsd::DATE_TIME
                    | xsd::DATE
                    | xsd::DURATION
                    | xsd::TIME
            ) {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                let src_type = solution_mappings
                    .rdf_node_types
                    .get(first_context.as_str())
                    .unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    xsd_cast_literal(
                        first_context.as_str(),
                        src_type,
                        &BaseRDFNodeType::Literal(nn.to_owned()),
                        global_cats,
                    )?
                    .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(nn.to_owned()).into_default_input_rdf_node_state(),
                );
            } else if iri == DATETIME_AS_NANOS {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                        .cast(DataType::UInt64)
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else if iri == DATETIME_AS_SECONDS {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        .cast(DataType::UInt64)
                        .div(lit(1000))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else if iri == NANOS_AS_DATETIME {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else if iri == SECONDS_AS_DATETIME {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .mul(Expr::Literal(LiteralValue::Scalar(Scalar::from(1000))))
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else if iri == MODULUS {
                assert_eq!(args.len(), 2);
                let first_context = args_contexts.get(&0).unwrap();
                let second_context = args_contexts.get(&1).unwrap();

                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    (col(first_context.as_str()) % col(second_context.as_str()))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else if iri == FLOOR_DATETIME_TO_SECONDS_INTERVAL {
                assert_eq!(args.len(), 2);
                let first_context = args_contexts.get(&0).unwrap();
                let second_context = args_contexts.get(&1).unwrap();

                let first_as_seconds = col(first_context.as_str())
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    .cast(DataType::UInt64)
                    .div(lit(1000));

                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    ((first_as_seconds.clone()
                        - (first_as_seconds % col(second_context.as_str())))
                    .mul(Expr::Literal(LiteralValue::Scalar(Scalar::from(1000))))
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
                    .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                        .into_default_input_rdf_node_state(),
                );
            } else {
                todo!("Function {nn} is not implemented yet")
            }
        }
        Function::StrBefore | Function::StrAfter => {
            assert_eq!(args.len(), 2);
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
            assert!(second_t.is_lit_type(xsd::STRING));
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
                let decoded =
                    maybe_decode_expr(col(first_context.as_str()), bt, bs, global_cats.clone());
                match func {
                    Function::StrBefore => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            decoded
                                .str()
                                .strip_suffix(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrAfter => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            decoded
                                .str()
                                .strip_prefix(second_decoded)
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
                let mut exprs = vec![];
                match func {
                    Function::StrBefore => {
                        exprs.push(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .str()
                                .strip_suffix(second_decoded)
                                .alias(LANG_STRING_VALUE_FIELD),
                        );
                    }
                    Function::StrAfter => {
                        exprs.push(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .str()
                                .strip_prefix(second_decoded)
                                .alias(LANG_STRING_VALUE_FIELD),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
                exprs.push(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(LANG_STRING_LANG_FIELD)
                        .alias(LANG_STRING_LANG_FIELD),
                );
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
                                exprs.push(
                                    decoded
                                        .str()
                                        .strip_suffix(second_decoded.clone())
                                        .alias(&bt.field_col_name()),
                                );
                            }
                            Function::StrAfter => {
                                exprs.push(
                                    decoded
                                        .str()
                                        .strip_prefix(second_decoded.clone())
                                        .alias(&bt.field_col_name()),
                                );
                            }
                            _ => panic!("Should never happen"),
                        }
                        keep_types.push(bt);
                    }
                    if bt.is_lang_string() {
                        exprs.push(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD)
                                .alias(LANG_STRING_LANG_FIELD),
                        );
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
        }
        Function::StrLen => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let mut expr = str_function(first_context.as_str(), t, global_cats);
            expr = expr.str().len_chars().cast(DataType::Int64);
            expr = expr.alias(outer_context.as_str());
            solution_mappings.mappings = solution_mappings.mappings.with_column(expr);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::LCase | Function::UCase | Function::SubStr => {
            if matches!(func, Function::LCase | Function::UCase) {
                assert_eq!(args.len(), 1);
            } else {
                assert!(args.len() == 2 || args.len() == 3)
            }
            let first_context = args_contexts.get(&0).unwrap();
            let starting_loc = if let Some(Expression::Literal(starting_loc_lit)) = args.get(1) {
                let starting_loc: i64 = starting_loc_lit.value().parse().unwrap();
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
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
                                            str_expr
                                                .str()
                                                .to_lowercase()
                                                .alias(outer_context.as_str()),
                                        );
                                }
                                Function::UCase => {
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
                                            str_expr
                                                .str()
                                                .to_uppercase()
                                                .alias(outer_context.as_str()),
                                        );
                                }
                                Function::SubStr => {
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
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
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
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
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
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
                                    solution_mappings.mappings =
                                        solution_mappings.mappings.with_column(
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
                                col(first_context.as_str()).struct_().with_fields(vec![
                                    maybe_decode_expr(
                                        col(first_context.as_str())
                                            .struct_()
                                            .field_by_name(LANG_STRING_LANG_FIELD),
                                        b,
                                        s,
                                        global_cats,
                                    )
                                    .alias(LANG_STRING_LANG_FIELD),
                                ]),
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
                                            .slice(
                                                starting_loc.as_ref().unwrap().clone(),
                                                length.clone(),
                                            )
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
                                    exprs.push(
                                        str_expr
                                            .str()
                                            .to_lowercase()
                                            .alias(LANG_STRING_VALUE_FIELD),
                                    );
                                }
                                Function::UCase => {
                                    exprs.push(
                                        str_expr
                                            .str()
                                            .to_uppercase()
                                            .alias(LANG_STRING_VALUE_FIELD),
                                    );
                                }
                                Function::SubStr => {
                                    exprs.push(
                                        str_expr
                                            .str()
                                            .slice(
                                                starting_loc.as_ref().unwrap().clone(),
                                                length.clone(),
                                            )
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
                        assert_eq!(exprs.len(), 1);
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
        }

        Function::StrStarts | Function::StrEnds | Function::Contains => {
            assert_eq!(args.len(), 2);
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
            assert!(second_t.is_lit_type(xsd::STRING));
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
                let decoded =
                    maybe_decode_expr(col(first_context.as_str()), bt, bs, global_cats.clone());
                match func {
                    Function::StrStarts => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            decoded
                                .str()
                                .starts_with(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrEnds => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            decoded
                                .str()
                                .ends_with(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::Contains => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            decoded
                                .str()
                                .contains_literal(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
            } else if t.is_lang_string() {
                match func {
                    Function::StrStarts => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .cast(DataType::String)
                                .str()
                                .starts_with(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrEnds => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .cast(DataType::String)
                                .str()
                                .ends_with(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::Contains => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .cast(DataType::String)
                                .str()
                                .contains_literal(second_decoded)
                                .alias(outer_context.as_str()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
            } else if t.is_none() {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(first_context.as_str()).alias(outer_context.as_str()));
            } else {
                todo!()
            }
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::IsBlank => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = if t.is_multi() {
                if t.map.contains_key(&BaseRDFNodeType::BlankNode) {
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(&BaseRDFNodeType::BlankNode.field_col_name())
                        .is_not_null()
                } else {
                    lit(false)
                }
            } else {
                if t.is_blank_node() {
                    col(first_context.as_str()).is_not_null()
                } else {
                    lit(false)
                }
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));

            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::IsIri => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = if t.is_multi() {
                let contains_iri = t.map.contains_key(&BaseRDFNodeType::IRI);
                if contains_iri {
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(&BaseRDFNodeType::IRI.field_col_name())
                        .is_not_null()
                } else {
                    lit(false)
                }
            } else {
                if t.is_iri() {
                    col(first_context.as_str()).is_not_null()
                } else {
                    lit(false)
                }
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));

            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::IsLiteral => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = if t.is_multi() {
                let mut exprs = vec![];
                for t in t.map.keys() {
                    if let BaseRDFNodeType::Literal(_) = t {
                        exprs.push(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(&t.field_col_name())
                                .is_not_null(),
                        );
                    }
                }
                let mut expr = exprs.pop().unwrap_or(lit(false));
                for e in exprs {
                    expr = expr.or(e)
                }
                expr
            } else {
                let b = t.get_base_type().unwrap();
                match b {
                    BaseRDFNodeType::Literal(_) => col(first_context.as_str()).is_null().not(),
                    _ => lit(false),
                }
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        }
        Function::Datatype => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = if t.is_multi() {
                let mut exprs = vec![];
                for t in t.map.keys() {
                    if let BaseRDFNodeType::Literal(l) = t {
                        exprs.push(lit(rdf_named_node_to_polars_literal_value(l)));
                    }
                }
                if !exprs.is_empty() {
                    coalesce(exprs.as_slice())
                } else {
                    lit(LiteralValue::untyped_null())
                }
            } else {
                let b = t.get_base_type().unwrap();
                match b {
                    BaseRDFNodeType::Literal(l) => lit(rdf_named_node_to_polars_literal_value(l)),
                    _ => lit(LiteralValue::untyped_null()),
                }
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
            );
        }
        _ => {
            todo!("{}", func)
        }
    }
    solution_mappings = drop_inner_contexts(solution_mappings, &args_contexts.values().collect());
    Ok(solution_mappings)
}

fn create_regex_expr(
    expr: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    pattern: &str,
    global_cats: Arc<Cats>,
) -> Expr {
    let do_regex = match t {
        BaseRDFNodeType::BlankNode | BaseRDFNodeType::None | BaseRDFNodeType::IRI => false,
        BaseRDFNodeType::Literal(l) => {
            matches!(l.as_ref(), xsd::STRING | rdf::LANG_STRING)
        }
    };
    if do_regex {
        maybe_decode_expr(expr, t, s, global_cats)
            .str()
            .contains(lit(pattern), true)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    }
}

fn create_regex_replace_expr(
    expr: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    pattern: &str,
    replacement: &Expr,
    global_cats: Arc<Cats>,
) -> Expr {
    let do_regex_replace = match t {
        BaseRDFNodeType::BlankNode | BaseRDFNodeType::None | BaseRDFNodeType::IRI => false,
        BaseRDFNodeType::Literal(l) => {
            matches!(l.as_ref(), xsd::STRING | rdf::LANG_STRING)
        }
    };
    if do_regex_replace {
        maybe_decode_expr(expr, t, s, global_cats)
            .str()
            .replace_all(lit(pattern), replacement.clone(), false)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::String)
    }
}

fn create_regex_literal(regex_literal: &Literal, flags_expr: Option<&Expression>) -> String {
    if !regex_literal.is_plain() {
        todo!("Non plain literal regex lit")
    }
    let flags = if let Some(third_expr) = flags_expr {
        if let Expression::Literal(l) = third_expr {
            if !l.is_plain() {
                todo!("Non plain literal flags for regex")
            }
            Some(l.value())
        } else {
            todo!("Non literal flag for regex")
        }
    } else {
        None
    };

    let pattern = add_regex_feature_flags(regex_literal.value(), flags);
    pattern
}

pub fn str_function(c: &str, t: &RDFNodeState, global_cats: Arc<Cats>) -> Expr {
    if t.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &t.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_IRI_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::BlankNode => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_BLANK_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::Literal(_) => {
                    if t.is_lang_string() {
                        cast_lang_string_to_string(c, t, s, global_cats.clone())
                    } else {
                        maybe_decode_expr(
                            col(c).struct_().field_by_name(&t.field_col_name()),
                            t,
                            s,
                            global_cats.clone(),
                        )
                    }
                }
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
            })
        }
        coalesce(to_coalesce.as_slice()).alias(c)
    } else {
        let b = t.get_base_type().unwrap();
        let s = t.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                maybe_decode_expr(col(c), b, s, global_cats)
            }
            BaseRDFNodeType::Literal(_) => {
                if t.is_lang_string() {
                    cast_lang_string_to_string(c, b, s, global_cats)
                } else {
                    maybe_decode_expr(col(c), b, s, global_cats)
                }
            }
            BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
        }
    }
}

pub fn xsd_cast_literal(
    c: &str,
    src: &RDFNodeState,
    trg: &BaseRDFNodeType,
    global_cats: Arc<Cats>,
) -> Result<Expr, QueryProcessingError> {
    let trg_type = trg.default_input_polars_data_type();
    let trg_nn = if let BaseRDFNodeType::Literal(nn) = trg {
        nn.as_ref()
    } else {
        panic!("Invalid state")
    };
    if src.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &src.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => cast_iri_to_xsd_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    trg_nn,
                    trg_type.clone(),
                    global_cats.clone(),
                )?,
                BaseRDFNodeType::BlankNode => {
                    return Err(QueryProcessingError::BadCastDatatype(
                        c.to_string(),
                        trg.clone(),
                        t.clone(),
                    ))
                }
                BaseRDFNodeType::Literal(src_nn) => cast_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    global_cats.clone(),
                    src_nn.as_ref(),
                    trg_nn,
                    trg_type.clone(),
                ),
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(trg_type.clone()),
            });
        }
        Ok(coalesce(to_coalesce.as_slice()).alias(c))
    } else {
        let t = src.get_base_type().unwrap();
        let s = src.get_base_state().unwrap();
        match &t {
            BaseRDFNodeType::IRI => {
                cast_iri_to_xsd_literal(col(c), t, s, trg_nn, trg_type.clone(), global_cats.clone())
            }
            BaseRDFNodeType::BlankNode => Err(QueryProcessingError::BadCastDatatype(
                c.to_string(),
                trg.clone(),
                t.clone(),
            )),
            BaseRDFNodeType::Literal(src_nn) => Ok(cast_literal(
                col(c),
                t,
                s,
                global_cats.clone(),
                src_nn.as_ref(),
                trg_nn,
                trg_type.clone(),
            )),
            BaseRDFNodeType::None => Ok(lit(LiteralValue::untyped_null()).cast(trg_type)),
        }
    }
}

fn cast_iri_to_xsd_literal(
    e: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    trg_nn: NamedNodeRef,
    trg_type: DataType,
    global_cats: Arc<Cats>,
) -> Result<Expr, QueryProcessingError> {
    if trg_nn == xsd::STRING {
        Ok(maybe_decode_expr(e, t, s, global_cats))
    } else {
        Ok(lit(LiteralValue::untyped_null()).cast(trg_type.clone()))
        // Err(QueryProcessingError::BadCastDatatype(
        //     c.to_string(),
        //     src.clone(),
        //     trg.clone(),
        // ))
    }
}

fn cast_literal(
    mut c: Expr,
    src_bt: &BaseRDFNodeType,
    src_bs: &BaseCatState,
    global_cats: Arc<Cats>,
    src: NamedNodeRef,
    trg: NamedNodeRef,
    trg_type: DataType,
) -> Expr {
    if src == xsd::STRING && trg != xsd::STRING {
        c = maybe_decode_expr(c, src_bt, src_bs, global_cats);
    }
    if src == xsd::STRING && trg == xsd::BOOLEAN {
        c.cast(DataType::String)
            .str()
            .to_lowercase()
            .eq(lit("true"))
    } else if src == xsd::STRING && trg == xsd::DATE_TIME {
        c.cast(DataType::String).str().to_datetime(
            None,
            None,
            StrptimeOptions {
                format: None,
                strict: true,
                exact: false,
                cache: false,
            },
            lit("raise"),
        )
    } else if src == xsd::STRING && trg == xsd::DATE {
        c.cast(DataType::String).str().to_date(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::TIME {
        c.cast(DataType::String).str().to_time(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::DURATION {
        //Todo handle durations
        c
    } else {
        c.cast(trg_type)
    }
}

pub fn add_regex_feature_flags(pattern: &str, flags: Option<&str>) -> String {
    if let Some(flags) = flags {
        //TODO: Validate flags..
        format!("(?{flags}){pattern}")
    } else {
        pattern.to_string()
    }
}
