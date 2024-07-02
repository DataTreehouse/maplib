use crate::constants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, FLOOR_DATETIME_TO_SECONDS_INTERVAL, MODULUS,
    NANOS_AS_DATETIME, SECONDS_AS_DATETIME,
};
use crate::errors::QueryProcessingError;
use log::{debug, warn};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, NamedNodeRef, Variable};
use polars::datatypes::{DataType, TimeUnit};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{
    coalesce, col, concat_str, is_in, lit, when, Expr, IntoLazy, LazyFrame, LiteralValue, Operator,
    Series,
};
use representation::multitype::{all_multi_main_cols, multi_has_this_type_column};
use representation::multitype::{non_multi_type_string, sparql_str_function};
use representation::query_context::Context;
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::SolutionMappings;
use representation::{
    literal_is_boolean, literal_is_datetime, literal_is_numeric, literal_is_string,
    BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::sync::Arc;

pub fn named_node(
    mut solution_mappings: SolutionMappings,
    nn: &NamedNode,
    context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        Expr::Literal(rdf_named_node_to_polars_literal_value(nn)).alias(context.as_str()),
    );
    solution_mappings
        .rdf_node_types
        .insert(context.as_str().to_string(), RDFNodeType::IRI);
    Ok(solution_mappings)
}

pub fn literal(
    mut solution_mappings: SolutionMappings,
    lit: &Literal,
    context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        Expr::Literal(rdf_literal_to_polars_literal_value(lit)).alias(context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        context.as_str().to_string(),
        RDFNodeType::Literal(lit.datatype().into_owned()),
    );
    Ok(solution_mappings)
}

pub fn variable(
    mut solution_mappings: SolutionMappings,
    v: &Variable,
    context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
        return Err(QueryProcessingError::VariableNotFound(
            v.as_str().to_string(),
            context.as_str().to_string(),
        ));
    }
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(col(v.as_str()).alias(context.as_str()));
    let existing_type = solution_mappings.rdf_node_types.get(v.as_str()).unwrap();
    solution_mappings
        .rdf_node_types
        .insert(context.as_str().to_string(), existing_type.clone());
    Ok(solution_mappings)
}

pub fn binary_expression(
    mut solution_mappings: SolutionMappings,
    op: Operator,
    left_context: &Context,
    right_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let expr = if op == Operator::Eq {
        let left_type = solution_mappings
            .rdf_node_types
            .get(left_context.as_str())
            .unwrap();
        let right_type = solution_mappings
            .rdf_node_types
            .get(right_context.as_str())
            .unwrap();
        typed_equals_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
        )
    } else {
        Expr::BinaryExpr {
            left: Arc::new(col(left_context.as_str())),
            op,
            right: Arc::new(col(right_context.as_str())),
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));

    let t = match op {
        Operator::LtEq
        | Operator::GtEq
        | Operator::Gt
        | Operator::Lt
        | Operator::And
        | Operator::Eq
        | Operator::Or => RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide => {
            let left_type = solution_mappings
                .rdf_node_types
                .get(left_context.as_str())
                .unwrap();
            let right_type = solution_mappings
                .rdf_node_types
                .get(right_context.as_str())
                .unwrap();
            let div_int = if op == Operator::Divide {
                if let RDFNodeType::Literal(right_lit) = right_type {
                    matches!(
                        right_lit.as_ref(),
                        xsd::INT
                            | xsd::LONG
                            | xsd::INTEGER
                            | xsd::BYTE
                            | xsd::SHORT
                            | xsd::UNSIGNED_INT
                            | xsd::UNSIGNED_LONG
                            | xsd::UNSIGNED_BYTE
                            | xsd::UNSIGNED_SHORT
                    )
                } else {
                    false
                }
            } else {
                false
            };

            if div_int {
                RDFNodeType::Literal(xsd::DOUBLE.into_owned())
                //TODO: Fix,
            } else {
                left_type.clone()
            }
        }
        _ => {
            panic!()
        }
    };

    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), t);
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![left_context, right_context]);
    Ok(solution_mappings)
}

pub fn unary_plus(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(col(inner_context.as_str()).alias(outer_context.as_str()));
    let existing_type = solution_mappings
        .rdf_node_types
        .get(inner_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn unary_minus(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings //TODO: This is probably wrong
        .mappings
        .with_column(
            (Expr::BinaryExpr {
                left: Arc::new(Expr::Literal(LiteralValue::Int32(0))),
                op: Operator::Minus,
                right: Arc::new(col(inner_context.as_str())),
            })
            .alias(outer_context.as_str()),
        );
    let existing_type = solution_mappings
        .rdf_node_types
        .get(inner_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn not_expression(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        col(inner_context.as_str())
            .not()
            .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
    );
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn bound(
    mut solution_mappings: SolutionMappings,
    v: &Variable,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let expr = col_null_expr(v.as_str(), &solution_mappings.rdf_node_types).not();
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
    );
    Ok(solution_mappings)
}

pub fn col_null_expr(c: &str, rdf_node_types: &HashMap<String, RDFNodeType>) -> Expr {
    match rdf_node_types.get(c).unwrap() {
        RDFNodeType::Literal(l) => {
            if l.as_ref() == rdf::LANG_STRING {
                col(c)
                    .struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .is_null()
            } else {
                col(c).is_null()
            }
        }
        RDFNodeType::MultiType(t) => create_all_types_null_expression(c, t),
        RDFNodeType::None => lit(true),
        _ => col(c).is_null(),
    }
}

pub fn if_expression(
    mut solution_mappings: SolutionMappings,
    left_context: &Context,
    middle_context: &Context,
    right_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        (Expr::Ternary {
            predicate: Arc::new(col(left_context.as_str())),
            truthy: Arc::new(col(middle_context.as_str())),
            falsy: Arc::new(col(right_context.as_str())),
        })
        .alias(outer_context.as_str()),
    );
    //Todo: generalize..
    let existing_type = solution_mappings
        .rdf_node_types
        .get(middle_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(
        solution_mappings,
        &vec![left_context, middle_context, right_context],
    );
    Ok(solution_mappings)
}

pub fn coalesce_expression(
    mut solution_mappings: SolutionMappings,
    inner_contexts: Vec<Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let mut coal_exprs = vec![];
    for c in &inner_contexts {
        coal_exprs.push(col(c.as_str()));
    }

    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(coalesce(coal_exprs.as_slice()).alias(outer_context.as_str()));
    //TODO: generalize
    let existing_type = solution_mappings
        .rdf_node_types
        .get(inner_contexts.first().unwrap().as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(solution_mappings, &inner_contexts.iter().collect());
    Ok(solution_mappings)
}

pub fn exists(
    solution_mappings: SolutionMappings,
    exists_lf: LazyFrame,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings,
        rdf_node_types,
    } = solution_mappings;
    let mut df = mappings.collect().unwrap();
    let exists_df = exists_lf
        .select([col(inner_context.as_str())])
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .expect("Collect lazy exists error");
    let mut ser = Series::from(
        is_in(
            //TODO: Fix - this can now work in lazy
            df.column(inner_context.as_str()).unwrap(),
            exists_df.column(inner_context.as_str()).unwrap(),
        )
        .unwrap(),
    );
    ser.rename(outer_context.as_str());
    df.with_column(ser).unwrap();
    let mut solution_mappings = SolutionMappings::new(df.lazy(), rdf_node_types);
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn func_expression(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &Vec<Expression>,
    args_contexts: HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    match func {
        Function::Year => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .year()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Month => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .month()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Day => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .day()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Hours => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .hour()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Minutes => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .minute()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Seconds => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .dt()
                    .second()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
        }
        Function::Abs => {
            assert_eq!(args.len(), 1);
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
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .ceil()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
            );
        }
        Function::Floor => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .floor()
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
            );
        }
        Function::Concat => {
            assert!(args.len() > 1);
            let SolutionMappings {
                mappings,
                rdf_node_types: datatypes,
            } = solution_mappings;
            let cols: Vec<_> = (0..args.len())
                .map(|i| col(args_contexts.get(&i).unwrap().as_str()))
                .collect();
            let new_mappings =
                mappings.with_column(concat_str(cols, "", true).alias(outer_context.as_str()));
            solution_mappings = SolutionMappings::new(new_mappings, datatypes);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
            );
        }
        Function::Round => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                col(first_context.as_str())
                    .round(0)
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
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                sparql_str_function(
                    first_context.as_str(),
                    solution_mappings
                        .rdf_node_types
                        .get(first_context.as_str())
                        .unwrap(),
                )
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
            );
        }
        Function::Lang => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            let dt = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            match dt {
                RDFNodeType::Literal(l) => {
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
                RDFNodeType::MultiType(ts) => {
                    let mut exprs = vec![];
                    //Prioritize column that is lang string
                    for t in ts {
                        if t.is_lang_string() {
                            exprs.push(
                                col(first_context.as_str())
                                    .struct_()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .cast(DataType::String),
                            )
                        }
                    }
                    //Then the rest..
                    for t in ts {
                        if !t.is_lang_string() && matches!(t, BaseRDFNodeType::Literal(_)) {
                            exprs.push(
                                when(
                                    col(first_context.as_str())
                                        .struct_()
                                        .field_by_name(&non_multi_type_string(t))
                                        .is_null()
                                        .not(),
                                )
                                .then(lit(""))
                                .otherwise(lit(LiteralValue::Null).cast(DataType::String)),
                            )
                        } else {
                            exprs.push(lit(LiteralValue::Null).cast(DataType::String))
                        }
                    }
                    solution_mappings.mappings = solution_mappings
                        .mappings
                        .with_column(coalesce(exprs.as_slice()).alias(outer_context.as_str()));
                }
                _ => {
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        lit(LiteralValue::Null)
                            .cast(DataType::String)
                            .alias(outer_context.as_str()),
                    );
                }
            }
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
            );
        }
        Function::LangMatches => {
            assert!(args.len() == 2);
            let lang_expr = col(args_contexts.get(&0).unwrap().as_str());

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
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
            } else {
                todo!("Handle this error.. ")
            }
        }
        Function::Regex => {
            assert!(args.len() == 2 || args.len() == 3);
            let text_context = args_contexts.get(&0).unwrap();
            if let Expression::Literal(regex_lit) = args.get(1).unwrap() {
                if !regex_lit.is_plain() {
                    todo!("Non plain literal regex lit")
                }
                let flags = if let Some(third_expr) = args.get(2) {
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

                let pattern = add_regex_feature_flags(regex_lit.value(), flags);
                debug!("Effective pattern is {}", pattern);
                let t = solution_mappings
                    .rdf_node_types
                    .get(text_context.as_str())
                    .unwrap();
                if let RDFNodeType::MultiType(_ts) = t {
                    todo!("Multitypes and regex")
                    // let mut exprs = vec![];
                    // for t in ts {
                    //     exprs.push(mul)
                    // }
                } else {
                    let t = BaseRDFNodeType::from_rdf_node_type(t);
                    let use_col = match t {
                        BaseRDFNodeType::BlankNode
                        | BaseRDFNodeType::None
                        | BaseRDFNodeType::IRI => None,
                        BaseRDFNodeType::Literal(l) => {
                            if l.as_ref() == xsd::STRING {
                                Some(col(text_context.as_str()))
                            } else if l.as_ref() == rdf::LANG_STRING {
                                Some(
                                    col(text_context.as_str())
                                        .struct_()
                                        .field_by_name(LANG_STRING_VALUE_FIELD),
                                )
                            } else {
                                warn!("Tried to apply REGEX to non-string.");
                                None
                            }
                        }
                    };
                    if let Some(use_col) = use_col {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            use_col
                                .cast(DataType::String)
                                .str()
                                .contains(lit(pattern), true)
                                .alias(outer_context.as_str()),
                        );
                    } else {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            lit(LiteralValue::Null)
                                .cast(DataType::Boolean)
                                .alias(outer_context.as_str()),
                        );
                    }
                    solution_mappings.rdf_node_types.insert(
                        outer_context.as_str().to_string(),
                        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                    );
                }
            } else {
                unimplemented!("Non literal regex")
            }
        }
        Function::Custom(nn) => {
            let iri = nn.as_str();
            if iri == xsd::INTEGER.as_str() {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .cast(DataType::Int64)
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                );
            } else if iri == xsd::STRING.as_str() {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .cast(DataType::String)
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::STRING.into_owned()),
                );
            } else if iri == DATETIME_AS_NANOS {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(&first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                        .cast(DataType::UInt64)
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                );
            } else if iri == DATETIME_AS_SECONDS {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(&first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        .cast(DataType::UInt64)
                        .div(lit(1000))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                );
            } else if iri == NANOS_AS_DATETIME {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(&first_context.as_str())
                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
                );
            } else if iri == SECONDS_AS_DATETIME {
                assert_eq!(args.len(), 1);
                let first_context = args_contexts.get(&0).unwrap();
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(&first_context.as_str())
                        .mul(Expr::Literal(LiteralValue::UInt64(1000)))
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
                );
            } else if iri == MODULUS {
                assert_eq!(args.len(), 2);
                let first_context = args_contexts.get(&0).unwrap();
                let second_context = args_contexts.get(&1).unwrap();

                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    (col(&first_context.as_str()) % col(&second_context.as_str()))
                        .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                );
            } else if iri == FLOOR_DATETIME_TO_SECONDS_INTERVAL {
                assert_eq!(args.len(), 2);
                let first_context = args_contexts.get(&0).unwrap();
                let second_context = args_contexts.get(&1).unwrap();

                let first_as_seconds = col(&first_context.as_str())
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    .cast(DataType::UInt64)
                    .div(lit(1000));

                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    ((first_as_seconds.clone()
                        - (first_as_seconds % col(&second_context.as_str())))
                    .mul(Expr::Literal(LiteralValue::UInt64(1000)))
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
                    .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
                );
            } else {
                todo!("{:?}", nn)
            }
        }
        Function::Contains => {
            assert_eq!(args.len(), 2);
            let first_context = args_contexts.get(&0).unwrap();
            let second_context = args_contexts.get(&1).unwrap();

            solution_mappings.mappings = solution_mappings.mappings.with_column(
                (col(&first_context.as_str())
                    .str()
                    .contains_literal(col(&second_context.as_str())))
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::StrStarts => {
            assert_eq!(args.len(), 2);
            let first_context = args_contexts.get(&0).unwrap();
            let second_context = args_contexts.get(&1).unwrap();

            solution_mappings.mappings = solution_mappings.mappings.with_column(
                (col(&first_context.as_str())
                    .str()
                    .starts_with(col(&second_context.as_str())))
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::StrEnds => {
            assert_eq!(args.len(), 2);
            let first_context = args_contexts.get(&0).unwrap();
            let second_context = args_contexts.get(&1).unwrap();

            solution_mappings.mappings = solution_mappings.mappings.with_column(
                (col(&first_context.as_str())
                    .str()
                    .ends_with(col(&second_context.as_str())))
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::IsBlank => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = match t {
                RDFNodeType::MultiType(types) => {
                    if types.contains(&BaseRDFNodeType::BlankNode) {
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(&multi_has_this_type_column(&BaseRDFNodeType::BlankNode))
                            .fill_null(lit(false))
                    } else {
                        lit(false)
                    }
                }
                RDFNodeType::BlankNode => col(first_context.as_str()).is_null().not(),
                _ => lit(false),
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));

            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::IsIri => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = match t {
                RDFNodeType::MultiType(types) => {
                    if types.contains(&BaseRDFNodeType::IRI) {
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(&multi_has_this_type_column(&BaseRDFNodeType::IRI))
                            .fill_null(lit(false))
                    } else {
                        lit(false)
                    }
                }
                RDFNodeType::IRI => col(first_context.as_str()).is_null().not(),
                _ => lit(false),
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));

            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::IsLiteral => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = match t {
                RDFNodeType::MultiType(types) => {
                    let mut exprs = vec![];
                    for t in types {
                        if let BaseRDFNodeType::Literal(_) = t {
                            exprs.push(
                                col(first_context.as_str())
                                    .struct_()
                                    .field_by_name(&multi_has_this_type_column(&t))
                                    .fill_null(lit(false)),
                            );
                        }
                    }
                    let mut expr = exprs.pop().unwrap_or(lit(false));
                    for e in exprs {
                        expr = expr.or(e)
                    }
                    expr
                }
                RDFNodeType::Literal(_) => col(first_context.as_str()).is_null().not(),
                _ => lit(false),
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
            );
        }
        Function::Datatype => {
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let expr = match t {
                RDFNodeType::MultiType(types) => {
                    let mut exprs = vec![];
                    for t in types {
                        if let BaseRDFNodeType::Literal(l) = t {
                            exprs.push(lit(rdf_named_node_to_polars_literal_value(l)));
                        }
                    }
                    if !exprs.is_empty() {
                        coalesce(exprs.as_slice())
                    } else {
                        lit(LiteralValue::Null)
                    }
                }
                RDFNodeType::Literal(l) => lit(rdf_named_node_to_polars_literal_value(l)),
                _ => lit(LiteralValue::Null),
            };
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(expr.alias(outer_context.as_str()));
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), RDFNodeType::IRI);
        }
        _ => {
            todo!("{}", func)
        }
    }
    solution_mappings = drop_inner_contexts(solution_mappings, &args_contexts.values().collect());
    Ok(solution_mappings)
}

pub fn in_expression(
    mut solution_mappings: SolutionMappings,
    left_context: &Context,
    right_contexts: &Vec<Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let mut expr = Expr::Literal(LiteralValue::Boolean(false));
    let left_type = solution_mappings
        .rdf_node_types
        .get(left_context.as_str())
        .unwrap();
    for right_context in right_contexts {
        let right_type = solution_mappings
            .rdf_node_types
            .get(right_context.as_str())
            .unwrap();
        expr = expr.or(typed_equals_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
        ));
    }

    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
    );
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![left_context]);
    solution_mappings = drop_inner_contexts(solution_mappings, &right_contexts.iter().collect());

    Ok(solution_mappings)
}

fn typed_equals_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeType,
    right_type: &RDFNodeType,
) -> Expr {
    if let RDFNodeType::MultiType(left_types) = left_type {
        if let RDFNodeType::MultiType(right_types) = right_type {
            let mut eq = lit(false);
            for lt in left_types {
                if right_types.contains(lt) {
                    eq = eq.or(col(left_col)
                        .struct_()
                        .field_by_name(&multi_has_this_type_column(lt))
                        .is_null()
                        .not()
                        .and(
                            col(left_col)
                                .struct_()
                                .field_by_name(&multi_has_this_type_column(lt)),
                        )
                        .and(
                            col(right_col)
                                .struct_()
                                .field_by_name(&multi_has_this_type_column(lt))
                                .is_null()
                                .not()
                                .and(
                                    col(right_col)
                                        .struct_()
                                        .field_by_name(&multi_has_this_type_column(lt)),
                                ),
                        )
                        .and(
                            col(left_col)
                                .struct_()
                                .field_by_name(&non_multi_type_string(lt))
                                .eq(col(left_col)
                                    .struct_()
                                    .field_by_name(&non_multi_type_string(lt))),
                        ));
                }
            }
            eq
        } else {
            let right_type = BaseRDFNodeType::from_rdf_node_type(right_type);
            if left_types.contains(&right_type) {
                col(left_col)
                    .struct_()
                    .field_by_name(&multi_has_this_type_column(&right_type))
                    .is_null()
                    .not()
                    .and(
                        col(left_col)
                            .struct_()
                            .field_by_name(&multi_has_this_type_column(&right_type)),
                    )
                    .and(
                        col(left_col)
                            .struct_()
                            .field_by_name(&non_multi_type_string(&right_type))
                            .eq(col(right_col)),
                    )
            } else {
                lit(false)
            }
        }
    } else if let RDFNodeType::MultiType(right_types) = right_type {
        let left_type = BaseRDFNodeType::from_rdf_node_type(left_type);
        if right_types.contains(&left_type) {
            col(right_col)
                .struct_()
                .field_by_name(&multi_has_this_type_column(&left_type))
                .is_null()
                .not()
                .and(
                    col(right_col)
                        .struct_()
                        .field_by_name(&multi_has_this_type_column(&left_type)),
                )
                .and(
                    col(right_col)
                        .struct_()
                        .field_by_name(&non_multi_type_string(&left_type))
                        .eq(col(left_col)),
                )
        } else {
            lit(false)
        }
    } else {
        if left_type == right_type {
            col(left_col).eq(col(right_col))
        } else {
            lit(false)
        }
    }
}

pub fn drop_inner_contexts(mut sm: SolutionMappings, contexts: &Vec<&Context>) -> SolutionMappings {
    let mut inner = vec![];
    for c in contexts {
        let cstr = c.as_str();
        sm.rdf_node_types.remove(cstr);
        inner.push(cstr.to_string());
    }
    sm.mappings = sm.mappings.drop_no_validate(inner);
    sm
}

pub fn compatible_operation(expression: Expression, l1: NamedNodeRef, l2: NamedNodeRef) -> bool {
    let compat = match expression {
        Expression::Equal(..)
        | Expression::LessOrEqual(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Greater(..)
        | Expression::Less(..) => {
            (literal_is_numeric(l1) && literal_is_numeric(l2))
                || (literal_is_boolean(l1) && literal_is_boolean(l2))
                || (literal_is_string(l1) && literal_is_string(l2))
                || (literal_is_datetime(l1) && literal_is_datetime(l2))
        }
        Expression::Or(..) | Expression::And(..) => {
            literal_is_boolean(l1) && literal_is_boolean(l2)
        }
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..) => literal_is_numeric(l1) && literal_is_numeric(l2),
        _ => todo!(),
    };
    compat
}

pub fn create_all_types_null_expression(c: &str, types: &Vec<BaseRDFNodeType>) -> Expr {
    let mut all_types_null: Option<Expr> = None;
    for x in all_multi_main_cols(types) {
        let e = col(c).struct_().field_by_name(&x).is_null();
        all_types_null = if let Some(all_types_null) = all_types_null {
            Some(all_types_null.and(e))
        } else {
            Some(e)
        };
    }
    all_types_null.unwrap()
}

pub fn add_regex_feature_flags(pattern: &str, flags: Option<&str>) -> String {
    if let Some(flags) = flags {
        //TODO: Validate flags..
        format!("(?{}){}", flags, pattern)
    } else {
        pattern.to_string()
    }
}
