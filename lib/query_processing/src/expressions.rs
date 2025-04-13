use crate::constants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, FLOOR_DATETIME_TO_SECONDS_INTERVAL, MODULUS,
    NANOS_AS_DATETIME, SECONDS_AS_DATETIME,
};
use crate::errors::QueryProcessingError;
use log::warn;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, NamedNodeRef, Variable};
use polars::datatypes::{CategoricalOrdering, DataType, TimeUnit};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{
    as_struct, coalesce, col, concat_str, lit, when, Expr, GetOutput, IntoColumn, LazyFrame,
    LiteralValue, NamedFrom, Operator, PlSmallStr, Scalar, StrptimeOptions,
};
use polars::series::Series;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::multitype::{
    all_multi_main_cols, convert_lf_col_to_multitype, MULTI_BLANK_DT, MULTI_IRI_DT,
};
use representation::multitype::{base_col_name, set_structs_all_null_to_null_row};
use representation::query_context::Context;
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::SolutionMappings;
use representation::{
    literal_is_boolean, literal_is_date, literal_is_datetime, literal_is_numeric,
    literal_is_string, BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::{HashMap, HashSet};
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
    expression: &Expression,
    left_context: &Context,
    right_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let left_type = solution_mappings
        .rdf_node_types
        .get(left_context.as_str())
        .unwrap();
    let right_type = solution_mappings
        .rdf_node_types
        .get(right_context.as_str())
        .unwrap();
    if &RDFNodeType::None == left_type || &RDFNodeType::None == right_type {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(BaseRDFNodeType::None.polars_data_type())
                .alias(outer_context.as_str()),
        );
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), RDFNodeType::None);
        solution_mappings =
            drop_inner_contexts(solution_mappings, &vec![left_context, right_context]);
        return Ok(solution_mappings);
    }
    let expr = if matches!(expression, Expression::Equal(..)) {
        typed_equals_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
        )
    } else if matches!(
        expression,
        Expression::Less(..)
            | Expression::Greater(..)
            | Expression::GreaterOrEqual(..)
            | Expression::LessOrEqual(..)
    ) {
        typed_comparison_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
            expression,
        )
    } else {
        let op = match expression {
            Expression::Or(_, _) => Operator::Or,
            Expression::And(_, _) => Operator::And,
            Expression::Add(_, _) => Operator::Plus,
            Expression::Subtract(_, _) => Operator::Minus,
            Expression::Multiply(_, _) => Operator::Multiply,
            Expression::Divide(_, _) => Operator::Divide,
            _ => panic!("Should never happen"),
        };

        Expr::BinaryExpr {
            left: Arc::new(col(left_context.as_str())),
            op,
            right: Arc::new(col(right_context.as_str())),
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));

    let t = match expression {
        Expression::LessOrEqual(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Greater(..)
        | Expression::Less(..)
        | Expression::And(..)
        | Expression::Equal(..)
        | Expression::Or(..) => RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..) => {
            let left_type = solution_mappings
                .rdf_node_types
                .get(left_context.as_str())
                .unwrap();
            let right_type = solution_mappings
                .rdf_node_types
                .get(right_context.as_str())
                .unwrap();
            let div_int = if matches!(expression, Expression::Divide(..)) {
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
                left: Arc::new(Expr::Literal(LiteralValue::Scalar(Scalar::from(0i32)))),
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
    let expr = expr_is_null_workaround(
        col(v.as_str()),
        solution_mappings.rdf_node_types.get(v.as_str()).unwrap(),
    )
    .not();
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
    );
    Ok(solution_mappings)
}

pub fn expr_is_null_workaround(expr: Expr, rdf_node_type: &RDFNodeType) -> Expr {
    match rdf_node_type {
        RDFNodeType::Literal(l) => {
            if l.as_ref() == rdf::LANG_STRING {
                expr.struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .is_null()
            } else {
                expr.is_null()
            }
        }
        RDFNodeType::MultiType(t) => create_all_types_null_expression(expr, t),
        _ => expr.is_null(),
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
    let mut basic_types = HashSet::new();
    for c in &inner_contexts {
        let dt = solution_mappings.rdf_node_types.get(c.as_str()).unwrap();
        if dt != &RDFNodeType::None {
            coal_exprs.push(col(c.as_str()));
            match dt {
                RDFNodeType::MultiType(types) => {
                    for t in types {
                        basic_types.insert(t.clone());
                    }
                }
                dt => {
                    basic_types.insert(BaseRDFNodeType::from_rdf_node_type(dt));
                }
            }
        }
    }
    let mut sorted_types: Vec<_> = basic_types.into_iter().collect();
    sorted_types.sort();
    if sorted_types.len() > 1 {
        for c in &inner_contexts {
            let dt = solution_mappings.rdf_node_types.get(c.as_str()).unwrap();
            if dt != &RDFNodeType::None {
                if !matches!(dt, RDFNodeType::MultiType(_)) {
                    solution_mappings.mappings = solution_mappings
                        .mappings
                        .with_column(convert_lf_col_to_multitype(c.as_str(), dt));
                    solution_mappings.rdf_node_types.insert(
                        c.as_str().to_string(),
                        RDFNodeType::MultiType(vec![BaseRDFNodeType::from_rdf_node_type(dt)]),
                    );
                }
                solution_mappings =
                    convert_multitype_col_to_wider(solution_mappings, c.as_str(), &sorted_types);
            }
        }
    }
    solution_mappings = set_structs_all_null_to_null_row(solution_mappings);

    if coal_exprs.is_empty() {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(BaseRDFNodeType::None.polars_data_type())
                .alias(outer_context.as_str()),
        );
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), RDFNodeType::None);
    } else {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(coalesce(coal_exprs.as_slice()).alias(outer_context.as_str()));
        let t = if sorted_types.len() > 1 {
            RDFNodeType::MultiType(sorted_types)
        } else {
            sorted_types.into_iter().next().unwrap().as_rdf_node_type()
        };
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t);
    }
    solution_mappings = drop_inner_contexts(solution_mappings, &inner_contexts.iter().collect());
    Ok(solution_mappings)
}

fn convert_multitype_col_to_wider(
    sm: SolutionMappings,
    c: &str,
    sorted_types: &Vec<BaseRDFNodeType>,
) -> SolutionMappings {
    let SolutionMappings {
        mut mappings,
        mut rdf_node_types,
        height_estimate,
    } = sm;
    let t = rdf_node_types.get(c).unwrap();
    if let RDFNodeType::MultiType(existing_types) = t {
        let mut struct_exprs = vec![];
        for t in sorted_types {
            let name = base_col_name(t);
            if existing_types.contains(t) {
                if t.is_lang_string() {
                    struct_exprs.push(
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .alias(LANG_STRING_VALUE_FIELD),
                    );
                    struct_exprs.push(
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .alias(LANG_STRING_LANG_FIELD),
                    );
                } else {
                    struct_exprs.push(col(c).struct_().field_by_name(&name).alias(&name));
                }
            } else if t.is_lang_string() {
                struct_exprs.push(
                    lit(LiteralValue::untyped_null())
                        .cast(DataType::Categorical(None, CategoricalOrdering::Physical))
                        .alias(LANG_STRING_VALUE_FIELD),
                );
                struct_exprs.push(
                    lit(LiteralValue::untyped_null())
                        .cast(DataType::Categorical(None, CategoricalOrdering::Physical))
                        .alias(LANG_STRING_LANG_FIELD),
                );
            } else {
                struct_exprs.push(
                    lit(LiteralValue::untyped_null())
                        .cast(t.polars_data_type())
                        .alias(&name),
                );
            }
        }
        mappings = mappings.with_column(as_struct(struct_exprs).alias(c));
        rdf_node_types.insert(c.to_string(), RDFNodeType::MultiType(sorted_types.clone()));
    } else {
        panic!("Should never be called with type not multi")
    }
    SolutionMappings::new(mappings, rdf_node_types, height_estimate)
}

pub fn exists(
    solution_mappings: SolutionMappings,
    exists_lf: LazyFrame,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        mut rdf_node_types,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let mut exists_df = exists_lf
        .select([col(inner_context.as_str())])
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .expect("Collect lazy exists error");
    let inner_context_col = exists_df.drop_in_place(inner_context.as_str()).unwrap();
    mappings = mappings.with_column(
        col(inner_context.as_str())
            .is_in(lit(inner_context_col.take_materialized_series()), false)
            .alias(outer_context.as_str()),
    );

    rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
    );
    let mut solution_mappings = SolutionMappings::new(mappings, rdf_node_types, height_upper_bound);
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn func_expression(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: HashMap<usize, Context>,
    outer_context: &Context,
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
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
                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
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
                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
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
            let cols: Vec<_> = (0..args.len())
                .map(|i| col(args_contexts.get(&i).unwrap().as_str()))
                .collect();
            let new_mappings =
                mappings.with_column(concat_str(cols, "", true).alias(outer_context.as_str()));
            solution_mappings = SolutionMappings::new(new_mappings, datatypes, height_upper_bound);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
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
                )
                .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
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
                                        .field_by_name(&base_col_name(t))
                                        .is_null()
                                        .not(),
                                )
                                .then(lit(""))
                                .otherwise(
                                    lit(LiteralValue::untyped_null()).cast(DataType::String),
                                ),
                            )
                        } else {
                            exprs.push(lit(LiteralValue::untyped_null()).cast(DataType::String))
                        }
                    }
                    solution_mappings.mappings = solution_mappings
                        .mappings
                        .with_column(coalesce(exprs.as_slice()).alias(outer_context.as_str()));
                }
                _ => {
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        lit(LiteralValue::untyped_null())
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
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
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
                            lit(LiteralValue::untyped_null())
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
                            Ok(Some(s.into_column()))
                        },
                        GetOutput::from_type(DataType::String),
                    ))
                .alias(outer_context.as_str()),
            );
            solution_mappings.mappings = solution_mappings.mappings.drop([col(&tmp_column)]);
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), RDFNodeType::IRI);
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
                            Ok(Some(s.into_column()))
                        },
                        GetOutput::from_type(DataType::String),
                    )
                    .alias(outer_context.as_str()),
            );
            solution_mappings.mappings = solution_mappings.mappings.drop([col(&tmp_column)]);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::STRING.into_owned()),
            );
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
                    )?
                    .alias(outer_context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    outer_context.as_str().to_string(),
                    RDFNodeType::Literal(nn.to_owned()),
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
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
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
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
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
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
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
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
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
                    RDFNodeType::Literal(xsd::INTEGER.into_owned()),
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
                    RDFNodeType::Literal(xsd::DATE_TIME.into_owned()),
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
            if t.is_lit_type(xsd::STRING) {
                match func {
                    Function::StrBefore => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .cast(DataType::String)
                                .str()
                                .strip_suffix(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrAfter => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .cast(DataType::String)
                                .str()
                                .strip_prefix(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
            } else if t.is_lang_string() {
                match func {
                    Function::StrBefore => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .str()
                                .strip_suffix(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrAfter => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .str()
                                .strip_prefix(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
            } else if t == &RDFNodeType::None {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(first_context.as_str()).alias(outer_context.as_str()));
            } else {
                todo!()
            }
            solution_mappings
                .rdf_node_types
                .insert(outer_context.as_str().to_string(), t.clone());
        }
        Function::StrLen => {
            assert_eq!(args.len(), 1);
            let first_context = args_contexts.get(&0).unwrap();
            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            let mut expr = str_function(first_context.as_str(), t);
            expr = expr.str().len_chars().cast(DataType::Int64);
            expr = expr.alias(outer_context.as_str());
            solution_mappings.mappings = solution_mappings.mappings.with_column(expr);
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
            );
        }
        Function::StrStarts | Function::StrEnds | Function::Contains => {
            assert_eq!(args.len(), 2);
            let first_context = args_contexts.get(&0).unwrap();
            let second_context = args_contexts.get(&1).unwrap();

            let t = solution_mappings
                .rdf_node_types
                .get(first_context.as_str())
                .unwrap();
            if t.is_lit_type(xsd::STRING) {
                match func {
                    Function::StrStarts => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .cast(DataType::String)
                                .str()
                                .starts_with(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::StrEnds => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .cast(DataType::String)
                                .str()
                                .ends_with(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    Function::Contains => {
                        solution_mappings.mappings = solution_mappings.mappings.with_column(
                            col(first_context.as_str())
                                .cast(DataType::String)
                                .str()
                                .contains_literal(col(second_context.as_str()))
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
                                .starts_with(col(second_context.as_str()))
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
                                .ends_with(col(second_context.as_str()))
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
                                .contains_literal(col(second_context.as_str()))
                                .alias(outer_context.as_str()),
                        );
                    }
                    _ => panic!("Should never happen"),
                }
            } else if t == &RDFNodeType::None {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(first_context.as_str()).alias(outer_context.as_str()));
            } else {
                todo!()
            }
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
                            .field_by_name(&base_col_name(&BaseRDFNodeType::BlankNode))
                            .is_not_null()
                    } else {
                        lit(false)
                    }
                }
                RDFNodeType::BlankNode => col(first_context.as_str()).is_not_null(),
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
                            .field_by_name(&base_col_name(&BaseRDFNodeType::IRI))
                            .is_not_null()
                    } else {
                        lit(false)
                    }
                }
                RDFNodeType::IRI => col(first_context.as_str()).is_not_null(),
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
                                    .field_by_name(&base_col_name(t))
                                    .is_not_null(),
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
                        lit(LiteralValue::untyped_null())
                    }
                }
                RDFNodeType::Literal(l) => lit(rdf_named_node_to_polars_literal_value(l)),
                _ => lit(LiteralValue::untyped_null()),
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
    let mut expr = Expr::Literal(LiteralValue::Scalar(Scalar::from(false)));
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
                        .field_by_name(&base_col_name(lt))
                        .is_not_null()
                        .and(
                            col(right_col)
                                .struct_()
                                .field_by_name(&base_col_name(lt))
                                .is_not_null(),
                        )
                        .and(
                            col(left_col)
                                .struct_()
                                .field_by_name(&base_col_name(lt))
                                .eq(col(left_col).struct_().field_by_name(&base_col_name(lt))),
                        ));
                }
            }
            eq
        } else {
            let right_type = BaseRDFNodeType::from_rdf_node_type(right_type);
            if left_types.contains(&right_type) {
                col(left_col)
                    .struct_()
                    .field_by_name(&base_col_name(&right_type))
                    .is_not_null()
                    .and(
                        col(left_col)
                            .struct_()
                            .field_by_name(&base_col_name(&right_type))
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
                .field_by_name(&base_col_name(&left_type))
                .is_not_null()
                .and(
                    col(right_col)
                        .struct_()
                        .field_by_name(&base_col_name(&left_type))
                        .eq(col(left_col)),
                )
        } else {
            lit(false)
        }
    } else if left_type == right_type {
        col(left_col).eq(col(right_col))
    } else {
        lit(false)
    }
}

fn typed_comparison_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeType,
    right_type: &RDFNodeType,
    expression: &Expression,
) -> Expr {
    let mut comps = vec![];
    if let RDFNodeType::MultiType(left_types) = left_type {
        if let RDFNodeType::MultiType(right_types) = right_type {
            for lt in left_types {
                for rt in right_types {
                    if let BaseRDFNodeType::Literal(lt_nn) = lt {
                        if let BaseRDFNodeType::Literal(rt_nn) = rt {
                            comps.push(comp(
                                col(left_col).struct_().field_by_name(&base_col_name(lt)),
                                col(right_col).struct_().field_by_name(&base_col_name(rt)),
                                lt_nn.as_ref(),
                                rt_nn.as_ref(),
                                expression,
                            ));
                        }
                    }
                }
            }
        } else if let RDFNodeType::Literal(rt_nn) = right_type {
            //Only left multi
            for lt in left_types {
                if let BaseRDFNodeType::Literal(lt_nn) = lt {
                    comps.push(comp(
                        col(left_col).struct_().field_by_name(&base_col_name(lt)),
                        col(right_col),
                        lt_nn.as_ref(),
                        rt_nn.as_ref(),
                        expression,
                    ));
                }
            }
        }
    } else if let RDFNodeType::MultiType(right_types) = right_type {
        if let RDFNodeType::Literal(lt_nn) = left_type {
            for rt in right_types {
                if let BaseRDFNodeType::Literal(rt_nn) = rt {
                    comps.push(comp(
                        col(left_col),
                        col(right_col).struct_().field_by_name(&base_col_name(rt)),
                        lt_nn.as_ref(),
                        rt_nn.as_ref(),
                        expression,
                    ));
                }
            }
        }
    } else if let RDFNodeType::Literal(lt_nn) = left_type {
        if let RDFNodeType::Literal(rt_nn) = right_type {
            comps.push(comp(
                col(left_col),
                col(right_col),
                lt_nn.as_ref(),
                rt_nn.as_ref(),
                expression,
            ));
        }
    }
    if comps.is_empty() {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    } else {
        coalesce(&comps)
    }
}

fn comp(
    e_left: Expr,
    e_right: Expr,
    dt_left: NamedNodeRef,
    dt_right: NamedNodeRef,
    expression: &Expression,
) -> Expr {
    let e = match expression {
        Expression::Greater(_, _) => e_left.clone().gt(e_right.clone()),
        Expression::GreaterOrEqual(_, _) => e_left.clone().gt_eq(e_right.clone()),
        Expression::Less(_, _) => e_left.clone().lt(e_right.clone()),
        Expression::LessOrEqual(_, _) => e_left.clone().lt_eq(e_right.clone()),
        _ => panic!("Should never happen"),
    };

    if compatible_operation(expression, dt_left, dt_right) {
        e
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
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

pub fn compatible_operation(expression: &Expression, l1: NamedNodeRef, l2: NamedNodeRef) -> bool {
    match expression {
        Expression::Equal(..)
        | Expression::LessOrEqual(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Greater(..)
        | Expression::Less(..) => {
            (literal_is_numeric(l1) && literal_is_numeric(l2))
                || (literal_is_boolean(l1) && literal_is_boolean(l2))
                || (literal_is_string(l1) && literal_is_string(l2))
                || (literal_is_datetime(l1) && literal_is_datetime(l2))
                || (literal_is_date(l1) && literal_is_date(l2))
        }
        Expression::Or(..) | Expression::And(..) => {
            literal_is_boolean(l1) && literal_is_boolean(l2)
        }
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..) => literal_is_numeric(l1) && literal_is_numeric(l2),
        _ => todo!(),
    }
}

pub fn create_all_types_null_expression(expr: Expr, types: &Vec<BaseRDFNodeType>) -> Expr {
    let mut all_types_null: Option<Expr> = None;
    for x in all_multi_main_cols(types) {
        let e = expr.clone().struct_().field_by_name(&x).is_null();
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

pub fn str_function(c: &str, t: &RDFNodeType) -> Expr {
    if let RDFNodeType::MultiType(types) = t {
        let mut to_coalesce = vec![];
        for t in types {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => col(c)
                    .struct_()
                    .field_by_name(MULTI_IRI_DT)
                    .cast(DataType::String),
                BaseRDFNodeType::BlankNode => col(c)
                    .struct_()
                    .field_by_name(MULTI_BLANK_DT)
                    .cast(DataType::String),
                BaseRDFNodeType::Literal(_) => {
                    if t.is_lang_string() {
                        cast_lang_string_to_string(c)
                    } else {
                        col(c)
                            .struct_()
                            .field_by_name(&base_col_name(t))
                            .cast(DataType::String)
                    }
                }
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
            })
        }
        coalesce(to_coalesce.as_slice()).alias(c)
    } else {
        let t = BaseRDFNodeType::from_rdf_node_type(t);
        match &t {
            BaseRDFNodeType::IRI => col(c).cast(DataType::String),
            BaseRDFNodeType::BlankNode => col(c).cast(DataType::String),
            BaseRDFNodeType::Literal(_) => {
                if t.is_lang_string() {
                    cast_lang_string_to_string(c)
                } else {
                    col(c).cast(DataType::String)
                }
            }
            BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
        }
    }
}

fn cast_lang_string_to_string(c: &str) -> Expr {
    col(c)
        .struct_()
        .field_by_name(LANG_STRING_VALUE_FIELD)
        .cast(DataType::String)
}

pub fn xsd_cast_literal(
    c: &str,
    src: &RDFNodeType,
    trg: &BaseRDFNodeType,
) -> Result<Expr, QueryProcessingError> {
    let trg_type = trg.polars_data_type();
    let trg_nn = if let BaseRDFNodeType::Literal(nn) = trg {
        nn.as_ref()
    } else {
        panic!("Invalid state")
    };
    if let RDFNodeType::MultiType(types) = src {
        let mut to_coalesce = vec![];
        for t in types {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => cast_iri_to_xsd_literal(
                    col(c).struct_().field_by_name(&base_col_name(t)),
                    c,
                    t,
                    trg,
                    trg_nn,
                    trg_type.clone(),
                )?,
                BaseRDFNodeType::BlankNode => {
                    return Err(QueryProcessingError::BadCastDatatype(
                        c.to_string(),
                        trg.clone(),
                        t.clone(),
                    ))
                }
                BaseRDFNodeType::Literal(src_nn) => cast_literal(
                    col(c).struct_().field_by_name(&base_col_name(t)),
                    src_nn.as_ref(),
                    trg_nn,
                    trg_type.clone(),
                ),
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(trg_type.clone()),
            })
        }
        Ok(coalesce(to_coalesce.as_slice()).alias(c))
    } else {
        let t = BaseRDFNodeType::from_rdf_node_type(src);
        match &t {
            BaseRDFNodeType::IRI => {
                cast_iri_to_xsd_literal(col(c), c, &t, trg, trg_nn, trg_type.clone())
            }
            BaseRDFNodeType::BlankNode => Err(QueryProcessingError::BadCastDatatype(
                c.to_string(),
                trg.clone(),
                t.clone(),
            )),
            BaseRDFNodeType::Literal(src_nn) => Ok(cast_literal(
                col(c),
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
    _c: &str,
    _src: &BaseRDFNodeType,
    _trg: &BaseRDFNodeType,
    trg_nn: NamedNodeRef,
    trg_type: DataType,
) -> Result<Expr, QueryProcessingError> {
    if trg_nn == xsd::STRING {
        Ok(cast_literal(e, xsd::STRING, trg_nn, trg_type.clone()))
    } else {
        Ok(lit(LiteralValue::untyped_null()).cast(trg_type.clone()))
        // Err(QueryProcessingError::BadCastDatatype(
        //     c.to_string(),
        //     src.clone(),
        //     trg.clone(),
        // ))
    }
}

fn cast_literal(c: Expr, src: NamedNodeRef, trg: NamedNodeRef, trg_type: DataType) -> Expr {
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

//if solution_mappings
//                     .rdf_node_types
//                     .get(first_context.as_str())
//                     .unwrap()
//                     .is_lit_type(xsd::STRING)
//                 {
//                     solution_mappings.mappings = solution_mappings.mappings.with_column(
//                         col(first_context.as_str())
//                             .str()
//                             .to_lowercase()
//                             .eq(lit("true"))
//                             .alias(outer_context.as_str()),
//                     );

pub fn contains_graph_pattern(e: &Expression) -> bool {
    match e {
        Expression::NamedNode(_)
        | Expression::Bound(_)
        | Expression::Literal(_)
        | Expression::Variable(_) => false,
        Expression::Or(l, r)
        | Expression::And(l, r)
        | Expression::Equal(l, r)
        | Expression::SameTerm(l, r)
        | Expression::Greater(l, r)
        | Expression::GreaterOrEqual(l, r)
        | Expression::Less(l, r)
        | Expression::LessOrEqual(l, r)
        | Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r) => contains_graph_pattern(l) | contains_graph_pattern(r),
        Expression::UnaryPlus(u) | Expression::UnaryMinus(u) | Expression::Not(u) => {
            contains_graph_pattern(u)
        }
        Expression::Exists(_) => true,
        Expression::In(l, r) => contains_graph_pattern(l) | r.iter().any(contains_graph_pattern),
        Expression::If(l, m, r) => {
            contains_graph_pattern(l) || contains_graph_pattern(m) || contains_graph_pattern(r)
        }
        Expression::Coalesce(e) | Expression::FunctionCall(_, e) => {
            e.iter().any(contains_graph_pattern)
        }
    }
}
