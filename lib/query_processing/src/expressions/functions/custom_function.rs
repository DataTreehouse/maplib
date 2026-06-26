use crate::constants::{
    DATETIME_AS_MICROS, DATETIME_AS_SECONDS, DECODE, FLOOR_DATETIME_TO_SECONDS_INTERVAL,
    MICROS_AS_DATETIME, MODULUS, SECONDS_AS_DATETIME, STRUUID_V5, UUID_V5,
};
use crate::errors::QueryProcessingError;
use crate::expressions::functions::struuid_v5::struuid_v5;
use crate::expressions::functions::uuid_v5::uuid_v5;
use crate::expressions::functions::xsd_cast_literal;
use crate::udf::UdfRegistry;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::datatypes::{DataType, TimeUnit};
use polars::frame::DataFrame;
use polars::prelude::{col, lit, Column, Expr, LiteralValue, Scalar};
use polars::prelude::{IntoColumn, IntoLazy};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
use std::ops::{Div, Mul};

pub fn custom_(
    nn: &NamedNode,
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
    udf_registry: Option<&UdfRegistry>,
) -> Result<SolutionMappings, QueryProcessingError> {
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
            | xsd::BYTE
            | xsd::SHORT
            | xsd::DECIMAL
            | xsd::DOUBLE
            | xsd::FLOAT
            | xsd::STRING
            | xsd::DATE_TIME
            | xsd::DATE
            | xsd::DURATION
            | xsd::TIME
    ) {
        if args.len() != 1 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "1".to_string(),
            ));
        }
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
    } else if nn.as_str() == DATETIME_AS_MICROS {
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
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .cast(DataType::UInt64)
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == DATETIME_AS_SECONDS {
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
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .cast(DataType::UInt64)
                .div(lit(1000))
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == MICROS_AS_DATETIME {
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
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                .into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == SECONDS_AS_DATETIME {
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
                .mul(Expr::Literal(LiteralValue::Scalar(Scalar::from(1000))))
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                .into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == MODULUS {
        if args.len() != 2 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "2".to_string(),
            ));
        }
        let first_context = args_contexts.get(&0).unwrap();
        let second_context = args_contexts.get(&1).unwrap();

        solution_mappings.mappings = solution_mappings.mappings.with_column(
            (col(first_context.as_str()) % col(second_context.as_str()))
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == FLOOR_DATETIME_TO_SECONDS_INTERVAL {
        if args.len() != 2 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "2".to_string(),
            ));
        }
        let first_context = args_contexts.get(&0).unwrap();
        let second_context = args_contexts.get(&1).unwrap();

        let first_as_seconds = col(first_context.as_str())
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .cast(DataType::UInt64)
            .div(lit(1000));

        solution_mappings.mappings = solution_mappings.mappings.with_column(
            ((first_as_seconds.clone() - (first_as_seconds % col(second_context.as_str())))
                .mul(Expr::Literal(LiteralValue::Scalar(Scalar::from(1000))))
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
            .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
                .into_default_input_rdf_node_state(),
        );
    } else if nn.as_str() == DECODE {
        if args.len() != 1 {
            return Err(QueryProcessingError::BadNumberOfFunctionArguments(
                func.clone(),
                args.len(),
                "1".to_string(),
            ));
        }
        let first_context = args_contexts.get(&0).unwrap();
        let mut t_new = solution_mappings
            .rdf_node_types
            .get(first_context.as_str())
            .unwrap()
            .clone();
        let is_multi = t_new.is_multi();
        let lit_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
        if let Some(bs) = t_new.map.get_mut(&lit_string) {
            if is_multi {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    col(first_context.as_str())
                        .struct_()
                        .with_fields(vec![maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(&lit_string.field_col_name()),
                            &lit_string,
                            bs,
                            global_cats.clone(),
                        )
                        .alias(&lit_string.field_col_name())])
                        .alias(outer_context.as_str()),
                );
            } else {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    maybe_decode_expr(
                        col(first_context.as_str()),
                        &lit_string,
                        bs,
                        global_cats.clone(),
                    )
                    .alias(outer_context.as_str()),
                );
            }
            *bs = BaseCatState::String;
        } else {
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_column(col(first_context.as_str()).alias(outer_context.as_str()));
        }

        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t_new);
    } else if nn.as_str() == STRUUID_V5 {
        solution_mappings = struuid_v5(
            solution_mappings,
            func,
            args,
            &args_contexts,
            outer_context,
            global_cats,
        )?;
    } else if nn.as_str() == UUID_V5 {
        solution_mappings = uuid_v5(
            solution_mappings,
            func,
            args,
            &args_contexts,
            outer_context,
            global_cats,
        )?;
    } else if let Some(registry) = udf_registry {
        if registry.has(nn) {
            let input_types = registry.input_types(nn).unwrap_or(&[]);
            let output_type = registry.output_type(nn).ok_or_else(|| {
                QueryProcessingError::UDFError(format!("UDF '{}' has no output type", nn))
            })?;
            if !input_types.is_empty() && input_types.len() != args.len() {
                return Err(QueryProcessingError::UDFError(format!(
                    "UDF '{}' expects {} args, got {}",
                    nn,
                    input_types.len(),
                    args.len()
                )));
            }
            let mut collected = solution_mappings
                .mappings
                .collect()
                .map_err(|e| QueryProcessingError::UDFError(e.to_string()))?;

            let arg_columns: Result<Vec<_>, QueryProcessingError> = (0..args.len())
                .map(|i| {
                    let ctx = args_contexts.get(&i).ok_or_else(|| {
                        QueryProcessingError::UDFError(format!("Missing arg context {}", i))
                    })?;
                    let col = collected.column(ctx.as_str()).map_err(|e| {
                        QueryProcessingError::UDFError(format!(
                            "UDF '{}': column '{}' not found: {}",
                            nn,
                            ctx.as_str(),
                            e
                        ))
                    })?;
                    let mut series = col.as_materialized_series().clone();
                    if let Some(expected_type) = input_types.get(i) {
                        let target_dtype = expected_type.default_input_polars_data_type();
                        series = series.cast(&target_dtype).map_err(|e| {
                            QueryProcessingError::UDFError(format!(
                                "UDF '{}': failed to cast argument {} to {:?}: {}",
                                nn, i, expected_type, e
                            ))
                        })?;
                    }
                    Ok(series.clone().with_name(format!("{}", i).into()))
                })
                .collect();
            let arg_columns = arg_columns?;

            let height = arg_columns.first().map(|s| s.len()).unwrap_or(0);
            let arg_columns: Vec<Column> =
                arg_columns.into_iter().map(|s| s.into_column()).collect();
            let input_df = DataFrame::new(height, arg_columns)
                .map_err(|e| QueryProcessingError::UDFError(e.to_string()))?;

            let result_df = registry.call(nn, input_df)?;

            let result_col = result_df
                .columns()
                .first()
                .ok_or_else(|| {
                    QueryProcessingError::UDFError(format!(
                        "UDF '{}' returned empty DataFrame",
                        nn
                    ))
                })?
                .as_materialized_series()
                .clone();

            let expected_output_dtype = output_type.default_input_polars_data_type();
            if result_col.dtype() != &expected_output_dtype {
                return Err(QueryProcessingError::UDFError(format!(
                    "UDF '{}' expected output dtype {:?}, got {:?}",
                    nn,
                    expected_output_dtype,
                    result_col.dtype()
                )));
            }

            let result_col = result_col.with_name(outer_context.as_str().into());

            collected
                .with_column(result_col.into_column())
                .map_err(|e| QueryProcessingError::UDFError(e.to_string()))?;

            solution_mappings.mappings = collected.lazy();
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                    .into_default_input_rdf_node_state(),
            );
        } else {
            return Err(QueryProcessingError::CustomFunctionNotFound(nn.to_string()));
        }
    } else {
        return Err(QueryProcessingError::UDFError(nn.to_string()));
    }
    Ok(solution_mappings)
}
