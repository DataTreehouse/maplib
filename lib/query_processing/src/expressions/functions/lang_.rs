use crate::errors::QueryProcessingError;
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, when, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn lang_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
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
        let s = dt.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::Literal(l) => {
                if l.as_ref() == rdf::LANG_STRING {
                    solution_mappings.mappings = solution_mappings.mappings.with_column(
                        maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD),
                            b,
                            s,
                            global_cats,
                        )
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
        BaseRDFNodeType::Literal(xsd::STRING.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
