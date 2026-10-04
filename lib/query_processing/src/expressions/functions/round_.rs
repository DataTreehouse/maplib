use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{as_struct, col, lit, when, Expr, LiteralValue, RoundMode};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn round_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let existing_type = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let mut rounded_exprs = vec![];
    let mut rounded_types = vec![];
    if existing_type.is_multi() {
        for t in existing_type.map.keys() {
            if t.is_numeric() {
                rounded_exprs.push(
                    sparql_round(
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(&t.field_col_name()),
                        t,
                    )
                    .alias(&t.field_col_name()),
                );
                rounded_types.push(t.clone());
            }
        }
    } else if existing_type.is_numeric() {
        rounded_exprs.push(sparql_round(
            col(first_context.as_str()),
            existing_type.get_base_type().unwrap(),
        ));
        rounded_types.push(existing_type.get_base_type().unwrap().clone());
    }
    if rounded_exprs.is_empty() {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        );
    } else if rounded_exprs.len() == 1 {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(rounded_exprs.pop().unwrap().alias(outer_context.as_str()));
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            rounded_types
                .pop()
                .unwrap()
                .into_default_stored_rdf_node_state(),
        );
    } else {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(as_struct(rounded_exprs).alias(outer_context.as_str()));
        let mut types_map = HashMap::new();
        for t in rounded_types {
            let s = t.default_input_cat_state();
            types_map.insert(t, s);
        }
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            RDFNodeState::from_map(types_map),
        );
    }
    Ok(solution_mappings)
}

/// SPARQL 1.1 17.4.4.2 (fn:numeric-round): returns the number with no fractional part
/// that is closest to the argument. If there are two such numbers, the one closest to
/// positive infinity is returned, e.g. round(-2.5) = -2.
///
/// Polars has no such rounding mode, so we round half away from zero and move negative
/// ties one step up. For a tie, x - round(x) is exactly 0.5 (also for floats), so the
/// comparison is exact.
fn sparql_round(e: Expr, t: &BaseRDFNodeType) -> Expr {
    let rounded = e.clone().round(0, RoundMode::HalfAwayFromZero);
    if t.is_lit_type(xsd::DECIMAL) || t.is_lit_type(xsd::DOUBLE) || t.is_lit_type(xsd::FLOAT) {
        let dt = t.default_input_polars_data_type();
        when((e - rounded.clone()).eq(lit(0.5).cast(dt.clone())))
            .then(rounded.clone() + lit(1).cast(dt))
            .otherwise(rounded)
    } else {
        // Integer types have no fractional part
        rounded
    }
}
