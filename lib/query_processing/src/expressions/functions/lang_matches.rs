use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn lang_matches(
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
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let b = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
    if let Some(s) = t.map.get(&b) {
        let lang_expr = maybe_decode_expr(col(first_context.as_str()), &b, s, global_cats.clone());
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
    Ok(solution_mappings)
}
