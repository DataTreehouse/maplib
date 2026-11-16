use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, concat_str, lit, when, Expr, LiteralValue};
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
    let b = BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None);
    let tag_expr = string_arg_expr(
        &solution_mappings,
        args_contexts,
        0,
        &b,
        global_cats.clone(),
    );
    let range_expr = if let Expression::Literal(l) = args.get(1).unwrap() {
        Some(lit(l.value().to_string()))
    } else {
        string_arg_expr(
            &solution_mappings,
            args_contexts,
            1,
            &b,
            global_cats.clone(),
        )
    };
    if let (Some(tag_expr), Some(range_expr)) = (tag_expr, range_expr) {
        // RFC 4647 basic filtering: "*" matches any non-empty tag, otherwise the tag must equal
        // the range or start with the range followed by "-", ignoring case.
        let tag = tag_expr.str().to_lowercase();
        let range = range_expr.str().to_lowercase();
        let expr = when(range.clone().eq(lit("*")))
            .then(tag.clone().neq(lit("")))
            .otherwise(
                tag.clone()
                    .eq(range.clone())
                    .or(tag
                        .str()
                        .starts_with(concat_str([range, lit("-")], "", false))),
            );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(expr.alias(outer_context.as_str()));
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
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
    Ok(solution_mappings)
}

/// The xsd:string values of an argument as a decoded string expression, None if the argument has
/// no xsd:string values. Values of other types are null, which makes the result an error (null).
fn string_arg_expr(
    solution_mappings: &SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    i: usize,
    b: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Option<Expr> {
    let context = args_contexts.get(&i).unwrap();
    let t = solution_mappings
        .rdf_node_types
        .get(context.as_str())
        .unwrap();
    let s = t.map.get(b)?;
    let c = if t.is_multi() {
        col(context.as_str())
            .struct_()
            .field_by_name(&b.field_col_name())
    } else {
        col(context.as_str())
    };
    Some(maybe_decode_expr(c, b, s, global_cats))
}
