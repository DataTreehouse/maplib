use crate::errors::QueryProcessingError;
use crate::expressions::functions::{create_regex_replace_expr, create_regex_string};

use oxrdf::vocab::xsd;
use polars::prelude::{coalesce, col};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn sparql_replace(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
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
    let replacement_expr = &args[2];
    if !replacement_state.is_lit_type(xsd::STRING) {
        return Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
            replacement_expr.clone(),
        ));
    }
    let replacement_bt = replacement_state.get_base_type().unwrap();
    let replacement_bs = replacement_state.get_base_state().unwrap();

    let pattern_context = args_contexts.get(&1).unwrap();
    let pattern_sparql_expr = args.get(1).unwrap();
    let pattern_type = solution_mappings
        .rdf_node_types
        .get(pattern_context.as_str())
        .unwrap();
    let flags = if let Some(flags_context) = args_contexts.get(&3) {
        let flags_sparql_expr = args.get(3).unwrap();
        let flags_type = solution_mappings
            .rdf_node_types
            .get(flags_context.as_str())
            .unwrap();
        Some((flags_sparql_expr, flags_type))
    } else {
        None
    };
    let pattern = create_regex_string(pattern_sparql_expr, pattern_type, flags)?;
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

        create_regex_replace_expr(use_col, t, s, &pattern, &replacement_expr, global_cats)
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::STRING.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
