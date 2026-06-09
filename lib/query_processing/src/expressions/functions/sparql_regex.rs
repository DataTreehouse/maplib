use crate::errors::QueryProcessingError;
use crate::expressions::functions::create_regex_expr::create_regex_expr;
use crate::expressions::functions::create_regex_string;
use oxrdf::vocab::xsd;
use polars::prelude::{coalesce, col};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn sparql_regex(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
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
    let pattern_context = args_contexts.get(&1).unwrap();
    let pattern_sparql_expr = args.get(1).unwrap();

    let pattern_type = solution_mappings
        .rdf_node_types
        .get(pattern_context.as_str())
        .unwrap();
    let flags = if let Some(flags_context) = args_contexts.get(&2) {
        let flags_sparql_expr = args.get(2).unwrap();
        let flags_type = solution_mappings
            .rdf_node_types
            .get(flags_context.as_str())
            .unwrap();
        Some((flags_sparql_expr, flags_type))
    } else {
        None
    };
    let pattern = create_regex_string(pattern_sparql_expr, pattern_type, flags)?;
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
        create_regex_expr(use_col, b, s, &pattern, global_cats.clone())
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
