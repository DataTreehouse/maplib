use crate::errors::QueryProcessingError;
use crate::expressions::functions::create_regex_expr::create_regex_expr;
use crate::expressions::functions::create_regex_string::create_regex_string;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, concat_str, lit, when, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState, LANG_STRING_VALUE_FIELD};
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
        Some((flags_sparql_expr, flags_type, flags_context))
    } else {
        None
    };
    let constant = create_regex_string(
        pattern_sparql_expr,
        pattern_type,
        flags.map(|(e, t, _)| (e, t)),
    );
    let (pattern, strict) = match constant {
        Ok(pattern) => (lit(pattern), true),
        // A pattern (or flags) that differs between solutions is matched per solution. An
        // invalid pattern then leaves only those solutions unbound, and a pattern or flags that
        // are not strings leave every solution unbound, as SPARQL errors do.
        Err(QueryProcessingError::ExpectedConstantLiteralArgument(_))
        | Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(_))
        | Err(QueryProcessingError::BadArgument(_)) => {
            match column_pattern(
                pattern_context,
                pattern_type,
                flags.map(|(_, t, c)| (t, c)),
                global_cats.clone(),
            )? {
                Some(p) => (p, false),
                None => (null_string(), false),
            }
        }
        Err(e) => return Err(e),
    };
    let expr = if t.is_multi() {
        let mut exprs = vec![];
        for (t, s) in &t.map {
            let replace_expr = create_regex_expr(
                col(text_context.as_str())
                    .struct_()
                    .field_by_name(&t.field_col_name()),
                t,
                s,
                pattern.clone(),
                strict,
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
        create_regex_expr(use_col, b, s, pattern, strict, global_cats.clone())
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
            .into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}

fn null_string() -> Expr {
    lit(LiteralValue::untyped_null()).cast(DataType::String)
}

/// The decoded string values of a column of type xsd:string, or None for other types.
fn string_column(
    context: &Context,
    t: &RDFNodeState,
    global_cats: LockedCats,
) -> Option<Expr> {
    if !t.is_lit_type(xsd::STRING) {
        return None;
    }
    Some(maybe_decode_expr(
        col(context.as_str()),
        t.get_base_type().unwrap(),
        t.get_base_state().unwrap(),
        global_cats,
    ))
}

/// The pattern of each solution, with the flags of each solution as an inline flag group.
/// None when the pattern or the flags are not strings, which matches nothing (unbound).
fn column_pattern(
    pattern_context: &Context,
    pattern_type: &RDFNodeState,
    flags: Option<(&RDFNodeState, &Context)>,
    global_cats: LockedCats,
) -> Result<Option<Expr>, QueryProcessingError> {
    let Some(pattern) = string_column(pattern_context, pattern_type, global_cats.clone()) else {
        return Ok(None);
    };
    let Some((flags_type, flags_context)) = flags else {
        return Ok(Some(pattern));
    };
    let Some(flags) = string_column(flags_context, flags_type, global_cats) else {
        return Ok(None);
    };
    let with_flags = concat_str(
        [lit("(?"), flags.clone(), lit(")"), pattern.clone()],
        "",
        false,
    );
    Ok(Some(
        when(flags.eq(lit("")))
            .then(pattern)
            .otherwise(with_flags),
    ))
}
