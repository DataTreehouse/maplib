use crate::errors::QueryProcessingError;
use crate::expressions::functions::eval_expression_to_string::eval_expression_to_string;
use crate::expressions::functions::keep_field_::keep_field;
use crate::expressions::functions::str_after::str_after;
use crate::expressions::functions::str_before::str_before;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{as_struct, coalesce, col, lit, when, DataType, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

fn null_string() -> Expr {
    lit(LiteralValue::untyped_null()).cast(DataType::String)
}

/// STRBEFORE and STRAFTER (SPARQL 1.1 17.4.3.9 and 17.4.3.10).
/// The second argument must be a constant string literal, possibly with a language tag.
/// The arguments must be argument compatible (17.4.3.1.1), otherwise the result is an error (unbound).
/// If there is no match, the result is the simple literal "", otherwise the result has the language tag of the first argument.
pub fn str_before_or_after(
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
    let second = args.get(1).unwrap();
    let (second_string, second_lang) = match second {
        Expression::Literal(l) => {
            if l.datatype() != xsd::STRING && l.datatype() != rdf::LANG_STRING {
                return Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
                    second.clone(),
                ));
            }
            (
                l.value().to_string(),
                l.language().map(|x| x.to_lowercase()),
            )
        }
        e => (eval_expression_to_string(e, true)?, None),
    };
    let is_before = match func {
        Function::StrBefore => true,
        Function::StrAfter => false,
        _ => unreachable!("Should never happen"),
    };
    let apply_func = |e: Expr| {
        let ss = second_string.clone();
        if is_before {
            e.apply(move |x| str_before(x, ss.clone()), keep_field)
        } else {
            e.apply(move |x| str_after(x, ss.clone()), keep_field)
        }
    };

    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let is_multi = t.is_multi();
    let mut string_exprs = vec![];
    let mut lang_value_exprs = vec![];
    let mut lang_lang_exprs = vec![];
    for (bt, bs) in &t.map {
        if bt.is_lit_type(xsd::STRING) {
            if second_lang.is_some() {
                // Not argument compatible
                continue;
            }
            let e = if is_multi {
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(&bt.field_col_name())
            } else {
                col(first_context.as_str())
            };
            let decoded = maybe_decode_expr(e, bt, bs, global_cats.clone());
            string_exprs.push(apply_func(decoded));
        } else if bt.is_lang_string() {
            let value = maybe_decode_expr(
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD),
                bt,
                bs,
                global_cats.clone(),
            );
            let lang = maybe_decode_expr(
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(LANG_STRING_LANG_FIELD),
                bt,
                bs,
                global_cats.clone(),
            );
            let compatible = if let Some(second_lang) = &second_lang {
                lang.clone()
                    .str()
                    .to_lowercase()
                    .eq(lit(second_lang.clone()))
            } else {
                lit(true)
            };
            let matched = value
                .clone()
                .str()
                .contains_literal(lit(second_string.clone()));
            string_exprs.push(
                when(compatible.clone().and(matched.clone().not()))
                    .then(lit(""))
                    .otherwise(null_string()),
            );
            let keep = compatible.and(matched);
            lang_value_exprs.push(
                when(keep.clone())
                    .then(apply_func(value))
                    .otherwise(null_string()),
            );
            lang_lang_exprs.push(when(keep).then(lang).otherwise(null_string()));
        }
    }

    let string_type = BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None);
    let lang_type = BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None);
    let (expr, state) = if string_exprs.is_empty() && lang_value_exprs.is_empty() {
        (
            lit(LiteralValue::untyped_null()).cast(
                BaseRDFNodeType::None
                    .into_default_input_rdf_node_state()
                    .polars_data_type(),
            ),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        )
    } else if lang_value_exprs.is_empty() {
        (
            coalesce(&string_exprs),
            string_type.into_default_input_rdf_node_state(),
        )
    } else {
        let mut struct_exprs = vec![
            coalesce(&lang_value_exprs).alias(LANG_STRING_VALUE_FIELD),
            coalesce(&lang_lang_exprs).alias(LANG_STRING_LANG_FIELD),
        ];
        let mut type_map = HashMap::new();
        type_map.insert(lang_type.clone(), lang_type.default_input_cat_state());
        if !string_exprs.is_empty() {
            struct_exprs.push(coalesce(&string_exprs).alias(string_type.field_col_name()));
            type_map.insert(string_type.clone(), string_type.default_input_cat_state());
        }
        let state = RDFNodeState::from_map(type_map);
        (
            set_struct_all_null_to_null_row(as_struct(struct_exprs), &state),
            state,
        )
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), state);
    Ok(solution_mappings)
}
