use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, lit};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{literal_is_numeric, BaseRDFNodeType};
use std::collections::HashMap;

pub fn is_literal(
    solution_mappings: SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    is_literal_where(solution_mappings, args_contexts, outer_context, |_| true)
}

/// isNUMERIC: true if the argument is a literal with a numeric datatype.
pub fn is_numeric(
    solution_mappings: SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    is_literal_where(
        solution_mappings,
        args_contexts,
        outer_context,
        |t| matches!(t, BaseRDFNodeType::Literal(nn, _) if literal_is_numeric(nn.as_ref())),
    )
}

/// True if the argument is a literal whose type satisfies `accept`.
fn is_literal_where(
    mut solution_mappings: SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    accept: impl Fn(&BaseRDFNodeType) -> bool,
) -> Result<SolutionMappings, QueryProcessingError> {
    let first_context = args_contexts.get(&0).unwrap();
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let expr = if t.is_multi() {
        let mut exprs = vec![];
        for t in t.map.keys() {
            if matches!(t, BaseRDFNodeType::Literal(_, _)) && accept(t) {
                exprs.push(
                    col(first_context.as_str())
                        .struct_()
                        .field_by_name(&t.field_col_name())
                        .is_not_null(),
                );
            }
        }
        let mut expr = exprs.pop().unwrap_or(lit(false));
        for e in exprs {
            expr = expr.or(e)
        }
        expr
    } else {
        let b = t.get_base_type().unwrap();
        match b {
            BaseRDFNodeType::Literal(_, _) if accept(&b) => {
                col(first_context.as_str()).is_null().not()
            }
            _ => lit(false),
        }
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
