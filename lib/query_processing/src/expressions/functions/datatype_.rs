use crate::errors::QueryProcessingError;
use polars::prelude::{coalesce, col, lit, when, LiteralValue};
use representation::query_context::Context;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use std::collections::HashMap;

pub fn datatype_(
    mut solution_mappings: SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let first_context = args_contexts.get(&0).unwrap();
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let iri_type = BaseRDFNodeType::IRI;
    let iri_state = BaseCatState::String;
    let iri_dtype = iri_type.polars_data_type(&iri_state, true);
    let expr = if t.is_multi() {
        let mut exprs = vec![];
        for t in t.map.keys() {
            if let BaseRDFNodeType::Literal(l) = t {
                exprs.push(
                    when(
                        col(first_context.as_str())
                            .struct_()
                            .field_by_name(&t.field_col_name())
                            .is_null()
                            .not(),
                    )
                    .then(lit(rdf_named_node_to_polars_literal_value(l)))
                    .otherwise(lit(LiteralValue::untyped_null()).cast(iri_dtype.clone())),
                );
            }
        }
        if !exprs.is_empty() {
            coalesce(exprs.as_slice())
        } else {
            lit(LiteralValue::untyped_null()).cast(iri_dtype)
        }
    } else {
        let b = t.get_base_type().unwrap();
        match b {
            BaseRDFNodeType::Literal(l) => lit(rdf_named_node_to_polars_literal_value(l)),
            _ => lit(LiteralValue::untyped_null()).cast(iri_dtype),
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeState::from_bases(iri_type, iri_state),
    );
    Ok(solution_mappings)
}
