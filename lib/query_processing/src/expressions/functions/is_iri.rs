use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, lit};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use std::collections::HashMap;

pub fn is_iri(
    mut solution_mappings: SolutionMappings,
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let first_context = args_contexts.get(&0).unwrap();
    let t = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let expr = if t.is_multi() {
        let contains_iri = t.map.contains_key(&BaseRDFNodeType::IRI);
        if contains_iri {
            col(first_context.as_str())
                .struct_()
                .field_by_name(&BaseRDFNodeType::IRI.field_col_name())
                .is_not_null()
        } else {
            lit(false)
        }
    } else {
        if t.is_iri() {
            col(first_context.as_str()).is_not_null()
        } else {
            lit(false)
        }
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
