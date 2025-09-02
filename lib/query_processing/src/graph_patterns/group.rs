use crate::errors::QueryProcessingError;
use polars::prelude::{by_name, Expr};
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeState;
use std::collections::HashMap;

pub fn group_by(
    solution_mappings: SolutionMappings,
    aggregate_expressions: Vec<Expr>,
    by: Vec<Expr>,
    dummy_varname: Option<String>,
    new_rdf_node_types: HashMap<String, RDFNodeState>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types: mut datatypes,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let grouped_mappings = mappings.group_by(by.as_slice());

    mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
    for (k, v) in new_rdf_node_types {
        datatypes.insert(k, v);
    }
    if let Some(dummy_varname) = dummy_varname {
        mappings = mappings.drop(by_name([&dummy_varname], false));
    }
    Ok(SolutionMappings::new(
        mappings,
        datatypes,
        height_upper_bound,
    ))
}
