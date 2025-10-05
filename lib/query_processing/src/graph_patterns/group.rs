use crate::errors::QueryProcessingError;
use polars::prelude::{by_name, Expr};
use representation::solution_mapping::SolutionMappings;

pub fn group_by(
    solution_mappings: SolutionMappings,
    aggregate_expressions: Vec<Expr>,
    by: Vec<Expr>,
    dummy_varname: Option<String>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types,
        height_estimate,
    } = solution_mappings;
    let grouped_mappings = mappings.group_by(by.as_slice());

    mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
    if let Some(dummy_varname) = dummy_varname {
        mappings = mappings.drop(by_name([&dummy_varname], false));
    }
    Ok(SolutionMappings::new(
        mappings,
        rdf_node_types,
        height_estimate,
    ))
}
