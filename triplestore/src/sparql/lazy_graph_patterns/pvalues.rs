use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use polars::prelude::IntoLazy;
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::join;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::sparql_to_polars::{
    polars_literal_values_to_series, sparql_literal_to_polars_literal_value,
    sparql_named_node_to_polars_literal_value,
};
use representation::RDFNodeType;
use spargebra::term::GroundTerm;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_pvalues(
        &self,
        solution_mappings: Option<SolutionMappings>,
        variables: &Vec<Variable>,
        bindings_name: &String,
        _context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        let sm = if let Some(parameters) = parameters {
            if let Some(EagerSolutionMappings {
                mappings,
                rdf_node_types,
            }) = parameters.get(bindings_name)
            {
                //Todo! Check that variables are in df..
                SolutionMappings {
                    mappings: mappings.clone().lazy(),
                    rdf_node_types: rdf_node_types.clone(),
                }
            } else {
                todo!("Handle this error.. ")
            }
        } else {
            todo!("Handle this error")
        };
        if let Some(mut mappings) = solution_mappings {
            mappings = join(mappings, sm)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}
