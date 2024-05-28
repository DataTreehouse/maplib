use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use polars::prelude::{IntoLazy, JoinType};

use query_processing::graph_patterns::join;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use std::collections::{HashMap, HashSet};

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
                let mapping_vars: HashSet<_> = mappings.get_column_names().into_iter().collect();
                let expected_vars: HashSet<_> = variables.iter().map(|x| x.as_str()).collect();
                if mapping_vars != expected_vars {
                    todo!("Handle mismatching variables in PValues")
                }

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
            //TODO: Remove this workaround
            mappings = mappings.as_eager().as_lazy();
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}
