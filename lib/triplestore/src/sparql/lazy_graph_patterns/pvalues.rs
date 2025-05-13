use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use polars::prelude::{IntoLazy, JoinType};

use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use representation::iri::{col_not_struct, lf_split_iri};
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    pub(crate) fn lazy_pvalues(
        &mut self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings_name: &String,
        _context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        _pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        //Todo: apply pushdowns.
        let sm = if let Some(parameters) = parameters {
            if let Some(EagerSolutionMappings {
                mappings,
                rdf_node_types,
            }) = parameters.get(bindings_name)
            {
                let mapping_vars: HashSet<_> = mappings
                    .get_column_names()
                    .into_iter()
                    .map(|x| x.as_str())
                    .collect();
                let expected_vars: HashSet<_> = variables.iter().map(|x| x.as_str()).collect();
                if mapping_vars != expected_vars {
                    todo!("Handle mismatching variables in PValues")
                }
                let mut iri_not_struct = HashSet::new();
                let height = mappings.height();

                for (c, t) in rdf_node_types {
                    if t.is_iri() && col_not_struct(mappings, c) {
                        iri_not_struct.insert(c);
                    }
                }
                let mut mappings = mappings.clone().lazy();
                for c in rdf_node_types.keys() {
                    if iri_not_struct.contains(&c) {
                        mappings = lf_split_iri(mappings, c);
                    }
                }

                SolutionMappings {
                    mappings,
                    rdf_node_types: rdf_node_types.clone(),
                    height_estimate: height,
                }
            } else {
                todo!("Handle this error.. ")
            }
        } else {
            todo!("Handle this error")
        };
        if let Some(mut mappings) = solution_mappings {
            //TODO: Remove this workaround
            mappings = mappings.as_eager(false).as_lazy();
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}
