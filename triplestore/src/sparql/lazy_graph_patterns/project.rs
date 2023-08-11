use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::{debug, warn};
use oxrdf::Variable;
use polars::prelude::{col, Expr};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_project(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing project graph pattern");
        let SolutionMappings {
            mut mappings,
            rdf_node_types: mut datatypes,
            ..
        } = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::ProjectInner),
        )?;
        let cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
        mappings = mappings.select(cols.as_slice());
        let mut new_datatypes = HashMap::new();
        for v in variables {
            if !datatypes.contains_key(v.as_str()) {
                warn!("Datatypes does not contain {}", v);
            } else {
                new_datatypes.insert(
                    v.as_str().to_string(),
                    datatypes.remove(v.as_str()).unwrap(),
                );
            }
        }
        Ok(SolutionMappings::new(
            mappings,
            variables.iter().map(|x| x.as_str().to_string()).collect(),
            new_datatypes,
        ))
    }
}
