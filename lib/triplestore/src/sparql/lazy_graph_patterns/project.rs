use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;
use oxrdf::Variable;

use query_processing::graph_patterns::project;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use crate::sparql::pushdowns::Pushdowns;

impl Triplestore {
    pub(crate) fn lazy_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing project graph pattern");
        let mut solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::ProjectInner),
            parameters,
            pushdowns,
        )?;

        solution_mappings = project(solution_mappings, variables)?;

        Ok(solution_mappings)
    }
}
