use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;
use oxrdf::Variable;

use crate::sparql::pushdowns::Pushdowns;
use query_processing::graph_patterns::project;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing project graph pattern");
        let inner_context = context.extension_with(PathEntry::ProjectInner);
        pushdowns.limit_to_variables(variables);
        pushdowns.add_graph_pattern_pushdowns(inner);
        let mut solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &inner_context,
            parameters,
            pushdowns,
        )?;

        solution_mappings = project(solution_mappings, variables)?;

        Ok(solution_mappings)
    }
}
