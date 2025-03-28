use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::trace;
use oxrdf::Variable;

use polars::prelude::JoinType;
use query_processing::graph_patterns::{join, project};
use query_processing::pushdowns::Pushdowns;
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
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing project graph pattern");
        let inner_context = context.extension_with(PathEntry::ProjectInner);
        pushdowns.limit_to_variables(variables);
        pushdowns.add_graph_pattern_pushdowns(inner);
        let mut project_solution_mappings = self.lazy_graph_pattern(
            inner,
            None,
            &inner_context,
            parameters,
            pushdowns,
            include_transient,
        )?;
        project_solution_mappings = project(project_solution_mappings, variables)?;

        let solution_mappings = if let Some(solution_mappings) = solution_mappings {
            join(
                solution_mappings,
                project_solution_mappings,
                JoinType::Inner,
            )?
        } else {
            project_solution_mappings
        };

        Ok(solution_mappings)
    }
}
