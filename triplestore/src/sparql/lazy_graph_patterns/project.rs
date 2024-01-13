use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use log::{debug};
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use query_processing::graph_patterns::project;

impl Triplestore {
    pub(crate) fn lazy_project(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing project graph pattern");
        let solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::ProjectInner),
        )?;
        Ok(project(solution_mappings, variables)?)
    }
}
