use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;
use query_processing::graph_patterns::distinct;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_distinct(
        &self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing distinct graph pattern");
        let solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
            parameters,
        )?;
        Ok(distinct(solution_mappings)?)
    }
}
