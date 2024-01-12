use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use log::debug;
use polars_core::frame::UniqueKeepStrategy;
use query_processing::graph_patterns::distinct;
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub(crate) fn lazy_distinct(
        &self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing distinct graph pattern");
        let solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
        )?;
        Ok(distinct(solution_mappings)?)
    }
}
