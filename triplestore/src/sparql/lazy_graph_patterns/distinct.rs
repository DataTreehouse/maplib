use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::debug;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub(crate) fn lazy_distinct(
        &self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing distinct graph pattern");
        let SolutionMappings {
            mappings,
            columns,
            rdf_node_types: datatypes,
        } = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
        )?;
        Ok(SolutionMappings::new(
            mappings.unique_stable(None, UniqueKeepStrategy::First),
            columns,
            datatypes,
        ))
    }
}
