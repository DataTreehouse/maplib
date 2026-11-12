use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{distinct, join};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use tracing::{instrument, trace};

impl Triplestore {
    #[instrument(skip_all)]
    pub(crate) fn lazy_distinct(
        &self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing distinct graph pattern");
        // DISTINCT applies to the inner pattern only, not to the outer solution mappings.
        let inner_solution_mappings = self.lazy_graph_pattern(
            inner,
            None,
            &context.extension_with(PathEntry::DistinctInner),
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        let mut sm = distinct(inner_solution_mappings)?;
        if let Some(solution_mappings) = solution_mappings {
            sm = join(
                solution_mappings,
                sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(sm)
    }
}
