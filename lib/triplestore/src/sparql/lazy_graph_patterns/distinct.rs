use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::pushdowns::Pushdowns;
use log::debug;
use query_processing::graph_patterns::distinct;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use std::time::Instant;

impl Triplestore {
    pub(crate) fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing distinct graph pattern");
        let solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
            parameters,
            pushdowns,
        )?;
        let now = Instant::now();
        let sm = distinct(solution_mappings)?;
        println!("sm height {}", sm.height_upper_bound);
        let eager_sm = sm.as_eager();
        println!("eager sm height {}", eager_sm.mappings.height());
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        Ok(eager_sm.as_lazy())

    }
}
