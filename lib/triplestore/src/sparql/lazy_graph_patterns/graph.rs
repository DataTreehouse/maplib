use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;
use std::collections::HashMap;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(crate) fn lazy_graph(
        &self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GraphInner);
        let sm = match name {
            NamedNodePattern::NamedNode(nn) => self.lazy_graph_pattern(
                inner,
                solution_mappings,
                &inner_context,
                parameters,
                pushdowns,
                query_settings,
                &QueryGraph::NamedGraph(NamedGraph::NamedGraph(nn.clone())),
            )?,
            NamedNodePattern::Variable(v) => {
                todo!()
            }
        };
        Ok(sm)
    }
}
