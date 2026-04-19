use std::collections::HashMap;
use std::sync::Arc;
use representation::dataset::NamedGraph;
use representation::solution_mapping::EagerSolutionMappings;
use spargebra::term::NamedNode;
use crate::errors::ChrontextError;
use triplestore::sparql::{QueryResult, QuerySettings};
use triplestore::Triplestore;
use virtualization::{Virtualization, VirtualizedDatabase};

#[derive(Clone)]
pub struct ChrontextSettings {
    pub virtualized_database: Arc<VirtualizedDatabase>,
    pub virtualization: Virtualization,
}

pub struct Engine {
}

impl Engine {
    pub fn new(
        _chrontext_settings: &ChrontextSettings,
        _triplestore: &Triplestore,
        _streaming: bool,
        _parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        _query_settings: QuerySettings,
        _graph: Option<NamedGraph>,
        _debug_no_results: bool,
    ) -> Engine {
        unimplemented!("Contact Data Treehouse to try")
    }

    pub fn query_blocking(
        &self,
        _query: &str,
        _prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<QueryResult, ChrontextError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}
