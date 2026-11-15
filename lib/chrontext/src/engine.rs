use crate::errors::ChrontextError;
use representation::dataset::NamedGraph;
use representation::result::QueryResult;
use representation::solution_mapping::EagerSolutionMappings;
use spargebra::term::{GroundTerm, NamedNode};
use std::collections::HashMap;
use std::sync::Arc;
use triplestore::sparql::QuerySettings;
use triplestore::Triplestore;
use virtualization::{Virtualization, VirtualizedDatabase};

/// A virtualized database together with the resources (and their templates) it holds.
#[derive(Clone)]
pub struct VirtualizationBackend {
    pub virtualized_database: Arc<VirtualizedDatabase>,
    pub virtualization: Virtualization,
}

/// The virtualizations of a model. Each resource belongs to exactly one virtualized database,
/// and queries over a resource are sent to the database it belongs to.
#[derive(Clone, Default)]
pub struct ChrontextSettings {
    pub backends: Vec<VirtualizationBackend>,
}

impl ChrontextSettings {
    pub fn new(
        virtualized_database: Arc<VirtualizedDatabase>,
        virtualization: Virtualization,
    ) -> ChrontextSettings {
        let mut settings = ChrontextSettings::default();
        settings.add_virtualization(virtualized_database, virtualization);
        settings
    }

    /// Adds a virtualized database with its resources.
    /// A resource that already belongs to another virtualized database is moved to this one,
    /// and virtualized databases that are left without resources are removed.
    pub fn add_virtualization(
        &mut self,
        virtualized_database: Arc<VirtualizedDatabase>,
        virtualization: Virtualization,
    ) {
        for backend in &mut self.backends {
            backend
                .virtualization
                .resources
                .retain(|r, _| !virtualization.resources.contains_key(r));
        }
        self.backends
            .retain(|b| !b.virtualization.resources.is_empty());
        if !virtualization.resources.is_empty() {
            self.backends.push(VirtualizationBackend {
                virtualized_database,
                virtualization,
            });
        }
    }

    /// All resources of all the virtualized databases.
    pub fn combined_virtualization(&self) -> Virtualization {
        let mut resources = HashMap::new();
        for b in &self.backends {
            for (r, t) in &b.virtualization.resources {
                resources.insert(r.clone(), t.clone());
            }
        }
        Virtualization { resources }
    }

    /// The index in backends of the virtualized database that holds the resource.
    pub fn backend_of_resource(&self, resource: &str) -> Option<usize> {
        self.backends
            .iter()
            .position(|b| b.virtualization.resources.contains_key(resource))
    }
}

pub struct Engine {}

impl Engine {
    pub fn new(
        _chrontext_settings: &ChrontextSettings,
        _triplestore: &Triplestore,
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
        _bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> Result<QueryResult, ChrontextError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}
