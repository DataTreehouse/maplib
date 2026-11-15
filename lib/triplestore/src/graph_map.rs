//! The triples of a triplestore by graph, predicate and subject and object type. Every change
//! goes through the methods here, which record what changed for the query cache, so that a
//! change can not be missed: the map is only readable otherwise.

use crate::query_cache::{QueryCache, Touched};
use crate::storage::Triples;
use oxrdf::NamedNode;
use representation::dataset::NamedGraph;
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) type TypesMap = HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>;
pub(crate) type PredicateMap = HashMap<NamedNode, TypesMap>;

pub(crate) struct GraphTriplesMap {
    map: HashMap<NamedGraph, PredicateMap>,
    cache: Arc<QueryCache>,
}

impl Deref for GraphTriplesMap {
    type Target = HashMap<NamedGraph, PredicateMap>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl GraphTriplesMap {
    pub(crate) fn new(map: HashMap<NamedGraph, PredicateMap>, cache: Arc<QueryCache>) -> Self {
        GraphTriplesMap { map, cache }
    }

    fn touched(&self, t: Touched) {
        self.cache.versions.record(t);
    }

    /// The tables of a predicate of a graph, created if missing.
    pub(crate) fn predicate_entry(&mut self, graph: &NamedGraph, predicate: &NamedNode) -> &mut TypesMap {
        self.touched(Touched::Cell(graph.clone(), predicate.clone()));
        self.map
            .entry(graph.clone())
            .or_default()
            .entry(predicate.clone())
            .or_default()
    }

    /// The tables of a predicate of a graph, if any.
    pub(crate) fn predicate_mut(
        &mut self,
        graph: &NamedGraph,
        predicate: &NamedNode,
    ) -> Option<&mut TypesMap> {
        let m = self.map.get_mut(graph)?.get_mut(predicate)?;
        self.cache
            .versions
            .record(Touched::Cell(graph.clone(), predicate.clone()));
        Some(m)
    }

    pub(crate) fn remove_predicate(
        &mut self,
        graph: &NamedGraph,
        predicate: &NamedNode,
    ) -> Option<TypesMap> {
        let m = self.map.get_mut(graph)?.remove(predicate)?;
        self.touched(Touched::Cell(graph.clone(), predicate.clone()));
        Some(m)
    }

    /// All tables of a graph, when a change can affect any predicate of it.
    pub(crate) fn graph_mut(&mut self, graph: &NamedGraph) -> Option<&mut PredicateMap> {
        let m = self.map.get_mut(graph)?;
        self.cache.versions.record(Touched::Graph(graph.clone()));
        Some(m)
    }

    /// Adds the graph without triples, if it is missing.
    /// The tables, to change how they are indexed. Indexing does not change which triples they
    /// hold, so this is not recorded: a change of the indexing options records everything.
    pub(crate) fn tables_for_indexing(&mut self) -> &mut HashMap<NamedGraph, PredicateMap> {
        &mut self.map
    }

    pub(crate) fn ensure_graph(&mut self, graph: &NamedGraph) {
        if !self.map.contains_key(graph) {
            self.touched(Touched::Graph(graph.clone()));
            self.map.insert(graph.clone(), HashMap::new());
        }
    }

    pub(crate) fn insert_graph(&mut self, graph: NamedGraph, predicates: PredicateMap) {
        self.touched(Touched::Graph(graph.clone()));
        self.map.insert(graph, predicates);
    }

    pub(crate) fn remove_graph(&mut self, graph: &NamedGraph) -> Option<PredicateMap> {
        let m = self.map.remove(graph)?;
        self.touched(Touched::Graph(graph.clone()));
        Some(m)
    }

    pub(crate) fn drain(&mut self) -> Vec<(NamedGraph, PredicateMap)> {
        let graphs: Vec<_> = self.map.drain().collect();
        self.cache
            .versions
            .record_all(graphs.iter().map(|(g, _)| Touched::Graph(g.clone())));
        graphs
    }
}
