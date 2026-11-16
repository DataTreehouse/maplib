//! Shortest paths as a magic predicate, in the style of full text search:
//!
//! ```sparql
//! PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
//! SELECT ?t ?len ?nodes WHERE {
//!   <source> maplib:shortestPath ?p .
//!   ?p maplib:object ?t ;
//!      maplib:predicate cim:Terminal.ConductingEquipment, cim:Terminal.ConnectivityNode ;
//!      maplib:inversePredicate cim:Terminal.ConductingEquipment, cim:Terminal.ConnectivityNode ;
//!      maplib:maxLength 40 ;
//!      maplib:limit 5 ;
//!      maplib:length ?len ;
//!      maplib:pathNodes ?nodes .
//!   ?t a cim:Breaker .
//! }
//! ```
//!
//! The other patterns of the basic graph pattern (and the solutions joined into it) are
//! evaluated first: the values they give the source, target and avoided variables are the
//! sources, targets and avoided nodes of the search. Patterns that are not connected through
//! their variables are evaluated separately, so the sources and targets are not multiplied
//! with each other. The search runs on an integer graph built from the triples of the edge
//! predicates, which is cached until those triples change.
//!
//! The options share their names with the length-constrained `maplib:path` predicate.

pub mod algo;

use super::magic_paths::{
    boolean_option, has_maplib_predicate, iri_option, literal_option, maplib_local_name,
    non_negative_integer_option, number_option, SHORTEST_PATH,
};
use super::path::{U32DataFrameCreator, LOOKUP_COLUMN, VALUE_COLUMN};
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use crate::Triplestore;
use algo::{search_many, FoundPath, Job, SearchGraph, SearchParams, Targets};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{col, DataType, IdxCa, IntoLazy, JoinType};
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, Series};
use query_processing::expressions::{literal_enc, named_node_local_enc};
use query_processing::graph_patterns::{distinct, join, union};
use query_processing::pushdowns::Pushdowns;
use representation::cats::{Cats, LockedCats};
use representation::dataset::{NamedGraph, QueryGraph};
use representation::multitype::compress_actual_multitypes;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// The scratch memory of the parallel searches, in bytes.
const SCRATCH_BUDGET: usize = 2 << 30;
/// The number of cached search graphs.
const CACHE_SIZE: usize = 4;

/// Whether a basic graph pattern uses `maplib:shortestPath`.
pub fn has_path_search(patterns: &[TriplePattern]) -> bool {
    patterns
        .iter()
        .any(|tp| has_maplib_predicate(tp, SHORTEST_PATH))
}

/// One end of a search: a variable, or constant terms.
#[derive(Debug, Clone)]
pub(crate) enum Endpoint {
    Variable(String),
    Constants(Vec<TermPattern>),
}

#[derive(Debug, Clone)]
struct PathSearchSpec {
    source: Endpoint,
    target: Option<Endpoint>,
    avoid: Option<Endpoint>,
    /// Edges followed from subject to object, and from object to subject.
    forward: Vec<NamedNode>,
    inverse: Vec<NamedNode>,
    /// Every predicate between two resources (but rdf:type), followed (forward, backward).
    any_predicate: Option<(bool, bool)>,
    /// Predicates left out of `any_predicate`.
    exclude: Vec<NamedNode>,
    weight: Option<NamedNode>,
    default_weight: f64,
    params: SearchParams,
    length_var: Option<String>,
    cost_var: Option<String>,
    id_var: Option<String>,
    node_var: Option<String>,
    index_var: Option<String>,
    step_predicate_var: Option<String>,
    step_inverse_var: Option<String>,
    nodes_var: Option<String>,
}

impl PathSearchSpec {
    fn per_node_rows(&self) -> bool {
        self.node_var.is_some()
            || self.index_var.is_some()
            || self.step_predicate_var.is_some()
            || self.step_inverse_var.is_some()
    }
}

use super::magic_paths::path_error as err;

pub(crate) fn var_name(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

pub(crate) fn output_var(param: &str, t: &TermPattern) -> Result<String, SparqlError> {
    var_name(t).ok_or_else(|| err(format!("maplib:{param} must be a variable, got {t}")))
}

pub(crate) fn add_endpoint(
    param: &str,
    current: &mut Option<Endpoint>,
    t: &TermPattern,
) -> Result<(), SparqlError> {
    match (var_name(t), current.as_mut()) {
        (Some(v), None) => *current = Some(Endpoint::Variable(v)),
        (None, None) => *current = Some(Endpoint::Constants(vec![t.clone()])),
        (None, Some(Endpoint::Constants(c))) => c.push(t.clone()),
        _ => {
            return Err(err(format!(
                "maplib:{param} takes one variable or any number of constants"
            )))
        }
    }
    Ok(())
}

/// Takes the path search out of the patterns: returns its specification and the other patterns.
fn parse_spec(
    patterns: &[TriplePattern],
) -> Result<(PathSearchSpec, Vec<TriplePattern>), SparqlError> {
    let magic: Vec<&TriplePattern> = patterns
        .iter()
        .filter(|tp| has_maplib_predicate(tp, SHORTEST_PATH))
        .collect();
    if magic.len() != 1 {
        return Err(err(
            "use one maplib:shortestPath per group, put further searches in their own { }",
        ));
    }
    let magic = magic[0];
    let node = &magic.object;
    if var_name(node).is_none() {
        return Err(err(
            "the object of maplib:shortestPath must be a variable or a blank node",
        ));
    }
    let source = match var_name(&magic.subject) {
        Some(v) => Endpoint::Variable(v),
        None => Endpoint::Constants(vec![magic.subject.clone()]),
    };
    let mut spec = PathSearchSpec {
        source,
        target: None,
        avoid: None,
        forward: vec![],
        inverse: vec![],
        any_predicate: None,
        exclude: vec![],
        weight: None,
        default_weight: 0.0,
        params: SearchParams {
            min_length: 1,
            ..Default::default()
        },
        length_var: None,
        cost_var: None,
        id_var: None,
        node_var: None,
        index_var: None,
        step_predicate_var: None,
        step_inverse_var: None,
        nodes_var: None,
    };
    let mut max_paths_set = false;
    let mut rest = vec![];
    for p in patterns {
        if std::ptr::eq(p, magic) {
            continue;
        }
        let NamedNodePattern::NamedNode(pred) = &p.predicate else {
            rest.push(p.clone());
            continue;
        };
        let Some(name) = maplib_local_name(pred).filter(|_| &p.subject == node) else {
            rest.push(p.clone());
            continue;
        };
        let o = &p.object;
        match name {
            "object" => add_endpoint(name, &mut spec.target, o)?,
            "avoid" => add_endpoint(name, &mut spec.avoid, o)?,
            "excludePredicate" => spec.exclude.push(iri_option(name, o)?.clone()),
            "predicate" => spec.forward.push(iri_option(name, o)?.clone()),
            "inversePredicate" => spec.inverse.push(iri_option(name, o)?.clone()),
            "anyPredicate" => {
                let l = literal_option(name, o)?;
                spec.any_predicate = Some(match l.value().to_lowercase().as_str() {
                    "forward" => (true, false),
                    "backward" => (false, true),
                    "both" => (true, true),
                    v => {
                        return Err(err(format!(
                            "maplib:anyPredicate must be \"forward\", \"backward\" or \"both\", got \"{v}\""
                        )))
                    }
                });
            }
            "minLength" => {
                spec.params.min_length = non_negative_integer_option(name, o)?.min(u32::MAX as u64) as u32
            }
            "maxLength" => {
                spec.params.max_length = Some(non_negative_integer_option(name, o)?.min(u32::MAX as u64) as u32)
            }
            "maxCost" => spec.params.max_cost = Some(number_option(name, o)?),
            "weight" => spec.weight = Some(iri_option(name, o)?.clone()),
            "defaultWeight" => spec.default_weight = number_option(name, o)?,
            "limit" => spec.params.limit = Some(non_negative_integer_option(name, o)? as usize),
            "kShortest" => spec.params.k = (non_negative_integer_option(name, o)? as usize).max(1),
            "allShortest" => spec.params.all = boolean_option(name, o)?,
            "maxPaths" => {
                spec.params.max_paths = non_negative_integer_option(name, o)? as usize;
                max_paths_set = true;
            }
            "length" => spec.length_var = Some(output_var(name, o)?),
            "cost" => spec.cost_var = Some(output_var(name, o)?),
            "pathIndex" => spec.id_var = Some(output_var(name, o)?),
            "pathNode" => spec.node_var = Some(output_var(name, o)?),
            "nodeIndex" => spec.index_var = Some(output_var(name, o)?),
            "stepPredicate" => spec.step_predicate_var = Some(output_var(name, o)?),
            "stepInverse" => spec.step_inverse_var = Some(output_var(name, o)?),
            "pathNodes" => spec.nodes_var = Some(output_var(name, o)?),
            _ => {
                return Err(err(format!(
                    "unknown option maplib:{name} of maplib:shortestPath"
                )))
            }
        }
    }
    if spec.forward.is_empty() && spec.inverse.is_empty() && spec.any_predicate.is_none() {
        return Err(err(
            "maplib:shortestPath needs maplib:predicate, maplib:inversePredicate or maplib:anyPredicate",
        ));
    }
    if spec.params.all && spec.params.k > 1 {
        return Err(err(
            "use either maplib:allShortest or maplib:kShortest, not both",
        ));
    }
    if max_paths_set && !spec.params.all {
        debug!("maplib:maxPaths only applies with maplib:allShortest");
    }
    spec.forward.sort();
    spec.forward.dedup();
    spec.inverse.sort();
    spec.inverse.dedup();
    Ok((spec, rest))
}

/// The integer graph of the edge predicates. Row i of `lookup` holds node i.
pub struct BuiltGraph {
    pub graph: SearchGraph,
    pub lookup: DataFrame,
    pub value_state: RDFNodeState,
    pub predicates: Vec<NamedNode>,
}

impl BuiltGraph {
    fn empty(predicates: Vec<NamedNode>) -> BuiltGraph {
        BuiltGraph {
            graph: SearchGraph::default(),
            lookup: DataFrame::new(
                0,
                vec![
                    Column::new(VALUE_COLUMN.into(), Vec::<u32>::new()),
                    Column::new(LOOKUP_COLUMN.into(), Vec::<u32>::new()),
                ],
            )
            .unwrap(),
            value_state: BaseRDFNodeType::IRI.into_default_stored_rdf_node_state(),
            predicates,
        }
    }
}

/// Search graphs by the dataset, edge predicates and the versions of their triples. A clone of
/// a triplestore starts with an empty cache, as its triples may change independently.
#[derive(Default)]
pub struct PathGraphCache {
    entries: Mutex<Vec<(String, Arc<BuiltGraph>)>>,
}

impl Clone for PathGraphCache {
    fn clone(&self) -> Self {
        PathGraphCache::default()
    }
}

pub(crate) fn u32_state() -> RDFNodeState {
    BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
        .into_default_stored_rdf_node_state()
}

impl Triplestore {
    /// Evaluates a basic graph pattern with a path search.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_path_search(
        &self,
        patterns: &[TriplePattern],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: &Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let (spec, rest) = parse_spec(patterns)?;

        // Group the other patterns (and the incoming solutions) by shared variables.
        let mut items: Vec<HashSet<String>> = rest.iter().map(pattern_vars).collect();
        let incoming_index = solution_mappings.as_ref().map(|sm| {
            items.push(sm.rdf_node_types.keys().cloned().collect());
            items.len() - 1
        });
        let groups = connected_groups(&items);
        let mut components: Vec<(HashSet<String>, SolutionMappings)> = vec![];
        let mut incoming = solution_mappings;
        for group in groups {
            let mut vars = HashSet::new();
            let mut tps = vec![];
            let mut with_incoming = false;
            for i in group {
                vars.extend(items[i].iter().cloned());
                if Some(i) == incoming_index {
                    with_incoming = true;
                } else {
                    tps.push(rest[i].clone());
                }
            }
            let initial = if with_incoming { incoming.take() } else { None };
            let sm = if tps.is_empty() {
                initial.unwrap()
            } else {
                self.lazy_graph_pattern(
                    &GraphPattern::Bgp { patterns: tps },
                    initial,
                    context,
                    parameters,
                    pushdowns.clone(),
                    query_settings,
                    dataset,
                )?
            };
            components.push((vars, sm));
        }
        let component_of = |v: &str| components.iter().position(|(vars, _)| vars.contains(v));

        let source_var = match &spec.source {
            Endpoint::Variable(v) => Some(v.clone()),
            _ => None,
        };
        let target_var = match &spec.target {
            Some(Endpoint::Variable(v)) => Some(v.clone()),
            _ => None,
        };
        let source_component = match &source_var {
            Some(v) => Some(component_of(v).ok_or_else(|| {
                err(format!(
                    "the source ?{v} of maplib:shortestPath must be bound by the other patterns of its group"
                ))
            })?),
            None => None,
        };
        let target_component = target_var.as_ref().and_then(|v| component_of(v));
        let avoid_component = match &spec.avoid {
            Some(Endpoint::Variable(v)) => Some(component_of(v).ok_or_else(|| {
                err(format!(
                    "the avoided nodes ?{v} must be bound by the other patterns of its group"
                ))
            })?),
            _ => None,
        };

        let (forward, inverse) = match spec.any_predicate {
            Some((f, b)) => {
                let mut all = self.resource_predicates(dataset);
                all.retain(|p| !spec.exclude.contains(p));
                (
                    if f { all.clone() } else { spec.forward.clone() },
                    if b { all } else { spec.inverse.clone() },
                )
            }
            None => (spec.forward.clone(), spec.inverse.clone()),
        };
        let built = self.path_search_graph(&forward, &inverse, dataset)?;
        let n = built.graph.n;
        let cats = self.global_cats.clone();

        // The jobs: sources with their targets.
        let pairs = source_component.is_some() && source_component == target_component;
        let jobs: Vec<Job> = if pairs {
            let (_, sm) = &components[source_component.unwrap()];
            let s = source_var.as_ref().unwrap();
            let t = target_var.as_ref().unwrap();
            let df = self.keys_of(sm.clone(), &[s, t], &built)?;
            let sk = df.column(&key_col(s)).unwrap().u32().unwrap().clone();
            let tk = df.column(&key_col(t)).unwrap().u32().unwrap().clone();
            let mut by_source: HashMap<u32, Vec<u32>> = HashMap::new();
            for (s, t) in sk.into_no_null_iter().zip(tk.into_no_null_iter()) {
                by_source.entry(s).or_default().push(t);
            }
            let mut jobs: Vec<Job> = by_source
                .into_iter()
                .map(|(source, mut t)| {
                    t.sort_unstable();
                    t.dedup();
                    Job {
                        source,
                        targets: Targets::Set(Arc::new(t)),
                    }
                })
                .collect();
            jobs.sort_by_key(|j| j.source);
            jobs
        } else {
            let sources = match (&spec.source, source_component) {
                (Endpoint::Variable(v), Some(c)) => {
                    self.key_vec(components[c].1.clone(), v, &built)?
                }
                (Endpoint::Constants(c), _) => self.constant_keys(c, &built)?,
                _ => unreachable!("Should never happen"),
            };
            let targets =
                match (&spec.target, target_component) {
                    (None, _) | (Some(Endpoint::Variable(_)), None) => Targets::All,
                    (Some(Endpoint::Variable(v)), Some(c)) => Targets::Set(Arc::new(
                        self.key_vec(components[c].1.clone(), v, &built)?,
                    )),
                    (Some(Endpoint::Constants(c)), _) => {
                        Targets::Set(Arc::new(self.constant_keys(c, &built)?))
                    }
                };
            sources
                .into_iter()
                .map(|source| Job {
                    source,
                    targets: targets.clone(),
                })
                .collect()
        };

        let mut params = spec.params.clone();
        if let Some(avoid) = &spec.avoid {
            let keys = match (avoid, avoid_component) {
                (Endpoint::Variable(v), Some(c)) => {
                    self.key_vec(components[c].1.clone(), v, &built)?
                }
                (Endpoint::Constants(c), _) => self.constant_keys(c, &built)?,
                _ => unreachable!("Should never happen"),
            };
            let mut mask = vec![false; n];
            for k in keys {
                mask[k as usize] = true;
            }
            params.avoid = Some(Arc::new(mask));
        }
        if let Some(w) = &spec.weight {
            params.weights = Some(Arc::new(self.node_weights(
                w,
                spec.default_weight,
                &built,
                dataset,
            )?));
        }

        let found = search_many(&built.graph, &jobs, &params, SCRATCH_BUDGET);
        utils::cancel::check()?;
        debug!(
            "Path search: {} sources, {} paths, graph of {} nodes and {} edges",
            jobs.len(),
            found.len(),
            n,
            built.graph.n_edges()
        );
        let mut out = self.path_output(&spec, &found, &built, &source_var, &target_var, cats)?;

        // Join the components that bind the ends of the paths, then the others. Components that
        // only give the avoided nodes are not joined.
        let mut order: Vec<usize> = vec![];
        for c in [source_component, target_component].into_iter().flatten() {
            if !order.contains(&c) {
                order.push(c);
            }
        }
        for c in 0..components.len() {
            if !order.contains(&c) && Some(c) != avoid_component {
                order.push(c);
            }
        }
        let mut components: Vec<Option<SolutionMappings>> =
            components.into_iter().map(|(_, sm)| Some(sm)).collect();
        for c in order {
            out = join(
                out,
                components[c].take().unwrap(),
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(out)
    }

    /// The predicates of the dataset with resources (IRIs or blank nodes) as objects, but
    /// rdf:type, sorted.
    pub(crate) fn resource_predicates(&self, dataset: &QueryGraph) -> Vec<NamedNode> {
        let graphs: Vec<NamedGraph> = match dataset {
            QueryGraph::NamedGraph(g) => vec![g.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        };
        let mut out = vec![];
        for g in &graphs {
            if let Some(map) = self.graph_triples_map.get(g) {
                for (p, tables) in map {
                    if p.as_ref() == rdf::TYPE {
                        continue;
                    }
                    if tables.keys().any(|(_, ot)| {
                        matches!(ot, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode)
                    }) {
                        out.push(p.clone());
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The search graph of the predicates in the dataset, from the cache if their triples have
    /// not changed.
    pub(crate) fn path_search_graph(
        &self,
        forward: &[NamedNode],
        inverse: &[NamedNode],
        dataset: &QueryGraph,
    ) -> Result<Arc<BuiltGraph>, SparqlError> {
        let mut predicates: Vec<NamedNode> = forward.iter().chain(inverse).cloned().collect();
        predicates.sort();
        predicates.dedup();
        let mut versions = vec![];
        for (g, map) in self.graph_triples_map.iter() {
            for p in &predicates {
                if let Some(tables) = map.get(p) {
                    for ((st, ot), t) in tables {
                        versions.push(format!(
                            "{g:?}|{p}|{st:?}|{ot:?}|{}|{}",
                            t.generation(),
                            t.height()
                        ));
                    }
                }
            }
        }
        versions.sort();
        let key = format!("{dataset:?}|{forward:?}|{inverse:?}|{}", versions.join(";"));
        {
            let entries = self.path_graph_cache.entries.lock().unwrap();
            if let Some((_, g)) = entries.iter().find(|(k, _)| k == &key) {
                return Ok(g.clone());
            }
        }
        let built =
            Arc::new(self.build_path_search_graph(&predicates, forward, inverse, dataset)?);
        let mut entries = self.path_graph_cache.entries.lock().unwrap();
        entries.retain(|(k, _)| k != &key);
        entries.insert(0, (key, built.clone()));
        entries.truncate(CACHE_SIZE);
        Ok(built)
    }

    fn build_path_search_graph(
        &self,
        predicates: &[NamedNode],
        forward: &[NamedNode],
        inverse: &[NamedNode],
        dataset: &QueryGraph,
    ) -> Result<BuiltGraph, SparqlError> {
        let mut creator = U32DataFrameCreator::new(dataset.clone());
        for p in predicates {
            creator.gather_namednode_dfs(&PropertyPathExpression::NamedNode(p.clone()), self)?;
        }
        creator.named_nodes.retain(|_, (df, _, _)| df.height() > 0);
        if creator.named_nodes.is_empty() {
            return Ok(BuiltGraph::empty(predicates.to_vec()));
        }
        let (lookup, mut lookup_dtypes, dfs) = creator.create_u32_dfs(self.global_cats.clone())?;
        let n = lookup.height();
        let mut from = vec![];
        let mut to = vec![];
        let mut labels = vec![];
        for (leaf, df) in dfs {
            let super::path::PathLeaf::NamedNode(nn) = leaf else {
                unreachable!("Should never happen")
            };
            let p = predicates.iter().position(|x| x == &nn).unwrap() as u32;
            let (fw, inv) = (forward.contains(&nn), inverse.contains(&nn));
            let s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
            let o = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
            for (s, o) in s.iter().zip(o.iter()) {
                if let (Some(s), Some(o)) = (s, o) {
                    if fw {
                        from.push(s);
                        to.push(o);
                        labels.push(2 * p);
                    }
                    if inv {
                        from.push(o);
                        to.push(s);
                        labels.push(2 * p + 1);
                    }
                }
            }
        }
        let graph = SearchGraph::from_labelled(n, &from, &to, &labels);
        let lookup = lookup
            .lazy()
            .with_column(col(LOOKUP_COLUMN).cast(DataType::UInt32))
            .collect()
            .unwrap();
        Ok(BuiltGraph {
            graph,
            lookup,
            value_state: lookup_dtypes.remove(VALUE_COLUMN).unwrap(),
            predicates: predicates.to_vec(),
        })
    }

    /// The distinct values of `vars` in the solutions with their node keys, in the columns
    /// `vars` and `key_col(var)`. Values that are not nodes of the graph are left out.
    pub(crate) fn keys_of(
        &self,
        sm: SolutionMappings,
        vars: &[&String],
        built: &BuiltGraph,
    ) -> Result<DataFrame, SparqlError> {
        let SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate,
        } = sm;
        let mut types = HashMap::new();
        for v in vars {
            types.insert(
                v.to_string(),
                rdf_node_types.get(v.as_str()).unwrap().clone(),
            );
        }
        let projected = SolutionMappings::new(
            mappings.select(vars.iter().map(|v| col(v.as_str())).collect::<Vec<_>>()),
            types,
            height_estimate,
        );
        let mut sm = distinct(projected)?;
        for v in vars {
            let k = key_col(v);
            let lookup = SolutionMappings::new(
                built.lookup.clone().lazy().select([
                    col(VALUE_COLUMN).alias(v.as_str()),
                    col(LOOKUP_COLUMN).alias(&k),
                ]),
                HashMap::from([
                    (v.to_string(), built.value_state.clone()),
                    (k.clone(), u32_state()),
                ]),
                built.lookup.height(),
            );
            sm = join(sm, lookup, JoinType::Inner, self.global_cats.clone(), None)?;
        }
        Ok(sm.mappings.collect().unwrap())
    }

    pub(crate) fn key_vec(
        &self,
        sm: SolutionMappings,
        var: &String,
        built: &BuiltGraph,
    ) -> Result<Vec<u32>, SparqlError> {
        let df = self.keys_of(sm, &[var], built)?;
        let mut keys: Vec<u32> = df
            .column(&key_col(var))
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    pub(crate) fn constant_keys(
        &self,
        constants: &[TermPattern],
        built: &BuiltGraph,
    ) -> Result<Vec<u32>, SparqlError> {
        let v = "constant".to_string();
        let sm = constant_mappings(constants, &v, &self.global_cats)?;
        self.key_vec(sm, &v, built)
    }

    /// The weight of each node: the value of the weight predicate, or the default.
    fn node_weights(
        &self,
        predicate: &NamedNode,
        default: f64,
        built: &BuiltGraph,
        dataset: &QueryGraph,
    ) -> Result<Vec<f64>, SparqlError> {
        let mut weights = vec![default; built.graph.n];
        let sm = self.get_multi_predicates_solution_mappings(
            Some(vec![predicate.clone()]),
            &Some(SUBJECT_COL_NAME.to_string()),
            &None,
            &Some(OBJECT_COL_NAME.to_string()),
            &None,
            &None,
            &None,
            &None,
            dataset,
        )?;
        let object_state = sm.rdf_node_types.get(OBJECT_COL_NAME).cloned();
        let Some(object_state) = object_state else {
            return Ok(weights);
        };
        let k = key_col(SUBJECT_COL_NAME);
        let lookup = SolutionMappings::new(
            built.lookup.clone().lazy().select([
                col(VALUE_COLUMN).alias(SUBJECT_COL_NAME),
                col(LOOKUP_COLUMN).alias(&k),
            ]),
            HashMap::from([
                (SUBJECT_COL_NAME.to_string(), built.value_state.clone()),
                (k.clone(), u32_state()),
            ]),
            built.lookup.height(),
        );
        let joined = join(sm, lookup, JoinType::Inner, self.global_cats.clone(), None)?;
        let df = joined.mappings.collect().unwrap();
        let objects = df.column(OBJECT_COL_NAME).unwrap().as_materialized_series();
        let values = numeric_values(objects, &object_state)?;
        let keys = df.column(&k).unwrap().u32().unwrap();
        for (key, w) in keys.iter().zip(values) {
            if let (Some(key), Some(w)) = (key, w) {
                if w.is_finite() {
                    weights[key as usize] = w.max(0.0);
                }
            }
        }
        Ok(weights)
    }

    /// The solution mappings of the paths found.
    fn path_output(
        &self,
        spec: &PathSearchSpec,
        found: &[FoundPath],
        built: &BuiltGraph,
        source_var: &Option<String>,
        target_var: &Option<String>,
        cats: LockedCats,
    ) -> Result<SolutionMappings, SparqlError> {
        let per_node = spec.per_node_rows();
        let rows: usize = if per_node {
            found.iter().map(|p| p.nodes.len()).sum()
        } else {
            found.len()
        };
        let mut src = Vec::with_capacity(rows);
        let mut tgt = Vec::with_capacity(rows);
        let mut length = Vec::with_capacity(rows);
        let mut cost = Vec::with_capacity(rows);
        let mut id = Vec::with_capacity(rows);
        let mut node = Vec::with_capacity(if per_node { rows } else { 0 });
        let mut index = Vec::with_capacity(if per_node { rows } else { 0 });
        let mut step_label: Vec<Option<u32>> = Vec::with_capacity(if per_node { rows } else { 0 });
        let mut nodes_json: Vec<String> = vec![];
        let node_strings = if spec.nodes_var.is_some() {
            Some(self.node_strings(found, built)?)
        } else {
            None
        };
        for p in found {
            let json = node_strings.as_ref().map(|m| {
                serde_json::to_string(
                    &p.nodes
                        .iter()
                        .map(|k| m.get(k).cloned().unwrap_or_default())
                        .collect::<Vec<String>>(),
                )
                .unwrap()
            });
            let reps = if per_node { p.nodes.len() } else { 1 };
            for i in 0..reps {
                src.push(p.source);
                tgt.push(p.target);
                length.push(p.length() as i64);
                cost.push(p.cost);
                id.push(p.path_id as i64);
                if let Some(j) = &json {
                    nodes_json.push(j.clone());
                }
                if per_node {
                    node.push(p.nodes[i]);
                    index.push(i as i64);
                    step_label.push(if i == 0 { None } else { Some(p.labels[i - 1]) });
                }
            }
        }
        let values = built.lookup.column(VALUE_COLUMN).unwrap();
        let mut columns: Vec<Column> = vec![];
        let mut types: HashMap<String, RDFNodeState> = HashMap::new();
        let mut gather = |name: &str, keys: Vec<u32>, columns: &mut Vec<Column>| {
            columns.push(
                values
                    .take(&IdxCa::from_vec("".into(), keys))
                    .unwrap()
                    .with_name(name.into()),
            );
            types.insert(name.to_string(), built.value_state.clone());
        };
        if let Some(s) = source_var {
            gather(s, src, &mut columns);
        }
        if let Some(t) = target_var {
            gather(t, tgt, &mut columns);
        }
        if let Some(v) = &spec.node_var {
            gather(v, node.clone(), &mut columns);
        }
        let integer = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None);
        let mut add = |name: &Option<String>, c: Column, t: RDFNodeState| {
            if let Some(name) = name {
                columns.push(c.with_name(name.into()));
                types.insert(name.clone(), t);
            }
        };
        add(
            &spec.length_var,
            Column::new("".into(), length),
            integer.clone().into_default_stored_rdf_node_state(),
        );
        add(
            &spec.cost_var,
            Column::new("".into(), cost),
            BaseRDFNodeType::Literal(xsd::DOUBLE.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );
        add(
            &spec.id_var,
            Column::new("".into(), id),
            integer.clone().into_default_stored_rdf_node_state(),
        );
        add(
            &spec.index_var,
            Column::new("".into(), index),
            integer.into_default_stored_rdf_node_state(),
        );
        add(
            &spec.step_predicate_var,
            Column::new(
                "".into(),
                step_label
                    .iter()
                    .map(|l| l.map(|l| built.predicates[(l / 2) as usize].as_str().to_string()))
                    .collect::<Vec<Option<String>>>(),
            ),
            RDFNodeState::from_bases(BaseRDFNodeType::IRI, BaseCatState::String),
        );
        add(
            &spec.step_inverse_var,
            Column::new(
                "".into(),
                step_label
                    .iter()
                    .map(|l| l.map(|l| l % 2 == 1))
                    .collect::<Vec<Option<bool>>>(),
            ),
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );
        add(
            &spec.nodes_var,
            Column::new("".into(), nodes_json),
            RDFNodeState::from_bases(
                BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None),
                BaseCatState::String,
            ),
        );
        let df = DataFrame::new(rows, columns).unwrap();
        let (df, types) = compress_actual_multitypes(df, types);
        let eager = cats
            .read()?
            .encode_solution_mappings(EagerSolutionMappings::new(df, types));
        Ok(eager.as_lazy())
    }

    /// The nodes of the paths as strings: IRIs as they are, blank nodes as _:label and literals
    /// by their lexical form.
    fn node_strings(
        &self,
        found: &[FoundPath],
        built: &BuiltGraph,
    ) -> Result<HashMap<u32, String>, SparqlError> {
        let mut keys: Vec<u32> = found.iter().flat_map(|p| p.nodes.iter().copied()).collect();
        keys.sort_unstable();
        keys.dedup();
        let values = built
            .lookup
            .column(VALUE_COLUMN)
            .unwrap()
            .take(&IdxCa::from_vec("".into(), keys.clone()))
            .unwrap();
        let cats = self.global_cats.read()?;
        let strings = term_strings(values.as_materialized_series(), &built.value_state, &cats)?;
        Ok(keys
            .into_iter()
            .zip(strings)
            .map(|(k, s)| (k, s.unwrap_or_default()))
            .collect())
    }
}

pub(crate) fn key_col(v: &str) -> String {
    format!("{v}__path_key")
}

pub(crate) fn pattern_vars(tp: &TriplePattern) -> HashSet<String> {
    let mut vars = HashSet::new();
    if let Some(v) = var_name(&tp.subject) {
        vars.insert(v);
    }
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        vars.insert(v.as_str().to_string());
    }
    if let Some(v) = var_name(&tp.object) {
        vars.insert(v);
    }
    vars
}

/// The groups of items that are connected through shared variables. Items without variables
/// are groups of their own.
pub(crate) fn connected_groups(items: &[HashSet<String>]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..items.len()).collect();
    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut r = i;
        while parent[r] != r {
            r = parent[r];
        }
        let mut i = i;
        while parent[i] != r {
            let next = parent[i];
            parent[i] = r;
            i = next;
        }
        r
    }
    let mut owner: HashMap<&String, usize> = HashMap::new();
    for (i, vars) in items.iter().enumerate() {
        for v in vars {
            if let Some(j) = owner.get(v) {
                let (a, b) = (find(&mut parent, i), find(&mut parent, *j));
                parent[a] = b;
            } else {
                owner.insert(v, i);
            }
        }
    }
    let mut groups: Vec<Vec<usize>> = vec![];
    let mut group_of: HashMap<usize, usize> = HashMap::new();
    for i in 0..items.len() {
        let r = find(&mut parent, i);
        let g = *group_of.entry(r).or_insert_with(|| {
            groups.push(vec![]);
            groups.len() - 1
        });
        groups[g].push(i);
    }
    groups
}

/// Solution mappings with the constants as the values of `var`.
pub(crate) fn constant_mappings(
    constants: &[TermPattern],
    var: &str,
    global_cats: &LockedCats,
) -> Result<SolutionMappings, SparqlError> {
    let mut sms = vec![];
    {
        let cats = global_cats.read()?;
        for c in constants {
            let (expr, state) = match c {
                TermPattern::NamedNode(nn) => {
                    let (e, local) = named_node_local_enc(nn, &cats);
                    (
                        e,
                        RDFNodeState::from_bases(
                            BaseRDFNodeType::IRI,
                            BaseCatState::CategoricalNative(local.map(LockedCats::new)),
                        ),
                    )
                }
                TermPattern::Literal(l) => {
                    let (e, bt, bs) = literal_enc(l, &cats)?;
                    (e, RDFNodeState::from_bases(bt, bs))
                }
                _ => return Err(err(format!("unsupported constant {c} in a path search"))),
            };
            sms.push(SolutionMappings::new(
                DataFrame::empty_with_height(1)
                    .lazy()
                    .with_column(expr.alias(var)),
                HashMap::from([(var.to_string(), state)]),
                1,
            ));
        }
    }
    Ok(union(sms, false, global_cats.clone())?)
}

/// The values of a numeric column as f64, with None for values that are not numbers.
fn numeric_values(ser: &Series, state: &RDFNodeState) -> Result<Vec<Option<f64>>, SparqlError> {
    let one = |ser: &Series, bt: &BaseRDFNodeType| -> Vec<Option<f64>> {
        let numeric = matches!(bt, BaseRDFNodeType::Literal(..))
            && (ser.dtype().is_primitive_numeric() || ser.dtype().is_decimal());
        if !numeric {
            return vec![None; ser.len()];
        }
        let c = ser.cast(&DataType::Float64).unwrap();
        c.f64().unwrap().iter().collect()
    };
    if !state.is_multi() {
        return Ok(one(ser, state.get_base_type().unwrap()));
    }
    let mut out = vec![None; ser.len()];
    let s = ser.struct_().unwrap();
    for bt in state.map.keys() {
        if bt.is_lang_string() {
            continue;
        }
        if let Ok(field) = s.field_by_name(&bt.field_col_name()) {
            for (o, v) in out.iter_mut().zip(one(&field, bt)) {
                if o.is_none() {
                    *o = v;
                }
            }
        }
    }
    Ok(out)
}

/// The values of a column as strings.
fn term_strings(
    ser: &Series,
    state: &RDFNodeState,
    cats: &Cats,
) -> Result<Vec<Option<String>>, SparqlError> {
    let one = |ser: &Series, bt: &BaseRDFNodeType, bs: &BaseCatState| -> Vec<Option<String>> {
        let decoded = match bs {
            BaseCatState::CategoricalNative(local) => {
                match cats.decode_of_type(ser, bt, local.clone()) {
                    Ok(s) => s,
                    Err(_) => return vec![None; ser.len()],
                }
            }
            BaseCatState::String => ser.clone(),
            BaseCatState::NonString => match ser.cast(&DataType::String) {
                Ok(s) => s,
                Err(_) => return vec![None; ser.len()],
            },
        };
        let prefix = if matches!(bt, BaseRDFNodeType::BlankNode) {
            "_:"
        } else {
            ""
        };
        match decoded.str() {
            Ok(s) => s
                .iter()
                .map(|x| x.map(|x| format!("{prefix}{x}")))
                .collect(),
            Err(_) => vec![None; ser.len()],
        }
    };
    if !state.is_multi() {
        let (bt, bs) = state.map.iter().next().unwrap();
        if bt.is_lang_string() {
            return Ok(vec![None; ser.len()]);
        }
        return Ok(one(ser, bt, bs));
    }
    let mut out = vec![None; ser.len()];
    let s = ser.struct_().unwrap();
    for (bt, bs) in &state.map {
        if bt.is_lang_string() {
            continue;
        }
        if let Ok(field) = s.field_by_name(&bt.field_col_name()) {
            for (o, v) in out.iter_mut().zip(one(&field, bt, bs)) {
                if o.is_none() {
                    *o = v;
                }
            }
        }
    }
    Ok(out)
}
