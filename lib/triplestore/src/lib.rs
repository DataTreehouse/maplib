extern crate core;

mod add_triples;
pub mod changes;
mod copy_triples;
pub mod cats;
mod dblf;
pub mod errors;
mod fts_rebuild;
mod geo_index;
mod graph_map;
mod vector_index;
mod map_df;
mod map_json;
pub use map_json::DocumentFormat;
mod map_xml;
pub mod query_cache;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod serialization;
pub mod sparql;
pub mod storage;
pub mod triples_read;
pub mod triples_write;

use crate::errors::TriplestoreError;
use crate::graph_map::GraphTriplesMap;
use crate::query_cache::{QueryCache, Touched};
use crate::storage::codes_to_ranks;
use crate::storage::Triples;
use external_sparql::federation::FederationSettings;
use file_io::create_folder_if_not_exists;
use fts::{FtsIndex, FtsOptions};
use geo::GeoIndex;
use vector::VectorIndex;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{
    as_struct, col, lit, AnyValue, DataFrame, Expr, IntoLazy, PlSmallStr, RankMethod,
    RankOptions,
};
use polars_core::frame::column::ScalarColumn;
use polars_core::prelude::IntoSeries;
use polars_core::prelude::{Column, IntoColumn, Series, SortMultipleOptions, StructChunked};
use pyo3::{Py, PyAny};
use range_set_blaze::RangeSetBlaze;
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use representation::cats::{
    cat_encode_triples, distinct_codes, extend_codes, CatTriples, CatType, Cats, EncodedTriples,
    LockedCats, OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME,
};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings};
use representation::{
    literal_iri_to_namednode, BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use utils::polars::collect_in_memory;

use query_processing::graph_patterns::union;
use query_processing::udf::UdfRegistry;
use representation::cats::maps::CatMaps;
use representation::dataset::NamedGraph;
use tracing::{debug, instrument};

// IN_MEMORY_COLLECT_LOOPS: several loops run one item per table in parallel with Rayon
// (partitioning triples to add, ranking and sorting them, building tables in
// add_global_cat_triples, compaction and splitting solution mappings for DELETE). Every
// LazyFrame these items collect, directly or through Triples::new, add_triples and so on, must
// be collected with `utils::polars::collect_in_memory`. The streaming engine blocks its thread
// in a way that lets Rayon hand it further items of the same loop, so streaming queries nest
// until no thread is left to drive them and adding triples deadlocks. The in-memory engine is
// plain Rayon fork-join and nests safely.

pub struct Triplestore {
    /// The maplib storage folder, set in hybrid and disk storage modes.
    pub storage_folder: Option<PathBuf>,
    pub storage_mode: StorageMode,
    /// Folder holding the triple tables as parquet files, set in disk storage mode only.
    pub(crate) triples_folder: Option<PathBuf>,
    graph_triples_map: GraphTriplesMap,
    parser_call: usize,
    indexing: IndexingOptions,
    fts_index: HashMap<NamedGraph, FtsIndex>,
    /// Geometry literals in DuckDB with a spatial index, when GeoSPARQL indexing is enabled.
    /// One per triplestore: it is keyed by cats, which are shared between the graphs.
    pub(crate) geo_index: Option<GeoIndex>,
    /// Vector literals with their parsed vectors, when vector indexing is enabled. One per
    /// graph, as searches return triples.
    pub(crate) vector_index: HashMap<NamedGraph, VectorIndex>,
    pub global_cats: LockedCats,
    n_triples_deleted: usize,
    pub udf_registry: Arc<UdfRegistry>,
    /// Search graphs of path searches.
    pub(crate) path_graph_cache: sparql::lazy_graph_patterns::path_search::PathGraphCache,
    /// Results of queries, invalidated by the changes `graph_triples_map` records.
    pub query_cache: Arc<QueryCache>,
    /// Which endpoints SERVICE may query and LOAD may fetch, and how.
    pub federation: Arc<FederationSettings>,
}

impl Triplestore {
    /// The counter that makes blank node labels from parsing and SPARQL updates unique.
    /// Streaming carries it from one batch triplestore to the next, so that blank nodes
    /// created in different batches do not get the same label.
    pub fn blank_node_scope(&self) -> usize {
        self.parser_call
    }

    pub fn set_blank_node_scope(&mut self, scope: usize) {
        self.parser_call = scope;
    }
}

const MAPLIB_STORAGE_FOLDER: &str = "maplib_storage";
const GC_LIMIT: &usize = &1_000_000;

impl Triplestore {
    pub fn add_udf(
        &mut self,
        iri: NamedNode,
        f: Py<PyAny>,
        output_type: BaseRDFNodeType,
        input_types: Option<Vec<BaseRDFNodeType>>,
    ) {
        let reg = Arc::get_mut(&mut self.udf_registry).expect("UDF registry is not borrowed");
        reg.add_udf(iri, f, output_type, input_types);
        // A query calling the function before it was registered may have been cached.
        self.query_cache.versions.record(Touched::All);
    }

    pub fn maybe_garbage_collect(&mut self) -> Result<u32, TriplestoreError> {
        if self.storage_folder.is_none() && &self.n_triples_deleted >= GC_LIMIT {
            self.garbage_collect()
        } else {
            Ok(0)
        }
    }

    pub fn garbage_collect(&mut self) -> Result<u32, TriplestoreError> {
        let mut predicate_and_graph_iris = Vec::new();
        for (g, m) in self.graph_triples_map.iter() {
            if let NamedGraph::NamedGraph(nn) = &g {
                predicate_and_graph_iris.push(nn);
            }
            for p in m.keys() {
                predicate_and_graph_iris.push(p);
            }
        }

        let pred_graph_series =
            Series::from_iter(predicate_and_graph_iris.iter().map(|x| x.as_str()));

        let (pred_graph_u32_series, locals) = self
            .global_cats
            .read()?
            .encode_series(&pred_graph_series, &BaseRDFNodeType::IRI);
        assert!(locals.is_none());

        let garbage_collected: Result<Vec<_>, TriplestoreError> = self
            .global_cats
            .write()?
            .cat_map
            .par_iter_mut()
            .map(|(ct, ce)| {
                let t = ct.as_base_rdf_node_type();
                let mut rs = ce.maps.range_set();
                if ct.as_base_rdf_node_type().is_iri() {
                    let pred_graph_rs = RangeSetBlaze::from_iter(
                        pred_graph_u32_series
                            .u32()
                            .unwrap()
                            .iter()
                            .map(|x| x.unwrap()),
                    );
                    rs = rs - pred_graph_rs;
                }
                // Marks the codes used by triples in a bitset over the code range, then
                // collects the codes of the map that are not marked.
                let size = rs.last().map(|x| x as usize + 1).unwrap_or(0);
                let mut used = vec![0u64; size.div_ceil(64)];
                let mut mark = |c: &Column| {
                    for u in c.u32().unwrap().iter().flatten() {
                        let u = u as usize;
                        if u < size {
                            used[u >> 6] |= 1 << (u & 63);
                        }
                    }
                };
                for g in self.graph_triples_map.values() {
                    for v in g.values() {
                        for ((st, ot), triples) in v {
                            if st != &t && ot != &t {
                                continue;
                            }
                            for (lf, _) in triples.get_lazy_frames(&None, &None)? {
                                let df = lf.collect().unwrap();
                                if st == &t {
                                    mark(df.column(SUBJECT_COL_NAME).unwrap());
                                }
                                if ot == &t {
                                    let o_col = df.column(OBJECT_COL_NAME).unwrap();
                                    if t.is_lang_string() {
                                        let st = o_col.struct_().unwrap();
                                        for f in [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD] {
                                            mark(&st.field_by_name(f).unwrap().into_column());
                                        }
                                    } else {
                                        mark(o_col);
                                    }
                                }
                            }
                        }
                    }
                }
                let is_used = |u: u32| used[(u as usize) >> 6] & (1 << (u as usize & 63)) != 0;
                let mut garbage: Vec<std::ops::RangeInclusive<u32>> = vec![];
                for r in rs.ranges() {
                    let mut run_start: Option<u32> = None;
                    for u in r.clone() {
                        if is_used(u) {
                            if let Some(start) = run_start.take() {
                                garbage.push(start..=u - 1);
                            }
                        } else if run_start.is_none() {
                            run_start = Some(u);
                        }
                    }
                    if let Some(start) = run_start {
                        garbage.push(start..=*r.end());
                    }
                }
                let rs = RangeSetBlaze::from_iter(garbage);
                let n_collected = rs.len() as u32;
                if !rs.is_empty() {
                    ce.maps.garbage_collect_cats(rs)
                }
                Ok(n_collected)
            })
            .collect();
        let garbage_collected = garbage_collected?;
        self.n_triples_deleted = 0;
        let gced = garbage_collected.iter().sum();
        debug!("Garbage collected {} cats", gced);
        Ok(gced)
    }

    pub fn compact(&mut self) -> Result<(), TriplestoreError> {
        let mut to_compact = Vec::new();
        for (graph, map) in self.graph_triples_map.drain() {
            for (pred, map) in map {
                for ((subject_type, object_type), triples) in map {
                    to_compact.push((
                        (
                            graph.clone(),
                            pred.clone(),
                            subject_type.clone(),
                            object_type.clone(),
                        ),
                        triples,
                    ));
                }
            }
        }
        let compacted: Result<Vec<_>, TriplestoreError> = to_compact
            .into_iter()
            .map(|(data, triples)| {
                let object_indexing_enabled = triples.object_indexing_enabled;
                let stored_triples =
                    triples.compact(&self.global_cats.read().expect("Should be readable"))?;
                Ok((data, object_indexing_enabled, stored_triples))
            })
            .collect();
        let compacted = compacted?;
        self.global_cats
            .write()
            .expect("Should be writable")
            .compact();
        // Collects in memory, see IN_MEMORY_COLLECT_LOOPS.
        let data_and_triples: Result<Vec<_>, TriplestoreError> = compacted
            .into_par_iter()
            .map(
                |((graph, pred, subject_type, object_type), object_indexing_enabled, st)| {
                    let triples = Triples::new_compacted(
                        collect_in_memory(st.get_lazy_frame()?).unwrap(),
                        subject_type.clone(),
                        object_type.clone(),
                        object_indexing_enabled,
                        self.global_cats.clone(),
                        self.triples_folder.as_deref(),
                    )?;
                    Ok((graph, pred, subject_type, object_type, triples))
                },
            )
            .collect();

        for (graph, pred, subject_type, object_type, triples) in data_and_triples? {
            self.graph_triples_map
                .predicate_entry(&graph, &pred)
                .insert((subject_type, object_type), triples);
        }
        // The cats are renumbered, and cached solutions refer to them.
        self.query_cache.versions.record(Touched::All);
        // Compaction renumbers the cats that the full text search and geo indexes refer to.
        self.rebuild_fts_indexes()?;
        self.rebuild_geo_index()?;
        self.rebuild_vector_indexes()?;
        Ok(())
    }

    pub fn graph_size(&self, named_graph: &NamedGraph) -> usize {
        let mut s = 0;
        if let Some(gr) = self.graph_triples_map.get(named_graph) {
            for v in gr.values() {
                for t in v.values() {
                    s += t.get_height();
                }
            }
        }
        s
    }

    /// Whether the graph holds any triple with the predicate.
    pub fn graph_has_predicate(&self, graph: &NamedGraph, predicate: &NamedNode) -> bool {
        self.graph_triples_map
            .get(graph)
            .and_then(|m| m.get(predicate))
            .is_some_and(|m| m.values().any(|t| t.get_height() > 0))
    }

    pub fn contains_graph(&self, graph: &NamedGraph) -> bool {
        self.graph_triples_map.contains_key(graph)
    }

    /// The graphs that hold triples.
    pub fn graph_names(&self) -> Vec<NamedGraph> {
        let mut graphs: Vec<_> = self
            .graph_triples_map
            .iter()
            .filter(|(_, m)| m.values().any(|m| !m.is_empty()))
            .map(|(g, _)| g.clone())
            .collect();
        graphs.sort_by_key(|g| g.to_string());
        graphs
    }

    pub fn add_cat_cache(&self, graphs: &[&NamedGraph]) -> Result<(), TriplestoreError> {
        if self.storage_folder.is_some() {
            let mut type_u32s_map: HashMap<BaseRDFNodeType, HashSet<u32>> = HashMap::new();
            for gr in graphs {
                if let Some(m) = self.graph_triples_map.get(*gr) {
                    let type_u32s: Result<Vec<Vec<_>>, TriplestoreError> = m
                        .par_iter()
                        .map(|(_, type_map)| {
                            let out_vec: Result<Vec<Vec<_>>, TriplestoreError> = type_map
                                .values()
                                .map(|triples| triples.get_u32s())
                                .collect();
                            let out_vec: Vec<_> = out_vec?.into_iter().flatten().collect();
                            Ok(out_vec)
                        })
                        .collect();
                    let type_u32s: Vec<_> = type_u32s?.into_iter().flatten().collect();
                    for (t, v) in type_u32s {
                        if let Some(u32_set) = type_u32s_map.get_mut(&t) {
                            u32_set.extend(v);
                        } else {
                            type_u32s_map.insert(t, v);
                        }
                    }
                }
            }
            let mut global_cats = self.global_cats.write()?;
            for (t, u32s) in type_u32s_map {
                let cat_enc = global_cats
                    .cat_map
                    .get_mut(&CatType::from_base_rdf_node_type(&t))
                    .unwrap();
                if let CatMaps::OnDisk(d) = &mut cat_enc.maps {
                    d.add_encs_to_cache(&u32s, t.is_iri());
                }
            }
        }
        Ok(())
    }

    pub fn detach_graph(
        &mut self,
        graph: &NamedGraph,
        preserve_name: bool,
    ) -> Result<Triplestore, TriplestoreError> {
        let mut us = HashMap::new();
        let mut predicates = vec![];
        if let Some(graph_triples_map) = self.graph_triples_map.get(graph) {
            for (k, v) in graph_triples_map {
                predicates.push(Some(k.as_str()));
                for ((st, ot), triples) in v {
                    if st.stored_cat() {
                        if !us.contains_key(st) {
                            us.insert(st.clone(), HashSet::new());
                        }
                        let uset = us.get_mut(st).unwrap();
                        let lfs = triples.get_lazy_frames(&None, &None)?;
                        for (lf, _) in lfs {
                            let subjects_df = lf.select([col(SUBJECT_COL_NAME)]).collect().unwrap();
                            let subjects_ser = subjects_df.column(SUBJECT_COL_NAME).unwrap();
                            for u in subjects_ser.u32().unwrap().iter() {
                                uset.insert(u.unwrap());
                            }
                        }
                    }
                    if ot.stored_cat() {
                        if !us.contains_key(ot) {
                            us.insert(ot.clone(), HashSet::new());
                        }
                        let uset = us.get_mut(ot).unwrap();
                        let lfs = triples.get_lazy_frames(&None, &None)?;
                        for (lf, _) in lfs {
                            let objects_df = lf.select([col(OBJECT_COL_NAME)]).collect().unwrap();
                            let objects_ser = objects_df.column(OBJECT_COL_NAME).unwrap();
                            if ot.is_lang_string() {
                                let langs_ser = objects_ser
                                    .struct_()
                                    .unwrap()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .unwrap();
                                for u in langs_ser.u32().unwrap().iter() {
                                    uset.insert(u.unwrap());
                                }
                                let vals_ser = objects_ser
                                    .struct_()
                                    .unwrap()
                                    .field_by_name(LANG_STRING_VALUE_FIELD)
                                    .unwrap();
                                for u in vals_ser.u32().unwrap().iter() {
                                    uset.insert(u.unwrap());
                                }
                            } else {
                                let objects_ser = objects_df.column(OBJECT_COL_NAME).unwrap();
                                for u in objects_ser.u32().unwrap().iter() {
                                    uset.insert(u.unwrap());
                                }
                            }
                        }
                    }
                }
            }
            // Adding the predicates
            let pred_us = self.global_cats.read()?.maybe_encode_iri_slice(&predicates);
            us.entry(BaseRDFNodeType::IRI).or_insert_with(HashSet::new);
            // if !us.contains_key(&BaseRDFNodeType::IRI) {
            //     us.insert(BaseRDFNodeType::IRI, HashSet::new());
            // }
            let us_iris = us.get_mut(&BaseRDFNodeType::IRI).unwrap();
            for up in pred_us.into_iter().flatten() {
                us_iris.insert(up);
            }
            // Creating the cats image
            let c = self.global_cats.read()?;
            let cats_image = c.image(&us, None);

            let new_graph_triples = self.graph_triples_map.remove_graph(graph).unwrap();

            let new_graph_name = if preserve_name {
                graph.clone()
            } else {
                NamedGraph::DefaultGraph
            };

            let new_fts_index = self.fts_index.remove(graph);
            let mut new_fts_index_map = HashMap::new();
            if let Some(new_fts_index) = new_fts_index {
                new_fts_index_map.insert(new_graph_name.clone(), new_fts_index);
            }

            if matches!(graph, NamedGraph::DefaultGraph) {
                self.graph_triples_map
                    .insert_graph(NamedGraph::default(), HashMap::new());
            }

            let query_cache = QueryCache::new();
            let mut detached = Triplestore {
                storage_folder: self.storage_folder.clone(),
                storage_mode: self.storage_mode,
                triples_folder: self.triples_folder.clone(),
                graph_triples_map: GraphTriplesMap::new(
                    HashMap::from_iter([(new_graph_name.clone(), new_graph_triples)]),
                    query_cache.clone(),
                ),
                query_cache,
                parser_call: self.parser_call,
                indexing: self.indexing.clone(),
                fts_index: new_fts_index_map,
                // The new triplestore has its own cats, so it gets its own geo and vector indexes.
                geo_index: None,
                vector_index: HashMap::new(),
                global_cats: LockedCats::new(cats_image),
                n_triples_deleted: 0,
                path_graph_cache: Default::default(),
                udf_registry: Arc::new(UdfRegistry::new()),
                federation: self.federation.clone(),
            };
            if detached.indexing.geo {
                detached.geo_index =
                    maybe_create_geo_index(&detached.indexing, detached.storage_folder.as_deref())?;
                detached.rebuild_geo_index()?;
            }
            detached.rebuild_vector_indexes()?;
            self.vector_index.remove(graph);
            Ok(detached)
        } else {
            Err(TriplestoreError::GraphDoesNotExist(graph.to_string()))
        }
    }
}

impl Triplestore {
    /// Removes all triples from the graph, keeping it as an empty graph.
    pub fn clear_graph(&mut self, graph: &NamedGraph) -> Result<(), TriplestoreError> {
        let existed = self.graph_triples_map.contains_key(graph);
        self.truncate(graph);
        if existed || graph == &NamedGraph::DefaultGraph {
            self.graph_triples_map.insert_graph(graph.clone(), HashMap::new());
        }
        Ok(())
    }

    /// Removes all triples with the predicate from the graph, returning how many were removed.
    pub fn drop_predicate(&mut self, graph: &NamedGraph, predicate: &NamedNode) -> usize {
        self.graph_triples_map
            .remove_predicate(graph, predicate)
            .map(|m| m.values().map(|t| t.get_height()).sum())
            .unwrap_or(0)
    }

    /// Forgets a graph that was only needed temporarily (e.g. while writing a file).
    pub fn remove_temporary_graph(&mut self, graph: &NamedGraph) {
        self.truncate(graph);
    }

    /// Removes the graph and its triples. With a storage folder, the categorical maps stay as
    /// they are (other graphs may use the same terms), and in disk storage mode the parquet files
    /// of the graph's tables are removed when the last reference to them is dropped (see
    /// `TriplesOnDisk`), so a clone of the triplestore that still holds the tables keeps them.
    pub fn truncate(&mut self, graph: &NamedGraph) {
        if let Some(m) = self.graph_triples_map.remove_graph(graph) {
            let removed: usize = m
                .values()
                .flat_map(|m| m.values())
                .map(|t| t.get_height())
                .sum();
            self.n_triples_deleted = self.n_triples_deleted.saturating_add(removed);
        }
        self.fts_index.remove(graph);
        self.vector_index.remove(graph);
    }

    /// Removes the graphs, their triples and their full text and vector indexes. Graphs that do
    /// not exist are ignored. In disk storage mode the files of the graphs' triple tables are
    /// deleted, see `truncate`.
    pub fn drop_graphs(&mut self, graphs: &[NamedGraph]) -> Result<(), TriplestoreError> {
        for graph in graphs {
            self.truncate(graph);
        }
        Ok(())
    }

    /// The number of triples in the graph, 0 if it does not exist.
    pub fn graph_height(&self, graph: &NamedGraph) -> usize {
        self.graph_triples_map
            .get(graph)
            .map(|m| {
                m.values()
                    .flat_map(|m| m.values())
                    .map(|t| t.get_height())
                    .sum()
            })
            .unwrap_or(0)
    }
}

/// Where the triplestore keeps its data.
/// - `Memory`: categorical maps and triple tables in memory.
/// - `Hybrid`: categorical maps on disk, triple tables in memory.
/// - `Disk`: categorical maps on disk, triple tables on disk as parquet files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageMode {
    #[default]
    Memory,
    Hybrid,
    Disk,
}

impl StorageMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageMode::Memory => "memory",
            StorageMode::Hybrid => "hybrid",
            StorageMode::Disk => "disk",
        }
    }

    pub fn uses_storage_folder(&self) -> bool {
        !matches!(self, StorageMode::Memory)
    }
}

impl std::str::FromStr for StorageMode {
    type Err = TriplestoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(StorageMode::Memory),
            "hybrid" => Ok(StorageMode::Hybrid),
            "disk" => Ok(StorageMode::Disk),
            _ => Err(TriplestoreError::StorageFolderError(format!(
                "Unknown storage mode {s:?}, expected \"memory\", \"hybrid\" or \"disk\""
            ))),
        }
    }
}

/// Storage mode together with the folder it stores data in.
#[derive(Clone, Debug, Default)]
pub struct StorageOptions {
    pub mode: StorageMode,
    pub folder: Option<String>,
}

impl StorageOptions {
    pub fn memory() -> Self {
        Self::default()
    }

    /// Resolves the storage mode from the user's arguments. Without an explicit mode, a storage
    /// folder means hybrid storage (the behaviour before storage modes existed) and no folder
    /// means in-memory storage.
    pub fn new(
        mode: Option<StorageMode>,
        folder: Option<String>,
    ) -> Result<Self, TriplestoreError> {
        let mode = mode.unwrap_or(if folder.is_some() {
            StorageMode::Hybrid
        } else {
            StorageMode::Memory
        });
        match (mode, &folder) {
            (StorageMode::Memory, Some(_)) => Err(TriplestoreError::StorageFolderError(
                "A storage folder cannot be used with storage=\"memory\"".to_string(),
            )),
            (StorageMode::Hybrid | StorageMode::Disk, None) => {
                Err(TriplestoreError::StorageFolderError(format!(
                    "storage=\"{}\" requires a storage folder",
                    mode.as_str()
                )))
            }
            _ => Ok(Self { mode, folder }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexingOptions {
    pub object_sort_all: bool,
    pub object_sort_some: Option<HashSet<NamedNode>>,
    #[serde(skip)]
    pub fts: bool,
    #[serde(skip)]
    pub fts_path: Option<PathBuf>,
    /// Only index string literals of these predicates for full text search. All when `None`.
    #[serde(skip)]
    pub fts_predicates: Option<Vec<NamedNode>>,
    /// Also index the local names of subject IRIs for full text search.
    #[serde(skip)]
    pub fts_local_names: bool,
    /// Also index IRIs in the object position for full text search, by the whole IRI.
    #[serde(skip)]
    pub fts_iris: bool,
    /// Index geometry literals for GeoSPARQL. Kept when serializing, and the index is rebuilt
    /// when the triplestore is read back.
    #[serde(default)]
    pub geo: bool,
    /// Index vector literals for vector search. Kept when serializing, and the index is rebuilt
    /// when the triplestore is read back.
    #[serde(default)]
    pub vectors: bool,
    /// Also build an approximate nearest neighbour index for vector search.
    #[serde(default)]
    pub vector_ann: bool,
}

impl IndexingOptions {
    pub fn new(
        object_sort_all: bool,
        object_sort_some: Option<HashSet<NamedNode>>,
        mut fts: bool,
        fts_path: Option<PathBuf>,
    ) -> Self {
        fts = fts || fts_path.is_some();
        Self {
            object_sort_all,
            object_sort_some,
            fts,
            fts_path,
            fts_predicates: None,
            fts_local_names: false,
            fts_iris: false,
            geo: false,
            vectors: false,
            vector_ann: false,
        }
    }

    /// Restricts the full text search index to some predicates, and/or adds subject IRI local
    /// names or object IRIs to it. Any of these enables full text search.
    pub fn with_fts_options(
        mut self,
        fts_predicates: Option<Vec<NamedNode>>,
        fts_local_names: bool,
        fts_iris: bool,
    ) -> Self {
        self.fts = self.fts || fts_predicates.is_some() || fts_local_names || fts_iris;
        self.fts_predicates = fts_predicates;
        self.fts_local_names = fts_local_names;
        self.fts_iris = fts_iris;
        self
    }

    pub fn fts_options(&self) -> FtsOptions {
        FtsOptions {
            predicates: self.fts_predicates.clone(),
            local_names: self.fts_local_names,
            iris: self.fts_iris,
        }
    }

    pub fn with_geo(mut self, geo: bool) -> Self {
        self.geo = geo;
        self
    }

    /// Enables vector indexing, optionally with an approximate nearest neighbour index.
    pub fn with_vectors(mut self, vectors: bool, vector_ann: bool) -> Self {
        self.vectors = vectors || vector_ann;
        self.vector_ann = vector_ann;
        self
    }

    pub fn new_default_object_sort(fts: bool, fts_path: Option<PathBuf>) -> IndexingOptions {
        IndexingOptions::new(
            false,
            Some(HashSet::from([
                rdfs::LABEL.into_owned(),
                rdf::TYPE.into_owned(),
            ])),
            fts,
            fts_path,
        )
    }

    pub fn set_fts_path(&mut self, fts_path: Option<PathBuf>) {
        self.fts_path = fts_path;
    }

    pub fn default_subject_object_index() -> bool {
        true
    }
}

impl Default for IndexingOptions {
    fn default() -> Self {
        IndexingOptions::new_default_object_sort(false, None)
    }
}

#[derive(Debug)]
pub struct TriplesToAdd {
    pub df: DataFrame,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub predicate: Option<NamedNode>,
    pub graph: NamedGraph,
    pub subject_cat_state: BaseCatState,
    pub object_cat_state: BaseCatState,
    pub predicate_cat_state: Option<BaseCatState>,
}

#[derive(Debug, Clone)]
pub struct NewTriples {
    pub df: Option<DataFrame>,
    pub graph: NamedGraph,
    pub predicate: NamedNode,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
}

impl NewTriples {
    pub fn to_eager_solution_mappings(self) -> Option<EagerSolutionMappings> {
        if let Some(df) = self.df {
            let mut map = HashMap::new();
            map.insert(
                SUBJECT_COL_NAME.to_string(),
                RDFNodeState::from_bases(
                    self.subject_type.clone(),
                    self.subject_type.default_stored_cat_state(),
                ),
            );
            map.insert(
                OBJECT_COL_NAME.to_string(),
                RDFNodeState::from_bases(
                    self.object_type.clone(),
                    self.object_type.default_stored_cat_state(),
                ),
            );
            Some(EagerSolutionMappings::new(df, map))
        } else {
            None
        }
    }
}

/// Merges new triples per predicate, so that a predicate with several subject or object types
/// gives one solution mapping, with multi-type columns where needed. Tables without triples are
/// left out, unless no table of the predicate has triples. Predicates are in the order in which
/// they first occur.
pub fn new_triples_by_predicate(
    new_triples: Vec<NewTriples>,
    global_cats: LockedCats,
) -> Result<Vec<(NamedNode, EagerSolutionMappings)>, TriplestoreError> {
    let mut by_predicate: HashMap<NamedNode, Vec<EagerSolutionMappings>> = HashMap::new();
    let mut order = vec![];
    for nt in new_triples {
        let p = nt.predicate.clone();
        if let Some(sm) = nt.to_eager_solution_mappings() {
            by_predicate
                .entry(p.clone())
                .or_insert_with(|| {
                    order.push(p);
                    vec![]
                })
                .push(sm);
        }
    }
    let mut out = Vec::with_capacity(order.len());
    for p in order {
        let mut sms = by_predicate.remove(&p).unwrap();
        let non_empty: Vec<_> = sms
            .iter()
            .enumerate()
            .filter(|(_, sm)| sm.mappings.height() > 0)
            .map(|(i, _)| i)
            .collect();
        let sm = match non_empty.len() {
            0 => sms.swap_remove(0),
            1 => sms.swap_remove(non_empty[0]),
            _ => {
                let lazy: Vec<_> = sms
                    .into_iter()
                    .filter(|sm| sm.mappings.height() > 0)
                    .map(|sm| sm.as_lazy())
                    .collect();
                union(lazy, true, global_cats.clone())
                    .map_err(|x| TriplestoreError::SparqlQueryError(x.to_string()))?
                    .as_eager()
            }
        };
        out.push((p, sm));
    }
    Ok(out)
}

impl Triplestore {
    #[instrument(skip_all)]
    pub fn new(
        storage: StorageOptions,
        indexing: Option<IndexingOptions>,
    ) -> Result<Triplestore, TriplestoreError> {
        let (pathbuf, triples_folder) = prepare_storage(&storage)?;
        let indexing = indexing.unwrap_or_default();
        let fts_index = maybe_create_fts_index_map(&indexing)?;
        let geo_index = maybe_create_geo_index(&indexing, pathbuf.as_deref())?;

        let locked_cats = LockedCats::new_empty(pathbuf.as_ref().map(|x| x.as_ref()));
        let query_cache = QueryCache::new();
        Ok(Triplestore {
            graph_triples_map: GraphTriplesMap::new(
                HashMap::from([(NamedGraph::DefaultGraph, Default::default())]),
                query_cache.clone(),
            ),
            query_cache,
            storage_folder: pathbuf,
            storage_mode: storage.mode,
            triples_folder,
            parser_call: 0,
            indexing,
            fts_index,
            geo_index,
            vector_index: HashMap::new(),
            global_cats: locked_cats,
            n_triples_deleted: 0,
            path_graph_cache: Default::default(),
            udf_registry: Arc::new(UdfRegistry::new()),
            federation: Arc::new(FederationSettings::default()),
        })
    }

    #[instrument(skip_all)]
    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        for graph in self.graph_triples_map.keys() {
            if indexing.fts {
                // Only doing anything if the fts index does not already exist.
                // If it exists, then it should be updated as well.
                if !self.fts_index.contains_key(graph) {
                    let index = FtsIndex::new(
                        indexing.fts_path.as_ref().map(|x| x.as_ref()),
                        indexing.fts_options(),
                    )
                    .map_err(TriplestoreError::FtsError)?;
                    self.fts_index.insert(graph.clone(), index);
                    for (predicate, map) in self.graph_triples_map.get(graph).unwrap() {
                        for ((subject_type, object_type), ts) in map {
                            for (lf, _) in ts.get_lazy_frames(&None, &None)? {
                                self.fts_index
                                    .get_mut(graph)
                                    .unwrap()
                                    .add_literal_string(
                                        &lf.collect().unwrap(),
                                        predicate,
                                        subject_type,
                                        &subject_type.default_stored_cat_state(),
                                        object_type,
                                        &object_type.default_stored_cat_state(),
                                        self.global_cats.clone(),
                                    )
                                    .map_err(TriplestoreError::FtsError)?;
                            }
                        }
                    }
                    self.fts_index
                        .get_mut(graph)
                        .unwrap()
                        .commit(true)
                        .map_err(TriplestoreError::FtsError)?;
                }
            }
        }
        if indexing.geo && self.geo_index.is_none() {
            self.geo_index = maybe_create_geo_index(&indexing, self.storage_folder.as_deref())?;
            self.rebuild_geo_index()?;
        }
        let rebuild_vectors = (indexing.vectors && !self.indexing.vectors)
            || (indexing.vector_ann && !self.indexing.vector_ann);
        // Indexes that exist are kept when the new options do not mention them.
        let mut indexing = indexing;
        indexing.vectors |= self.indexing.vectors;
        indexing.vector_ann |= self.indexing.vector_ann;
        if self.indexing != indexing {
            self.query_cache.versions.record(Touched::All);
        }
        // Object indexes are otherwise only created when triples are added, so the triples
        // already stored are sorted by object (or their object sort dropped) here.
        let global_cats = self.global_cats.clone();
        let cats = global_cats.read()?;
        for map in self.graph_triples_map.tables_for_indexing().values_mut() {
            for (predicate, map) in map.iter_mut() {
                for ((_, object_type), triples) in map.iter_mut() {
                    let enabled = storage::can_and_should_index_object(
                        object_type,
                        predicate,
                        &indexing,
                    );
                    triples.set_object_indexing(enabled, &cats)?;
                }
            }
        }
        drop(cats);
        self.indexing = indexing;
        if rebuild_vectors {
            self.rebuild_vector_indexes()?;
        }
        Ok(())
    }

    /// Adds an empty graph, with an empty full text search index if FTS indexing is enabled,
    /// so that triples added to a new named graph are indexed like the default graph.
    fn add_graph_if_not_exists(&mut self, graph: &NamedGraph) -> Result<(), TriplestoreError> {
        self.graph_triples_map.ensure_graph(graph);
        if self.indexing.fts && !self.fts_index.contains_key(graph) {
            let index = FtsIndex::new(
                self.indexing.fts_path.as_ref().map(|x| x.as_ref()),
                self.indexing.fts_options(),
            )
            .map_err(TriplestoreError::FtsError)?;
            self.fts_index.insert(graph.clone(), index);
        }
        Ok(())
    }
}

fn maybe_create_fts_index_map(
    indexing: &IndexingOptions,
) -> Result<HashMap<NamedGraph, FtsIndex>, TriplestoreError> {
    let fts_index = if indexing.fts {
        Some(
            FtsIndex::new(
                indexing.fts_path.as_ref().map(|x| x.as_ref()),
                indexing.fts_options(),
            )
            .map_err(TriplestoreError::FtsError)?,
        )
    } else {
        None
    };
    let fts_index_map = if let Some(fts_index) = fts_index {
        HashMap::from([(NamedGraph::DefaultGraph, fts_index)])
    } else {
        HashMap::new()
    };
    Ok(fts_index_map)
}

pub(crate) fn maybe_create_geo_index(
    indexing: &IndexingOptions,
    storage_folder: Option<&Path>,
) -> Result<Option<GeoIndex>, TriplestoreError> {
    if indexing.geo {
        Ok(Some(
            GeoIndex::new(storage_folder).map_err(TriplestoreError::GeoError)?,
        ))
    } else {
        Ok(None)
    }
}

const TRIPLES_FOLDER: &str = "triples";

/// Resets the storage folder and returns it, along with the folder for triple tables in disk mode.
fn prepare_storage(
    storage: &StorageOptions,
) -> Result<(Option<PathBuf>, Option<PathBuf>), TriplestoreError> {
    let storage_folder = maybe_delete_and_recreate_storage_folder(storage.folder.as_ref())?;
    let triples_folder = if storage.mode == StorageMode::Disk {
        let mut p = storage_folder
            .clone()
            .expect("Disk storage always has a storage folder");
        p.push(TRIPLES_FOLDER);
        create_folder_if_not_exists(p.as_path()).map_err(TriplestoreError::FileIOError)?;
        Some(p)
    } else {
        None
    };
    Ok((storage_folder, triples_folder))
}

fn maybe_delete_and_recreate_storage_folder(
    storage_folder: Option<&String>,
) -> Result<Option<PathBuf>, TriplestoreError> {
    Ok(if let Some(storage_folder) = storage_folder {
        let mut pathbuf = Path::new(storage_folder).to_path_buf();
        create_folder_if_not_exists(pathbuf.as_path()).map_err(TriplestoreError::FileIOError)?;

        if pathbuf.exists() {
            for f in std::fs::read_dir(&pathbuf).map_err(|x| {
                TriplestoreError::StorageFolderError(format!(
                    "Error reading existing maplib storage folder {}",
                    x
                ))
            })? {
                let f = f.map_err(|x| {
                    TriplestoreError::StorageFolderError(format!(
                        "Error finding existing maplib storage folder {}",
                        x
                    ))
                })?;
                let p = f.path();
                if p.is_dir() && matches!(f.file_name().to_str().unwrap(), MAPLIB_STORAGE_FOLDER) {
                    std::fs::remove_dir_all(f.path()).map_err(|x| {
                        TriplestoreError::StorageFolderError(format!(
                            "Error removing maplib storage folder {}",
                            x
                        ))
                    })?;
                }
            }
        }
        pathbuf.push(MAPLIB_STORAGE_FOLDER);
        create_folder_if_not_exists(pathbuf.as_path()).map_err(TriplestoreError::FileIOError)?;
        Some(pathbuf)
    } else {
        None
    })
}

struct TriplesToAddPartitionedPredicate {
    pub df: DataFrame,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub predicate: NamedNode,
    pub graph: NamedGraph,
    pub subject_cat_state: BaseCatState,
    pub object_cat_state: BaseCatState,
}

/// Splits the triples to add into one data frame per predicate.
fn partition_triples_to_add(
    mut ts: Vec<TriplesToAdd>,
    global_cats: &LockedCats,
) -> Vec<TriplesToAddPartitionedPredicate> {
    // Collects in memory, see IN_MEMORY_COLLECT_LOOPS.
    ts.par_drain(..)
        .map(|t| {
            let TriplesToAdd {
                df,
                subject_type,
                object_type,
                predicate,
                graph,
                subject_cat_state,
                predicate_cat_state,
                object_cat_state,
            } = t;
            let df_preds = {
                let cats = global_cats.read().unwrap();
                partition_unpartitioned_predicate(
                    df,
                    &subject_type,
                    &subject_cat_state,
                    &object_type,
                    &object_cat_state,
                    predicate,
                    predicate_cat_state.as_ref(),
                    &cats,
                )
            };
            let all_partitioned: Vec<_> = df_preds
                .into_iter()
                .map(|(df, predicate)| TriplesToAddPartitionedPredicate {
                    df,
                    subject_type: subject_type.clone(),
                    object_type: object_type.clone(),
                    predicate,
                    graph: graph.clone(),
                    subject_cat_state: subject_cat_state.clone(),
                    object_cat_state: object_cat_state.clone(),
                })
                .collect();
            all_partitioned
        })
        .flatten()
        .collect()
}

#[instrument(skip_all)]
pub fn prepare_add_triples_par(
    ts: Vec<TriplesToAdd>,
    global_cats: LockedCats,
) -> Result<Vec<CatTriples>, TriplestoreError> {
    let all_partitioned = partition_triples_to_add(ts, &global_cats);
    let all_partitioned: Vec<_> = all_partitioned
        .into_par_iter()
        .map(
            |TriplesToAddPartitionedPredicate {
                 df,
                 subject_type,
                 object_type,
                 predicate,
                 graph,
                 subject_cat_state,
                 object_cat_state,
             }| {
                let cats = global_cats.read().unwrap();
                cat_encode_triples(
                    df,
                    subject_type,
                    object_type,
                    predicate,
                    graph,
                    subject_cat_state,
                    object_cat_state,
                    &cats,
                )
            },
        )
        .collect();
    Ok(all_partitioned)
}

/// Whether the triples can be encoded directly with global codes: the dictionaries are in
/// memory and no column comes with a local dictionary of its own.
pub(crate) fn can_encode_globally(ts: &[TriplesToAdd], global_cats: &LockedCats) -> bool {
    let no_locals = |s: &BaseCatState| !matches!(s, BaseCatState::CategoricalNative(Some(_)));
    global_cats.read().unwrap().is_in_memory()
        && ts.iter().all(|t| {
            no_locals(&t.subject_cat_state)
                && no_locals(&t.object_cat_state)
                && t.predicate_cat_state.as_ref().is_none_or(no_locals)
        })
}

/// Like `prepare_add_triples_par`, but encodes the string columns directly with global codes
/// (see `Cats::encode_strings_global`), so the result has no local dictionaries. Requires
/// `can_encode_globally`.
#[instrument(skip_all)]
pub(crate) fn prepare_add_triples_global(
    ts: Vec<TriplesToAdd>,
    global_cats: LockedCats,
) -> Result<Vec<CatTriples>, TriplestoreError> {
    let mut parts = partition_triples_to_add(ts, &global_cats);
    // The string columns (or lang string fields) to encode: (part, column, field, length if
    // the column is a constant).
    let mut targets: Vec<(usize, &'static str, Option<&'static str>, Option<usize>)> = vec![];
    let mut to_encode: Vec<(BaseRDFNodeType, Series)> = vec![];
    for (i, p) in parts.iter().enumerate() {
        for (c, t, state) in [
            (SUBJECT_COL_NAME, &p.subject_type, &p.subject_cat_state),
            (OBJECT_COL_NAME, &p.object_type, &p.object_cat_state),
        ] {
            if !(t.stored_cat() && matches!(state, BaseCatState::String)) {
                continue;
            }
            let column = p.df.column(c).unwrap();
            if t.is_lang_string() {
                let st = column.struct_().unwrap();
                for f in [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD] {
                    to_encode.push((t.clone(), st.field_by_name(f).unwrap()));
                    targets.push((i, c, Some(f), None));
                }
            } else if let Column::Scalar(sc) = column {
                // A constant (e.g. from a template) is encoded once.
                to_encode.push((t.clone(), sc.as_single_value_series()));
                targets.push((i, c, None, Some(sc.len())));
            } else {
                to_encode.push((t.clone(), column.as_materialized_series().clone()));
                targets.push((i, c, None, None));
            }
        }
    }
    let encoded = global_cats.write()?.encode_strings_global(to_encode);
    let mut encoded = targets.into_iter().zip(encoded).peekable();
    while let Some(((i, c, field, scalar_len), mut s)) = encoded.next() {
        let df = &mut parts[i].df;
        if field.is_some() {
            // The value field is followed by the lang field of the same column.
            let (_, mut lang) = encoded.next().unwrap();
            s.rename(LANG_STRING_VALUE_FIELD.into());
            lang.rename(LANG_STRING_LANG_FIELD.into());
            let st = StructChunked::from_series(c.into(), s.len(), [s, lang].iter())
                .map_err(TriplestoreError::LazyLoadError)?;
            df.with_column(st.into_series().into_column()).unwrap();
        } else if let Some(len) = scalar_len {
            s.rename(c.into());
            df.with_column(Column::Scalar(ScalarColumn::from_single_value_series(
                s, len,
            )))
            .unwrap();
        } else {
            s.rename(c.into());
            df.with_column(s.into_column()).unwrap();
        }
    }
    Ok(parts
        .into_iter()
        .map(|p| {
            let cat_type =
                |t: &BaseRDFNodeType| t.stored_cat().then(|| CatType::from_base_rdf_node_type(t));
            CatTriples {
                encoded_triples: EncodedTriples {
                    df: p.df,
                    subject: cat_type(&p.subject_type),
                    subject_local_cat_uuid: None,
                    object: cat_type(&p.object_type),
                    object_local_cat_uuid: None,
                },
                predicate: p.predicate,
                graph: p.graph,
                subject_type: p.subject_type,
                object_type: p.object_type,
                local_cats: vec![],
            }
        })
        .collect())
}

pub fn add_rank_sort_triples(
    encoded: EncodedTriples,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Result<EncodedTriples, TriplestoreError> {
    // Always sort S,O and deduplicate
    let EncodedTriples {
        df,
        subject,
        subject_local_cat_uuid,
        object,
        object_local_cat_uuid,
    } = encoded;
    assert!(subject_local_cat_uuid.is_none());
    assert!(object_local_cat_uuid.is_none());

    let subj_rank_expr = create_rank_expr(&df, SUBJECT_COL_NAME, subj_type, global_cats.clone())?
        .alias(SUBJECT_RANK_COL_NAME);
    let obj_rank_expr = create_rank_expr(&df, OBJECT_COL_NAME, obj_type, global_cats.clone())?
        .alias(OBJECT_RANK_COL_NAME);

    let mut lf = df.lazy();
    lf = lf.with_column(subj_rank_expr);
    lf = lf.with_column(obj_rank_expr);

    lf = lf.sort_by_exprs(
        vec![col(SUBJECT_RANK_COL_NAME), col(OBJECT_RANK_COL_NAME)],
        SortMultipleOptions {
            descending: vec![false, false],
            nulls_last: vec![false, false],
            multithreaded: true,
            maintain_order: false,
            limit: None,
        },
    );

    lf = lf.filter(
        repeated_from_last_row_expr(SUBJECT_COL_NAME)
            .and(repeated_from_last_row_expr(OBJECT_COL_NAME))
            .not(),
    );
    let df = collect_in_memory(lf).unwrap();
    Ok(EncodedTriples {
        df,
        subject,
        subject_local_cat_uuid,
        object,
        object_local_cat_uuid,
    })
}

fn create_rank_expr(
    df: &DataFrame,
    c: &str,
    t: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Result<Expr, TriplestoreError> {
    let rank_expr = if t.stored_cat() {
        let mut codes = Vec::new();
        let column = df.column(c).unwrap();
        if t.is_lang_string() {
            let st = column.struct_().unwrap();
            for f in [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD] {
                extend_codes(&mut codes, &st.field_by_name(f).unwrap().into_column());
            }
        } else {
            extend_codes(&mut codes, column);
        }
        let codes = distinct_codes(codes);
        let rank_map = Arc::new(global_cats.read()?.sort_ranks(&codes, t));
        if t.is_lang_string() {
            let mut e = col(c).map(
                move |x| {
                    let mut c_values = codes_to_ranks(
                        x.struct_()?.field_by_name(LANG_STRING_VALUE_FIELD)?.u32()?,
                        &rank_map,
                    )
                    .into_series();
                    c_values.rename(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD));
                    let mut c_langs = codes_to_ranks(
                        x.struct_()?.field_by_name(LANG_STRING_LANG_FIELD)?.u32()?,
                        &rank_map,
                    )
                    .into_series();
                    c_langs.rename(PlSmallStr::from_str(LANG_STRING_LANG_FIELD));
                    let mut df = DataFrame::new(
                        x.len(),
                        vec![c_values.into_column(), c_langs.into_column()],
                    )?;
                    df = df
                        .lazy()
                        .with_column(
                            as_struct(vec![
                                col(LANG_STRING_VALUE_FIELD),
                                col(LANG_STRING_LANG_FIELD),
                            ])
                            .alias(x.name().as_str()),
                        )
                        .select([col(x.name().as_str())])
                        .collect()
                        .unwrap();
                    let c = df.drop_in_place(x.name().as_str())?;
                    Ok(c)
                },
                |_, f| Ok(f.clone()),
            );
            e = e.rank(
                RankOptions {
                    method: RankMethod::Min,
                    descending: false,
                },
                None,
            );
            e
        } else {
            col(c).map(
                move |x| Ok(codes_to_ranks(x.u32()?, &rank_map).into_column()),
                |_, f| Ok(f.clone()),
            )
        }
    } else {
        col(c).rank(
            RankOptions {
                method: RankMethod::Min,
                descending: false,
            },
            None,
        )
    };
    Ok(rank_expr)
}

#[allow(clippy::too_many_arguments)]
pub fn partition_unpartitioned_predicate(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    subject_state: &BaseCatState,
    object_type: &BaseRDFNodeType,
    object_state: &BaseCatState,
    predicate: Option<NamedNode>,
    predicate_cat_state: Option<&BaseCatState>,
    global_cats: &Cats,
) -> Vec<(DataFrame, NamedNode)> {
    if df.height() == 0 {
        return vec![];
    }
    let mut out_df_vec = vec![];

    let map = HashMap::from([
        (
            SUBJECT_COL_NAME.to_string(),
            RDFNodeState::from_bases(subject_type.clone(), subject_state.clone()),
        ),
        (
            OBJECT_COL_NAME.to_string(),
            RDFNodeState::from_bases(object_type.clone(), object_state.clone()),
        ),
    ]);

    let mut lf = df.lazy();
    // Important to remove null structs such as lang strings
    for (c, t) in &map {
        lf = lf.with_column(set_struct_all_null_to_null_row(col(c), t).alias(c));
    }
    let mut df = collect_in_memory(lf).unwrap();
    if let Some(predicate) = predicate {
        df = df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
        if let Some(df) = drop_nulls(df) {
            out_df_vec.push((df, predicate));
        }
    } else {
        let partitions = df.partition_by([PREDICATE_COL_NAME], true).unwrap();
        for mut part in partitions {
            let predicate;
            {
                let any_predicate = part.column(PREDICATE_COL_NAME).unwrap().get(0).unwrap();
                if let AnyValue::String(p) = any_predicate {
                    predicate = literal_iri_to_namednode(p);
                } else if let AnyValue::StringOwned(s) = any_predicate {
                    predicate = literal_iri_to_namednode(s.as_str());
                } else if let AnyValue::UInt32(u) = any_predicate {
                    let cat_state = predicate_cat_state.unwrap();
                    predicate = global_cats.decode_iri_u32(&u, cat_state.get_local_cats())
                } else {
                    panic!("Predicate: {any_predicate:?}");
                }
            }
            part = part.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
            if let Some(part) = drop_nulls(part) {
                out_df_vec.push((part, predicate));
            }
        }
    }
    out_df_vec
}

fn drop_nulls(mut df: DataFrame) -> Option<DataFrame> {
    df = df.drop_nulls::<String>(None).unwrap();
    if df.height() == 0 {
        None
    } else {
        Some(df)
    }
}

pub(crate) fn repeated_from_last_row_expr(c: &str) -> Expr {
    col(c)
        .shift(lit(1))
        .is_not_null()
        .and(col(c).shift(lit(1)).eq(col(c)))
}
