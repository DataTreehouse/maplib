extern crate core;

pub mod cats;
mod dblf;
pub mod errors;
mod map_df;
mod map_json;
mod map_xml;
pub mod native_parquet_write;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod serialization;
pub mod sparql;
pub mod storage;
pub mod triples_read;
pub mod triples_write;

use crate::errors::TriplestoreError;
use crate::storage::Triples;
use file_io::create_folder_if_not_exists;
use fts::FtsIndex;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{
    as_struct, col, lit, AnyValue, DataFrame, Expr, IntoLazy, PlSmallStr, RankMethod, RankOptions,
};
use polars_core::prelude::{IntoColumn, Series, SortMultipleOptions};
use pyo3::{Py, PyAny};
use range_set_blaze::RangeSetBlaze;
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use representation::cats::{
    cat_encode_triples, CatTriples, CatType, Cats, EncodedTriples, LockedCats,
    OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME,
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
use std::time::Instant;

use query_processing::udf::UdfRegistry;
use representation::cats::maps::CatMaps;
use representation::dataset::NamedGraph;
use tracing::{debug, instrument, trace};

#[derive(Clone)]
pub struct Triplestore {
    pub storage_folder: Option<PathBuf>,
    graph_triples_map: HashMap<
        NamedGraph,
        HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    >,
    graph_transient_triples_map: HashMap<
        NamedGraph,
        HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    >,
    parser_call: usize,
    indexing: IndexingOptions,
    fts_index: HashMap<NamedGraph, FtsIndex>,
    pub global_cats: LockedCats,
    n_triples_deleted: usize,
    pub udf_registry: Arc<UdfRegistry>,
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
        reg.add_udf(iri, f, output_type, input_types)
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
        for (g, m) in self
            .graph_triples_map
            .iter()
            .chain(self.graph_transient_triples_map.iter())
        {
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
                for g in self
                    .graph_triples_map
                    .values()
                    .chain(self.graph_transient_triples_map.values())
                {
                    for v in g.values() {
                        for ((st, ot), triples) in v {
                            if st == &t {
                                let lfs = triples.get_lazy_frames(&None, &None)?;
                                for (lf, _) in lfs {
                                    let df = lf.select([col(SUBJECT_COL_NAME)]).collect().unwrap();
                                    let s_col = df.column(SUBJECT_COL_NAME).unwrap();
                                    let s_rs = RangeSetBlaze::from_iter(
                                        s_col.u32().unwrap().iter().map(|x| x.unwrap()),
                                    );
                                    rs = rs - s_rs;
                                }
                            }
                            if ot == &t {
                                let lfs = triples.get_lazy_frames(&None, &None)?;
                                for (lf, _) in lfs {
                                    let df = lf.select([col(OBJECT_COL_NAME)]).collect().unwrap();
                                    let s_col = df.column(OBJECT_COL_NAME).unwrap();
                                    let s_rs = RangeSetBlaze::from_iter(
                                        s_col.u32().unwrap().iter().map(|x| x.unwrap()),
                                    );
                                    rs = rs - s_rs;
                                }
                            }
                        }
                    }
                }
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
        for ((graph, map), transient) in self
            .graph_triples_map
            .drain()
            .map(|x| (x, false))
            .chain(self.graph_transient_triples_map.drain().map(|x| (x, true)))
        {
            for (pred, map) in map {
                for ((subject_type, object_type), triples) in map {
                    to_compact.push((
                        (
                            transient,
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
        let data_and_triples: Result<Vec<_>, TriplestoreError> = compacted
            .into_par_iter()
            .map(
                |(
                    (transient, graph, pred, subject_type, object_type),
                    object_indexing_enabled,
                    st,
                )| {
                    let triples = Triples::new_compacted(
                        st.get_lazy_frame()?.collect().unwrap(),
                        subject_type.clone(),
                        object_type.clone(),
                        object_indexing_enabled,
                        self.global_cats.clone(),
                    )?;
                    Ok((transient, graph, pred, subject_type, object_type, triples))
                },
            )
            .collect();

        for (transient, graph, pred, subject_type, object_type, triples) in data_and_triples? {
            let use_graph_map = if transient {
                &mut self.graph_transient_triples_map
            } else {
                &mut self.graph_triples_map
            };
            if !use_graph_map.contains_key(&graph) {
                use_graph_map.insert(graph.clone(), HashMap::new());
            }
            let pred_map = use_graph_map.get_mut(&graph).unwrap();
            if !pred_map.contains_key(&pred) {
                pred_map.insert(pred.clone(), HashMap::new());
            }
            let triples_map = pred_map.get_mut(&pred).unwrap();
            triples_map.insert((subject_type, object_type), triples);
        }

        Ok(())
    }

    pub fn graph_size(&self, named_graph: &NamedGraph) -> usize {
        let mut s = 0;
        if let Some(gr) = self.graph_triples_map.get(named_graph) {
            for v in gr.values() {
                for t in v.values() {
                    s = s + t.get_height();
                }
            }
        }
        s
    }

    pub fn contains_graph(&self, graph: &NamedGraph) -> bool {
        self.graph_triples_map.contains_key(graph)
    }

    pub fn add_cat_cache(&self, graphs: &[&NamedGraph]) -> Result<(), TriplestoreError> {
        if self.storage_folder.is_some() {
            let mut type_u32s_map: HashMap<BaseRDFNodeType, HashSet<u32>> = HashMap::new();
            for gr in graphs {
                if let Some(m) = self.graph_triples_map.get(*gr) {
                    let type_u32s: Result<
                        Vec<Vec<(BaseRDFNodeType, HashSet<u32>)>>,
                        TriplestoreError,
                    > = m
                        .par_iter()
                        .map(|(_, type_map)| {
                            let out_vec: Result<Vec<Vec<_>>, TriplestoreError> = type_map
                                .iter()
                                .map(|(_, triples)| triples.get_u32s())
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
                        let uset = us.get_mut(&st).unwrap();
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
                        if !us.contains_key(&ot) {
                            us.insert(ot.clone(), HashSet::new());
                        }
                        let uset = us.get_mut(&ot).unwrap();
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
            if !us.contains_key(&BaseRDFNodeType::IRI) {
                us.insert(BaseRDFNodeType::IRI, HashSet::new());
            }
            let us_iris = us.get_mut(&BaseRDFNodeType::IRI).unwrap();
            for up in pred_us {
                if let Some(up) = up {
                    us_iris.insert(up);
                }
            }
            // Creating the cats image
            let c = self.global_cats.read()?;
            let cats_image = c.image(&us, None);

            let new_graph_triples = self.graph_triples_map.remove(graph).unwrap();
            let new_graph_transient_triples = self.graph_transient_triples_map.remove(graph);

            let new_graph_name = if preserve_name {
                graph.clone()
            } else {
                NamedGraph::DefaultGraph
            };

            let mut new_graph_transient_triples_map = HashMap::new();
            if let Some(new_graph_transient_triples) = new_graph_transient_triples {
                new_graph_transient_triples_map
                    .insert(new_graph_name.clone(), new_graph_transient_triples);
            }
            let new_fts_index = self.fts_index.remove(graph);
            let mut new_fts_index_map = HashMap::new();
            if let Some(new_fts_index) = new_fts_index {
                new_fts_index_map.insert(new_graph_name.clone(), new_fts_index);
            }

            if matches!(graph, NamedGraph::DefaultGraph) {
                self.graph_triples_map
                    .insert(NamedGraph::default(), HashMap::new());
            }

            Ok(Triplestore {
                storage_folder: self.storage_folder.clone(),
                graph_triples_map: HashMap::from_iter([(
                    new_graph_name.clone(),
                    new_graph_triples,
                )]),
                graph_transient_triples_map: new_graph_transient_triples_map,
                parser_call: self.parser_call,
                indexing: self.indexing.clone(),
                fts_index: new_fts_index_map,
                global_cats: LockedCats::new(cats_image),
                n_triples_deleted: 0,
                udf_registry: Arc::new(UdfRegistry::new()),
            })
        } else {
            Err(TriplestoreError::GraphDoesNotExist(graph.to_string()))
        }
    }
}

impl Triplestore {
    pub fn truncate(&mut self, graph: &NamedGraph) {
        if self.storage_folder.is_some() {
            todo!("Should drop this folder.. ")
        }
        self.graph_triples_map.remove(graph);
        self.graph_transient_triples_map.remove(graph);
        self.fts_index.remove(graph);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexingOptions {
    pub object_sort_all: bool,
    pub object_sort_some: Option<HashSet<NamedNode>>,
    #[serde(skip)]
    pub fts: bool,
    #[serde(skip)]
    pub fts_path: Option<PathBuf>,
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
        }
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

impl Triplestore {
    #[instrument(skip_all)]
    pub fn new(
        storage_folder: Option<String>,
        indexing: Option<IndexingOptions>,
    ) -> Result<Triplestore, TriplestoreError> {
        let pathbuf = maybe_delete_and_recreate_storage_folder(storage_folder.as_ref())?;
        let indexing = indexing.unwrap_or_default();
        let fts_index = maybe_create_fts_index_map(&indexing)?;

        let locked_cats = LockedCats::new_empty(pathbuf.as_ref().map(|x| x.as_ref()));
        Ok(Triplestore {
            graph_triples_map: HashMap::from([(NamedGraph::DefaultGraph, Default::default())]),
            graph_transient_triples_map: HashMap::from([(
                NamedGraph::DefaultGraph,
                Default::default(),
            )]),
            storage_folder: pathbuf,
            parser_call: 0,
            indexing,
            fts_index,
            global_cats: locked_cats,
            n_triples_deleted: 0,
            udf_registry: Arc::new(UdfRegistry::new()),
        })
    }

    #[instrument(skip_all)]
    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        for graph in self.graph_triples_map.keys() {
            if indexing.fts {
                // Only doing anything if the fts index does not already exist.
                // If it exists, then it should be updated as well.
                if !self.fts_index.contains_key(graph) {
                    let index = FtsIndex::new(indexing.fts_path.as_ref().map(|x| x.as_ref()))
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
                                        &subject_type,
                                        &subject_type.default_stored_cat_state(),
                                        &object_type,
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
        self.indexing = indexing;
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn add_triples_vec(
        &mut self,
        ts: Vec<TriplesToAdd>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let prepare_triples_now = Instant::now();
        let dfs_to_add = prepare_add_triples_par(ts, self.global_cats.clone())?;
        trace!(
            "Preparing triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();
        let new_triples = self.add_local_cat_triples(dfs_to_add, transient)?;
        trace!(
            "Adding triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(new_triples)
    }

    #[instrument(skip_all)]
    fn add_local_cat_triples(
        &mut self,
        local_cat_triples: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_globalize = Instant::now();
        let cat_triples = self.globalize(local_cat_triples);
        trace!(
            "Globalizing took: {} seconds",
            start_globalize.elapsed().as_secs_f32()
        );
        let start_sort_rank = Instant::now();
        let cat_triples: Result<_, TriplestoreError> = cat_triples
            .into_par_iter()
            .map(|mut t| {
                t.encoded_triples = add_rank_sort_triples(
                    t.encoded_triples,
                    &t.subject_type,
                    &t.object_type,
                    self.global_cats.clone(),
                )?;
                Ok(t)
            })
            .collect();
        let cat_triples = cat_triples?;
        trace!(
            "Sorting and adding rank took {} seconds",
            start_sort_rank.elapsed().as_secs_f32()
        );
        let start_add_global = Instant::now();
        let res = self.add_global_cat_triples(cat_triples, transient);
        trace!(
            "Add global took: {} seconds",
            start_add_global.elapsed().as_secs_f32()
        );
        res
    }

    #[instrument(skip_all)]
    fn add_global_cat_triples(
        &mut self,
        global_cat_triples: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_prep_add = Instant::now();
        let mut add_map: HashMap<_, (_, Vec<_>)> = HashMap::new();
        for CatTriples {
            encoded_triples,
            predicate,
            graph,
            subject_type,
            object_type,
            local_cats: _,
        } in global_cat_triples
        {
            self.add_graph_if_not_exists(&graph, transient);
            let use_map = if transient {
                &mut self.graph_transient_triples_map.get_mut(&graph).unwrap()
            } else {
                &mut self.graph_triples_map.get_mut(&graph).unwrap()
            };
            let mut map = HashMap::new();
            map.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.clone().into_default_stored_rdf_node_state(),
            );
            map.insert(
                OBJECT_COL_NAME.to_string(),
                object_type.clone().into_default_stored_rdf_node_state(),
            );

            let k = (subject_type.clone(), object_type.clone());
            let triples = if let Some(m) = use_map.get_mut(&predicate) {
                m.remove(&k)
            } else {
                None
            };
            let (s, o) = k;

            let k2 = (graph.clone(), predicate.clone(), s, o);

            if !add_map.contains_key(&k2) {
                add_map.insert(k2.clone(), (triples, vec![]));
            }
            let (_, v) = add_map.get_mut(&k2).unwrap();
            v.push(encoded_triples);
        }
        let cats = self.global_cats.clone();
        let mut add_vec = vec![];
        for ((graph, p, s, o), (t1, t2)) in add_map.into_iter() {
            add_vec.push((graph, p, s, o, t1, t2));
        }
        trace!(
            "Preparing for add took {}",
            start_prep_add.elapsed().as_secs_f32()
        );
        let indexing = self.indexing.clone();
        //Why does not par iter work?
        let r: Result<Vec<_>, TriplestoreError> = add_vec
            .into_iter()
            .map(move |(graph, p, s, o, triples, v)| {
                let mut new_triples_vec = vec![];
                let mut v_iter = v.into_iter();
                let mut triples = if let Some(triples) = triples {
                    triples
                } else {
                    let et = v_iter.next().unwrap();
                    let new_triples = NewTriples {
                        df: Some(
                            et.df
                                .clone()
                                .select([SUBJECT_COL_NAME, OBJECT_COL_NAME])
                                .unwrap(),
                        ),
                        graph: graph.clone(),
                        predicate: p.clone(),
                        subject_type: s.clone(),
                        object_type: o.clone(),
                    };
                    new_triples_vec.push(new_triples);
                    let triples =
                        Triples::new(et.df, s.clone(), o.clone(), &p, &indexing, cats.clone())?;
                    triples
                };
                for enc in v_iter {
                    let new_triples_opt = triples.add_triples(enc.df.clone(), cats.clone())?;
                    let new_triples = NewTriples {
                        df: new_triples_opt
                            .map(|x| x.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap()),
                        graph: graph.clone(),
                        predicate: p.clone(),
                        subject_type: s.clone(),
                        object_type: o.clone(),
                    };
                    new_triples_vec.push(new_triples);
                }
                let k = (graph, p, s, o);
                Ok(((k, triples), new_triples_vec))
            })
            .collect();
        let (to_add_map, out_new_triples): (HashMap<_, _>, Vec<_>) = r?.into_iter().unzip();

        let out_new_triples: Vec<_> = out_new_triples.into_iter().flatten().collect();
        for ((graph, p, s, o), v) in to_add_map {
            let use_map = if transient {
                self.graph_transient_triples_map.get_mut(&graph).unwrap()
            } else {
                self.graph_triples_map.get_mut(&graph).unwrap()
            };
            if !use_map.contains_key(&p) {
                use_map.insert(p.clone(), HashMap::new());
            }
            let k = (s, o);
            let m = use_map.get_mut(&p).unwrap();
            m.insert(k, v);
        }
        for nt in &out_new_triples {
            if matches!(nt.object_type, BaseRDFNodeType::Literal(..)) {
                if let Some(df) = &nt.df {
                    if let Some(fts_index) = self.fts_index.get_mut(&nt.graph) {
                        let fts_now = Instant::now();
                        fts_index
                            .add_literal_string(
                                df,
                                &nt.predicate,
                                &nt.subject_type,
                                &nt.subject_type.default_stored_cat_state(),
                                &nt.object_type,
                                &nt.object_type.default_stored_cat_state(),
                                self.global_cats.clone(),
                            )
                            .map_err(TriplestoreError::FtsError)?;
                        trace!(
                            "Adding to fts index took {} seconds",
                            fts_now.elapsed().as_secs_f32()
                        );
                        fts_index.commit(true).map_err(TriplestoreError::FtsError)?;
                    }
                }
            }
        }

        Ok(out_new_triples)
    }

    fn add_graph_if_not_exists(&mut self, graph: &NamedGraph, transient: bool) {
        if transient {
            if !self.graph_transient_triples_map.contains_key(graph) {
                self.graph_transient_triples_map
                    .insert(graph.clone(), HashMap::new());
            }
        } else {
            if !self.graph_triples_map.contains_key(graph) {
                self.graph_triples_map.insert(graph.clone(), HashMap::new());
            }
        }
    }
}

fn maybe_create_fts_index_map(
    indexing: &IndexingOptions,
) -> Result<HashMap<NamedGraph, FtsIndex>, TriplestoreError> {
    let fts_index = if indexing.fts {
        Some(
            FtsIndex::new(indexing.fts_path.as_ref().map(|x| x.as_ref()))
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

#[instrument(skip_all)]
pub fn prepare_add_triples_par(
    mut ts: Vec<TriplesToAdd>,
    global_cats: LockedCats,
) -> Result<Vec<CatTriples>, TriplestoreError> {
    let all_partitioned: Vec<TriplesToAddPartitionedPredicate> = ts
        .par_drain(..)
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
        .collect();
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
    let df = lf.collect().unwrap();
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
        let u32_set: HashSet<u32> = if t.is_lang_string() {
            let mut u32_set: HashSet<_> = df
                .column(c)
                .unwrap()
                .struct_()
                .unwrap()
                .field_by_name(LANG_STRING_VALUE_FIELD)
                .unwrap()
                .u32()
                .unwrap()
                .iter()
                .map(|x| x.unwrap())
                .collect();
            u32_set.extend(
                df.column(c)
                    .unwrap()
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap()
                    .u32()
                    .unwrap()
                    .iter()
                    .map(|x| x.unwrap()),
            );
            u32_set
        } else {
            df.column(c)
                .unwrap()
                .u32()
                .unwrap()
                .iter()
                .map(|x| x.unwrap())
                .collect()
        };
        let rank_map = global_cats.read()?.local_rank_map(&u32_set, t);
        if t.is_lang_string() {
            let mut e = col(c).map(
                move |x| {
                    let mut u32_values = Vec::with_capacity(x.len());
                    for u in x
                        .struct_()?
                        .field_by_name(LANG_STRING_VALUE_FIELD)?
                        .u32()
                        .unwrap()
                        .iter()
                    {
                        if let Some(u) = u {
                            u32_values.push(rank_map.get(&u).cloned());
                        } else {
                            u32_values.push(None)
                        }
                    }
                    let mut u32_langs = Vec::with_capacity(x.len());
                    for u in x
                        .struct_()?
                        .field_by_name(LANG_STRING_LANG_FIELD)?
                        .u32()
                        .unwrap()
                        .iter()
                    {
                        if let Some(u) = u {
                            u32_langs.push(rank_map.get(&u).cloned());
                        } else {
                            u32_langs.push(None)
                        }
                    }
                    let mut c_values = Series::from_iter(u32_values.into_iter());
                    c_values.rename(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD));
                    let mut c_langs = Series::from_iter(u32_langs.into_iter());
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
                move |x| {
                    let mut u32s = Vec::with_capacity(x.len());
                    for u in x.u32()?.iter() {
                        if let Some(u) = u {
                            u32s.push(rank_map.get(&u).cloned());
                        } else {
                            u32s.push(None)
                        }
                    }
                    let mut c = Series::from_iter(u32s.into_iter());
                    c.rename(x.name().clone());
                    Ok(c.into_column())
                },
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
    let mut df = lf.collect().unwrap();
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
