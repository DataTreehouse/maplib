extern crate core;

pub mod cats;
mod dblf;
pub mod errors;
pub mod native_parquet_write;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod sparql;
mod storage;
pub mod triples_read;
pub mod triples_write;

use crate::errors::TriplestoreError;
use crate::storage::{repeated_from_last_row_expr, Triples};
use file_io::create_folder_if_not_exists;
use fts::FtsIndex;
use log::trace;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{col, AnyValue, DataFrame, IntoLazy, RankMethod, RankOptions};
use polars_core::prelude::SortMultipleOptions;
use query_processing::type_constraints::ConstraintBaseRDFNodeType;
use rayon::iter::ParallelDrainRange;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{
    cat_encode_triples, decode_expr, literal_is_cat, CatTriples, CatType, Cats,
    OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME,
};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings};
use representation::{
    literal_iri_to_namednode, BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum StoredBaseRDFNodeType {
    IRI(NamedNode),
    Blank,
    Literal(NamedNode),
}

impl StoredBaseRDFNodeType {
    fn from_base_and_prefix(
        base: &BaseRDFNodeType,
        prefix: &Option<CatType>,
    ) -> StoredBaseRDFNodeType {
        match base {
            BaseRDFNodeType::IRI => {
                if let Some(prefix) = prefix {
                    if let CatType::Prefix(p) = prefix {
                        StoredBaseRDFNodeType::IRI(p.clone())
                    } else {
                        unreachable!("Should never happen")
                    }
                } else {
                    unreachable!("Should never happen")
                }
            }
            BaseRDFNodeType::BlankNode => StoredBaseRDFNodeType::Blank,
            BaseRDFNodeType::Literal(l) => StoredBaseRDFNodeType::Literal(l.clone()),
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        }
    }
}

impl StoredBaseRDFNodeType {
    pub(crate) fn as_constrained(&self) -> ConstraintBaseRDFNodeType {
        match self {
            StoredBaseRDFNodeType::IRI(i) => {
                ConstraintBaseRDFNodeType::IRI(Some(HashSet::from([i.clone()])))
            }
            StoredBaseRDFNodeType::Blank => ConstraintBaseRDFNodeType::BlankNode,
            StoredBaseRDFNodeType::Literal(l) => ConstraintBaseRDFNodeType::Literal(l.clone()),
        }
    }

    pub fn as_cat_type(&self) -> Option<CatType> {
        match self {
            StoredBaseRDFNodeType::IRI(nn) => Some(CatType::Prefix(nn.clone())),
            StoredBaseRDFNodeType::Blank => Some(CatType::Blank),
            StoredBaseRDFNodeType::Literal(l) => {
                if literal_is_cat(l.as_ref()) {
                    Some(CatType::Literal(l.to_owned()))
                } else {
                    None
                }
            }
        }
    }
}

impl StoredBaseRDFNodeType {
    pub fn matches(&self, other: &BaseRDFNodeType) -> bool {
        match self {
            StoredBaseRDFNodeType::IRI(_) => {
                matches!(other, BaseRDFNodeType::IRI)
            }
            StoredBaseRDFNodeType::Blank => {
                matches!(other, BaseRDFNodeType::BlankNode)
            }
            StoredBaseRDFNodeType::Literal(l) => {
                if let BaseRDFNodeType::Literal(l_other) = other {
                    l == l_other
                } else {
                    false
                }
            }
        }
    }

    pub fn as_base_rdf_node_type(&self) -> BaseRDFNodeType {
        match self {
            StoredBaseRDFNodeType::IRI(_) => BaseRDFNodeType::IRI,
            StoredBaseRDFNodeType::Blank => BaseRDFNodeType::BlankNode,
            StoredBaseRDFNodeType::Literal(nn) => BaseRDFNodeType::Literal(nn.clone()),
        }
    }
}

#[derive(Clone)]
pub struct Triplestore {
    pub storage_folder: Option<PathBuf>,
    triples_map:
        HashMap<NamedNode, HashMap<(StoredBaseRDFNodeType, StoredBaseRDFNodeType), Triples>>,
    transient_triples_map:
        HashMap<NamedNode, HashMap<(StoredBaseRDFNodeType, StoredBaseRDFNodeType), Triples>>,
    parser_call: usize,
    indexing: IndexingOptions,
    fts_index: Option<FtsIndex>,
    pub cats: Arc<Cats>,
}

impl Triplestore {
    pub fn truncate(&mut self) {
        if self.storage_folder.is_some() {
            todo!("Should drop this folder.. ")
        }
        self.triples_map = HashMap::new();
    }
}

#[derive(Clone)]
pub struct IndexingOptions {
    pub object_sort_all: bool,
    pub object_sort_some: Option<HashSet<NamedNode>>,
    pub fts_path: Option<PathBuf>,
}

impl IndexingOptions {
    pub fn set_fts_path(&mut self, fts_path: Option<PathBuf>) {
        self.fts_path = fts_path;
    }
}

impl Default for IndexingOptions {
    fn default() -> IndexingOptions {
        IndexingOptions {
            object_sort_all: false,
            object_sort_some: Some(HashSet::from([
                rdfs::LABEL.into_owned(),
                rdf::TYPE.into_owned(),
            ])),
            fts_path: None,
        }
    }
}

pub struct TriplesToAdd {
    pub df: DataFrame,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub predicate: Option<NamedNode>,
    pub subject_cat_state: BaseCatState,
    pub object_cat_state: BaseCatState,
    pub predicate_cat_state: Option<BaseCatState>,
}

#[derive(Debug)]
pub struct NewTriples {
    pub df: Option<DataFrame>,
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
    pub fn new(
        storage_folder: Option<String>,
        indexing: Option<IndexingOptions>,
    ) -> Result<Triplestore, TriplestoreError> {
        let pathbuf = if let Some(storage_folder) = &storage_folder {
            let mut pathbuf = Path::new(storage_folder).to_path_buf();
            create_folder_if_not_exists(pathbuf.as_path())
                .map_err(TriplestoreError::FileIOError)?;
            let ext = format!("ts_{}", Uuid::new_v4());
            pathbuf.push(&ext);
            create_folder_if_not_exists(pathbuf.as_path())
                .map_err(TriplestoreError::FileIOError)?;
            Some(pathbuf)
        } else {
            None
        };
        let indexing = indexing.unwrap_or_default();
        let fts_index = if let Some(fts_path) = &indexing.fts_path {
            Some(FtsIndex::new(fts_path).map_err(TriplestoreError::FtsError)?)
        } else {
            None
        };
        Ok(Triplestore {
            triples_map: HashMap::new(),
            transient_triples_map: HashMap::new(),
            storage_folder: pathbuf,
            parser_call: 0,
            indexing,
            fts_index,
            cats: Arc::new(Cats::new_empty()),
        })
    }

    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        if let Some(fts_path) = &indexing.fts_path {
            // Only doing anything if the fts index does not already exist.
            // If it exists, then it should be updated as well.
            if self.fts_index.is_none() {
                self.fts_index = Some(FtsIndex::new(fts_path).map_err(TriplestoreError::FtsError)?);
                for (predicate, map) in &self.triples_map {
                    for ((subject_type, object_type), ts) in map {
                        for (lf, _) in ts.get_lazy_frames(&None, &None)? {
                            self.fts_index
                                .as_mut()
                                .unwrap()
                                .add_literal_string(
                                    &lf.collect().unwrap(),
                                    predicate,
                                    &subject_type.as_base_rdf_node_type(),
                                    &subject_type
                                        .as_base_rdf_node_type()
                                        .default_stored_cat_state(),
                                    &object_type.as_base_rdf_node_type(),
                                    &object_type
                                        .as_base_rdf_node_type()
                                        .default_stored_cat_state(),
                                    self.cats.clone(),
                                )
                                .map_err(TriplestoreError::FtsError)?;
                        }
                    }
                }
            }
        }
        self.indexing = indexing;
        Ok(())
    }

    pub fn add_triples_vec(
        &mut self,
        ts: Vec<TriplesToAdd>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let prepare_triples_now = Instant::now();
        let dfs_to_add = prepare_add_triples_par(ts, self.cats.clone());
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

    fn add_local_cat_triples(
        &mut self,
        local_cats: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let global_cats = self.globalize(local_cats);
        self.add_global_cat_triples(global_cats, transient)
    }

    fn add_global_cat_triples(
        &mut self,
        global_cat_triples: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let use_map = if transient {
            &mut self.transient_triples_map
        } else {
            &mut self.triples_map
        };

        let mut add_map: HashMap<_, (_, Vec<_>)> = HashMap::new();
        for CatTriples {
            encoded_triples,
            predicate,
            subject_type,
            object_type,
            local_cats: _,
        } in global_cat_triples
        {
            for encoded_triple in encoded_triples {
                let mut map = HashMap::new();
                map.insert(
                    SUBJECT_COL_NAME.to_string(),
                    subject_type.clone().into_default_stored_rdf_node_state(),
                );
                map.insert(
                    OBJECT_COL_NAME.to_string(),
                    object_type.clone().into_default_stored_rdf_node_state(),
                );

                let subject_type_stored = StoredBaseRDFNodeType::from_base_and_prefix(
                    &subject_type,
                    &encoded_triple.subject,
                );
                let object_type_stored = StoredBaseRDFNodeType::from_base_and_prefix(
                    &object_type,
                    &encoded_triple.object,
                );
                let k = (subject_type_stored.clone(), object_type_stored.clone());
                let triples = if let Some(m) = use_map.get_mut(&predicate) {
                    m.remove(&k)
                } else {
                    None
                };
                let (s, o) = k;
                let k2 = (predicate.clone(), s, o);

                if !add_map.contains_key(&k2) {
                    add_map.insert(k2.clone(), (triples, vec![]));
                }
                let (_, v) = add_map.get_mut(&k2).unwrap();
                v.push(encoded_triple);
            }
        }
        let storage_folder = self.storage_folder.clone();
        let indexing = self.indexing.clone();
        let cats = self.cats.clone();
        //Why does not par iter work?
        let r: Result<Vec<_>, TriplestoreError> = add_map
            .into_iter()
            .map(move |((p, s, o), (triples, v))| {
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
                        predicate: p.clone(),
                        subject_type: s.as_base_rdf_node_type(),
                        object_type: o.as_base_rdf_node_type(),
                    };
                    new_triples_vec.push(new_triples);

                    let triples = Triples::new(
                        et.df,
                        storage_folder.as_ref(),
                        s.clone(),
                        o.clone(),
                        &p,
                        &indexing,
                        cats.clone(),
                    )?;
                    triples
                };
                for enc in v_iter {
                    let new_triples_opt = triples.add_triples(
                        enc.df.clone(),
                        storage_folder.as_ref(),
                        cats.clone(),
                    )?;
                    let new_triples = NewTriples {
                        df: new_triples_opt
                            .map(|x| x.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap()),
                        predicate: p.clone(),
                        subject_type: s.as_base_rdf_node_type(),
                        object_type: o.as_base_rdf_node_type(),
                    };
                    new_triples_vec.push(new_triples);
                }
                let k = (p, s, o);
                Ok(((k, triples), new_triples_vec))
            })
            .collect();
        let (to_add_map, out_new_triples): (HashMap<_, _>, Vec<_>) = r?.into_iter().unzip();

        let out_new_triples: Vec<_> = out_new_triples.into_iter().flatten().collect();
        for ((p, s, o), v) in to_add_map {
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
                    if let Some(fts_index) = &mut self.fts_index {
                        let fts_now = Instant::now();
                        fts_index
                            .add_literal_string(
                                df,
                                &nt.predicate,
                                &nt.subject_type,
                                &nt.subject_type.default_stored_cat_state(),
                                &nt.object_type,
                                &nt.object_type.default_stored_cat_state(),
                                self.cats.clone(),
                            )
                            .map_err(TriplestoreError::FtsError)?;
                        trace!(
                            "Adding to fts index took {} seconds",
                            fts_now.elapsed().as_secs_f32()
                        );
                    }
                }
            }
        }

        Ok(out_new_triples)
    }
}

struct TriplesToAddPartitionedPredicate {
    pub df: DataFrame,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub predicate: NamedNode,
    pub subject_cat_state: BaseCatState,
    pub object_cat_state: BaseCatState,
}

pub fn prepare_add_triples_par(
    mut ts: Vec<TriplesToAdd>,
    global_cats: Arc<Cats>,
) -> Vec<CatTriples> {
    let df_vecs_to_add: Vec<Vec<TriplesToAddPartitionedPredicate>> = ts
        .par_drain(..)
        .map(|t| {
            let TriplesToAdd {
                df,
                subject_type,
                object_type,
                predicate,
                subject_cat_state,
                predicate_cat_state,
                object_cat_state,
            } = t;
            let df_preds = partition_unpartitioned_predicate(
                df,
                &subject_type,
                &object_type,
                predicate,
                predicate_cat_state.as_ref(),
                global_cats.as_ref(),
            );
            let all_partitioned: Vec<_> = df_preds
                .into_iter()
                .map(|(df, predicate)| TriplesToAddPartitionedPredicate {
                    df,
                    subject_type: subject_type.clone(),
                    object_type: object_type.clone(),
                    predicate,
                    subject_cat_state: subject_cat_state.clone(),
                    object_cat_state: object_cat_state.clone(),
                })
                .collect();
            all_partitioned
        })
        .collect();
    let mut all_partitioned: Vec<_> = flatten(df_vecs_to_add);
    all_partitioned = all_partitioned
        .into_par_iter()
        .map(|mut t| {
            t.df = sort_triples_add_rank(
                t.df.clone(),
                &t.subject_type,
                &t.subject_cat_state,
                &t.object_type,
                &t.object_cat_state,
                global_cats.clone(),
                true,
            );
            t
        })
        .collect();
    all_partitioned
        .into_par_iter()
        .map(
            |TriplesToAddPartitionedPredicate {
                 df,
                 subject_type,
                 object_type,
                 predicate,
                 subject_cat_state,
                 object_cat_state,
             }| {
                cat_encode_triples(
                    df,
                    subject_type,
                    object_type,
                    predicate,
                    subject_cat_state,
                    object_cat_state,
                    global_cats.as_ref(),
                )
            },
        )
        .collect()
}

pub fn sort_triples_add_rank(
    df: DataFrame,
    subj_type: &BaseRDFNodeType,
    subj_cat_state: &BaseCatState,
    obj_type: &BaseRDFNodeType,
    obj_cat_state: &BaseCatState,
    global_cats: Arc<Cats>,
    deduplicate: bool,
) -> DataFrame {
    // Always sort S,O and deduplicate
    let mut lf = df.lazy();
    let mut subj_col_expr = col(SUBJECT_COL_NAME);
    if matches!(subj_cat_state, BaseCatState::CategoricalNative(..)) {
        subj_col_expr = decode_expr(
            subj_col_expr,
            subj_type.clone(),
            subj_cat_state.get_local_cats(),
            global_cats.clone(),
            false,
        );
    }
    let mut obj_col_expr = col(OBJECT_COL_NAME);
    if matches!(obj_cat_state, BaseCatState::CategoricalNative(..)) {
        obj_col_expr = decode_expr(
            obj_col_expr,
            obj_type.clone(),
            obj_cat_state.get_local_cats(),
            global_cats.clone(),
            false,
        );
    }
    lf = lf.with_column(
        subj_col_expr
            .rank(
                RankOptions {
                    method: RankMethod::Min,
                    descending: false,
                },
                None,
            )
            .alias(SUBJECT_RANK_COL_NAME),
    );
    lf = lf.with_column(
        obj_col_expr
            .rank(
                RankOptions {
                    method: RankMethod::Min,
                    descending: false,
                },
                None,
            )
            .alias(OBJECT_RANK_COL_NAME),
    );

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
    if deduplicate {
        lf = lf.filter(
            repeated_from_last_row_expr(SUBJECT_COL_NAME)
                .and(repeated_from_last_row_expr(OBJECT_COL_NAME))
                .not(),
        );
    }
    let df = lf.collect().unwrap();
    df
}

pub fn partition_unpartitioned_predicate(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
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
            subject_type.clone().into_default_input_rdf_node_state(),
        ),
        (
            OBJECT_COL_NAME.to_string(),
            object_type.clone().into_default_input_rdf_node_state(),
        ),
    ]);

    let mut lf = df.lazy();
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

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
