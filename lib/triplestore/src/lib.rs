extern crate core;

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
use crate::storage::Triples;
use file_io::create_folder_if_not_exists;
use fts::FtsIndex;
use log::trace;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{
    col, concat, AnyValue, DataFrame, IntoLazy, JoinArgs, JoinType, MaintainOrderJoin, UnionArgs,
};
use polars_core::datatypes::CategoricalOrdering;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelRefMutIterator, ParallelDrainRange};
use representation::multitype::{
    lf_column_to_categorical, lf_columns_to_categorical, set_struct_all_null_to_null_row,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{
    literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME,
    VERB_COL_NAME,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;

#[derive(Clone)]
pub struct Triplestore {
    pub storage_folder: Option<PathBuf>,
    triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    transient_triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    parser_call: usize,
    indexing: IndexingOptions,
    fts_index: Option<FtsIndex>,
    has_unindexed: bool,
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
    pub subject_type: RDFNodeType,
    pub object_type: RDFNodeType,
    pub static_verb_column: Option<NamedNode>,
}

#[derive(Debug)]
pub struct TripleDF {
    df: DataFrame,
    predicate: NamedNode,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
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
                self.subject_type.as_rdf_node_type(),
            );
            map.insert(
                OBJECT_COL_NAME.to_string(),
                self.object_type.as_rdf_node_type(),
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
            has_unindexed: false,
        })
    }

    pub fn index_unindexed(&mut self) -> Result<(), TriplestoreError> {
        let r: Result<Vec<_>, TriplestoreError> = self
            .triples_map
            .par_iter_mut()
            .map(|(_, map)| {
                for (_, v) in map.iter_mut() {
                    v.index_unindexed_maybe_segments(None, self.storage_folder.as_ref())?;
                }
                Ok(())
            })
            .collect();
        r?;
        self.has_unindexed = false;
        Ok(())
    }

    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        self.index_unindexed()?;
        for (k, m) in &mut self.triples_map {
            for (_, ts) in m {
                ts.add_index(k, &indexing, self.storage_folder.as_ref())?
            }
        }
        if let Some(fts_path) = &indexing.fts_path {
            // Only doing anything if the fts index does not already exist.
            // If it exists, then it should be updated as well.
            if self.fts_index.is_none() {
                self.fts_index = Some(FtsIndex::new(fts_path).map_err(TriplestoreError::FtsError)?);
                for (predicate, map) in &self.triples_map {
                    for ((subject_type, object_type), ts) in map {
                        for (lf, _) in ts.get_lazy_frames(&None, &None, false)? {
                            self.fts_index
                                .as_mut()
                                .unwrap()
                                .add_literal_string(
                                    &lf.collect().unwrap(),
                                    predicate,
                                    subject_type,
                                    object_type,
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
        call_uuid: &str,
        transient: bool,
        delay_index: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let prepare_triples_now = Instant::now();
        let dfs_to_add = prepare_triples_par(ts);
        trace!(
            "Preparing triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();

        let new_triples = self.add_triples_df(dfs_to_add, call_uuid, transient, delay_index)?;
        if delay_index {
            self.has_unindexed = true;
        }
        trace!(
            "Adding triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(new_triples)
    }

    pub fn delete_triples_vec(
        &mut self,
        ts: Vec<TriplesToAdd>,
        call_uuid: &str,
        delay_index: bool,
    ) -> Result<(), TriplestoreError> {
        let prepare_triples_now = Instant::now();
        let dfs_to_add = prepare_triples_par(ts);
        trace!(
            "Deleting triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();
        self.delete_triples_df(dfs_to_add, call_uuid, delay_index)?;
        if delay_index {
            self.has_unindexed = true;
        }
        trace!(
            "Deleting triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(())
    }

    fn add_triples_df(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &str,
        transient: bool,
        delay_index: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let mut out_new_triples = vec![];
        let use_map = if transient {
            &mut self.transient_triples_map
        } else {
            &mut self.triples_map
        };
        for TripleDF {
            mut df,
            predicate,
            subject_type,
            object_type,
        } in triples_df
        {
            trace!(
                "Adding predicate {predicate} subject type {subject_type} object type {object_type}"
            );
            if matches!(object_type, BaseRDFNodeType::Literal(..)) {
                //TODO: Get what is actually updated from db and then only add those things
                if let Some(fts_index) = &mut self.fts_index {
                    let fts_now = Instant::now();
                    fts_index
                        .add_literal_string(&df, &predicate, &subject_type, &object_type)
                        .map_err(TriplestoreError::FtsError)?;
                    trace!(
                        "Adding to fts index took {} seconds",
                        fts_now.elapsed().as_secs_f32()
                    );
                }
            }
            let cast_now = Instant::now();
            let mut map = HashMap::new();
            map.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.as_rdf_node_type(),
            );
            map.insert(OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type());
            let mut lf = df.lazy();
            lf = lf_columns_to_categorical(lf, &map, CategoricalOrdering::Physical);
            df = lf.collect().unwrap();
            let k = (subject_type.clone(), object_type.clone());
            let mut added_triples = false;
            trace!(
                "Casting to cat took {} seconds",
                cast_now.elapsed().as_secs_f32()
            );
            let add_now = Instant::now();
            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(t) = m.get_mut(&k) {
                    let new_triples_opt =
                        t.add_triples(df.clone(), self.storage_folder.as_ref(), delay_index)?;
                    if !delay_index {
                        let new_triples = NewTriples {
                            df: new_triples_opt,
                            predicate: predicate.clone(),
                            subject_type: subject_type.clone(),
                            object_type: object_type.clone(),
                        };
                        out_new_triples.push(new_triples);
                    } else {
                        let new_triples = NewTriples {
                            df: Some(df.clone()),
                            predicate: predicate.clone(),
                            subject_type: subject_type.clone(),
                            object_type: object_type.clone(),
                        };
                        out_new_triples.push(new_triples);
                    }
                    added_triples = true;
                }
            } else {
                use_map.insert(predicate.clone(), HashMap::new());
            };
            if !added_triples {
                let triples = Triples::new(
                    df.clone(),
                    call_uuid,
                    self.storage_folder.as_ref(),
                    subject_type.clone(),
                    object_type.clone(),
                    &predicate,
                    &self.indexing,
                    delay_index,
                )?;
                if !delay_index {
                    let new_triples_now = Instant::now();
                    let mut lfs = triples.get_lazy_frames(&None, &None, false)?;
                    assert_eq!(lfs.len(), 1);
                    let (lf, _) = lfs.pop().unwrap();
                    let df = lf.collect().unwrap();
                    let new_triples = NewTriples {
                        df: Some(df),
                        predicate: predicate.clone(),
                        subject_type,
                        object_type,
                    };
                    out_new_triples.push(new_triples);
                    trace!(
                        "Creating new triples out took {} seconds",
                        new_triples_now.elapsed().as_secs_f32()
                    );
                } else {
                    let new_triples = NewTriples {
                        df: Some(df),
                        predicate: predicate.clone(),
                        subject_type,
                        object_type,
                    };
                    out_new_triples.push(new_triples);
                }
                let m = use_map.get_mut(&predicate).unwrap();
                m.insert(k, triples);
            }
            trace!(
                "Adding triples to map took  {} seconds",
                add_now.elapsed().as_secs_f32()
            );
        }
        Ok(out_new_triples)
    }

    fn delete_triples_df(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &str,
        delay_index: bool,
    ) -> Result<(), TriplestoreError> {
        for tdf in triples_df {
            let df = get_df_after_deletion(&tdf, &self.triples_map)?;
            self.delete_if_exists(&tdf.predicate, &tdf.subject_type, &tdf.object_type, false);
            if let Some(df) = df {
                let new_tdf = TripleDF {
                    df,
                    predicate: tdf.predicate.clone(),
                    subject_type: tdf.subject_type.clone(),
                    object_type: tdf.object_type.clone(),
                };
                self.add_triples_df(vec![new_tdf], call_uuid, false, delay_index)?;
            }

            let df = get_df_after_deletion(&tdf, &self.transient_triples_map)?;
            self.delete_if_exists(&tdf.predicate, &tdf.subject_type, &tdf.object_type, true);
            if let Some(df) = df {
                let new_tdf = TripleDF {
                    df,
                    predicate: tdf.predicate.clone(),
                    subject_type: tdf.subject_type.clone(),
                    object_type: tdf.object_type.clone(),
                };
                self.add_triples_df(vec![new_tdf], call_uuid, true, delay_index)?;
            }
        }
        Ok(())
    }

    fn delete_if_exists(
        &mut self,
        predicate: &NamedNode,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        transient: bool,
    ) {
        let use_map = if transient {
            &mut self.transient_triples_map
        } else {
            &mut self.triples_map
        };
        let map_empty = if let Some(m1) = use_map.get_mut(predicate) {
            let type_ = (subject_type.clone(), object_type.clone());
            m1.remove(&type_);
            m1.is_empty()
        } else {
            false
        };
        if map_empty {
            use_map.remove(predicate);
        }
    }
}

fn get_df_after_deletion(
    tdf: &TripleDF,
    map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
) -> Result<Option<DataFrame>, TriplestoreError> {
    if let Some(m) = map.get(&tdf.predicate) {
        let type_ = (tdf.subject_type.clone(), tdf.object_type.clone());
        if let Some(triples) = m.get(&type_) {
            let lfs = triples.get_lazy_frames(&None, &None, true)?;
            let lfs_only: Vec<_> = lfs.into_iter().map(|(lf, _)| lf).collect();
            let mut lf = concat(
                lfs_only,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    diagonal: false,
                    from_partitioned_ds: false,
                    maintain_order: false,
                },
            )
            .unwrap();
            let (s, o) = type_;
            let mut to_delete = tdf.df.clone().lazy();
            to_delete = lf_column_to_categorical(
                to_delete,
                SUBJECT_COL_NAME,
                &s.as_rdf_node_type(),
                CategoricalOrdering::Physical,
            );
            to_delete = lf_column_to_categorical(
                to_delete,
                OBJECT_COL_NAME,
                &o.as_rdf_node_type(),
                CategoricalOrdering::Physical,
            );

            lf = lf.join(
                to_delete,
                [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
                [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
                JoinArgs {
                    how: JoinType::Anti,
                    validation: Default::default(),
                    suffix: None,
                    slice: None,
                    nulls_equal: false,
                    coalesce: Default::default(),
                    maintain_order: MaintainOrderJoin::None,
                },
            );
            let df = lf.collect().unwrap();
            if df.height() > 0 {
                return Ok(Some(df));
            }
        }
    }
    Ok(None)
}

pub fn prepare_triples_par(mut ts: Vec<TriplesToAdd>) -> Vec<TripleDF> {
    let df_vecs_to_add: Vec<Vec<TripleDF>> = ts
        .par_drain(..)
        .map(|t| {
            let TriplesToAdd {
                df,
                subject_type,
                object_type,
                static_verb_column,
            } = t;
            assert!(!matches!(subject_type, RDFNodeType::MultiType(..)));
            assert!(!matches!(object_type, RDFNodeType::MultiType(..)));
            prepare_triples(
                df,
                &BaseRDFNodeType::from_rdf_node_type(&subject_type),
                &BaseRDFNodeType::from_rdf_node_type(&object_type),
                static_verb_column,
            )
        })
        .collect();

    flatten(df_vecs_to_add)
}

pub fn prepare_triples(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    static_verb_column: Option<NamedNode>,
) -> Vec<TripleDF> {
    if df.height() == 0 {
        return vec![];
    }
    let mut out_df_vec = vec![];
    let map = HashMap::from([
        (
            SUBJECT_COL_NAME.to_string(),
            subject_type.as_rdf_node_type(),
        ),
        (OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type()),
    ]);

    let height = df.height();
    let mut lf = df.lazy();
    for (c, t) in &map {
        lf = lf.with_column(set_struct_all_null_to_null_row(col(c), t).alias(c));
    }
    let sm = SolutionMappings::new(lf, map, height);
    let EagerSolutionMappings {
        mappings: mut df,
        rdf_node_types: _,
    } = sm.as_eager(false);

    if let Some(static_verb_column) = static_verb_column {
        df = df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
        if let Some(tdf) = prepare_triples_df(df, static_verb_column, subject_type, object_type) {
            out_df_vec.push(tdf);
        }
    } else {
        let partitions = df.partition_by([VERB_COL_NAME], true).unwrap();
        for mut part in partitions {
            let predicate;
            {
                let any_predicate = part.column(VERB_COL_NAME).unwrap().get(0);
                if let Ok(AnyValue::String(p)) = any_predicate {
                    predicate = literal_iri_to_namednode(p);
                } else if let Ok(AnyValue::Categorical(a, b, _0)) = any_predicate {
                    predicate = literal_iri_to_namednode(b.get(a));
                } else if let Ok(AnyValue::CategoricalOwned(a, b, _0)) = any_predicate {
                    predicate = literal_iri_to_namednode(b.get(a));
                } else if let Ok(AnyValue::StringOwned(s)) = any_predicate {
                    predicate = literal_iri_to_namednode(s.as_str());
                } else {
                    panic!("Predicate: {any_predicate:?}");
                }
            }
            part = part.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
            if let Some(tdf) = prepare_triples_df(part, predicate, subject_type, object_type) {
                out_df_vec.push(tdf);
            }
        }
    }
    out_df_vec
}

fn prepare_triples_df(
    mut df: DataFrame,
    predicate: NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> Option<TripleDF> {
    df = df.drop_nulls::<String>(None).unwrap();
    if df.height() == 0 {
        return None;
    }

    //TODO: add polars datatype harmonization here.
    Some(TripleDF {
        df,
        predicate,
        subject_type: subject_type.clone(),
        object_type: object_type.clone(),
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
