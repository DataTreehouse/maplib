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
use log::debug;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{AnyValue, DataFrame, IntoLazy};
use polars_core::datatypes::CategoricalOrdering;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use representation::multitype::{lf_columns_to_categorical, set_structs_all_null_to_null_row};
use representation::solution_mapping::EagerSolutionMappings;
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
        for (_, map) in self.triples_map.iter_mut() {
            for (_, v) in map.iter_mut() {
                v.index_unindexed(&self.storage_folder, None)?;
            }
        }
        self.has_unindexed = false;
        Ok(())
    }

    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        self.index_unindexed()?;
        for (k, m) in &mut self.triples_map {
            for ((_, object_type), ts) in m {
                ts.add_index(object_type, &self.storage_folder, k, &indexing)?
            }
        }
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
        mut ts: Vec<TriplesToAdd>,
        call_uuid: &str,
        transient: bool,
        delay_index: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let prepare_triples_now = Instant::now();
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
        debug!(
            "Preparing triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();
        let dfs_to_add = flatten(df_vecs_to_add);
        let new_triples = self.add_triples_df(dfs_to_add, call_uuid, transient, delay_index)?;
        if delay_index {
            self.has_unindexed = true;
        }
        debug!(
            "Adding triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(new_triples)
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
            debug!(
                "Adding predicate {} subject type {} object type {}",
                predicate, subject_type, object_type
            );
            if matches!(object_type, BaseRDFNodeType::Literal(..)) {
                //TODO: Get what is actually updated from db and then only add those things
                if let Some(fts_index) = &mut self.fts_index {
                    let fts_now = Instant::now();
                    fts_index
                        .add_literal_string(&df, &predicate, &subject_type, &object_type)
                        .map_err(TriplestoreError::FtsError)?;
                    debug!(
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
            debug!(
                "Casting to cat took {} seconds",
                cast_now.elapsed().as_secs_f32()
            );
            let add_now = Instant::now();
            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(t) = m.get_mut(&k) {
                    let new_triples_opt =
                        t.add_triples(df.clone(), &self.storage_folder, delay_index)?;
                    if !delay_index {
                        let new_triples = NewTriples {
                            df: new_triples_opt,
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
                    df,
                    call_uuid,
                    &self.storage_folder,
                    subject_type.clone(),
                    object_type.clone(),
                    &predicate,
                    &self.indexing,
                    delay_index,
                )?;
                if !delay_index {
                    let new_triples_now = Instant::now();
                    let mut lfs = triples.get_lazy_frames(&None, &None)?;
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
                    debug!(
                        "Creating new triples out took {} seconds",
                        new_triples_now.elapsed().as_secs_f32()
                    );
                }
                let m = use_map.get_mut(&predicate).unwrap();
                m.insert(k, triples);
            }
            debug!(
                "Adding triples to map took  {} seconds",
                add_now.elapsed().as_secs_f32()
            );
        }
        Ok(out_new_triples)
    }

    //     fn subtract_from_transient(
    //         &mut self,
    //         triples_df: Vec<TripleDF>,
    //         call_uuid: &String,
    //         transient: bool,
    //     ) -> Result<Vec<TripleDF>, TriplestoreError> {
    //         let mut new_triples_df = vec![];
    //         if transient {
    //             for tdf in triples_df {
    //                 if let Some(m) = self.triples_map.get(&tdf.predicate) {
    //                     if let Some(SolutionMappings {
    //                         mappings: lf,
    //                         rdf_node_types: _,
    //                     }) = multiple_tt_to_lf(
    //                         m,
    //                         None,
    //                         Some(&tdf.subject_type),
    //                         Some(&tdf.object_type),
    //                         None,
    //                         None,
    //                     )
    //                     .map_err(|x| TriplestoreError::SubtractTransientTriplesError(x.to_string()))?
    //                     {
    //                         let TripleDF {
    //                             df,
    //                             predicate,
    //                             subject_type,
    //                             object_type,
    //                         } = tdf;
    //                         let join_on = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
    //                         let df = df
    //                             .lazy()
    //                             .join(lf, &join_on, &join_on, JoinArgs::new(JoinType::Anti))
    //                             .collect()
    //                             .unwrap();
    //                         if df.height() > 0 {
    //                             new_triples_df.push(TripleDF {
    //                                 df,
    //                                 predicate,
    //                                 subject_type,
    //                                 object_type,
    //                             })
    //                         }
    //                     } else {
    //                         new_triples_df.push(tdf);
    //                     }
    //                 } else {
    //                     new_triples_df.push(tdf);
    //                 }
    //             }
    //         } else {
    //             let mut updated_transient_triples_df = vec![];
    //             for tdf in &triples_df {
    //                 if let Some(m) = self.transient_df_map.get(&tdf.predicate) {
    //                     if let Some(SolutionMappings {
    //                         mappings: lf,
    //                         rdf_node_types: _,
    //                     }) = multiple_tt_to_lf(
    //                         m,
    //                         None,
    //                         Some(&tdf.subject_type),
    //                         Some(&tdf.object_type),
    //                         None,
    //                         None,
    //                     )
    //                     .map_err(|x| TriplestoreError::SubtractTransientTriplesError(x.to_string()))?
    //                     {
    //                         let join_on = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
    //                         let df = lf
    //                             .join(
    //                                 tdf.df.clone().lazy(),
    //                                 &join_on,
    //                                 &join_on,
    //                                 JoinArgs::new(JoinType::Anti),
    //                             )
    //                             .collect()
    //                             .unwrap();
    //                         updated_transient_triples_df.push(TripleDF {
    //                             df,
    //                             predicate: tdf.predicate.clone(),
    //                             subject_type: tdf.subject_type.clone(),
    //                             object_type: tdf.object_type.clone(),
    //                         });
    //                     }
    //                 }
    //             }
    //             if !updated_transient_triples_df.is_empty() {
    //                 self.add_triples_df(updated_transient_triples_df, call_uuid, true, true)?;
    //             }
    //         }
    //         Ok(new_triples_df)
    //     }
}

pub fn prepare_triples(
    df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    static_verb_column: Option<NamedNode>,
) -> Vec<TripleDF> {
    let mut out_df_vec = vec![];
    let map = HashMap::from([
        (
            SUBJECT_COL_NAME.to_string(),
            subject_type.as_rdf_node_type(),
        ),
        (OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type()),
    ]);
    let sm = EagerSolutionMappings::new(df, map).as_lazy();
    let EagerSolutionMappings {
        mappings: mut df,
        rdf_node_types: _,
    } = set_structs_all_null_to_null_row(sm).as_eager(false);

    if df.height() == 0 {
        return vec![];
    }

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
                } else if let Ok(AnyValue::StringOwned(s)) = any_predicate {
                    predicate = literal_iri_to_namednode(s.as_str());
                } else {
                    panic!("Predicate: {:?}", any_predicate);
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
