extern crate core;

mod dblf;
pub mod errors;
mod io_funcs;
pub mod native_parquet_write;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod sparql;
mod storage;
pub mod triples_read;
pub mod triples_write;

use crate::errors::TriplestoreError;
use crate::io_funcs::{create_folder_if_not_exists};
use crate::storage::Triples;
use log::debug;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::NamedNode;
use polars::prelude::{AnyValue, DataFrame, IntoLazy, UniqueKeepStrategy};
use polars_core::datatypes::CategoricalOrdering;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use representation::multitype::lf_columns_to_categorical;
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
    deduplicated: bool,
    triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    transient_triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    parser_call: usize,
    indexing: IndexingOptions,
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
    pub enabled: bool,
    pub object_sort_all: bool,
    pub object_sort_some: Option<HashSet<NamedNode>>,
}

impl Default for IndexingOptions {
    fn default() -> IndexingOptions {
        IndexingOptions {
            enabled: true,
            object_sort_all: false,
            object_sort_some: Some(HashSet::from([
                rdfs::LABEL.into_owned(),
                rdf::TYPE.into_owned(),
            ])),
        }
    }
}

pub struct TriplesToAdd {
    pub df: DataFrame,
    pub subject_type: RDFNodeType,
    pub object_type: RDFNodeType,
    pub static_verb_column: Option<NamedNode>,
    pub has_unique_subset: bool,
}

#[derive(Debug)]
pub struct TripleDF {
    df: DataFrame,
    predicate: NamedNode,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
    unique: bool,
}

impl Triplestore {
    pub fn new(
        storage_folder: Option<String>,
        indexing: Option<IndexingOptions>,
    ) -> Result<Triplestore, TriplestoreError> {
        let pathbuf = if let Some(storage_folder) = &storage_folder {
            let mut pathbuf = Path::new(storage_folder).to_path_buf();
            create_folder_if_not_exists(pathbuf.as_path())?;
            let ext = format!("ts_{}", Uuid::new_v4().to_string());
            pathbuf.push(&ext);
            create_folder_if_not_exists(pathbuf.as_path())?;
            Some(pathbuf)
        } else {
            None
        };
        Ok(Triplestore {
            triples_map: HashMap::new(),
            transient_triples_map: HashMap::new(),
            deduplicated: true,
            storage_folder:pathbuf,
            parser_call: 0,
            indexing: indexing.unwrap_or(IndexingOptions::default()),
        })
    }

    pub fn is_deduplicated(&self) -> bool {
        self.deduplicated
    }

    pub fn deduplicate(&mut self) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        deduplicate_and_index_map(&mut self.triples_map, &self.storage_folder)?;
        deduplicate_and_index_map(&mut self.transient_triples_map, &self.storage_folder)?;
        self.deduplicated = true;
        debug!("Deduplication took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }

    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), TriplestoreError> {
        if indexing.enabled {
            for (k, m) in &mut self.triples_map {
                for ((_, object_type), ts) in m {
                    ts.add_index(object_type, &self.storage_folder, k, &indexing)?
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
        deduplicate: bool,
    ) -> Result<(), TriplestoreError> {
        let df_vecs_to_add: Vec<Vec<TripleDF>> = ts
            .par_drain(..)
            .map(|t| {
                let TriplesToAdd {
                    df,
                    subject_type,
                    object_type,
                    static_verb_column,
                    has_unique_subset,
                } = t;
                assert!(!matches!(subject_type, RDFNodeType::MultiType(..)));
                assert!(!matches!(object_type, RDFNodeType::MultiType(..)));
                prepare_triples(
                    df,
                    &BaseRDFNodeType::from_rdf_node_type(&subject_type),
                    &BaseRDFNodeType::from_rdf_node_type(&object_type),
                    static_verb_column,
                    has_unique_subset,
                    deduplicate,
                )
            })
            .collect();
        let dfs_to_add = flatten(df_vecs_to_add);
        self.add_triples_df(dfs_to_add, call_uuid, transient)?;
        Ok(())
    }

    fn add_triples_df(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &str,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
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
            unique,
        } in triples_df
        {
            let mut lf = df.lazy();
            let mut map = HashMap::new();
            map.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.as_rdf_node_type(),
            );
            map.insert(OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type());
            lf = lf_columns_to_categorical(lf, &map, CategoricalOrdering::Physical);
            df = lf.collect().unwrap();
            let k = (subject_type.clone(), object_type.clone());
            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(t) = m.get_mut(&k) {
                    t.add_triples(df, &self.storage_folder)?
                } else {
                    m.insert(
                        k,
                        Triples::new(
                            df,
                            unique,
                            call_uuid,
                            &self.storage_folder,
                            subject_type,
                            object_type,
                            &predicate,
                            &self.indexing,
                        )?,
                    );
                }
            } else {
                use_map.insert(
                    predicate.clone(),
                    HashMap::from([(
                        k,
                        Triples::new(
                            df,
                            unique,
                            call_uuid,
                            &self.storage_folder,
                            subject_type,
                            object_type,
                            &predicate,
                            &self.indexing,
                        )?,
                    )]),
                );
            }
        }
        Ok(())
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
    mut df: DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    static_verb_column: Option<NamedNode>,
    has_unique_subset: bool,
    deduplicate: bool,
) -> Vec<TripleDF> {
    let now = Instant::now();
    let mut out_df_vec = vec![];
    if df.height() == 0 {
        return vec![];
    }

    if let Some(static_verb_column) = static_verb_column {
        df = df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
        if let Some(tdf) = prepare_triples_df(
            df,
            static_verb_column,
            subject_type,
            object_type,
            has_unique_subset,
            deduplicate,
        ) {
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
            if let Some(tdf) = prepare_triples_df(
                part,
                predicate,
                subject_type,
                object_type,
                has_unique_subset,
                deduplicate,
            ) {
                out_df_vec.push(tdf);
            }
        }
    }
    debug!(
        "Adding triples took {} seconds",
        now.elapsed().as_secs_f32()
    );
    out_df_vec
}

fn prepare_triples_df(
    mut df: DataFrame,
    predicate: NamedNode,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
    has_unique_subset: bool,
    deduplicate: bool,
) -> Option<TripleDF> {
    let now = Instant::now();
    df = df.drop_nulls::<String>(None).unwrap();
    if df.height() == 0 {
        return None;
    }
    debug!(
        "Prepare single triple df after drop null before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );
    let mut unique = has_unique_subset;
    if deduplicate && !has_unique_subset {
        df = df
            .unique::<(), ()>(None, UniqueKeepStrategy::First, None)
            .unwrap();
        unique = true;
    }

    //TODO: add polars datatype harmonization here.
    debug!(
        "Prepare single triple df before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );
    Some(TripleDF {
        df,
        predicate,
        subject_type: subject_type.clone(),
        object_type: object_type.clone(),
        unique,
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}

fn deduplicate_and_index_map(
    df_map: &mut HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    storage_folder: &Option<PathBuf>,
) -> Result<(), TriplestoreError> {
    for map in df_map.values_mut() {
        for v in map.values_mut() {
            v.deduplicate_and_index(storage_folder)?;
        }
    }
    Ok(())
}
