extern crate core;
mod dblf;
pub mod errors;
pub mod indexing;
mod io_funcs;
pub mod native_parquet_write;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod sparql;
pub mod triples_read;
pub mod triples_write;

use crate::errors::TriplestoreError;
use crate::io_funcs::{create_folder_if_not_exists, delete_tmp_parquets_in_caching_folder};
use log::debug;
use oxrdf::NamedNode;
use parquet_io::{
    scan_parquet, write_parquet, ParquetIOError,
};
use polars::prelude::{
    concat, concat_lf_diagonal, lit, AnyValue, DataFrame, IntoLazy,
    LazyFrame, UnionArgs, UniqueKeepStrategy,
};
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use representation::multitype::lf_columns_to_categorical;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::solution_mapping::SolutionMappings;
use representation::{
    literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME,
    VERB_COL_NAME,
};
use std::collections::HashMap;
use std::fs::remove_file;
use std::io;
use std::path::Path;
use std::time::Instant;
use polars_core::datatypes::CategoricalOrdering;
use polars_core::utils::concat_df;
use uuid::Uuid;

#[derive(Clone)]
pub struct Triplestore {
    pub caching_folder: Option<String>,
    deduplicated: bool,
    triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    transient_triples_map: HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    parser_call: usize,
}

impl Triplestore {
    pub fn truncate(&mut self) {
        if self.caching_folder.is_some() {
            todo!("Should drop this folder.. ")
        }
        self.triples_map = HashMap::new();
    }
}

#[derive(Clone)]
struct Triples {
    unique: bool,
    call_uuid: String,
    unsorted: Option<StoredTriples>,
}

impl Triples {
    pub(crate) fn deduplicate(&mut self,
                              caching_folder: &Option<String>) -> Result<(), TriplestoreError> {
        if !self.unique {
            if let Some(unsorted) = &mut self.unsorted {
                unsorted.deduplicate(caching_folder)?
            }
            self.unique = true;
        }
        Ok(())
    }
}

impl Triples {
    fn new(
        df: DataFrame,
        unique: bool,
        call_uuid: &str,
        caching_folder: &Option<String>,
    ) -> Result<Self, TriplestoreError> {
        let stored = StoredTriples::new(df, caching_folder)?;
        Ok(Triples {
            unique,
            call_uuid: call_uuid.to_string(),
            unsorted: Some(stored),
        })
    }
}

impl Triples {
    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        if let Some(unsorted) = &self.unsorted {
            unsorted.get_lazy_frames()
        } else {
            todo!()
        }
    }

    pub(crate) fn get_solution_mappings(
        &self,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        named_node: Option<&NamedNode>,
    ) -> Result<SolutionMappings, TriplestoreError> {
        let lfs = self.get_lazy_frames()?;
        let mut lf = concat_lf_diagonal(lfs, UnionArgs::default()).unwrap();
        let mut map = HashMap::from([
            (
                SUBJECT_COL_NAME.to_string(),
                subject_type.as_rdf_node_type(),
            ),
            (OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type()),
        ]);
        if let Some(named_node) = named_node {
            lf = lf.with_column(
                lit(rdf_named_node_to_polars_literal_value(named_node)).alias(VERB_COL_NAME),
            );
            map.insert(VERB_COL_NAME.to_string(), RDFNodeType::IRI);
        }
        Ok(SolutionMappings::new(lf, map))
    }

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        unique: bool,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        if let Some(unsorted) = &mut self.unsorted {
            unsorted.add_triples(df, caching_folder)?;
            self.unique = false;
        } else {
            self.unsorted = Some(StoredTriples::new(df, caching_folder)?);
            self.unique = unique;
        }
        Ok(())
    }
}

#[derive(Clone)]
enum StoredTriples {
    TriplesOnDisk(TriplesOnDisk),
    TriplesInMemory(Box<TriplesInMemory>),
}

impl StoredTriples {
    pub(crate) fn deduplicate(
        &mut self,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        match self {
            StoredTriples::TriplesOnDisk(t) => {
                t.deduplicate(caching_folder.as_ref().unwrap())?
            }
            StoredTriples::TriplesInMemory(t) => t.deduplicate(),
        }
        Ok(())
    }
}

impl StoredTriples {
    fn new(
        df: DataFrame,
        caching_folder: &Option<String>,
    ) -> Result<Self, TriplestoreError> {
        Ok(if let Some(caching_folder) = &caching_folder {
            StoredTriples::TriplesOnDisk(TriplesOnDisk::new(
                df,
                caching_folder,
            )?)
        } else {
            StoredTriples::TriplesInMemory(Box::new(TriplesInMemory::new(df)))
        })
    }
}

impl StoredTriples {
    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        match self {
            StoredTriples::TriplesOnDisk(t) => t.get_lazy_frames(),
            StoredTriples::TriplesInMemory(t) => t.get_lazy_frames(),
        }
    }

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        match self {
            StoredTriples::TriplesOnDisk(t) => {
                t.add_triples(df, caching_folder.as_ref().unwrap())
            }
            StoredTriples::TriplesInMemory(t) => t.add_triples(df),
        }
    }
}

#[derive(Clone)]
struct TriplesOnDisk {
    df_paths: Vec<String>,
}

impl TriplesOnDisk {
    pub(crate) fn deduplicate(
        &mut self,
        caching_folder: &String,
    ) -> Result<(), TriplestoreError> {
        let lf_results: Vec<Result<LazyFrame, ParquetIOError>> = self
            .df_paths
            .par_iter()
            .map(scan_parquet)
            .collect();
        let mut lfs = vec![];
        for lf_res in lf_results {
            lfs.push(lf_res.map_err(TriplestoreError::ParquetIOError)?);
        }
        let unique_df = concat(
            lfs,
            UnionArgs {
                parallel: true,
                rechunk: true,
                to_supertypes: false,
                diagonal: false,
                from_partitioned_ds: false,
            },
        )
        .unwrap()
        .unique(None, UniqueKeepStrategy::First)
        .collect()
        .unwrap();
        //TODO: Implement trick with len to avoid IO
        let removed: Vec<Result<(), io::Error>> = self
            .df_paths
            .par_iter()
            .map(|x| remove_file(Path::new(x)))
            .collect();
        for r in removed {
            r.map_err(TriplestoreError::RemoveParquetFileError)?
        }
        let TriplesOnDisk { df_paths } =
            TriplesOnDisk::new(unique_df, caching_folder)?;
        self.df_paths = df_paths;
        Ok(())
    }
}

impl TriplesOnDisk {
    fn new(
        df: DataFrame,
        caching_folder: &String,
    ) -> Result<Self, TriplestoreError> {
        let mut tod = TriplesOnDisk { df_paths: vec![] };
        tod.add_triples(df, caching_folder)?;
        Ok(tod)
    }
}

impl TriplesOnDisk {
    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        let lf_results: Vec<Result<LazyFrame, ParquetIOError>> =
            self.df_paths.par_iter().map(scan_parquet).collect();
        let mut lfs = vec![];
        for lfr in lf_results {
            lfs.push(lfr.map_err(TriplestoreError::ParquetIOError)?);
        }
        Ok(lfs)
    }

    pub(crate) fn add_triples(
        &mut self,
        mut df: DataFrame,
        caching_folder: &String,
    ) -> Result<(), TriplestoreError> {
        let folder_path = Path::new(caching_folder);
        let file_name = format!(
            "tmp_{}.parquet",
            Uuid::new_v4()
        );
        let mut file_path_buf = folder_path.to_path_buf();
        file_path_buf.push(file_name);
        let file_path = file_path_buf.as_path();
        write_parquet(&mut df, file_path).unwrap();
        file_path.to_str().unwrap().to_string();
        self.df_paths.push(file_path.to_str().unwrap().to_string());
        Ok(())
    }
}

#[derive(Clone)]
struct TriplesInMemory {
    dfs: Vec<DataFrame>,
}

impl TriplesInMemory {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self { dfs: vec![df] }
    }

    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        Ok(vec![concat_df(&self.dfs).unwrap().lazy()])
    }

    pub(crate) fn add_triples(&mut self, df: DataFrame) -> Result<(), TriplestoreError> {
        self.dfs.push(df);
        Ok(())
    }

    pub(crate) fn deduplicate(&mut self) {
        let drained: Vec<LazyFrame> = self
            .dfs
            .drain(..)
            .map(|x| x.lazy())
            .collect();
        let mut lf = concat(
            drained.as_slice(),
            UnionArgs {
                parallel: true,
                rechunk: true,
                to_supertypes: false,
                diagonal: false,
                from_partitioned_ds: false,
            },
        )
        .unwrap();
        lf = lf.unique(None, UniqueKeepStrategy::Any);
        self.dfs.push(lf.collect().unwrap());
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
    pub fn new(caching_folder: Option<String>) -> Result<Triplestore, TriplestoreError> {
        if let Some(caching_folder) = &caching_folder {
            let path = Path::new(caching_folder);
            create_folder_if_not_exists(path)?;
            delete_tmp_parquets_in_caching_folder(path)?;
        }
        Ok(Triplestore {
            triples_map: HashMap::new(),
            transient_triples_map: HashMap::new(),
            deduplicated: true,
            caching_folder,
            parser_call: 0,
        })
    }

    pub fn is_deduplicated(&self) -> bool {
        self.deduplicated
    }

    pub fn deduplicate(&mut self) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        deduplicate_map(&mut self.triples_map,  &self.caching_folder)?;
        deduplicate_map(&mut self.transient_triples_map, &self.caching_folder)?;
        self.deduplicated = true;
        debug!("Deduplication took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }

    pub fn add_triples_vec(
        &mut self,
        mut ts: Vec<TriplesToAdd>,
        call_uuid: &String,
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
            let k = (subject_type, object_type);
            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(t) = m.get_mut(&k) {
                    t.add_triples(df, unique, &self.caching_folder)?
                } else {
                    m.insert(
                        k,
                        Triples::new(
                            df,
                            unique,
                            call_uuid,
                            &self.caching_folder,
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
                            &self.caching_folder,
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

fn deduplicate_map(
    df_map: &mut HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
    caching_folder: &Option<String>,
) -> Result<(), TriplestoreError> {
    for map in df_map.values_mut() {
        for v in map.values_mut() {
            v.deduplicate(caching_folder)?;
        }
    }
    Ok(())
}
