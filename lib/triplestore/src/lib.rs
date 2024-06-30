extern crate core;

pub mod conversion;
pub mod errors;
mod io_funcs;
pub mod native_parquet_write;
mod ntriples_write;
pub mod query_solutions;
pub mod rdfs_inferencing;
pub mod sparql;
pub mod triples_read;

use crate::errors::TriplestoreError;
use crate::io_funcs::{create_folder_if_not_exists, delete_tmp_parquets_in_caching_folder};
use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use log::debug;
use oxrdf::NamedNode;
use parquet_io::{
    property_to_filename, scan_parquet, split_write_tmp_df, write_parquet, ParquetIOError,
};
use polars::prelude::{
    col, concat, AnyValue, DataFrame, IntoLazy, JoinArgs, JoinType, LazyFrame, UnionArgs,
    UniqueKeepStrategy,
};
use polars_core::utils::concat_df;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use representation::multitype::lf_column_to_categorical;
use representation::solution_mapping::SolutionMappings;
use representation::{
    literal_iri_to_namednode, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME,
};
use std::collections::HashMap;
use std::fs::remove_file;
use std::io;
use std::path::Path;
use std::time::Instant;
use uuid::Uuid;

pub struct Triplestore {
    deduplicated: bool,
    pub(crate) caching_folder: Option<String>,
    df_map: HashMap<NamedNode, HashMap<(RDFNodeType, RDFNodeType), TripleTable>>,
    transient_df_map: HashMap<NamedNode, HashMap<(RDFNodeType, RDFNodeType), TripleTable>>,
    parser_call: usize,
}

pub struct TripleTable {
    dfs: Option<Vec<DataFrame>>,
    df_paths: Option<Vec<String>>,
    unique: bool,
    call_uuid: String,
}

impl TripleTable {
    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        if let Some(dfs) = &self.dfs {
            Ok(vec![concat_df(dfs).unwrap().lazy()])
        } else if let Some(paths) = &self.df_paths {
            let lf_results: Vec<Result<LazyFrame, ParquetIOError>> =
                paths.par_iter().map(scan_parquet).collect();
            let mut lfs = vec![];
            for lfr in lf_results {
                lfs.push(lfr.map_err(TriplestoreError::ParquetIOError)?);
            }
            Ok(lfs)
        } else {
            panic!("TripleTable in invalid state")
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
    subject_type: RDFNodeType,
    object_type: RDFNodeType,
}

impl Triplestore {
    pub fn new(caching_folder: Option<String>) -> Result<Triplestore, TriplestoreError> {
        if let Some(caching_folder) = &caching_folder {
            let path = Path::new(caching_folder);
            create_folder_if_not_exists(path)?;
            delete_tmp_parquets_in_caching_folder(path)?;
        }
        Ok(Triplestore {
            df_map: HashMap::new(),
            transient_df_map: HashMap::new(),
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
        deduplicate_map(&mut self.df_map, &self.caching_folder)?;
        deduplicate_map(&mut self.transient_df_map, &self.caching_folder)?;
        self.deduplicated = true;
        debug!("Deduplication took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }

    pub fn add_triples_vec(
        &mut self,
        mut ts: Vec<TriplesToAdd>,
        call_uuid: &String,
        transient: bool,
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
                    &subject_type,
                    &object_type,
                    static_verb_column,
                    has_unique_subset,
                )
            })
            .collect();
        let dfs_to_add = flatten(df_vecs_to_add);
        self.add_triples_df(dfs_to_add, call_uuid, transient, false)?;
        Ok(())
    }

    fn add_triples_df(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &String,
        transient: bool,
        overwrite: bool,
    ) -> Result<(), TriplestoreError> {
        // if !overwrite {
        //     triples_df = self.subtract_from_transient(triples_df, call_uuid, transient)?;
        // }
        if self.caching_folder.is_some() {
            self.add_triples_df_with_caching_folder(triples_df, call_uuid, transient, overwrite)?;
        } else {
            self.add_triples_df_without_folder(triples_df, call_uuid, transient, overwrite);
        }
        Ok(())
    }

    fn add_triples_df_with_caching_folder(
        &mut self,
        mut triples_df: Vec<TripleDF>,
        call_uuid: &String,
        transient: bool,
        overwrite: bool,
    ) -> Result<(), TriplestoreError> {
        let folder_path = Path::new(self.caching_folder.as_ref().unwrap());
        let file_paths: Vec<_> = triples_df
            .par_drain(..)
            .map(|tdf| {
                let TripleDF {
                    mut df,
                    predicate,
                    subject_type,
                    object_type,
                } = tdf;
                let file_name = format!(
                    "tmp_{}_{}{}.parquet",
                    property_to_filename(predicate.as_str()),
                    Uuid::new_v4(),
                    if transient { "_transient" } else { "" }
                );
                let mut file_path_buf = folder_path.to_path_buf();
                file_path_buf.push(file_name);
                let file_path = file_path_buf.as_path();
                (
                    file_path.to_str().unwrap().to_string(),
                    write_parquet(&mut df, file_path),
                    predicate,
                    subject_type,
                    object_type,
                )
            })
            .collect();

        let use_map = if transient {
            &mut self.transient_df_map
        } else {
            &mut self.df_map
        };

        for (file_path, res, predicate, subject_type, object_type) in file_paths {
            res.map_err(TriplestoreError::ParquetIOError)?;
            let k = (subject_type, object_type);
            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&k) {
                    if overwrite {
                        v.df_paths = Some(vec![]);
                    }
                    v.df_paths.as_mut().unwrap().push(file_path);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        k,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    );
                }
            } else {
                use_map.insert(
                    predicate,
                    HashMap::from([(
                        k,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    )]),
                );
            }
        }
        Ok(())
    }

    fn add_triples_df_without_folder(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &String,
        transient: bool,
        overwrite: bool,
    ) {
        for TripleDF {
            mut df,
            predicate,
            subject_type,
            object_type,
        } in triples_df
        {
            let use_map = if transient {
                &mut self.transient_df_map
            } else {
                &mut self.df_map
            };
            let mut lf = df.lazy();
            let mut map = HashMap::new();
            map.insert(SUBJECT_COL_NAME.to_string(), subject_type.clone());
            map.insert(OBJECT_COL_NAME.to_string(), object_type.clone());
            for c in map.keys() {
                lf = lf_column_to_categorical(lf, c, &map)
            }
            df = lf.collect().unwrap();
            let k = (subject_type, object_type);

            if let Some(m) = use_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&k) {
                    if overwrite {
                        v.dfs = Some(vec![]);
                    }
                    v.dfs.as_mut().unwrap().push(df);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        k,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    );
                }
            } else {
                use_map.insert(
                    predicate,
                    HashMap::from([(
                        k,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    )]),
                );
            }
        }
    }
    fn subtract_from_transient(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &String,
        transient: bool,
    ) -> Result<Vec<TripleDF>, TriplestoreError> {
        let mut new_triples_df = vec![];
        if transient {
            for tdf in triples_df {
                if let Some(m) = self.df_map.get(&tdf.predicate) {
                    if let Some(SolutionMappings {
                        mappings: lf,
                        rdf_node_types: _,
                    }) = multiple_tt_to_lf(
                        m,
                        None,
                        Some(&tdf.subject_type),
                        Some(&tdf.object_type),
                        None,
                        None,
                    )
                    .map_err(|x| TriplestoreError::SubtractTransientTriplesError(x.to_string()))?
                    {
                        let TripleDF {
                            df,
                            predicate,
                            subject_type,
                            object_type,
                        } = tdf;
                        let join_on = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
                        let df = df
                            .lazy()
                            .join(lf, &join_on, &join_on, JoinArgs::new(JoinType::Anti))
                            .collect()
                            .unwrap();
                        if df.height() > 0 {
                            new_triples_df.push(TripleDF {
                                df,
                                predicate,
                                subject_type,
                                object_type,
                            })
                        }
                    } else {
                        new_triples_df.push(tdf);
                    }
                } else {
                    new_triples_df.push(tdf);
                }
            }
        } else {
            let mut updated_transient_triples_df = vec![];
            for tdf in &triples_df {
                if let Some(m) = self.transient_df_map.get(&tdf.predicate) {
                    if let Some(SolutionMappings {
                        mappings: lf,
                        rdf_node_types: _,
                    }) = multiple_tt_to_lf(
                        m,
                        None,
                        Some(&tdf.subject_type),
                        Some(&tdf.object_type),
                        None,
                        None,
                    )
                    .map_err(|x| TriplestoreError::SubtractTransientTriplesError(x.to_string()))?
                    {
                        let join_on = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
                        let df = lf
                            .join(
                                tdf.df.clone().lazy(),
                                &join_on,
                                &join_on,
                                JoinArgs::new(JoinType::Anti),
                            )
                            .collect()
                            .unwrap();
                        updated_transient_triples_df.push(TripleDF {
                            df,
                            predicate: tdf.predicate.clone(),
                            subject_type: tdf.subject_type.clone(),
                            object_type: tdf.object_type.clone(),
                        });
                    }
                }
            }
            if !updated_transient_triples_df.is_empty() {
                self.add_triples_df(updated_transient_triples_df, call_uuid, true, true)?;
            }
        }
        Ok(new_triples_df)
    }
}

pub fn prepare_triples(
    mut df: DataFrame,
    subject_type: &RDFNodeType,
    object_type: &RDFNodeType,
    static_verb_column: Option<NamedNode>,
    has_unique_subset: bool,
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
            &subject_type,
            &object_type,
            has_unique_subset,
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
                } else {
                    panic!("Predicate: {:?}", any_predicate);
                }
            }
            part = part.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
            if let Some(tdf) = prepare_triples_df(
                part,
                predicate,
                &subject_type,
                &object_type,
                has_unique_subset,
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
    subject_type: &RDFNodeType,
    object_type: &RDFNodeType,
    has_unique_subset: bool,
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
    if !has_unique_subset {
        df = df.unique(None, UniqueKeepStrategy::First, None).unwrap();
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
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}

fn deduplicate_map(
    df_map: &mut HashMap<NamedNode, HashMap<(RDFNodeType, RDFNodeType), TripleTable>>,
    caching_folder: &Option<String>,
) -> Result<(), TriplestoreError> {
    for (predicate, map) in df_map {
        for v in map.values_mut() {
            if !v.unique {
                if caching_folder.is_some() {
                    let lf_results: Vec<Result<LazyFrame, ParquetIOError>> = v
                        .df_paths
                        .as_ref()
                        .unwrap()
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
                    let removed: Vec<Result<(), io::Error>> = v
                        .df_paths
                        .as_ref()
                        .unwrap()
                        .par_iter()
                        .map(|x| remove_file(Path::new(x)))
                        .collect();
                    for r in removed {
                        r.map_err(TriplestoreError::RemoveParquetFileError)?
                    }
                    let paths = split_write_tmp_df(
                        caching_folder.as_ref().unwrap(),
                        unique_df,
                        predicate.as_str(),
                    )
                    .map_err(TriplestoreError::ParquetIOError)?;
                    v.df_paths = Some(paths);
                    v.unique = true;
                } else {
                    let drained: Vec<LazyFrame> = v
                        .dfs
                        .as_mut()
                        .unwrap()
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
                    lf = lf.unique(None, UniqueKeepStrategy::First);
                    v.dfs.as_mut().unwrap().push(lf.collect().unwrap());
                    v.unique = true;
                }
            }
        }
    }
    Ok(())
}
