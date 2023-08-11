mod constants;
pub(crate) mod conversion;
pub mod errors;
mod export_triples;
mod io_funcs;
pub mod native_parquet_write;
mod ntriples_write;
pub mod sparql;

use crate::errors::TriplestoreError;
use crate::io_funcs::{create_folder_if_not_exists, delete_tmp_parquets_in_caching_folder};
use log::debug;
use oxrdf::vocab::xsd;
use parquet_io::{
    property_to_filename, read_parquet, split_write_tmp_df, write_parquet, ParquetIOError,
};
use polars::prelude::{concat, IntoLazy, LazyFrame, UnionArgs};
use polars_core::datatypes::AnyValue;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::DataType;
use polars_core::series::Series;
use polars_core::utils::concat_df;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use representation::RDFNodeType;
use std::collections::HashMap;
use std::fs::remove_file;
use std::io;
use std::path::Path;
use std::time::Instant;
use uuid::Uuid;

const LANGUAGE_TAG_COLUMN: &str = "language_tag";

pub struct Triplestore {
    deduplicated: bool,
    pub(crate) caching_folder: Option<String>,
    df_map: HashMap<String, HashMap<RDFNodeType, TripleTable>>,
}

pub struct TripleTable {
    dfs: Option<Vec<DataFrame>>,
    df_paths: Option<Vec<String>>,
    unique: bool,
    call_uuid: String,
    tmp_df: Option<DataFrame>,
}

impl TripleTable {
    pub(crate) fn len(&self) -> usize {
        if let Some(dfs) = &self.dfs {
            dfs.len()
        } else if let Some(paths) = &self.df_paths {
            paths.len()
        } else {
            panic!("TripleTable in invalid state")
        }
    }

    pub(crate) fn get_df(&mut self, idx: usize) -> Result<&DataFrame, TriplestoreError> {
        if let Some(dfs) = &self.dfs {
            Ok(dfs.get(idx).unwrap())
        } else if let Some(paths) = &self.df_paths {
            let tmp_df = read_parquet(paths.get(idx).unwrap())
                .map_err(|x| TriplestoreError::ParquetIOError(x))?
                .collect()
                .unwrap();
            self.tmp_df = Some(tmp_df);
            Ok(self.tmp_df.as_ref().unwrap())
        } else {
            panic!("TripleTable in invalid state")
        }
    }

    pub(crate) fn forget_tmp_df(&mut self) {
        self.tmp_df = None;
    }

    pub(crate) fn get_lazy_frames(&self) -> Result<Vec<LazyFrame>, TriplestoreError> {
        if let Some(dfs) = &self.dfs {
            Ok(vec![concat_df(dfs).unwrap().lazy()])
        } else if let Some(paths) = &self.df_paths {
            let lf_results: Vec<Result<LazyFrame, ParquetIOError>> =
                paths.par_iter().map(|x| read_parquet(x)).collect();
            let mut lfs = vec![];
            for lfr in lf_results {
                lfs.push(lfr.map_err(|x| TriplestoreError::ParquetIOError(x))?);
            }
            Ok(lfs)
        } else {
            panic!("TripleTable in invalid state")
        }
    }
}

pub struct TriplesToAdd {
    pub df: DataFrame,
    pub object_type: RDFNodeType,
    pub language_tag: Option<String>,
    pub static_verb_column: Option<String>,
    pub has_unique_subset: bool,
}

pub struct TripleDF {
    df: DataFrame,
    predicate: String,
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
            deduplicated: true,
            caching_folder,
        })
    }

    pub fn deduplicate(&mut self) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        for (predicate, map) in &mut self.df_map {
            for (_, v) in map {
                if !v.unique {
                    if self.caching_folder.is_some() {
                        let lf_results: Vec<Result<LazyFrame, ParquetIOError>> = v
                            .df_paths
                            .as_ref()
                            .unwrap()
                            .par_iter()
                            .map(|x| read_parquet(x))
                            .collect();
                        let mut lfs = vec![];
                        for lf_res in lf_results {
                            lfs.push(lf_res.map_err(|x| TriplestoreError::ParquetIOError(x))?);
                        }
                        let unique_df = concat(lfs, UnionArgs::default())
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
                            r.map_err(|x| TriplestoreError::RemoveParquetFileError(x))?
                        }
                        let paths = split_write_tmp_df(
                            self.caching_folder.as_ref().unwrap(),
                            unique_df,
                            predicate,
                        )
                        .map_err(|x| TriplestoreError::ParquetIOError(x))?;
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
                        let mut lf = concat(drained.as_slice(), UnionArgs::default()).unwrap();
                        lf = lf.unique(None, UniqueKeepStrategy::First);
                        v.dfs.as_mut().unwrap().push(lf.collect().unwrap());
                        v.unique = true;
                    }
                }
            }
        }
        self.deduplicated = true;
        debug!("Deduplication took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }

    pub fn add_triples_vec(
        &mut self,
        mut ts: Vec<TriplesToAdd>,
        call_uuid: &String,
    ) -> Result<(), TriplestoreError> {
        let df_vecs_to_add: Vec<Vec<TripleDF>> = ts
            .par_drain(..)
            .map(|t| {
                let TriplesToAdd {
                    df,
                    object_type,
                    language_tag,
                    static_verb_column,
                    has_unique_subset,
                } = t;
                let prepared_triples = prepare_triples(
                    df,
                    &object_type,
                    &language_tag,
                    static_verb_column,
                    has_unique_subset,
                );
                prepared_triples
            })
            .collect();
        let dfs_to_add = flatten(df_vecs_to_add);
        self.add_triples_df(dfs_to_add, call_uuid)?;
        Ok(())
    }

    fn add_triples_df(
        &mut self,
        triples_df: Vec<TripleDF>,
        call_uuid: &String,
    ) -> Result<(), TriplestoreError> {
        if let Some(_) = &self.caching_folder {
            self.add_triples_df_with_caching_folder(triples_df, call_uuid)?;
        } else {
            self.add_triples_df_without_folder(triples_df, call_uuid);
        }
        Ok(())
    }

    fn add_triples_df_with_caching_folder(
        &mut self,
        mut triples_df: Vec<TripleDF>,
        call_uuid: &String,
    ) -> Result<(), TriplestoreError> {
        let folder_path = Path::new(self.caching_folder.as_ref().unwrap());
        let file_paths: Vec<(String, Result<_, _>, String, RDFNodeType)> = triples_df
            .par_drain(..)
            .map(|tdf| {
                let TripleDF {
                    mut df,
                    predicate,
                    object_type,
                } = tdf;
                let file_name = format!(
                    "tmp_{}_{}.parquet",
                    property_to_filename(&predicate),
                    Uuid::new_v4()
                );
                let mut file_path_buf = folder_path.to_path_buf();
                file_path_buf.push(file_name);
                let file_path = file_path_buf.as_path();
                (
                    file_path.to_str().unwrap().to_string(),
                    write_parquet(&mut df, &file_path),
                    predicate,
                    object_type,
                )
            })
            .collect();
        for (file_path, res, predicate, object_type) in file_paths {
            res.map_err(|x| TriplestoreError::ParquetIOError(x))?;
            //Safe to assume everything is unique
            if let Some(m) = self.df_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&object_type) {
                    v.df_paths.as_mut().unwrap().push(file_path);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        object_type,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                            tmp_df: None,
                        },
                    );
                }
            } else {
                self.df_map.insert(
                    predicate,
                    HashMap::from([(
                        object_type,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                            tmp_df: None,
                        },
                    )]),
                );
            }
        }
        Ok(())
    }

    fn add_triples_df_without_folder(&mut self, triples_df: Vec<TripleDF>, call_uuid: &String) {
        for TripleDF {
            df,
            predicate,
            object_type,
        } in triples_df
        {
            //Safe to assume everything is unique
            if let Some(m) = self.df_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&object_type) {
                    v.dfs.as_mut().unwrap().push(df);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        object_type,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                            tmp_df: None,
                        },
                    );
                }
            } else {
                self.df_map.insert(
                    predicate,
                    HashMap::from([(
                        object_type,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                            tmp_df: None,
                        },
                    )]),
                );
            }
        }
    }
}

pub fn prepare_triples(
    mut df: DataFrame,
    object_type: &RDFNodeType,
    language_tag: &Option<String>,
    static_verb_column: Option<String>,
    has_unique_subset: bool,
) -> Vec<TripleDF> {
    let now = Instant::now();
    let mut out_df_vec = vec![];
    if df.height() == 0 {
        return vec![];
    }
    if let Some(static_verb_column) = static_verb_column {
        df = df.select(["subject", "object"]).unwrap();
        if let Some(tdf) = prepare_triples_df(
            df,
            static_verb_column,
            object_type,
            language_tag,
            has_unique_subset,
        ) {
            out_df_vec.push(tdf);
        }
    } else {
        let partitions = df.partition_by(["verb"], true).unwrap();
        for mut part in partitions {
            let predicate;
            {
                let any_predicate = part.column("verb").unwrap().get(0);
                if let Ok(AnyValue::Utf8(p)) = any_predicate {
                    predicate = p.to_string();
                } else {
                    panic!()
                }
            }
            part = part.select(["subject", "object"]).unwrap();
            if let Some(tdf) = prepare_triples_df(
                part,
                predicate,
                object_type,
                language_tag,
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
    predicate: String,
    object_type: &RDFNodeType,
    language_tag: &Option<String>,
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
    debug!(
        "Prepare single triple df unique before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );

    if let RDFNodeType::Literal(lit) = object_type {
        if lit.as_ref() == xsd::STRING {
            if let Some(tag) = language_tag {
                let lt_ser = Series::new_empty(LANGUAGE_TAG_COLUMN, &DataType::Utf8)
                    .extend_constant(AnyValue::Utf8(tag), df.height())
                    .unwrap();
                df.with_column(lt_ser).unwrap();
            } else {
                let lt_ser = Series::full_null(LANGUAGE_TAG_COLUMN, df.height(), &DataType::Utf8);
                df.with_column(lt_ser).unwrap();
            }
        }
    }
    //TODO: add polars datatype harmonization here.
    debug!(
        "Prepare single triple df before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );
    Some(TripleDF {
        df,
        predicate,
        object_type: object_type.clone(),
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
