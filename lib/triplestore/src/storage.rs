use crate::errors::TriplestoreError;
use crate::CreateIndexOptions;
use oxrdf::{Subject, Term};
use parquet_io::{scan_parquet, write_parquet};
use polars::prelude::{col, concat, IdxSize, IntoLazy, LazyFrame, PlSmallStr, UnionArgs};
use polars_core::datatypes::{AnyValue, CategoricalChunked, LogicalType};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{CategoricalOrdering, DataType, Series, SortMultipleOptions};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};

use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;

const OFFSET_STEP: usize = 100;

#[derive(Clone)]
pub(crate) struct Triples {
    pub(crate) unique: bool,
    height_upper_bound: usize,
    call_uuid: String, //TODO: For uniqueness of blank nodes, an optimization
    unsorted: Option<Vec<StoredTriples>>,
    subject_sort: Option<StoredTriples>,
    subject_sparse_index: Option<BTreeMap<String, usize>>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<BTreeMap<String, usize>>,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
}

impl Triples {
    pub(crate) fn deduplicate(
        &mut self,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        if !self.unique {
            if let Some(unsorted) = &mut self.unsorted {
                let lfs_heights: Result<Vec<_>, _> = unsorted
                    .par_drain(..)
                    .map(|x| {
                        let lfs = x.get_lazy_frame(None)?;
                        Ok(lfs)
                    })
                    .collect();
                let lfs: Vec<_> = lfs_heights?
                    .into_iter()
                    .flatten()
                    .map(|(lf, _)| lf)
                    .collect();
                let df = concat(
                    lfs,
                    UnionArgs {
                        parallel: true,
                        rechunk: false,
                        to_supertypes: false,
                        diagonal: false,
                        from_partitioned_ds: false,
                    },
                )
                .unwrap()
                .unique(None, UniqueKeepStrategy::Any)
                .collect()
                .unwrap();
                self.height_upper_bound = df.height();
                unsorted.push(StoredTriples::new(df, caching_folder)?);
            }
            self.unique = true;
        }
        Ok(())
    }
}

impl Triples {
    pub(crate) fn new(
        df: DataFrame,
        unique: bool,
        call_uuid: &str,
        caching_folder: &Option<String>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
    ) -> Result<Self, TriplestoreError> {
        let height_upper_bound = df.height();
        let stored = StoredTriples::new(df, caching_folder)?;
        Ok(Triples {
            height_upper_bound,
            unique,
            call_uuid: call_uuid.to_string(),
            unsorted: Some(vec![stored]),
            subject_sparse_index: None,
            object_sort: None,
            subject_sort: None,
            object_sparse_index: None,
            subject_type,
            object_type,
        })
    }

    pub(crate) fn add_index(
        &mut self,
        create_index_options: CreateIndexOptions,
        object_type: &BaseRDFNodeType,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        if self.height_upper_bound > OFFSET_STEP * 3 && create_index_options.subject_object_sort {
            let mut lfs: Vec<_> = self
                .get_lazy_frames(&None, &None)?
                .into_iter()
                .map(|(lf, _)| lf)
                .collect();
            let mut lf = if !self.unique {
                // We repeat this here to avoid writing to disk
                concat(
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
                .unique(None, UniqueKeepStrategy::Any)
            } else {
                lfs.remove(0)
            };

            lf = cast_col_to_cat(lf, SUBJECT_COL_NAME, true);
            let (mut df, subj_sparse_map) = create_sorted_df_and_sparse_map(lf, SUBJECT_COL_NAME);
            df = cast_col_to_cat(df.lazy(), SUBJECT_COL_NAME, false)
                .collect()
                .unwrap();
            self.subject_sparse_index = Some(subj_sparse_map);
            self.subject_sort = Some(StoredTriples::new(df.clone(), caching_folder)?);

            if object_type.is_iri() || object_type.is_blank_node() {
                lf = cast_col_to_cat(df.lazy(), OBJECT_COL_NAME, true);
                let (mut df, obj_sparse_map) =
                    create_sorted_df_and_sparse_map(lf, OBJECT_COL_NAME);
                df = cast_col_to_cat(df.lazy(), OBJECT_COL_NAME, false)
                    .collect()
                    .unwrap();
                self.object_sparse_index = Some(obj_sparse_map);
                self.object_sort = Some(StoredTriples::new(df, caching_folder)?);
            }
            self.unsorted = None;
        }
        Ok(())
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<Subject>>,
        objects: &Option<Vec<Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if let Some(unsorted) = &self.unsorted {
            let lfs: Result<Vec<_>, _> = unsorted
                .par_iter()
                .map(|x| x.get_lazy_frame(None))
                .collect();
            let lfs: Vec<_> = lfs?.into_iter().flatten().collect();
            return Ok(lfs);
        } else if let Some(subjects) = subjects {
            if let Some(sorted) = &self.subject_sort {
                println!("Got from subjects! {:?}", subjects);
                println!("Objects are.. {:?}", objects);
                let strings: Vec<_> = subjects
                    .iter().filter(|x| {
                    if &self.subject_type == &BaseRDFNodeType::IRI {
                        matches!(x, Subject::NamedNode(_))
                    } else if &self.subject_type == &BaseRDFNodeType::BlankNode {
                        matches!(x, Subject::BlankNode(_))
                    } else {
                        false
                    }
                })
                    .map(|x| match x {
                        Subject::NamedNode(nn) => nn.as_str(),
                        Subject::BlankNode(bl) => bl.as_str(),
                    })
                    .collect();
                let offsets = get_lookup_offsets(
                    strings,
                    self.subject_sparse_index.as_ref().unwrap(),
                    self.height_upper_bound,
                );
                return Ok(sorted.get_lazy_frame(Some(offsets))?);
            }
        } else if let Some(objects) = objects {
            println!("Got from objects! {:?}", objects);
            if let Some(sorted) = &self.object_sort {
                println!("Got from objects! inside stored");

                let strings: Vec<_> = objects
                    .iter()
                    .filter(|x| {
                        if &self.object_type == &BaseRDFNodeType::IRI {
                            matches!(x, Term::NamedNode(_))
                        } else if &self.object_type == &BaseRDFNodeType::BlankNode {
                            matches!(x, Term::BlankNode(_))
                        } else {
                            false
                        }
                    })
                    .map(|x| match x {
                        Term::NamedNode(nn) => nn.as_str(),
                        Term::BlankNode(bl) => bl.as_str(),
                        _ => panic!("Invalid state")
                    })
                    .collect();
                let offsets = get_lookup_offsets(
                    strings,
                    self.object_sparse_index.as_ref().unwrap(),
                    self.height_upper_bound,
                );
                return Ok(sorted.get_lazy_frame(Some(offsets))?);
            }
        }
        Ok(self.subject_sort.as_ref().unwrap().get_lazy_frame(None)?)
    }

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        unique: bool,
        caching_folder: &Option<String>,
    ) -> Result<(), TriplestoreError> {
        if let Some(unsorted) = &mut self.unsorted {
            let new_stored = StoredTriples::new(df, caching_folder)?;
            unsorted.push(new_stored);
            self.unique = false;
        } else if let Some(sorted) = &self.subject_sort {
            let (stored, sparse) = update_column_sorted_index(
                df.clone(),
                unique,
                caching_folder,
                sorted,
                SUBJECT_COL_NAME,
            )?;
            self.subject_sparse_index = Some(sparse);
            self.subject_sort = Some(stored);
            if let Some(sorted) = &self.object_sort {
                let (stored, sparse) = update_column_sorted_index(
                    df,
                    unique,
                    caching_folder,
                    sorted,
                    OBJECT_COL_NAME,
                )?;
                self.object_sparse_index = Some(sparse);
                self.object_sort = Some(stored);
            }
        } else {
            let new_stored = StoredTriples::new(df, caching_folder)?;
            self.unsorted = Some(vec![new_stored]);
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
    fn new(df: DataFrame, caching_folder: &Option<String>) -> Result<Self, TriplestoreError> {
        Ok(if let Some(caching_folder) = &caching_folder {
            StoredTriples::TriplesOnDisk(TriplesOnDisk::new(df, caching_folder)?)
        } else {
            StoredTriples::TriplesInMemory(Box::new(TriplesInMemory::new(df)))
        })
    }

    pub(crate) fn get_lazy_frame(
        &self,
        offsets: Option<Vec<(usize, usize)>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        let (lf, height) = match self {
            StoredTriples::TriplesOnDisk(t) => t.get_lazy_frame()?,
            StoredTriples::TriplesInMemory(t) => t.get_lazy_frame()?,
        };
        if let Some(offsets) = offsets {
            let output: Result<Vec<_>, _> = offsets
                .into_par_iter()
                .map(|(offset, len)| {
                    let lf = lf.clone().slice(offset as i64, len as IdxSize);
                    Ok((lf, len))
                })
                .collect();
            Ok(output?)
        } else {
            Ok(vec![(lf, height)])
        }
    }
}

#[derive(Clone)]
struct TriplesOnDisk {
    height: usize,
    df_path: String,
}

impl TriplesOnDisk {
    fn new(mut df: DataFrame, caching_folder: &String) -> Result<Self, TriplestoreError> {
        let height = df.height();
        let folder_path = Path::new(caching_folder);
        let file_name = format!("tmp_{}.parquet", Uuid::new_v4());
        let mut file_path_buf = folder_path.to_path_buf();
        file_path_buf.push(file_name);
        let file_path = file_path_buf.as_path();
        write_parquet(&mut df, file_path).unwrap();
        Ok(Self {
            height,
            df_path: file_path.to_str().unwrap().to_string(),
        })
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        Ok((
            scan_parquet(&self.df_path).map_err(TriplestoreError::ParquetIOError)?,
            self.height,
        ))
    }
}

#[derive(Clone)]
struct TriplesInMemory {
    df: Option<DataFrame>,
}

impl TriplesInMemory {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self { df: Some(df) }
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        let height = self.df.as_ref().unwrap().height();
        Ok((self.df.as_ref().unwrap().clone().lazy(), height))
    }
}

fn get_lookup_interval(
    trg: &str,
    sparse_map: &BTreeMap<String, usize>,
    height: usize,
) -> (usize, usize) {
    let mut from = 0;
    //Todo: remove this clone..
    let mut range_backwards = sparse_map.range(..trg.to_string());
    while let Some((s, prev)) = range_backwards.next_back() {
        if s != trg {
            from = *prev;
            break;
        }
    }
    //Todo: remove this clone..
    let range_forwards = sparse_map.range(trg.to_string()..);
    let mut to = height;
    for (s, next) in range_forwards {
        if s != trg {
            to = *next;
            break;
        }
    }
    (from, to)
}

fn get_lookup_offsets(
    mut trgs: Vec<&str>,
    sparse_map: &BTreeMap<String, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    trgs.sort();
    let offsets = trgs
        .iter()
        .map(|trg| get_lookup_interval(trg, sparse_map, height));
    let mut out_offsets = vec![];
    let mut last_offset: Option<(usize, usize)> = None;
    for (from, to) in offsets {
        if let Some((last_from, last_to)) = last_offset {
            if from <= last_to {
                last_offset = Some((last_from, to));
            } else {
                out_offsets.push((last_from, last_to));
                last_offset = Some((from, to));
            }
        } else {
            last_offset = Some((from, to));
        }
    }
    out_offsets.push(last_offset.unwrap());
    out_offsets
        .into_iter()
        .map(|(from, to)| (from, to - from))
        .collect()
}

fn update_at_offset(
    cat_chunked: &CategoricalChunked,
    offset: usize,
    sparse_map: &mut BTreeMap<String, usize>,
) {
    let any = cat_chunked.get_any_value(offset).unwrap();
    let s = match any {
        AnyValue::Null => None,
        AnyValue::Categorical(c, rev, _) => Some(rev.get(c).to_string()),
        AnyValue::CategoricalOwned(c, rev, _) => Some(rev.get(c).to_string()),
        _ => panic!(),
    };
    if let Some(s) = s {
        let e = sparse_map.entry(s);
        e.or_insert(offset);
    }
}

fn cast_col_to_cat(mut lf: LazyFrame, c: &str, lexsort: bool) -> LazyFrame {
    lf = lf.with_column(col(c).cast(DataType::Categorical(
        None,
        if lexsort {
            CategoricalOrdering::Lexical
        } else {
            CategoricalOrdering::Physical
        },
    )));
    lf
}

fn update_column_sorted_index(
    df: DataFrame,
    unique: bool,
    caching_folder: &Option<String>,
    stored_triples: &StoredTriples,
    c: &str,
) -> Result<(StoredTriples, BTreeMap<String, usize>), TriplestoreError> {
    let mut lf = df.lazy();
    if !unique {
        lf = lf.unique(None, UniqueKeepStrategy::Any);
    }
    let existing_lfs_heights = stored_triples.get_lazy_frame(None)?;
    let mut existing_lfs: Vec<_> = existing_lfs_heights.into_iter().map(|(lf, _)| lf).collect();
    existing_lfs.push(lf);
    let mut lf = concat(
        existing_lfs,
        UnionArgs {
            parallel: true,
            rechunk: true,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
        },
    )
    .unwrap();
    lf = cast_col_to_cat(lf, c, true);

    let (mut df, sparse_map) = create_sorted_df_and_sparse_map(lf, c);
    df = cast_col_to_cat(df.lazy(), c, false).collect().unwrap();
    let stored = StoredTriples::new(df, caching_folder)?;

    Ok((stored, sparse_map))
}

fn create_sorted_df_and_sparse_map(
    mut lf: LazyFrame,
    c: &str,
) -> (DataFrame, BTreeMap<String, usize>) {
    lf = lf.sort(
        vec![PlSmallStr::from_str(c)],
        SortMultipleOptions {
            descending: vec![false],
            nulls_last: vec![false],
            multithreaded: true,
            maintain_order: false,
            limit: None,
        },
    );
    let df = lf.collect().unwrap();
    let ser = df.column(c).unwrap().as_materialized_series();
    let sparse_map = create_sparse_map(ser);
    (df, sparse_map)
}

fn create_sparse_map(ser: &Series) -> BTreeMap<String, usize> {
    let cat = ser.categorical().unwrap();
    let mut sparse_map = BTreeMap::new();
    let mut current_offset = 0;
    while current_offset < ser.len() {
        update_at_offset(cat, current_offset, &mut sparse_map);
        current_offset += OFFSET_STEP;
    }
    sparse_map
}
