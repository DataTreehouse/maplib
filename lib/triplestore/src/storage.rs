use crate::errors::TriplestoreError;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{
    as_struct, col, concat, lit, Expr, IdxSize, IntoLazy, IpcWriter, LazyFrame, PlSmallStr,
    ScanArgsIpc, UnionArgs,
};
use polars_core::datatypes::{AnyValue, CategoricalChunked, LogicalType};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{
    CategoricalOrdering, CompatLevel, DataType, Series, SortMultipleOptions,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelDrainRange};
use std::cmp;

use crate::IndexingOptions;
use oxrdf::vocab::xsd;
use polars::io::SerWriter;
use polars_core::utils::Container;
use representation::multitype::{lf_column_from_categorical, lf_column_to_categorical};
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::collections::{BTreeMap, HashMap};
use std::fs::{remove_file, File};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const OFFSET_STEP: usize = 100;
const MIN_SIZE_CACHING: usize = 100_000_000; //100MB

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct Triples {
    pub(crate) unique: bool,
    height: usize,
    call_uuid: String, //TODO: For uniqueness of blank nodes, an optimization
    unsorted: Option<Vec<StoredTriples>>,
    indexing_enabled: bool,
    object_indexing_enabled: bool,
    subject_sort: Option<StoredTriples>,
    subject_sparse_index: Option<BTreeMap<String, usize>>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<BTreeMap<String, usize>>,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
}

impl Triples {
    pub(crate) fn deduplicate_and_index(
        &mut self,
        storage_folder: &Option<PathBuf>,
    ) -> Result<(), TriplestoreError> {
        if self.indexing_enabled {
            self.do_indexing_if_enabled(storage_folder)?;
        } else if !self.unique {
            if let Some(unsorted) = &mut self.unsorted {
                let lfs_heights: Result<Vec<_>, _> = unsorted
                    .par_drain(..)
                    .map(|x| {
                        let lfs = x.get_lazy_frames(None)?;
                        Ok(lfs)
                    })
                    .collect();
                let mut lfs: Vec<_> = lfs_heights?
                    .into_iter()
                    .flatten()
                    .map(|(lf, _)| lf)
                    .collect();
                let lf = if lfs.len() > 1 {
                    concat(
                        lfs,
                        UnionArgs {
                            parallel: true,
                            rechunk: true,
                            to_supertypes: false,
                            diagonal: false,
                            from_partitioned_ds: false,
                            maintain_order: false,
                        },
                    )
                    .unwrap()
                } else {
                    lfs.pop().unwrap()
                };
                let df = lf
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .unwrap();
                self.height = df.height();
                unsorted.push(StoredTriples::new(
                    df,
                    &self.subject_type,
                    &self.object_type,
                    storage_folder,
                )?);
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
        storage_folder: &Option<PathBuf>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
    ) -> Result<Self, TriplestoreError> {
        let height = df.height();
        let stored = StoredTriples::new(df, &subject_type, &object_type, storage_folder)?;
        let object_indexing_enabled = can_and_should_index_object(&object_type, verb_iri, indexing);
        let mut triples = Triples {
            height,
            unique,
            call_uuid: call_uuid.to_string(),
            unsorted: Some(vec![stored]),
            indexing_enabled: indexing.enabled,
            subject_sparse_index: None,
            object_sort: None,
            subject_sort: None,
            object_sparse_index: None,
            subject_type,
            object_type,
            object_indexing_enabled,
        };
        triples.do_indexing_if_enabled(storage_folder)?;
        Ok(triples)
    }

    pub(crate) fn add_index(
        &mut self,
        object_type: &BaseRDFNodeType,
        storage_folder: &Option<PathBuf>,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
    ) -> Result<(), TriplestoreError> {
        let object_indexing_enabled = can_and_should_index_object(&object_type, verb_iri, indexing);
        self.indexing_enabled = true;
        self.object_indexing_enabled = object_indexing_enabled;
        self.do_indexing_if_enabled(storage_folder)?;
        Ok(())
    }

    fn do_indexing_if_enabled(
        &mut self,
        storage_folder: &Option<PathBuf>,
    ) -> Result<(), TriplestoreError> {
        if self.indexing_enabled {
            if self.unsorted.is_some() {
                let mut lfs: Vec<_> = self
                    .get_lazy_frames_impl(&None, &None)?
                    .into_iter()
                    .map(|(lf, _)| lf)
                    .collect();
                let lf = if lfs.len() > 1 {
                    concat(
                        lfs,
                        UnionArgs {
                            parallel: true,
                            rechunk: true,
                            to_supertypes: false,
                            diagonal: false,
                            from_partitioned_ds: false,
                            maintain_order: false,
                        },
                    )
                    .unwrap()
                } else {
                    lfs.pop().unwrap()
                };
                let IndexedTriples {
                    subject_sort,
                    subject_sparse_index,
                    object_sort,
                    object_sparse_index,
                    height,
                } = create_indices(
                    lf,
                    self.unique,
                    storage_folder,
                    self.object_indexing_enabled,
                    &self.subject_type,
                    &self.object_type,
                )?;
                self.height = height;
                self.subject_sort = Some(subject_sort);
                self.subject_sparse_index = Some(subject_sparse_index);
                self.object_sort = object_sort;
                self.object_sparse_index = object_sparse_index;
                self.unique = true;
                self.unsorted = None;
            }
        }
        Ok(())
    }

    pub(crate) fn get_lazy_frames_deduplicated(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if !self.unique || (self.indexing_enabled && self.subject_sort.is_none()) {
            panic!("Must deduplicate and/or build index first");
        }
        self.get_lazy_frames_impl(subjects, objects)
    }

    fn get_lazy_frames_impl(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if let Some(unsorted) = &self.unsorted {
            let lfs: Result<Vec<_>, _> = unsorted
                .par_iter()
                .map(|x| x.get_lazy_frames(None))
                .collect();
            let lfs: Vec<_> = lfs?.into_iter().flatten().collect();
            return Ok(lfs);
        } else if let Some(subjects) = subjects {
            if let Some(sorted) = &self.subject_sort {
                let strings: Vec<_> = subjects
                    .iter()
                    .map(|x| match *x {
                        Subject::NamedNode(nn) => nn.as_str(),
                        Subject::BlankNode(bl) => bl.as_str(),
                    })
                    .collect();
                let offsets = get_lookup_offsets(
                    strings,
                    self.subject_sparse_index.as_ref().unwrap(),
                    self.height,
                );
                return Ok(sorted.get_lazy_frames(Some(offsets))?);
            }
        } else if let Some(objects) = objects {
            if let Some(sorted) = &self.object_sort {
                let allow_pushdown = if let BaseRDFNodeType::Literal(t) = &self.object_type {
                    t.as_ref() == xsd::STRING
                } else {
                    true
                };

                let offsets = if allow_pushdown {
                    #[allow(unreachable_patterns)]
                    let strings: Vec<_> = objects
                        .iter()
                        .map(|x| match *x {
                            Term::NamedNode(nn) => nn.as_str(),
                            Term::BlankNode(bl) => bl.as_str(),
                            Term::Literal(l) => l.value(),
                            _ => panic!("Invalid state"),
                        })
                        .collect();
                    let offsets = get_lookup_offsets(
                        strings,
                        self.object_sparse_index.as_ref().unwrap(),
                        self.height,
                    );
                    Some(offsets)
                } else {
                    None
                };
                return Ok(sorted.get_lazy_frames(offsets)?);
            }
        }
        Ok(self.subject_sort.as_ref().unwrap().get_lazy_frames(None)?)
    }

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        storage_folder: &Option<PathBuf>,
    ) -> Result<(), TriplestoreError> {
        if self.indexing_enabled && self.subject_sort.is_some() {
            if let Some(sorted) = self.subject_sort.take() {
                let (stored, height, sparse) = update_column_sorted_index(
                    df.clone(),
                    storage_folder,
                    &sorted,
                    true,
                    &self.subject_type,
                    &self.object_type,
                )?;
                sorted.wipe()?;
                self.subject_sparse_index = Some(sparse);
                self.subject_sort = Some(stored);
                self.height = height;
            } else {
                panic!("Triplestore in invalid state");
            }
            if self.object_indexing_enabled {
                if let Some(sorted) = self.object_sort.take() {
                    let (stored, height, sparse) = update_column_sorted_index(
                        df,
                        storage_folder,
                        &sorted,
                        false,
                        &self.subject_type,
                        &self.object_type,
                    )?;
                    sorted.wipe()?;
                    self.object_sparse_index = Some(sparse);
                    self.object_sort = Some(stored);
                    self.height = height;
                } else {
                    panic!("Triplestore in invalid state");
                }
            }
        } else if let Some(unsorted) = &mut self.unsorted {
            let new_stored =
                StoredTriples::new(df, &self.subject_type, &self.object_type, storage_folder)?;
            unsorted.push(new_stored);
            self.unique = false;
        } else {
            panic!("Triplestore in invalid state");
        }
        Ok(())
    }
}

struct IndexedTriples {
    subject_sort: StoredTriples,
    subject_sparse_index: BTreeMap<String, usize>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<BTreeMap<String, usize>>,
    height: usize,
}

fn create_indices(
    mut lf: LazyFrame,
    unique: bool,
    storage_folder: &Option<PathBuf>,
    should_index_by_objects: bool,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
) -> Result<IndexedTriples, TriplestoreError> {
    lf = sort_indexed_lf(lf, true, false);
    let (df, subj_sparse_map) = create_unique_df_and_sparse_map(lf, true, unique);
    let height = df.height();
    let subject_sparse_index = subj_sparse_map;
    let subject_sort = StoredTriples::new(df.clone(), subj_type, obj_type, storage_folder)?;

    let mut object_sort = None;
    let mut object_sparse_index = None;

    if should_index_by_objects {
        lf = sort_indexed_lf(df.lazy(), false, false);
        //Unique is true due to previous unique operation.
        let (df, obj_sparse_map) = create_unique_df_and_sparse_map(lf, false, true);
        object_sort = Some(StoredTriples::new(df, subj_type, obj_type, storage_folder)?);
        object_sparse_index = Some(obj_sparse_map);
    }
    Ok(IndexedTriples {
        subject_sort,
        subject_sparse_index,
        object_sort,
        object_sparse_index,
        height,
    })
}

fn can_and_should_index_object(
    object_type: &BaseRDFNodeType,
    verb_iri: &NamedNode,
    indexing: &IndexingOptions,
) -> bool {
    let can_index_object = if object_type.is_iri() || object_type.is_blank_node() {
        true
    } else if let BaseRDFNodeType::Literal(l) = object_type {
        l.as_ref() == xsd::STRING
    } else {
        false
    };

    let should_index_by_objects = if can_index_object {
        if indexing.object_sort_all {
            true
        } else if let Some(object_sort_some) = &indexing.object_sort_some {
            object_sort_some.contains(verb_iri)
        } else {
            false
        }
    } else {
        false
    };
    should_index_by_objects
}

#[derive(Clone)]
enum StoredTriples {
    TriplesOnDisk(TriplesOnDisk),
    TriplesInMemory(Box<TriplesInMemory>),
}

impl StoredTriples {
    fn new(
        df: DataFrame,
        subj_type: &BaseRDFNodeType,
        obj_type: &BaseRDFNodeType,
        storage_folder: &Option<PathBuf>,
    ) -> Result<Self, TriplestoreError> {
        if let Some(storage_folder) = &storage_folder {
            if MIN_SIZE_CACHING < df.estimated_size() {
                return Ok(StoredTriples::TriplesOnDisk(TriplesOnDisk::new(
                    df,
                    subj_type,
                    obj_type,
                    storage_folder,
                )?));
            }
        }
        Ok(StoredTriples::TriplesInMemory(Box::new(
            TriplesInMemory::new(df),
        )))
    }

    pub(crate) fn get_lazy_frames(
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

    pub(crate) fn wipe(self) -> Result<(), TriplestoreError> {
        if let Self::TriplesOnDisk(t) = self {
            t.wipe()?
        };
        Ok(())
    }
}

#[derive(Clone)]
struct TriplesOnDisk {
    height: usize,
    df_path: String,
    subj_type: BaseRDFNodeType,
    obj_type: BaseRDFNodeType,
}

impl TriplesOnDisk {
    fn new(
        mut df: DataFrame,
        subj_type: &BaseRDFNodeType,
        obj_type: &BaseRDFNodeType,
        storage_folder: &PathBuf,
    ) -> Result<Self, TriplestoreError> {
        let height = df.height();
        let file_name = format!("tmp_{}.ipc", Uuid::new_v4());
        let mut file_path_buf = storage_folder.clone();
        file_path_buf.push(file_name);
        let file_path = file_path_buf.as_path();

        write_ipc(&mut df, file_path, subj_type, obj_type)?;
        Ok(Self {
            height,
            df_path: file_path.to_str().unwrap().to_string(),
            subj_type: subj_type.clone(),
            obj_type: obj_type.clone(),
        })
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        let p = Path::new(&self.df_path);
        Ok((scan_ipc(p, &self.subj_type, &self.obj_type)?, self.height))
    }

    pub(crate) fn wipe(self) -> Result<(), TriplestoreError> {
        let p = Path::new(&self.df_path);
        remove_file(p).map_err(TriplestoreError::RemoveFileError)?;
        Ok(())
    }
}

fn write_ipc(
    df: &mut DataFrame,
    file_path: &Path,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
) -> Result<(), TriplestoreError> {
    let map = HashMap::from([
        (SUBJECT_COL_NAME.to_string(), subj_type.as_rdf_node_type()),
        (OBJECT_COL_NAME.to_string(), obj_type.as_rdf_node_type()),
    ]);
    *df = lf_column_from_categorical(
        lf_column_from_categorical(df.clone().lazy(), SUBJECT_COL_NAME, &map),
        OBJECT_COL_NAME,
        &map,
    )
    .collect()
    .unwrap();

    let file = File::create(file_path).map_err(|x| TriplestoreError::IPCIOError(x.to_string()))?;
    let mut w = IpcWriter::new(file)
        .with_parallel(true)
        .with_compression(None)
        .with_compat_level(CompatLevel::newest());
    w.finish(df)
        .map_err(|x| TriplestoreError::IPCIOError(x.to_string()))?;
    Ok(())
}

fn scan_ipc(
    file_path: &Path,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
) -> Result<LazyFrame, TriplestoreError> {
    let mut lf = LazyFrame::scan_ipc(
        file_path,
        ScanArgsIpc {
            n_rows: None,
            cache: false,
            rechunk: false,
            row_index: None,
            cloud_options: None,
            hive_options: Default::default(),
            include_file_paths: None,
        },
    )
    .map_err(|x| TriplestoreError::IPCIOError(x.to_string()))?;
    lf = lf_column_to_categorical(
        lf,
        SUBJECT_COL_NAME,
        &subj_type.as_rdf_node_type(),
        CategoricalOrdering::Physical,
    );
    lf = lf_column_to_categorical(
        lf,
        OBJECT_COL_NAME,
        &obj_type.as_rdf_node_type(),
        CategoricalOrdering::Physical,
    );
    Ok(lf)
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
            from = cmp::min(prev.clone(), height);
            break;
        }
    }
    //Todo: remove this clone..
    let range_forwards = sparse_map.range(trg.to_string()..);
    let mut to = height - 1;
    for (s, next) in range_forwards {
        if s != trg {
            to = next.clone();
            break;
        }
    }
    // We correct here since ranges are exclusive in Polars slice
    let exclusive_to = to + 1;
    (from, exclusive_to)
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
    if let Some(last_offset) = last_offset {
        out_offsets.push(last_offset);
    }
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

fn get_col(subject: bool) -> &'static str {
    if subject {
        SUBJECT_COL_NAME
    } else {
        OBJECT_COL_NAME
    }
}

fn sort_indexed_lf(lf: LazyFrame, is_subject: bool, also_other: bool) -> LazyFrame {
    let c = get_col(is_subject);
    let mut lf = cast_col_to_cat(lf, c, true);

    let mut by = vec![PlSmallStr::from_str(c)];
    if also_other {
        let other_c = get_col(!is_subject);
        by.push(PlSmallStr::from_str(&other_c));
    }

    lf = lf.sort(
        by,
        SortMultipleOptions {
            descending: vec![false],
            nulls_last: vec![false],
            multithreaded: true,
            maintain_order: false,
            limit: None,
        },
    );
    lf
}

fn update_column_sorted_index(
    df: DataFrame,
    storage_folder: &Option<PathBuf>,
    stored_triples: &StoredTriples,
    is_subject: bool,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> Result<(StoredTriples, usize, BTreeMap<String, usize>), TriplestoreError> {
    let c = get_col(is_subject);
    let mut lf = sort_indexed_lf(df.lazy(), is_subject, false);
    let existing_lfs_heights = stored_triples.get_lazy_frames(None)?;
    let existing_lfs: Vec<_> = existing_lfs_heights.into_iter().map(|(lf, _)| lf).collect();
    for mut elf in existing_lfs {
        elf = cast_col_to_cat(elf, c, true);

        let other_type = if is_subject {
            object_type
        } else {
            subject_type
        };
        //This exercise is a workaround for a bug in polars with struct and merge_sorted
        if other_type.is_lang_string() {
            let other_c = get_col(!is_subject);
            elf = elf.unnest([other_c]);
            lf = lf.unnest([other_c]);
        }

        lf = lf.merge_sorted(elf, PlSmallStr::from_str(c)).unwrap();

        if other_type.is_lang_string() {
            let other_c = get_col(!is_subject);
            lf = lf
                .with_column(
                    as_struct(vec![
                        col(LANG_STRING_VALUE_FIELD),
                        col(LANG_STRING_LANG_FIELD),
                    ])
                    .alias(other_c),
                )
                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);
        }
    }
    let (df, sparse_map) = create_unique_df_and_sparse_map(lf, is_subject, false);
    let height = df.height();
    let stored = StoredTriples::new(df, subject_type, object_type, storage_folder)?;

    Ok((stored, height, sparse_map))
}

fn repeated_from_last_row_expr(c: &str) -> Expr {
    col(c)
        .shift(lit(1))
        .is_not_null()
        .and(col(c).shift(lit(1)).eq(col(c)))
}

fn create_unique_df_and_sparse_map(
    mut lf: LazyFrame,
    is_subject: bool,
    unique: bool,
) -> (DataFrame, BTreeMap<String, usize>) {
    let c = get_col(is_subject);
    if !unique {
        lf = sort_indexed_lf(lf, is_subject, true);
        let other_c = get_col(!is_subject);
        lf = lf.with_column(
            repeated_from_last_row_expr(c)
                .and(repeated_from_last_row_expr(other_c))
                .alias("is_duplicated"),
        );
        lf = lf.filter(col("is_duplicated").not());
        lf = cast_col_to_cat(lf, c, false);
    }

    lf = lf.select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);

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
    //Ensure that we have both ends
    let final_offset = ser.len() - 1;
    if current_offset != final_offset {
        update_at_offset(cat, final_offset, &mut sparse_map);
    }
    sparse_map
}
