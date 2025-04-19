use crate::errors::TriplestoreError;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{as_struct, col, concat, lit, Expr, IdxSize, IntoLazy, IpcWriter, LazyFrame, PlSmallStr, ScanArgsIpc, UnionArgs};
use polars_core::datatypes::{AnyValue, CategoricalChunked, DataType, LogicalType};
use polars_core::frame::DataFrame;
use polars_core::prelude::{CategoricalOrdering, CompatLevel, Series, SortMultipleOptions};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp;

use crate::IndexingOptions;
use log::debug;
use oxrdf::vocab::xsd;
use polars::io::SerWriter;
use polars_core::utils::{concat_df, Container};
use representation::multitype::{lf_column_from_categorical, lf_column_to_categorical};
use representation::{
    BaseRDFNodeType, IRI_PREFIX_FIELD, IRI_SUFFIX_FIELD, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::{BTreeMap, HashMap};
use std::fs::{remove_file, File};
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;

const OFFSET_STEP: usize = 100;
const MIN_SIZE_CACHING: usize = 100_000_000; //100MB

const EXISTING_COL: &str = "existing";

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct Triples {
    height: usize,
    call_uuid: String, //TODO: For uniqueness of blank nodes, an optimization
    object_indexing_enabled: bool,
    subject_sort: Option<StoredTriples>,
    subject_sparse_index: Option<BTreeMap<String, usize>>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<BTreeMap<String, usize>>,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    unindexed: Vec<DataFrame>,
}

impl Triples {
    pub(crate) fn new(
        df: DataFrame,
        call_uuid: &str,
        storage_folder: &Option<PathBuf>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
        delay_index: bool,
    ) -> Result<Self, TriplestoreError> {
        let object_indexing_enabled = can_and_should_index_object(&object_type, verb_iri, indexing);
        let height = df.height();
        let mut lf = df.lazy();
        lf = cast_col_to_cat(lf, SUBJECT_COL_NAME, false, &subject_type);
        lf = cast_col_to_cat(lf, OBJECT_COL_NAME, false, &object_type);
        if delay_index {
            let triples = Triples {
                height,
                call_uuid: call_uuid.to_owned(),
                object_indexing_enabled,
                subject_sort: None,
                subject_sparse_index: None,
                object_sort: None,
                object_sparse_index: None,
                subject_type,
                object_type,
                unindexed: vec![lf.collect().unwrap()],
            };
            Ok(triples)
        } else {
            let IndexedTriples {
                subject_sort,
                subject_sparse_index,
                object_sort,
                object_sparse_index,
                height,
            } = create_indices(
                lf,
                storage_folder,
                object_indexing_enabled,
                &subject_type,
                &object_type,
            )?;
            let triples = Triples {
                height,
                call_uuid: call_uuid.to_owned(),
                object_indexing_enabled,
                subject_sort: Some(subject_sort),
                subject_sparse_index: Some(subject_sparse_index),
                object_sort,
                object_sparse_index,
                subject_type,
                object_type,
                unindexed: vec![],
            };

            Ok(triples)
        }
    }

    pub(crate) fn is_indexed(&self) -> bool {
        self.subject_sort.is_some() || self.object_sort.is_some()
    }

    pub fn index_unindexed(
        &mut self,
        storage_folder: &Option<PathBuf>,
        df: Option<DataFrame>,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        if let Some(df) = df {
            self.unindexed.push(df);
        }
        if self.unindexed.is_empty() {
            return Ok(None);
        }
        let df = if self.unindexed.len() == 1 {
            self.unindexed.pop().unwrap()
        } else {
            concat_df(self.unindexed.as_slice()).unwrap()
        };
        self.unindexed.clear();
        if self.subject_sort.is_none() {
            let IndexedTriples {
                subject_sort,
                subject_sparse_index,
                object_sort,
                object_sparse_index,
                height,
            } = create_indices(
                df.lazy(),
                storage_folder,
                self.object_indexing_enabled,
                &self.subject_type,
                &self.object_type,
            )?;
            let mut lfs = subject_sort.get_lazy_frames(None)?;
            assert_eq!(lfs.len(), 1);
            let (lf, _) = lfs.pop().unwrap();
            let df = lf.collect().unwrap();

            self.subject_sort = Some(subject_sort);
            self.subject_sparse_index = Some(subject_sparse_index);
            self.object_sort = object_sort;
            self.object_sparse_index = object_sparse_index;
            self.height = height;
            Ok(Some(df))
        } else {
            self.add_triples(df, storage_folder, false)
        }
    }

    pub(crate) fn add_index(
        &mut self,
        object_type: &BaseRDFNodeType,
        storage_folder: &Option<PathBuf>,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
    ) -> Result<(), TriplestoreError> {
        let object_indexing_enabled = can_and_should_index_object(object_type, verb_iri, indexing);
        self.object_indexing_enabled = object_indexing_enabled;
        self.maybe_do_object_indexing(storage_folder)?;
        Ok(())
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        assert!(self.unindexed.is_empty());
        if let Some(subjects) = subjects {
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
            return self
                .subject_sort
                .as_ref()
                .unwrap()
                .get_lazy_frames(Some(offsets));
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
                return sorted.get_lazy_frames(offsets);
            }
        }
        self.subject_sort.as_ref().unwrap().get_lazy_frames(None)
    }

    pub(crate) fn add_triples(
        &mut self,
        mut df: DataFrame,
        storage_folder: &Option<PathBuf>,
        delay_index: bool,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let mut lf = df.lazy();
        lf = cast_col_to_cat(lf, SUBJECT_COL_NAME, false, &self.subject_type);
        lf = cast_col_to_cat(lf, OBJECT_COL_NAME, false, &self.object_type);
        df = lf.collect().unwrap();
        if delay_index {
            assert!(self.subject_sort.is_none());
            self.unindexed.push(df);
            Ok(None)
        } else if !self.unindexed.is_empty() {
            let df = self.index_unindexed(storage_folder, Some(df))?;
            Ok(df)
        } else {
            let height_before = self.height;
            let (stored, height, sparse, new_triples) = update_column_sorted_index(
                df.clone(),
                storage_folder,
                self.subject_sort.as_ref().unwrap(),
                true,
                &self.subject_type,
                &self.object_type,
            )?;
            assert!(height >= height_before);
            self.subject_sort.as_mut().unwrap().wipe()?;
            self.subject_sparse_index = Some(sparse);
            self.subject_sort = Some(stored);
            self.height = height;
            if self.object_indexing_enabled {
                if let Some(mut sorted) = self.object_sort.take() {
                    let (stored, obj_height, sparse, _) = update_column_sorted_index(
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
                    assert_eq!(height, obj_height);
                    self.height = height;
                } else {
                    panic!("Triplestore in invalid state");
                }
            }
            Ok(new_triples)
        }
    }

    fn maybe_do_object_indexing(
        &mut self,
        storage_folder: &Option<PathBuf>,
    ) -> Result<(), TriplestoreError> {
        if self.object_indexing_enabled && self.object_sort.is_none() {
            let mut lfs = self.get_lazy_frames(&None, &None)?;
            assert_eq!(lfs.len(), 1);
            let (lf, _) = lfs.pop().unwrap();
            let (object_sort, obj_map) =
                create_object_index(lf, &self.subject_type, &self.object_type, storage_folder)?;
            self.object_sort = Some(object_sort);
            self.object_sparse_index = Some(obj_map);
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
    lf: LazyFrame,
    storage_folder: &Option<PathBuf>,
    should_index_by_objects: bool,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
) -> Result<IndexedTriples, TriplestoreError> {
    let now = Instant::now();
    let (df, subject_sparse_index) =
        create_unique_df_and_sparse_map(lf, true, true, false, subj_type, obj_type);
    debug!(
        "Creating subject sparse map took {} seconds",
        now.elapsed().as_secs_f32()
    );
    let store_now = Instant::now();
    let height = df.height();
    let subject_sort = StoredTriples::new(df.clone(), subj_type, obj_type, storage_folder)?;
    debug!("Storing triples took {}", store_now.elapsed().as_secs_f32());
    let mut object_sort = None;
    let mut object_sparse_index = None;

    if should_index_by_objects {
        let object_now = Instant::now();
        let (new_object_sort, new_object_sparse_index) =
            create_object_index(df.lazy(), subj_type, obj_type, storage_folder)?;
        object_sort = Some(new_object_sort);
        object_sparse_index = Some(new_object_sparse_index);
        debug!(
            "Indexing by objects took {}",
            object_now.elapsed().as_secs_f32()
        );
    }
    Ok(IndexedTriples {
        subject_sort,
        subject_sparse_index,
        object_sort,
        object_sparse_index,
        height,
    })
}

fn create_object_index(
    df: LazyFrame,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    storage_folder: &Option<PathBuf>,
) -> Result<(StoredTriples, BTreeMap<String, usize>), TriplestoreError> {
    let lf = sort_indexed_lf(df.lazy(), false, false, false, subj_type, obj_type);
    // No need to deduplicate as subject index creation has deduplicated
    let (df, obj_sparse_map) =
        create_unique_df_and_sparse_map(lf, false, false, false, subj_type, obj_type);
    let object_sort = StoredTriples::new(df, subj_type, obj_type, storage_folder)?;
    let object_sparse_index = obj_sparse_map;
    Ok((object_sort, object_sparse_index))
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

    if can_index_object {
        if indexing.object_sort_all {
            true
        } else if let Some(object_sort_some) = &indexing.object_sort_some {
            object_sort_some.contains(verb_iri)
        } else {
            false
        }
    } else {
        false
    }
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

    pub(crate) fn wipe(&mut self) -> Result<(), TriplestoreError> {
        if let Self::TriplesOnDisk(t) = self {
            t.wipe()?
        };
        Ok(())
    }

    pub(crate) fn get_height(&self) -> usize {
        match self {
            StoredTriples::TriplesOnDisk(t) => {t.get_height()}
            StoredTriples::TriplesInMemory(t) => {t.get_height()}
        }
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

    pub(crate) fn wipe(&mut self) -> Result<(), TriplestoreError> {
        let p = Path::new(&self.df_path);
        remove_file(p).map_err(TriplestoreError::RemoveFileError)?;
        Ok(())
    }

    pub(crate) fn get_height(&self) -> usize {
        self.height
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

    pub(crate) fn get_height(&self) -> usize {
        self.df.as_ref().unwrap().height()
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
            from = cmp::min(*prev, height);
            break;
        }
    }
    //Todo: remove this clone..
    let range_forwards = sparse_map.range(trg.to_string()..);
    let mut to = height - 1;
    for (s, next) in range_forwards {
        if s != trg {
            to = *next;
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

fn update_cat_at_offset(
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

fn update_iri_at_offset(
    prefix: &CategoricalChunked,
    suffix: &CategoricalChunked,
    offset: usize,
    sparse_map: &mut BTreeMap<String, usize>,
) {
    let any_prefix = prefix.get_any_value(offset).unwrap();
    let any_suffix = suffix.get_any_value(offset).unwrap();

    let p = match any_prefix {
        AnyValue::Null => None,
        AnyValue::Categorical(c, rev, _) => Some(rev.get(c).to_string()),
        AnyValue::CategoricalOwned(c, rev, _) => Some(rev.get(c).to_string()),
        _ => panic!(),
    };
    let s = match any_suffix {
        AnyValue::Null => None,
        AnyValue::Categorical(c, rev, _) => Some(rev.get(c).to_string()),
        AnyValue::CategoricalOwned(c, rev, _) => Some(rev.get(c).to_string()),
        _ => panic!(),
    };
    if let (Some(p), Some(s)) = (p, s) {
        let iri = format!("{}{}", p, s);
        let e = sparse_map.entry(iri);
        e.or_insert(offset);
    }
}

fn cast_col_to_cat(mut lf: LazyFrame, c: &str, lexsort: bool, t: &BaseRDFNodeType) -> LazyFrame {
    let cat_order = if lexsort {
        CategoricalOrdering::Lexical
    } else {
        CategoricalOrdering::Physical
    };
    if t.is_iri() {
        lf = lf.with_column(
            as_struct(vec![
                col(c)
                    .struct_()
                    .field_by_name(IRI_PREFIX_FIELD)
                    .cast(DataType::Categorical(None, cat_order)),
                col(c)
                    .struct_()
                    .field_by_name(IRI_SUFFIX_FIELD)
                    .cast(DataType::Categorical(None, cat_order)),
            ])
            .alias(c),
        );
    } else if t.is_lang_string() {
        lf = lf.with_column(
            as_struct(vec![
                col(c)
                    .struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .cast(DataType::Categorical(None, cat_order)),
                col(c)
                    .struct_()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .cast(DataType::Categorical(None, cat_order)),
            ])
            .alias(c),
        );
    } else if t.polars_data_type() == DataType::String {
        lf = lf.with_column(col(c).cast(DataType::Categorical(None, cat_order)));
    }

    lf
}

fn get_col(subject: bool) -> &'static str {
    if subject {
        SUBJECT_COL_NAME
    } else {
        OBJECT_COL_NAME
    }
}

fn get_type<'a>(
    subject: bool,
    subject_type: &'a BaseRDFNodeType,
    object_type: &'a BaseRDFNodeType,
) -> &'a BaseRDFNodeType {
    if subject {
        subject_type
    } else {
        object_type
    }
}

fn sort_indexed_lf(
    lf: LazyFrame,
    is_subject: bool,
    also_other: bool,
    sort_on_existing: bool,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> LazyFrame {
    let c = get_col(is_subject);
    let t = get_type(is_subject, subject_type, object_type);
    let mut lf = cast_col_to_cat(lf, c, true, t);

    let mut by = get_sort_exprs(is_subject, subject_type, object_type);
    let mut descending = vec![];
    for _ in 0..by.len() {
        descending.push(false);
    }
    if also_other {
        let sort_exprs = get_sort_exprs(!is_subject, subject_type, object_type);
        for _ in 0..sort_exprs.len() {
            descending.push(false);
        }
        by.extend(sort_exprs);
    }
    if sort_on_existing {
        by.push(col(EXISTING_COL));
        descending.push(true);
    }

    lf = lf.sort_by_exprs(
        by,
        SortMultipleOptions {
            descending,
            nulls_last: vec![false],
            multithreaded: true,
            maintain_order: false,
            limit: None,
        },
    );
    lf
}

fn get_sort_exprs(
    is_subject: bool,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> Vec<Expr> {
    let mut exprs = vec![];
    let c = get_col(is_subject);
    let t = get_type(is_subject, subject_type, object_type);
    if t.is_iri() {
        exprs.push(col(c).struct_().field_by_name(IRI_PREFIX_FIELD));
        exprs.push(col(c).struct_().field_by_name(IRI_SUFFIX_FIELD));
    } else if t.is_lang_string() {
        exprs.push(col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD));
        exprs.push(col(c).struct_().field_by_name(LANG_STRING_LANG_FIELD));
    } else {
        exprs.push(col(c));
    }
    exprs
}

fn update_column_sorted_index(
    df: DataFrame,
    storage_folder: &Option<PathBuf>,
    stored_triples: &StoredTriples,
    is_subject: bool,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> Result<
    (
        StoredTriples,
        usize,
        BTreeMap<String, usize>,
        Option<DataFrame>,
    ),
    TriplestoreError,
> {
    let height_before = stored_triples.get_height();
    let c = get_col(is_subject);
    let t = get_type(is_subject, subject_type, object_type);
    let mut lf = sort_indexed_lf(
        df.lazy(),
        is_subject,
        false,
        false,
        subject_type,
        object_type,
    );
    let existing_lfs_heights = stored_triples.get_lazy_frames(None)?;
    let existing_lfs: Vec<_> = existing_lfs_heights.into_iter().map(|(lf, _)| lf).collect();
    assert_eq!(existing_lfs.len(), 1);
    let sort_on_existing = is_subject;
    if sort_on_existing {
        lf = lf.with_column(lit(false).alias(EXISTING_COL));
    }

    for mut elf in existing_lfs {
        elf = cast_col_to_cat(elf, c, true, t);

        if sort_on_existing {
            elf = elf.with_column(lit(true).alias(EXISTING_COL));
        }

        //lf = lf.merge_sorted(elf, PlSmallStr::from_str(c)).unwrap();
        lf = concat(
            [lf, elf],
            UnionArgs {
                parallel: true,
                rechunk: false,
                to_supertypes: false,
                diagonal: true,
                from_partitioned_ds: false,
                maintain_order: false,
            },
        ).unwrap();
    }

    let (df, sparse_map) = create_unique_df_and_sparse_map(
        lf,
        is_subject,
        true,
        sort_on_existing,
        subject_type,
        object_type,
    );
    let height = df.height();
    assert!(height_before <= height);
    let new_triples = if sort_on_existing {
        let new_triples = df
            .clone()
            .lazy()
            .filter(col(EXISTING_COL).not())
            .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
            .collect()
            .unwrap();
        if new_triples.height() > 0 {
            Some(new_triples)
        } else {
            None
        }
    } else {
        None
    };

    let stored = StoredTriples::new(df, subject_type, object_type, storage_folder)?;

    Ok((stored, height, sparse_map, new_triples))
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
    deduplicate: bool,
    sort_on_existing: bool,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> (DataFrame, BTreeMap<String, usize>) {
    let deduplicate_now = Instant::now();
    let c = get_col(is_subject);
    if deduplicate {
        lf = sort_indexed_lf(
            lf,
            is_subject,
            true,
            sort_on_existing,
            subject_type,
            object_type,
        );
        let other_c = get_col(!is_subject);
        lf = lf.with_column(
            repeated_from_last_row_expr(c)
                .and(repeated_from_last_row_expr(other_c))
                .alias("is_duplicated"),
        );
        lf = lf.filter(col("is_duplicated").not());
        let t = get_type(is_subject, subject_type, object_type);
        lf = cast_col_to_cat(lf, c, false, t);
    }
    let mut cols = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];

    if sort_on_existing {
        cols.push(col(EXISTING_COL));
    }

    lf = lf.select(cols);

    let df = lf.collect().unwrap();

    debug!(
        "Creating deduplicated df took {} seconds",
        deduplicate_now.elapsed().as_secs_f32()
    );
    let sparse_now = Instant::now();
    let ser = df.column(c).unwrap().as_materialized_series();
    let t = get_type(is_subject, subject_type, object_type);
    let sparse_map = create_sparse_map(ser, t);
    debug!(
        "Creating sparse map took {} seconds",
        sparse_now.elapsed().as_secs_f32()
    );
    (df, sparse_map)
}

fn create_sparse_map(ser: &Series, t: &BaseRDFNodeType) -> BTreeMap<String, usize> {
    let mut iri_prefix_series = None;
    let mut iri_suffix_series = None;
    let mut cat_series = None;
    if t.is_iri() {
        let st = ser.struct_().unwrap();
        iri_prefix_series = Some(st.field_by_name(IRI_PREFIX_FIELD).unwrap());
        iri_suffix_series = Some(st.field_by_name(IRI_SUFFIX_FIELD).unwrap());
    } else {
        cat_series = Some(ser.categorical().unwrap());
    }
    let mut sparse_map = BTreeMap::new();
    let mut current_offset = 0;
    while current_offset < ser.len() {
        if let Some(cat_series) = &cat_series {
            update_cat_at_offset(*cat_series, current_offset, &mut sparse_map);
        } else {
            update_iri_at_offset(
                iri_prefix_series.as_ref().unwrap().categorical().unwrap(),
                iri_suffix_series.as_ref().unwrap().categorical().unwrap(),
                current_offset,
                &mut sparse_map,
            );
        }
        current_offset += OFFSET_STEP;
    }
    //Ensure that we have both ends
    let final_offset = ser.len() - 1;
    if current_offset != final_offset {
        if let Some(cat_series) = &cat_series {
            update_cat_at_offset(*cat_series, final_offset, &mut sparse_map);
        } else {
            update_iri_at_offset(
                iri_prefix_series.as_ref().unwrap().categorical().unwrap(),
                iri_suffix_series.as_ref().unwrap().categorical().unwrap(),
                final_offset,
                &mut sparse_map,
            );
        }
    }
    sparse_map
}
