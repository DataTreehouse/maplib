mod deduplication;
mod delete;
pub mod remote_segments;
pub mod so_index;

use crate::errors::TriplestoreError;
use crate::statistics::sketch::SegmentSketch;
use crate::storage::so_index::SubjectObjectIndex;
use crate::IndexingOptions;
use file_io::remote::RemoteLocation;
use file_io::{scan_parquet, FileIOError};
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use polars::prelude::{
    as_struct, col, concat, lit, IdxCa, IdxSize, IntoLazy, LazyFrame, PlSmallStr, UnionArgs,
};
use polars::io::parquet::metadata::FileMetadataRef;
use polars::prelude::{
    ParallelStrategy, ParquetCompression, ParquetReader, ParquetWriter, SerReader,
};
use polars_core::frame::DataFrame;
use polars_core::prelude::{ChunkCompareEq, DataType};
use polars_core::prelude::{
    Column, IntoColumn, Series, SortMultipleOptions, StringChunked, StringChunkedBuilder,
    UInt32Chunked, UInt64Chunked,
};
use polars_core::series::IsSorted;
use polars_core::utils::Container;
use representation::bundle::SerializedFile;
use representation::cats::{
    distinct_codes, extend_codes, CatEncs, Cats, LockedCats, RankMap, OBJECT_RANK_COL_NAME,
    SUBJECT_RANK_COL_NAME,
};
use representation::rdf_to_polars::{default_datetime_type, normalize_datetime_columns};
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use simd_json::prelude::ArrayTrait;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tracing::{debug, instrument, trace, warn};
use utils::bloom::SplitBlockBloom;
use utils::fast_path::{self, FastPath};
use utils::polars::collect_in_memory;
use uuid::Uuid;

const OFFSET_STEP: usize = 100;
/// Row group size of triple tables stored on disk. Lookups through the sparse indices read whole
/// row groups, so smaller groups make point lookups cheaper and full scans slightly slower.
pub(crate) const DISK_ROW_GROUP_SIZE: usize = 16_384;
const SPARSE_INDEX_KEY_COLUMN_NAME: &str = "key";
const SPARSE_INDEX_VALUE_COLUMN_NAME: &str = "value";
/// Bits per key of the per-segment code filters (default 10, about 1% false positives); 0 turns
/// the filters off, both when segments are built and when they are deserialized.
pub const SEGMENT_FILTER_BITS_ENV: &str = "MAPLIB_SEGMENT_FILTER_BITS";
const DEFAULT_SEGMENT_FILTER_BITS: usize = 10;

pub(crate) fn segment_filter_bits() -> usize {
    std::env::var(SEGMENT_FILTER_BITS_ENV)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(DEFAULT_SEGMENT_FILTER_BITS)
}

/// A Bloom filter of the dictionary codes of the subjects (or objects) of a segment. Segments
/// are immutable, so a filter is never updated: deletes leave it a superset, which is still
/// correct, and merges and compaction build new segments with new filters.
pub(crate) type CodeFilter = Arc<SplitBlockBloom>;

/// The dictionary codes of lookup targets, aligned with the targets; None for a target without a
/// code, which is always looked up.
pub(crate) type TargetCodes<'a> = Option<&'a [Option<u32>]>;

/// The number of runs of equal codes of a column of codes without nulls: the number of distinct
/// codes when the column is sorted so that equal codes are adjacent. None if the column does not
/// hold codes.
pub(crate) fn code_runs(c: &Series) -> Option<usize> {
    let ca = c.u32().ok()?;
    if ca.null_count() > 0 {
        return None;
    }
    let mut runs = 0usize;
    let mut prev = None;
    for arr in ca.downcast_iter() {
        for &u in arr.values().as_slice() {
            if prev != Some(u) {
                runs += 1;
                prev = Some(u);
            }
        }
    }
    Some(runs)
}

/// A filter of the codes of a column sorted so that equal codes are adjacent, built from the
/// runs of equal codes (`runs` of them). None if the filters are off or the column does not hold
/// codes.
fn build_code_filter(c: &Series, runs: usize) -> Option<CodeFilter> {
    let bits = segment_filter_bits();
    let ca = c.u32().ok()?;
    if bits == 0 || ca.is_empty() {
        return None;
    }
    let mut f = SplitBlockBloom::new(runs, bits);
    let mut prev = None;
    for u in ca.iter().flatten() {
        if prev != Some(u) {
            f.insert_code(u);
            prev = Some(u);
        }
    }
    Some(Arc::new(f))
}

/// The targets whose code may be in `filter`. Without a filter or codes, all targets.
fn filter_targets<'a, T: ?Sized>(
    targets: &[&'a T],
    codes: TargetCodes,
    filter: &Option<CodeFilter>,
) -> Vec<&'a T> {
    match (codes, filter) {
        (Some(codes), Some(f)) => {
            let kept: Vec<&'a T> = targets
                .iter()
                .zip(codes)
                .filter(|(_, c)| c.is_none_or(|c| f.may_contain_code(c)))
                .map(|(t, _)| *t)
                .collect();
            fast_path::hit_when(kept.len() < targets.len(), FastPath::TableFilterSkip);
            kept
        }
        _ => targets.to_vec(),
    }
}

// const MIN_SIZE_CACHING: usize = 100_000_000; //100MB

#[derive(Clone, Debug)]
struct SparseIndex {
    map: BTreeMap<Arc<String>, usize>,
}

impl SparseIndex {
    pub fn as_data_frame(&self) -> DataFrame {
        let mut key_series_builder =
            StringChunkedBuilder::new(SPARSE_INDEX_KEY_COLUMN_NAME.into(), self.map.len());
        let mut value_u32s = Vec::with_capacity(self.map.len());
        for (k, v) in self.map.iter() {
            key_series_builder.append_value(k.as_str());
            value_u32s.push(*v as u32);
        }
        let keys = key_series_builder.finish().into_column();
        let values = UInt32Chunked::from_vec(SPARSE_INDEX_VALUE_COLUMN_NAME.into(), value_u32s)
            .into_column();
        DataFrame::new(keys.len(), vec![keys, values]).unwrap()
    }

    pub fn from_data_frame(mut df: DataFrame) -> Self {
        df.rechunk_mut();
        let mut map = BTreeMap::new();
        let keys = df
            .column(SPARSE_INDEX_KEY_COLUMN_NAME)
            .unwrap()
            .str()
            .unwrap();
        let values = df
            .column(SPARSE_INDEX_VALUE_COLUMN_NAME)
            .unwrap()
            .u32()
            .unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            map.insert(Arc::new(k.unwrap().to_string()), v.unwrap() as usize);
        }
        Self { map }
    }
}

/// The triples of one graph, by predicate and by subject and object type.
pub type PredicateTriples =
    HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>;

#[derive(Clone)]
pub struct Triples {
    segments: Vec<TriplesSegment>,
    height: usize,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub object_indexing_enabled: bool,
    subject_object_index: Option<SubjectObjectIndex>,
    /// Set in disk storage mode: new segments are stored as parquet files in this folder.
    triples_folder: Option<PathBuf>,
    /// Changes whenever the triples change, for caches of data derived from them.
    generation: u64,
    /// Triples added by small inserts that are not in a sorted segment yet: the subject and
    /// object columns, unsorted, and deduplicated against the segments and each other. Merged
    /// into a segment when it grows past `MAX_BUFFER_HEIGHT` (see `deduplicate_and_insert`), so
    /// that a small update neither sorts nor indexes. Always in memory, also in disk storage
    /// mode. Every read of the triples includes it; reads that need the sorted order of the
    /// segments (the sparse indices) read it whole, as it is small.
    buffer: Option<DataFrame>,
}

static NEXT_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub(crate) fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl Triples {
    /// Changes whenever the triples change: new tables get a new generation, and so do tables
    /// that triples are added to.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The number of triples.
    pub fn height(&self) -> usize {
        self.height
    }

    /// Whether the triples are held in memory, as opposed to parquet files in disk storage mode.
    pub(crate) fn in_memory(&self) -> bool {
        self.triples_folder.is_none()
    }

    /// The sketches of every segment, see [TriplesSegment::sketch], and of the write buffer when
    /// it holds triples (sketched each time: it has at most `MAX_BUFFER_HEIGHT` rows, and the
    /// statistics merged from these are kept per generation); None when a segment has none.
    pub(crate) fn segment_sketches(&self, compute: bool) -> Option<Vec<Arc<SegmentSketch>>> {
        let mut sketches: Vec<Arc<SegmentSketch>> = self
            .segments
            .iter()
            .map(|s| s.sketch(&self.subject_type, &self.object_type, compute))
            .collect::<Option<_>>()?;
        if let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) {
            sketches.push(Arc::new(crate::statistics::sketch_segment(
                buffer,
                &self.subject_type,
                &self.object_type,
            )));
        }
        Some(sketches)
    }

    /// Creates triples from serialized parquet files. In disk storage mode (`triples_folder` set)
    /// the files are copied into the triples folder, otherwise they are read into memory. Files
    /// in object storage with a known `height` (number of triples) are not copied in disk
    /// storage mode, but downloaded when they are first read, see [`remote_segments`].
    #[allow(clippy::too_many_arguments)]
    pub fn from_serialization(
        subject_object_file: SerializedFile<'_>,
        subject_object_index_df: DataFrame,
        object_subject_file: Option<SerializedFile<'_>>,
        object_subject_index_df: Option<DataFrame>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        triples_folder: Option<&Path>,
        height: Option<usize>,
        subject_filter: Option<SplitBlockBloom>,
        object_filter: Option<SplitBlockBloom>,
    ) -> Result<Self, TriplestoreError> {
        let object_indexing_enabled = object_subject_file.is_some();
        // Models serialized before all xsd:dateTime values were stored as UTC instants can hold
        // naive datetime objects, which are read as UTC wall clock times.
        let utc_objects = matches!(
            object_type.default_stored_polars_data_type(),
            DataType::Datetime(..)
        );
        let subject_sort = StoredTriples::from_parquet_file(
            subject_object_file,
            triples_folder,
            height,
            utc_objects,
        )?;
        let object_sort = object_subject_file
            .map(|f| StoredTriples::from_parquet_file(f, triples_folder, height, utc_objects))
            .transpose()?;
        let height = subject_sort.height();
        let mut segment = TriplesSegment {
            height,
            subject_sort: Some(subject_sort),
            subject_sparse_index: Some(SparseIndex::from_data_frame(subject_object_index_df)),
            object_sort,
            object_sparse_index: object_subject_index_df.map(SparseIndex::from_data_frame),
            subject_filter: subject_filter.map(Arc::new),
            object_filter: object_filter.map(Arc::new),
            distinct_subjects: None,
            distinct_objects: None,
            sketch: OnceLock::new(),
        };
        segment.recount_distinct();

        Ok(Self {
            segments: vec![segment],
            height,
            subject_type,
            object_type,
            object_indexing_enabled,
            subject_object_index: None,
            triples_folder: triples_folder.map(|x| x.to_owned()),
            generation: next_generation(),
            buffer: None,
        })
    }

    /// The number of triples.
    pub fn len(&self) -> usize {
        self.height
    }

    pub fn is_empty(&self) -> bool {
        self.height == 0
    }

    pub fn remove_overlapping_triples_from_dataframe(
        &mut self,
        df: &DataFrame,
        cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        if self.subject_object_index.is_none() {
            if let Some(mut index) = self.temporary_lookup_index(df, cats)? {
                return Ok(index.maybe_insert_deduplicate(df, false));
            }
        }
        self.ensure_subject_object_index()?;
        let maybe_df = self
            .subject_object_index
            .as_mut()
            .unwrap()
            .maybe_insert_deduplicate(df, false);
        Ok(maybe_df)
    }

    pub fn get_sparse_indices_as_data_frames(&self, subject_object_sort: bool) -> Vec<DataFrame> {
        let mut sparse_indices = Vec::new();
        for seg in &self.segments {
            if let Some(ind) = seg.get_sparse_index_as_data_frame(subject_object_sort) {
                sparse_indices.push(ind);
            }
        }
        sparse_indices
    }

    pub fn compact(self, cats: &Cats) -> Result<StoredTriples, TriplestoreError> {
        let t_u32s = self.get_codes()?;

        let mut subject_sorts = Vec::new();
        for seg in &self.segments {
            subject_sorts.push(seg.subject_sort.as_ref().unwrap().get_lazy_frame()?);
        }
        if let Some((lf, _)) = self.buffer_lazy_frame() {
            subject_sorts.push(lf);
        }
        let need_sort = subject_sorts.len() > 1;
        let mut lf = if subject_sorts.len() > 1 {
            concat(
                subject_sorts,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    diagonal: false,
                    strict: false,
                    from_partitioned_ds: false,
                    maintain_order: false,
                },
            )
            .unwrap()
        } else {
            subject_sorts.pop().unwrap()
        };

        for (t, u32s) in t_u32s {
            let mut use_cols = vec![];
            if self.subject_type == t {
                use_cols.push(SUBJECT_COL_NAME);
            }
            if self.object_type == t {
                use_cols.push(OBJECT_COL_NAME);
            };
            let rank_map = Arc::new(cats.global_ranks(&u32s, &t));

            for use_col in use_cols {
                if t.is_lang_string() {
                    let rank_map_cloned1 = rank_map.clone();
                    let rank_map_cloned2 = rank_map.clone();
                    lf = lf.with_column(
                        as_struct(vec![
                            col(use_col)
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .map(
                                    move |x| {
                                        Ok(codes_to_ranks(x.u32()?, &rank_map_cloned1)
                                            .into_column())
                                    },
                                    |_, y| Ok(y.clone()),
                                )
                                .alias(LANG_STRING_VALUE_FIELD),
                            col(use_col)
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD)
                                .map(
                                    move |x| {
                                        Ok(codes_to_ranks(x.u32()?, &rank_map_cloned2)
                                            .into_column())
                                    },
                                    |_, y| Ok(y.clone()),
                                )
                                .alias(LANG_STRING_LANG_FIELD),
                        ])
                        .alias(OBJECT_COL_NAME),
                    );
                } else {
                    let rank_map_cloned = rank_map.clone();
                    lf = lf.with_column(
                        col(use_col)
                            .map(
                                move |x| {
                                    Ok(codes_to_ranks(x.u32()?, &rank_map_cloned).into_column())
                                },
                                |_, y| Ok(y.clone()),
                            )
                            .alias(use_col),
                    );
                }
            }
        }
        let sort_options = SortMultipleOptions {
            descending: vec![false],
            nulls_last: vec![false],
            multithreaded: true,
            maintain_order: false,
            limit: None,
        };
        if need_sort {
            if self.object_type.is_lang_string() {
                lf = lf.sort_by_exprs(
                    vec![
                        col(SUBJECT_COL_NAME),
                        col(OBJECT_COL_NAME)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD),
                        col(OBJECT_COL_NAME)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD),
                    ],
                    sort_options,
                );
            } else if self.subject_type.stored_cat() && self.object_type.stored_cat() {
                // Both columns hold u32 codes, which are now in string order: a single packed
                // key sorts much faster than two columns.
                let key = col(SUBJECT_COL_NAME).cast(DataType::UInt64) * lit(1u64 << 32)
                    + col(OBJECT_COL_NAME).cast(DataType::UInt64);
                lf = lf.sort_by_exprs(vec![key], sort_options);
            } else {
                lf = lf.sort(
                    vec![
                        PlSmallStr::from_str(SUBJECT_COL_NAME),
                        PlSmallStr::from_str(OBJECT_COL_NAME),
                    ],
                    sort_options,
                );
            }
        };
        let mut df = collect_in_memory(lf).unwrap();
        if self.subject_type.stored_cat() {
            // The subject codes are now ranks in string order and the triples are sorted by
            // subject, so the codes should be sorted. Telling Polars lets it use its fast paths
            // for sorted data. Codes added later are not in string order, so this only holds
            // for compacted triples; it is checked, as a wrong flag would give wrong results.
            let mut subjects = df
                .column(SUBJECT_COL_NAME)
                .unwrap()
                .as_materialized_series()
                .clone();
            let sorted = {
                let ca = subjects.u32().unwrap();
                ca.null_count() == 0
                    && ca
                        .into_no_null_iter()
                        .zip(ca.into_no_null_iter().skip(1))
                        .all(|(a, b)| a <= b)
            };
            if sorted {
                subjects.set_sorted_flag(IsSorted::Ascending);
                df.with_column(subjects.into_column()).unwrap();
            } else {
                warn!("Compacted triples are not sorted by subject code");
            }
        }
        let st = StoredTriples::TriplesInMemory(Box::new(TriplesInMemory::new(df)));
        Ok(st)
    }

    pub fn get_height(&self) -> usize {
        let mut h = self.buffer_height();
        for s in &self.segments {
            h += s.height;
        }
        h
    }

    /// The number of triples in the write buffer.
    pub(crate) fn buffer_height(&self) -> usize {
        self.buffer.as_ref().map(|b| b.height()).unwrap_or(0)
    }

    /// The write buffer as a lazy frame with its height, if it holds triples.
    fn buffer_lazy_frame(&self) -> Option<(LazyFrame, usize)> {
        self.buffer
            .as_ref()
            .filter(|b| b.height() > 0)
            .map(|b| (b.clone().lazy(), b.height()))
    }

    pub fn get_u32s(&self) -> Result<Vec<(BaseRDFNodeType, HashSet<u32>)>, TriplestoreError> {
        let mut select_cols = Vec::new();
        if self.subject_type.stored_cat() {
            select_cols.push(col(SUBJECT_COL_NAME));
        }
        if self.object_type.stored_cat() {
            select_cols.push(col(OBJECT_COL_NAME));
        }
        if select_cols.is_empty() {
            return Ok(Vec::new());
        }

        let maybe_lfs = self.get_lazy_frame_slices()?;
        if let Some(lfs) = maybe_lfs {
            let mut type_u32s: HashMap<BaseRDFNodeType, HashSet<u32>> = HashMap::new();

            for mut lf in lfs {
                lf = lf.select(select_cols.as_slice());
                let df = lf.collect().map_err(TriplestoreError::LazyLoadError)?;

                if self.subject_type.stored_cat() {
                    if !type_u32s.contains_key(&self.subject_type) {
                        type_u32s.insert(self.subject_type.clone(), HashSet::new());
                    }
                    let u32s_set = type_u32s.get_mut(&self.subject_type).unwrap();
                    let s_u32s = df.column(SUBJECT_COL_NAME).unwrap().u32().unwrap();
                    u32s_set.extend(s_u32s.iter().map(|x| x.unwrap()));
                }
                if self.object_type.stored_cat() {
                    if !type_u32s.contains_key(&self.object_type) {
                        type_u32s.insert(self.object_type.clone(), HashSet::new());
                    }
                    let u32s_set = type_u32s.get_mut(&self.object_type).unwrap();
                    if self.object_type.is_lang_string() {
                        let o_struct = df.column(OBJECT_COL_NAME).unwrap().struct_().unwrap();
                        let o_u32_value_field =
                            o_struct.field_by_name(LANG_STRING_VALUE_FIELD).unwrap();
                        let o_u32_value_u32s = o_u32_value_field.u32().unwrap();
                        let o_u32_langs_field =
                            o_struct.field_by_name(LANG_STRING_LANG_FIELD).unwrap();
                        let o_u32_langs_u32s = o_u32_langs_field.u32().unwrap();
                        u32s_set.extend(
                            o_u32_value_u32s
                                .iter()
                                .chain(o_u32_langs_u32s.iter())
                                .map(|x| x.unwrap()),
                        );
                    } else {
                        let s_u32s = df.column(OBJECT_COL_NAME).unwrap().u32().unwrap();
                        u32s_set.extend(s_u32s.iter().map(|x| x.unwrap()));
                    }
                }
            }
            Ok(type_u32s.into_iter().collect())
        } else {
            Ok(Vec::new())
        }
    }
}

impl Triples {
    /// The distinct (sorted) codes of each categorical type used by the triples.
    pub fn get_codes(&self) -> Result<Vec<(BaseRDFNodeType, Vec<u32>)>, TriplestoreError> {
        let mut type_codes: HashMap<BaseRDFNodeType, Vec<u32>> = HashMap::new();
        let s_cat = self.subject_type.stored_cat();
        let o_cat = self.object_type.stored_cat();
        if !s_cat && !o_cat {
            return Ok(vec![]);
        }
        if let Some(lfs) = self.get_lazy_frame_slices()? {
            for lf in lfs {
                let df = lf.collect().map_err(TriplestoreError::LazyLoadError)?;
                if s_cat {
                    extend_codes(
                        type_codes.entry(self.subject_type.clone()).or_default(),
                        df.column(SUBJECT_COL_NAME).unwrap(),
                    );
                }
                if o_cat {
                    let v = type_codes.entry(self.object_type.clone()).or_default();
                    let o = df.column(OBJECT_COL_NAME).unwrap();
                    if self.object_type.is_lang_string() {
                        let st = o.struct_().unwrap();
                        for f in [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD] {
                            extend_codes(v, &st.field_by_name(f).unwrap().into_column());
                        }
                    } else {
                        extend_codes(v, o);
                    }
                }
            }
        }
        Ok(type_codes
            .into_iter()
            .map(|(t, v)| (t, distinct_codes(v)))
            .collect())
    }
}

impl Triples {
    pub fn new(
        mut df: DataFrame,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        predicate_iri: &NamedNode,
        indexing: &IndexingOptions,
        cats: LockedCats,
        triples_folder: Option<&Path>,
    ) -> Result<Self, TriplestoreError> {
        let object_indexing_enabled =
            can_and_should_index_object(&object_type, predicate_iri, indexing);
        let height = df.height();
        let mut segments = vec![];
        // The triples are already sorted and deduplicated (`add_rank_sort_triples`). The
        // deduplication index, which holds every triple of the table, is only built when more
        // triples are added (`ensure_subject_object_index`), so tables that are only read and
        // queried never hold it.
        df.rechunk_mut();
        let df = without_all_valid_validity(df);
        let cats = cats.read().unwrap();

        let segment = TriplesSegment::new(
            df,
            &subject_type,
            &object_type,
            object_indexing_enabled,
            &cats,
            false,
            triples_folder,
        )?;
        segments.push(segment);

        Ok(Self {
            segments,
            height,
            subject_type,
            object_type,
            object_indexing_enabled,
            subject_object_index: None,
            triples_folder: triples_folder.map(|x| x.to_owned()),
            generation: next_generation(),
            buffer: None,
        })
    }

    pub fn new_compacted(
        mut df: DataFrame,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        object_indexing_enabled: bool,
        cats: LockedCats,
        triples_folder: Option<&Path>,
    ) -> Result<Self, TriplestoreError> {
        df.rechunk_mut();
        let height = df.height();
        let mut segments = vec![];
        // The deduplication index is only built when triples are added.
        let subject_object_index = None;
        let cats = cats.read().unwrap();
        let segment = TriplesSegment::new(
            df,
            &subject_type,
            &object_type,
            object_indexing_enabled,
            &cats,
            true,
            triples_folder,
        )?;
        segments.push(segment);

        Ok(Self {
            segments,
            height,
            subject_type,
            object_type,
            object_indexing_enabled,
            subject_object_index,
            triples_folder: triples_folder.map(|x| x.to_owned()),
            generation: next_generation(),
            buffer: None,
        })
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&NamedOrBlankNode>>,
        objects: &Option<Vec<&Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        self.get_lazy_frames_with_codes(subjects, objects, None, None)
    }

    /// As [Self::get_lazy_frames], with the dictionary codes of the subjects and objects (aligned
    /// with them): segments whose filters reject every target are not read.
    pub(crate) fn get_lazy_frames_with_codes(
        &self,
        subjects: &Option<Vec<&NamedOrBlankNode>>,
        objects: &Option<Vec<&Term>>,
        subject_codes: TargetCodes,
        object_codes: TargetCodes,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        let mut all_sms = vec![];
        for s in &self.segments {
            all_sms.extend(s.get_lazy_frames(
                subjects,
                objects,
                subject_codes,
                object_codes,
                &self.object_type,
            )?);
        }
        // The segments give (at least) the rows of the subjects or objects, and so does the
        // whole buffer.
        all_sms.extend(self.buffer_lazy_frame());
        Ok(all_sms)
    }

    /// Whether some segment has a subject (or object, if `by_object`) filter, so that codes of
    /// lookup targets are worth finding.
    pub(crate) fn has_code_filters(&self, by_object: bool) -> bool {
        self.segments.iter().any(|s| s.code_filter(by_object).is_some())
    }

    /// Whether a lookup of subjects (or objects, if `by_object`) may have to download a segment
    /// from object storage, which a segment filter that leaves it out saves.
    pub(crate) fn has_remote_segments(&self, by_object: bool) -> bool {
        self.segments.iter().any(|s| s.is_remote_only(by_object))
    }

    /// False if the table can not hold a subject (or object, if `by_object`) with `code`: no
    /// segment's filter holds it and the write buffer, which has no filter, is empty.
    pub(crate) fn may_hold_code(&self, code: u32, by_object: bool) -> bool {
        self.buffer_height() > 0 || self.segments_may_hold_code(code, by_object)
    }

    /// False if no segment can hold a subject (or object) with `code`; says nothing about the
    /// write buffer.
    pub(crate) fn segments_may_hold_code(&self, code: u32, by_object: bool) -> bool {
        self.segments.iter().any(|s| s.may_hold_code(code, by_object))
    }

    /// The exact number of distinct subjects (or objects, if `by_object`) of the table, when it
    /// is known: the table has a single segment with a known count and the write buffer is empty.
    pub fn exact_distinct(&self, by_object: bool) -> Option<usize> {
        match self.segments.as_slice() {
            [s] if self.buffer_height() == 0 => s.distinct_count(by_object),
            _ => None,
        }
    }

    /// The subject (or object) filter of the single segment of a compacted table, for
    /// serialization.
    pub(crate) fn code_filter_bytes(&self, by_object: bool) -> Option<Vec<u8>> {
        match self.segments.as_slice() {
            [s] => s.code_filter(by_object).map(|f| f.to_bytes()),
            _ => None,
        }
    }

    /// The largest number of triples that share a subject (`subject`) or an object, or None when
    /// it is not known cheaply: a segment is not in memory, or the column is not dictionary codes.
    /// An upper bound on how many triples a solution can join with on that position, for the
    /// size estimates of joins. Kept per generation of the triples, since it scans the column.
    pub(crate) fn max_rows_per_key(&self, subject: bool) -> Option<usize> {
        type Cache = std::sync::Mutex<HashMap<(u64, bool), Option<usize>>>;
        static CACHE: std::sync::OnceLock<Cache> = std::sync::OnceLock::new();
        let cache = CACHE.get_or_init(Default::default);
        let key = (self.generation, subject);
        if let Some(v) = cache.lock().ok().and_then(|c| c.get(&key).copied()) {
            return v;
        }
        let v = self.compute_max_rows_per_key(subject);
        if let Ok(mut c) = cache.lock() {
            // Generations are never reused, so old entries only take space.
            if c.len() > 100_000 {
                c.clear();
            }
            c.insert(key, v);
        }
        v
    }

    fn compute_max_rows_per_key(&self, subject: bool) -> Option<usize> {
        let column = if subject {
            SUBJECT_COL_NAME
        } else {
            OBJECT_COL_NAME
        };
        let mut total = 0usize;
        for s in &self.segments {
            if s.height == 0 {
                continue;
            }
            // Sorted on the column, so equal keys are next to each other.
            let sorted = if subject {
                s.subject_sort.as_ref()
            } else {
                s.object_sort.as_ref()
            };
            let Some(StoredTriples::TriplesInMemory(mem)) = sorted else {
                return None;
            };
            let df = mem.df.as_ref()?;
            let codes = df.column(column).ok()?.u32().ok()?;
            let (mut max, mut run, mut last) = (0usize, 0usize, None);
            for c in codes.iter() {
                if c.is_some() && c == last {
                    run += 1;
                } else {
                    run = 1;
                    last = c;
                }
                max = max.max(run);
            }
            total = total.saturating_add(max);
        }
        // The write buffer is small and in no order: count its keys.
        if let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) {
            let codes = buffer.column(column).ok()?.u32().ok()?;
            let mut counts: HashMap<u32, usize> = HashMap::new();
            let mut max = 0usize;
            for c in codes.iter().flatten() {
                let n = counts.entry(c).or_insert(0);
                *n += 1;
                max = max.max(*n);
            }
            total = total.saturating_add(max);
        }
        Some(total)
    }

    /// The rows of a single subject with dictionary code `code`, read eagerly when every segment
    /// is in memory: the sparse index range of each segment, filtered on the code. A table that
    /// does not hold the subject gives no frames, so a query plan need not include it. None when
    /// a segment is not in memory, for the caller to fall back to lazy frames.
    pub(crate) fn get_in_memory_subject_rows(
        &self,
        subject: &NamedOrBlankNode,
        code: u32,
    ) -> Result<Option<Vec<DataFrame>>, TriplestoreError> {
        let strings = get_subject_strings(&[subject]);
        let mut out = vec![];
        for s in &self.segments {
            if !s.may_hold_code(code, false) {
                continue;
            }
            let (Some(StoredTriples::TriplesInMemory(mem)), Some(index)) =
                (&s.subject_sort, &s.subject_sparse_index)
            else {
                return Ok(None);
            };
            let Some(df) = mem.df.as_ref() else {
                return Ok(None);
            };
            if s.height == 0 || df.height() == 0 {
                continue;
            }
            let offsets = get_lookup_offsets(&strings, &index.map, s.height);
            let ranges = coalesce_lookup_ranges(offsets, s.height)
                .unwrap_or_else(|| vec![(0, df.height())]);
            for (offset, len) in ranges {
                let slice = df.slice(offset as i64, len);
                let subjects = slice
                    .column(SUBJECT_COL_NAME)
                    .map_err(TriplestoreError::LazyLoadError)?
                    .u32()
                    .map_err(TriplestoreError::LazyLoadError)?;
                // The rows of a subject are next to each other in the subject sorted table, so
                // they are a zero copy slice. A filter would hand a tiny frame to the thread pool.
                let (mut first, mut last, mut n) = (None, 0usize, 0usize);
                for (i, s) in subjects.iter().enumerate() {
                    if s == Some(code) {
                        first.get_or_insert(i);
                        last = i;
                        n += 1;
                    }
                }
                let Some(first) = first else {
                    continue;
                };
                if n == last - first + 1 {
                    out.push(slice.slice(first as i64, n));
                } else {
                    out.push(
                        slice
                            .filter_seq(&subjects.equal(code))
                            .map_err(TriplestoreError::LazyLoadError)?,
                    );
                }
            }
        }
        if let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) {
            let mask = buffer
                .column(SUBJECT_COL_NAME)
                .map_err(TriplestoreError::LazyLoadError)?
                .u32()
                .map_err(TriplestoreError::LazyLoadError)?
                .equal(code);
            if mask.any() {
                out.push(
                    buffer
                        .filter_seq(&mask)
                        .map_err(TriplestoreError::LazyLoadError)?,
                );
            }
        }
        Ok(Some(out))
    }

    /// The (from, to) dictionary codes of the rows whose subject (or object, if `by_object`) is
    /// one of the `targets` (string and code, sorted by string) with the sorted `codes`,
    /// appended to `out`, read from the sparse index ranges of each segment whose filter may
    /// hold one of them. Returns the number of rows read, or None when a segment is not in
    /// memory or, for `by_object`, has no object sort; the caller then reads the table some
    /// other way. Both columns must hold dictionary codes (u32).
    pub(crate) fn in_memory_edges_from(
        &self,
        targets: &[(&str, u32)],
        codes: &[u32],
        by_object: bool,
        out: &mut Vec<(u32, u32)>,
    ) -> Result<Option<usize>, TriplestoreError> {
        let (from_col, to_col) = if by_object {
            (OBJECT_COL_NAME, SUBJECT_COL_NAME)
        } else {
            (SUBJECT_COL_NAME, OBJECT_COL_NAME)
        };
        let mut rows = 0;
        for s in &self.segments {
            let (sort, index) = if by_object {
                (&s.object_sort, &s.object_sparse_index)
            } else {
                (&s.subject_sort, &s.subject_sparse_index)
            };
            let (Some(StoredTriples::TriplesInMemory(mem)), Some(index)) = (sort, index) else {
                return Ok(None);
            };
            let Some(df) = mem.df.as_ref() else {
                return Ok(None);
            };
            if s.height == 0 || df.height() == 0 {
                continue;
            }
            let strings: Vec<&str> = targets
                .iter()
                .filter(|(_, c)| s.may_hold_code(*c, by_object))
                .map(|(t, _)| *t)
                .collect();
            if strings.is_empty() {
                continue;
            }
            let offsets = get_lookup_offsets(&strings, &index.map, s.height);
            let ranges = coalesce_lookup_ranges(offsets, s.height)
                .unwrap_or_else(|| vec![(0, df.height())]);
            let (Ok(from), Ok(to)) = (
                df.column(from_col).and_then(|c| c.u32().cloned()),
                df.column(to_col).and_then(|c| c.u32().cloned()),
            ) else {
                return Ok(None);
            };
            for (offset, len) in ranges {
                rows += len;
                let from = from.slice(offset as i64, len);
                let to = to.slice(offset as i64, len);
                for (f, t) in from.iter().zip(to.iter()) {
                    if let (Some(f), Some(t)) = (f, t) {
                        if codes.binary_search(&f).is_ok() {
                            out.push((f, t));
                        }
                    }
                }
            }
        }
        // The buffer is in no order and has no sparse index, so all of it is scanned.
        if let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) {
            let (Ok(from), Ok(to)) = (
                buffer.column(from_col).and_then(|c| c.u32().cloned()),
                buffer.column(to_col).and_then(|c| c.u32().cloned()),
            ) else {
                return Ok(None);
            };
            rows += buffer.height();
            for (f, t) in from.iter().zip(to.iter()) {
                if let (Some(f), Some(t)) = (f, t) {
                    if codes.binary_search(&f).is_ok() {
                        out.push((f, t));
                    }
                }
            }
        }
        Ok(Some(rows))
    }

    /// The number of segments, and the buffer if it holds triples, each of which is one lookup
    /// in [Self::in_memory_edges_from].
    pub(crate) fn n_segments(&self) -> usize {
        self.segments.len() + usize::from(self.buffer_height() > 0)
    }

    pub(crate) fn get_all_triples_lazy_frames(
        &self,
        subject_sort: bool,
    ) -> Result<Vec<LazyFrame>, TriplestoreError> {
        let mut all_lfs = vec![];
        for s in &self.segments {
            if subject_sort {
                all_lfs.push(s.get_subject_sort_lazy_frame()?);
            } else {
                if let Some(lf) = s.get_object_sort_lazy_frame()? {
                    all_lfs.push(lf);
                }
            }
        }
        // The buffer is in neither order. Serialization compacts the tables first, which merges
        // the buffer into the single segment.
        if subject_sort || self.object_indexing_enabled {
            all_lfs.extend(self.buffer_lazy_frame().map(|(lf, _)| lf));
        }
        Ok(all_lfs)
    }

    pub(crate) fn get_lazy_frame_slices(&self) -> Result<Option<Vec<LazyFrame>>, TriplestoreError> {
        let mut all_lfs = vec![];
        for s in &self.segments {
            let lf_is = s.get_lazy_frames(&None, &None, None, None, &self.object_type)?;
            for (lf, _) in lf_is {
                all_lfs.push(lf);
            }
        }
        all_lfs.extend(self.buffer_lazy_frame().map(|(lf, _)| lf));
        if all_lfs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(all_lfs))
        }
    }

    /// The stored subject or object column (`c`) of every segment and of the buffer, as stored: dictionary codes
    /// for IRIs, blank nodes and strings. Segments in memory hand out their column without
    /// copying or planning a query; segments on disk read just that column.
    pub(crate) fn stored_columns(&self, c: &str) -> Result<Vec<Column>, TriplestoreError> {
        let mut out = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            let stored = seg.subject_sort.as_ref().unwrap();
            if let StoredTriples::TriplesInMemory(mem) = stored {
                out.push(mem.df.as_ref().unwrap().column(c).unwrap().clone());
            } else {
                let df = stored
                    .get_lazy_frame()?
                    .select([col(c)])
                    .collect()
                    .map_err(TriplestoreError::LazyLoadError)?;
                out.push(df.column(c).unwrap().clone());
            }
        }
        if let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) {
            out.push(buffer.column(c).unwrap().clone());
        }
        Ok(out)
    }

    pub fn get_lazy_frame_between_subject_strings(
        &self,
        from: &str,
        to: &str,
        global_cats: LockedCats,
    ) -> Result<Option<LazyFrame>, TriplestoreError> {
        let mut use_lfs = vec![];
        for seg in self.segments.iter() {
            if let Some(df) = seg.get_data_frame_between_subject_strings(
                from,
                to,
                global_cats.clone(),
                &self.subject_type,
            )? {
                use_lfs.push(df.lazy());
            }
        }
        if let Some(df) =
            self.buffer_between_subject_strings(from, to, global_cats, &self.subject_type)?
        {
            use_lfs.push(df.lazy());
        }
        if use_lfs.is_empty() {
            Ok(None)
        } else {
            let lf = concat(
                use_lfs,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    diagonal: false,
                    from_partitioned_ds: false,
                    maintain_order: true,
                    strict: false,
                },
            )
            .unwrap();
            Ok(Some(lf))
        }
    }

    /// The rows of the buffer whose subject string is in `[from, to]`.
    fn buffer_between_subject_strings(
        &self,
        from: &str,
        to: &str,
        global_cats: LockedCats,
        subject_type: &BaseRDFNodeType,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let Some(buffer) = self.buffer.as_ref().filter(|b| b.height() > 0) else {
            return Ok(None);
        };
        let subjects = global_cats
            .read()?
            .decode_of_type(
                buffer
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .as_materialized_series(),
                subject_type,
                None,
            )
            .map_err(TriplestoreError::DecodeError)?;
        let mask: polars_core::prelude::BooleanChunked = subjects
            .str()
            .unwrap()
            .iter()
            .map(|s| s.map(|s| from <= s && s <= to))
            .collect();
        if !mask.any() {
            return Ok(None);
        }
        Ok(Some(
            buffer
                .filter_seq(&mask)
                .map_err(TriplestoreError::LazyLoadError)?,
        ))
    }

    #[instrument(skip_all)]
    pub(crate) fn add_triples(
        &mut self,
        mut df: DataFrame,
        global_cats: LockedCats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        df.rechunk_mut_par();

        let total_now = Instant::now();
        let global_cats = global_cats.read()?;
        let df = self.deduplicate_and_insert(df, global_cats.deref())?;
        debug!(
            "Deduplicate and insert took {}",
            total_now.elapsed().as_secs_f32()
        );
        Ok(df)
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct TriplesSegment {
    height: usize,
    subject_sort: Option<StoredTriples>,
    subject_sparse_index: Option<SparseIndex>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<SparseIndex>,
    /// The codes of the subjects, when they are dictionary codes and the filters are on.
    subject_filter: Option<CodeFilter>,
    /// The codes of the objects, when there is an object sparse index over dictionary codes.
    object_filter: Option<CodeFilter>,
    /// The exact number of distinct subjects, when the subjects are dictionary codes; None when
    /// unknown (e.g. after a delete from a segment on disk).
    distinct_subjects: Option<usize>,
    /// The exact number of distinct objects, when the segment has an object sort over
    /// dictionary codes; None when unknown.
    distinct_objects: Option<usize>,
    /// The sketches of the segment's columns for the table statistics, made when the segment
    /// is created from triples in memory, or when they are first needed for a segment read
    /// from storage ([TriplesSegment::sketch]).
    sketch: OnceLock<Arc<SegmentSketch>>,
}

impl TriplesSegment {
    /// The exact number of distinct subjects (or objects, if `by_object`), if known.
    pub(crate) fn distinct_count(&self, by_object: bool) -> Option<usize> {
        if by_object {
            self.distinct_objects
        } else {
            self.distinct_subjects
        }
    }

    /// Counts the distinct subjects and objects of a segment read from storage, from the sorts
    /// held in memory; unknown for the sorts that are not.
    fn recount_distinct(&mut self) {
        self.distinct_subjects = self
            .subject_sort
            .as_ref()
            .and_then(|s| s.in_memory_code_runs(SUBJECT_COL_NAME));
        self.distinct_objects = self
            .object_sort
            .as_ref()
            .and_then(|s| s.in_memory_code_runs(OBJECT_COL_NAME));
    }

    fn code_filter(&self, by_object: bool) -> Option<&CodeFilter> {
        if by_object {
            self.object_filter.as_ref()
        } else {
            self.subject_filter.as_ref()
        }
    }

    /// Whether the sort that lookups of subjects (or objects, if `by_object`) read is in object
    /// storage and not downloaded yet.
    fn is_remote_only(&self, by_object: bool) -> bool {
        let sort = if by_object {
            &self.object_sort
        } else {
            &self.subject_sort
        };
        sort.as_ref().is_some_and(|s| s.is_remote_only())
    }

    /// False if the segment holds no subject (or object, if `by_object`) with `code`.
    #[inline]
    pub(crate) fn may_hold_code(&self, code: u32, by_object: bool) -> bool {
        let may = self.code_filter(by_object).is_none_or(|f| f.may_contain_code(code));
        fast_path::hit_when(!may, FastPath::SegmentFilterSkip);
        may
    }

    pub(crate) fn get_sparse_index_as_data_frame(
        &self,
        subject_object_sort: bool,
    ) -> Option<DataFrame> {
        let use_index = if subject_object_sort {
            self.subject_sparse_index.as_ref().unwrap()
        } else {
            if let Some(object_sparse_index) = &self.object_sparse_index {
                object_sparse_index
            } else {
                return None;
            }
        };
        Some(use_index.as_data_frame())
    }
}

impl TriplesSegment {
    pub fn get_data_frame_between_subject_strings(
        &self,
        from: &str,
        to: &str,
        global_cats: LockedCats,
        subject_type: &BaseRDFNodeType,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let subject_index = self.subject_sparse_index.as_ref().unwrap();
        // The range of the iteration is exclusive, therefore the first hit is before from.
        let mut range_backwards = subject_index.map.range(..from.to_string()).rev();
        let mut from_i = if let Some((_, prev)) = range_backwards.next() {
            *prev
        } else {
            0
        };
        // The range of this iteration is inclusive, therefore we must keep going until s is bigger than to.
        let range_forwards = subject_index.map.range(to.to_string()..);
        let mut to_i = None;
        for (s, next) in range_forwards {
            if to < s.as_str() {
                to_i = Some(*next);
                break;
            }
        }
        let mut to_i = to_i.unwrap_or(self.height);
        // At this point the true range is between from_i and to_i
        let height = to_i.saturating_sub(from_i);
        if height > 0 {
            let lf = self.get_subject_sort_lazy_frame()?;
            let lf_subj = lf.clone().select([col(SUBJECT_COL_NAME)]);
            let subjects_start = global_cats
                .read()?
                .decode_of_type(
                    collect_in_memory(
                        lf_subj
                            .clone()
                            .slice(from_i as i64, (OFFSET_STEP * 2) as u32),
                    )
                    .unwrap()
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .as_materialized_series(),
                    subject_type,
                    None,
                )
                .map_err(TriplestoreError::DecodeError)?;

            to_i = to_i.saturating_sub(OFFSET_STEP * 2);
            // The to_i may be exactly at the sparse index, so without + 1 we may miss it.
            let subjects_end = global_cats
                .read()?
                .decode_of_type(
                    collect_in_memory(lf_subj.slice(to_i as i64, (OFFSET_STEP * 2 + 1) as u32))
                        .unwrap()
                        .column(SUBJECT_COL_NAME)
                        .unwrap()
                        .as_materialized_series(),
                    subject_type,
                    None,
                )
                .map_err(TriplestoreError::DecodeError)?;

            // case exact:
            // from = "c"
            // ["a", "b", "c"]
            // then if starting at 0
            // we should stop when from_i == 2 and "c" == "c" i.e. from == s

            // case non_exact:
            // from = "c"
            // ["a", "b", "d"]
            // then if starting at 0
            // we should stop when from_i == 2 and "c" < "d", i.e. from < s
            for s in subjects_start.str().unwrap().iter() {
                let s = s.unwrap();
                if from <= s {
                    break;
                }
                from_i += 1;
            }
            // to = "c"
            // ["a", "b", "c", "d"]
            // then if starting at 0
            // we should stop when i == 3 and "c" < "d" i.e. to < s
            // to_i is exclusive and should be set to 3

            for s in subjects_end.str().unwrap().iter() {
                let s = s.unwrap();
                if to < s {
                    break;
                }
                to_i += 1;
            }
            assert!(from_i <= to_i);
            let height = to_i.saturating_sub(from_i);
            if height == 0 {
                return Ok(None);
            }

            let ret = collect_in_memory(lf.slice(from_i as i64, height as u32)).unwrap();
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }
}

impl TriplesSegment {
    pub(crate) fn new(
        mut df: DataFrame,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        object_indexing_enabled: bool,
        cats: &Cats,
        compacted: bool,
        triples_folder: Option<&Path>,
    ) -> Result<Self, TriplestoreError> {
        df.rechunk_mut();
        let sketch = crate::statistics::sketch_segment(&df, subject_type, object_type);
        let indexed = create_indices(
            df,
            object_indexing_enabled,
            subject_type,
            object_type,
            cats,
            compacted,
            triples_folder,
        )?;
        let mut segment = indexed.into_segment();
        segment.sketch = OnceLock::from(Arc::new(sketch));
        Ok(segment)
    }

    /// The sketches of the segment's columns. Made from the stored triples when the segment has
    /// none yet (it was read from storage), if `compute` and the triples are in memory.
    pub(crate) fn sketch(
        &self,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        compute: bool,
    ) -> Option<Arc<SegmentSketch>> {
        if let Some(s) = self.sketch.get() {
            return Some(s.clone());
        }
        if !compute || !matches!(self.subject_sort, Some(StoredTriples::TriplesInMemory(_))) {
            return None;
        }
        let df = collect_in_memory(
            self.get_subject_sort_lazy_frame()
                .ok()?
                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]),
        )
        .ok()?;
        let s = Arc::new(crate::statistics::sketch_segment(&df, subject_type, object_type));
        Some(self.sketch.get_or_init(|| s).clone())
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&NamedOrBlankNode>>,
        objects: &Option<Vec<&Term>>,
        subject_codes: TargetCodes,
        object_codes: TargetCodes,
        object_type: &BaseRDFNodeType,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if let Some(subjects) = subjects {
            let subjects = filter_targets(subjects, subject_codes, &self.subject_filter);
            if subjects.is_empty() {
                return Ok(vec![]);
            }
            let strings = get_subject_strings(&subjects);
            let offsets = get_lookup_offsets(
                &strings,
                &self.subject_sparse_index.as_ref().unwrap().map,
                self.height,
            );
            return self
                .subject_sort
                .as_ref()
                .unwrap()
                .get_lazy_frames(coalesce_lookup_ranges(offsets, self.height));
        } else if let Some(objects) = objects {
            if let Some(sorted) = &self.object_sort {
                let allow_pushdown = if let BaseRDFNodeType::Literal(t, _) = object_type {
                    t.as_ref() == xsd::STRING
                } else {
                    true
                };

                let offsets = if allow_pushdown {
                    let objects = filter_targets(objects, object_codes, &self.object_filter);
                    if objects.is_empty() {
                        return Ok(vec![]);
                    }
                    let strings = get_object_strings(&objects);
                    let offsets = get_lookup_offsets(
                        strings.as_slice(),
                        &self.object_sparse_index.as_ref().unwrap().map,
                        self.height,
                    );
                    coalesce_lookup_ranges(offsets, self.height)
                } else {
                    None
                };

                return sorted.get_lazy_frames(offsets);
            }
        }
        self.subject_sort.as_ref().unwrap().get_lazy_frames(None)
    }

    pub fn get_subject_sort_lazy_frame(&self) -> Result<LazyFrame, TriplestoreError> {
        self.subject_sort.as_ref().unwrap().get_lazy_frame()
    }

    pub fn get_object_sort_lazy_frame(&self) -> Result<Option<LazyFrame>, TriplestoreError> {
        if let Some(object_sort) = &self.object_sort {
            Ok(Some(object_sort.get_lazy_frame()?))
        } else {
            Ok(None)
        }
    }
}

struct IndexedTriples {
    subject_sort: StoredTriples,
    subject_sparse_index: SparseIndex,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<SparseIndex>,
    subject_filter: Option<CodeFilter>,
    object_filter: Option<CodeFilter>,
    distinct_subjects: Option<usize>,
    distinct_objects: Option<usize>,
    height: usize,
}

impl IndexedTriples {
    fn into_segment(self) -> TriplesSegment {
        TriplesSegment {
            height: self.height,
            subject_sort: Some(self.subject_sort),
            subject_sparse_index: Some(self.subject_sparse_index),
            object_sort: self.object_sort,
            object_sparse_index: self.object_sparse_index,
            subject_filter: self.subject_filter,
            object_filter: self.object_filter,
            distinct_subjects: self.distinct_subjects,
            distinct_objects: self.distinct_objects,
            sketch: OnceLock::new(),
        }
    }
}

// DF should always be sorted by subjects underlying string
// When object indexing LF should have two u32-cols that can sort the objects and subjects by the (underlying) string.
fn create_indices(
    mut df: DataFrame,
    should_index_by_objects: bool,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    cats: &Cats,
    compacted: bool,
    triples_folder: Option<&Path>,
) -> Result<IndexedTriples, TriplestoreError> {
    assert!(df.height() > 0);
    let now = Instant::now();
    let subject_encs = maybe_get_cat_encs(cats, subj_type);
    //Should already be sorted by subject, object
    let subject_sparse_index = create_sparse_index(
        df.column(SUBJECT_COL_NAME)
            .unwrap()
            .as_materialized_series(),
        subject_encs,
    );
    // Sorted by subject, so equal subjects are adjacent.
    let distinct_subjects = if subj_type.stored_cat() {
        code_runs(df.column(SUBJECT_COL_NAME).unwrap().as_materialized_series())
    } else {
        None
    };
    let subject_filter = distinct_subjects.and_then(|runs| {
        build_code_filter(df.column(SUBJECT_COL_NAME).unwrap().as_materialized_series(), runs)
    });
    trace!(
        "Creating subject sparse map took {} seconds",
        now.elapsed().as_secs_f32()
    );
    let store_now = Instant::now();
    let height = df.height();

    let subject_sort = StoredTriples::new(df.clone(), triples_folder)?;
    trace!("Storing triples took {}", store_now.elapsed().as_secs_f32());
    let mut object_sort = None;
    let mut object_sparse_index = None;
    let mut object_filter = None;
    let mut distinct_objects = None;

    if should_index_by_objects {
        let object_now = Instant::now();
        let sort_by = if compacted {
            if obj_type.is_lang_string() {
                vec![
                    col(OBJECT_COL_NAME)
                        .struct_()
                        .field_by_name(LANG_STRING_VALUE_FIELD),
                    col(OBJECT_COL_NAME)
                        .struct_()
                        .field_by_name(LANG_STRING_LANG_FIELD),
                    col(SUBJECT_COL_NAME),
                ]
            } else {
                vec![col(OBJECT_COL_NAME), col(SUBJECT_COL_NAME)]
            }
        } else {
            vec![col(OBJECT_RANK_COL_NAME), col(SUBJECT_RANK_COL_NAME)]
        };
        df = collect_in_memory(df.lazy().sort_by_exprs(
            sort_by,
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                multithreaded: true,
                maintain_order: false,
                limit: None,
            },
        ))
        .unwrap();
        let object_encs = maybe_get_cat_encs(cats, obj_type);
        let sparse = create_sparse_index(
            df.column(OBJECT_COL_NAME).unwrap().as_materialized_series(),
            object_encs,
        );

        if obj_type.stored_cat() && !obj_type.is_lang_string() {
            let objects = df.column(OBJECT_COL_NAME).unwrap().as_materialized_series();
            distinct_objects = code_runs(objects);
            object_filter = distinct_objects.and_then(|runs| build_code_filter(objects, runs));
        }
        object_sort = Some(StoredTriples::new(df, triples_folder)?);
        object_sparse_index = Some(sparse);
        trace!(
            "Indexing by objects took {}",
            object_now.elapsed().as_secs_f32()
        );
    }
    Ok(IndexedTriples {
        subject_sort,
        subject_sparse_index,
        object_sort,
        object_sparse_index,
        subject_filter,
        object_filter,
        distinct_subjects,
        distinct_objects,
        height,
    })
}

fn maybe_get_cat_encs<'a>(c: &'a Cats, t: &BaseRDFNodeType) -> Option<&'a CatEncs> {
    if t.stored_cat() {
        Some(c.get_cat_encs(t))
    } else {
        None
    }
}

pub fn can_and_should_index_object(
    object_type: &BaseRDFNodeType,
    predicate_iri: &NamedNode,
    indexing: &IndexingOptions,
) -> bool {
    let can_index_object = can_index(object_type);

    if can_index_object {
        if indexing.object_sort_all {
            true
        } else if let Some(object_sort_some) = &indexing.object_sort_some {
            object_sort_some.contains(predicate_iri)
        } else {
            false
        }
    } else {
        false
    }
}

fn can_index(t: &BaseRDFNodeType) -> bool {
    if matches!(t, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
        true
    } else if let BaseRDFNodeType::Literal(l, _) = t {
        l.as_ref() == xsd::STRING
    } else {
        false
    }
}

#[derive(Clone)]
pub enum StoredTriples {
    TriplesInMemory(Box<TriplesInMemory>),
    TriplesOnDisk(Arc<TriplesOnDisk>),
    /// The rows of a table on disk that remain after triples were deleted from it, see
    /// [`delete`].
    TriplesOnDiskView(Box<delete::DiskView>),
}

impl StoredTriples {
    pub fn get_lazy_frame(&self) -> Result<LazyFrame, TriplestoreError> {
        Ok(self.get_lazy_frame_and_height()?.0)
    }

    fn get_lazy_frame_and_height(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        match self {
            StoredTriples::TriplesInMemory(mem) => mem.get_lazy_frame(),
            StoredTriples::TriplesOnDisk(disk) => disk.get_lazy_frame(),
            StoredTriples::TriplesOnDiskView(view) => view.get_lazy_frame(),
        }
    }

    /// Whether reading the triples needs a download from object storage first.
    fn is_remote_only(&self) -> bool {
        match self {
            StoredTriples::TriplesInMemory(_) => false,
            StoredTriples::TriplesOnDisk(disk) => disk.is_remote_only(),
            StoredTriples::TriplesOnDiskView(view) => view.is_remote_only(),
        }
    }

    /// The number of runs of equal codes of column `c` (see [code_runs]), when the triples are
    /// held in memory.
    fn in_memory_code_runs(&self, c: &str) -> Option<usize> {
        match self {
            StoredTriples::TriplesInMemory(mem) => {
                code_runs(mem.df.as_ref()?.column(c).ok()?.as_materialized_series())
            }
            _ => None,
        }
    }

    fn height(&self) -> usize {
        match self {
            StoredTriples::TriplesInMemory(mem) => mem.df.as_ref().unwrap().height(),
            StoredTriples::TriplesOnDisk(disk) => disk.height,
            StoredTriples::TriplesOnDiskView(view) => view.height(),
        }
    }

    fn new(df: DataFrame, triples_folder: Option<&Path>) -> Result<Self, TriplestoreError> {
        let df = df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
        if let Some(triples_folder) = triples_folder {
            Ok(StoredTriples::TriplesOnDisk(Arc::new(TriplesOnDisk::new(
                df,
                triples_folder,
            )?)))
        } else {
            Ok(StoredTriples::TriplesInMemory(Box::new(
                TriplesInMemory::new(df),
            )))
        }
    }

    fn from_parquet_file(
        file: SerializedFile<'_>,
        triples_folder: Option<&Path>,
        height: Option<usize>,
        utc_objects: bool,
    ) -> Result<Self, TriplestoreError> {
        if let Some(triples_folder) = triples_folder {
            let mut on_disk = match (file, height) {
                (SerializedFile::Remote(location), Some(height)) => {
                    TriplesOnDisk::remote(location, height, triples_folder)
                }
                (file, _) => TriplesOnDisk::copy_from(file, triples_folder)?,
            };
            on_disk.utc_objects = utc_objects;
            Ok(StoredTriples::TriplesOnDisk(Arc::new(on_disk)))
        } else {
            let df = match file {
                SerializedFile::Path(file) => scan_parquet(&file)
                    .map_err(TriplestoreError::FileIOError)?
                    .collect()
                    .map_err(TriplestoreError::LazyLoadError)?,
                SerializedFile::InBundle(..) | SerializedFile::Remote(..) => file
                    .read_parquet()
                    .map_err(TriplestoreError::LazyLoadError)?,
            };
            let df = if utc_objects {
                normalize_datetime_columns(df)
            } else {
                df
            };
            Ok(StoredTriples::TriplesInMemory(Box::new(
                TriplesInMemory::new(df),
            )))
        }
    }

    pub(crate) fn get_lazy_frames(
        &self,
        offsets: Option<Vec<(usize, usize)>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        let Some(offsets) = offsets else {
            return Ok(vec![self.get_lazy_frame_and_height()?]);
        };
        if let StoredTriples::TriplesInMemory(mem) = self {
            // Gather the rows of all ranges at once, rather than slicing and concatenating many
            // lazy frames.
            let df = mem.df.as_ref().unwrap();
            let rows: usize = offsets.iter().map(|(_, len)| len).sum();
            let out = if offsets.len() == 1 {
                let (offset, len) = offsets[0];
                df.slice(offset as i64, len)
            } else {
                let mut idx: Vec<IdxSize> = Vec::with_capacity(rows);
                for (offset, len) in &offsets {
                    idx.extend((*offset as IdxSize)..((offset + len) as IdxSize));
                }
                let idx = IdxCa::from_vec(PlSmallStr::EMPTY, idx);
                df.take(&idx).unwrap()
            };
            return Ok(vec![(out.lazy(), rows)]);
        }
        if offsets.is_empty() {
            return Ok(vec![]);
        }
        let disk = match self {
            StoredTriples::TriplesOnDisk(disk) => disk,
            StoredTriples::TriplesOnDiskView(view) => {
                let df = view.read_ranges(&offsets)?;
                let rows = df.height();
                return Ok(vec![(df.lazy(), rows)]);
            }
            StoredTriples::TriplesInMemory(_) => unreachable!(),
        };
        // Read the ranges eagerly: a lazy scan per range plans and opens the file and parses
        // its footer every time, which costs more than reading the few row groups needed.
        let df = disk.read_ranges(&offsets)?;
        let rows = df.height();
        Ok(vec![(df.lazy(), rows)])
    }
}

#[derive(Clone)]
pub struct TriplesInMemory {
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

/// A triple table stored as a parquet file. Shared through an `Arc` between clones of the
/// triplestore; the file is removed when the last reference is dropped. A table in object
/// storage is downloaded to its path when it is read, see [`remote_segments`].
pub struct TriplesOnDisk {
    path: PathBuf,
    height: usize,
    remote: Option<Arc<remote_segments::RemoteSegment>>,
    /// The parquet metadata of the file, read on the first lookup.
    metadata: OnceLock<FileMetadataRef>,
    /// Whether the objects are datetimes to read as UTC instants: tables of models serialized
    /// before all xsd:dateTime values were stored as UTC instants can hold naive datetimes.
    utc_objects: bool,
}

impl TriplesOnDisk {
    fn new_path(triples_folder: &Path) -> PathBuf {
        triples_folder.join(format!("{}.parquet", Uuid::new_v4()))
    }

    fn new(mut df: DataFrame, triples_folder: &Path) -> Result<Self, TriplestoreError> {
        let path = Self::new_path(triples_folder);
        let height = df.height();
        let file = File::create(&path)
            .map_err(|e| TriplestoreError::FileIOError(FileIOError::FileCreateIOError(e)))?;
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Lz4Raw)
            .with_row_group_size(Some(DISK_ROW_GROUP_SIZE))
            .set_parallel(true)
            .finish(&mut df)
            .map_err(|e| TriplestoreError::FileIOError(FileIOError::WriteParquetError(e)))?;
        Ok(Self {
            path,
            height,
            remote: None,
            metadata: OnceLock::new(),
            utc_objects: false,
        })
    }

    fn remote(location: RemoteLocation, height: usize, triples_folder: &Path) -> Self {
        let path = Self::new_path(triples_folder);
        Self {
            remote: Some(remote_segments::RemoteSegment::new(location, path.clone())),
            path,
            height,
            metadata: OnceLock::new(),
            utc_objects: false,
        }
    }

    /// Whether the table is in object storage and has not been downloaded (or was removed
    /// from the cache).
    pub fn is_remote_only(&self) -> bool {
        self.remote.as_ref().is_some_and(|r| !r.is_local())
    }

    fn copy_from(
        file: SerializedFile<'_>,
        triples_folder: &Path,
    ) -> Result<Self, TriplestoreError> {
        let path = Self::new_path(triples_folder);
        file.copy_to(&path)
            .map_err(|e| TriplestoreError::FileIOError(FileIOError::FileCreateIOError(e)))?;
        let mut on_disk = Self {
            path,
            height: 0,
            remote: None,
            metadata: OnceLock::new(),
            utc_objects: false,
        };
        let height_df = on_disk
            .scan()?
            .select([polars::prelude::len()])
            .collect()
            .map_err(TriplestoreError::LazyLoadError)?;
        on_disk.height = height_df
            .columns()
            .first()
            .unwrap()
            .get(0)
            .unwrap()
            .extract::<usize>()
            .unwrap();
        Ok(on_disk)
    }

    /// The path of the local file, downloading it first if the table is in object storage.
    fn local_path(&self) -> Result<&Path, TriplestoreError> {
        if let Some(remote) = &self.remote {
            remote
                .ensure_local()
                .map_err(|e| TriplestoreError::FileIOError(FileIOError::FileCreateIOError(e)))?;
        }
        Ok(&self.path)
    }

    fn scan(&self) -> Result<LazyFrame, TriplestoreError> {
        let lf = scan_parquet(self.local_path()?).map_err(TriplestoreError::FileIOError)?;
        Ok(if self.utc_objects {
            lf.with_column(col(OBJECT_COL_NAME).cast(default_datetime_type()))
        } else {
            lf
        })
    }

    fn open(path: &Path) -> Result<File, TriplestoreError> {
        File::open(path)
            .map_err(|e| TriplestoreError::FileIOError(FileIOError::FileCreateIOError(e)))
    }

    /// Reads the subject and object columns of the rows in `ranges` (`(offset, len)`, sorted and
    /// not overlapping). Only the row groups that overlap the ranges are read, and the file
    /// metadata is parsed once per table.
    fn read_ranges(&self, ranges: &[(usize, usize)]) -> Result<DataFrame, TriplestoreError> {
        let path = self.local_path()?;
        let metadata = if let Some(metadata) = self.metadata.get() {
            metadata.clone()
        } else {
            let mut reader = ParquetReader::new(Self::open(path)?);
            let metadata = reader
                .get_metadata()
                .map_err(TriplestoreError::LazyLoadError)?
                .clone();
            self.metadata.get_or_init(|| metadata).clone()
        };
        let columns = Some(vec![
            SUBJECT_COL_NAME.to_string(),
            OBJECT_COL_NAME.to_string(),
        ]);
        let mut out: Option<DataFrame> = None;
        for &(offset, len) in ranges {
            let mut reader = ParquetReader::new(Self::open(path)?)
                .with_columns(columns.clone())
                .with_slice(Some((offset, len)))
                .read_parallel(ParallelStrategy::None);
            reader.set_metadata(metadata.clone());
            let df = reader.finish().map_err(TriplestoreError::LazyLoadError)?;
            if let Some(out) = &mut out {
                out.vstack_mut_owned(df)
                    .map_err(TriplestoreError::LazyLoadError)?;
            } else {
                out = Some(df);
            }
        }
        let out = out.expect("Lookups read at least one range");
        Ok(if self.utc_objects {
            normalize_datetime_columns(out)
        } else {
            out
        })
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        Ok((self.scan()?, self.height))
    }
}

impl Drop for TriplesOnDisk {
    fn drop(&mut self) {
        if self.remote.is_some() {
            // The cached copy, if any, is removed by the RemoteSegment.
            return;
        }
        if let Err(e) = std::fs::remove_file(&self.path) {
            warn!("Could not remove triples file {:?}: {}", self.path, e);
        }
    }
}

fn get_subject_strings<'a>(subjects: &[&'a NamedOrBlankNode]) -> Vec<&'a str> {
    let mut strings: Vec<_> = subjects
        .iter()
        .map(|x| match *x {
            NamedOrBlankNode::NamedNode(nn) => nn.as_str(),
            NamedOrBlankNode::BlankNode(bl) => bl.as_str(),
        })
        .collect();
    strings.sort_unstable();
    strings
}

fn get_object_strings<'a>(objects: &[&'a Term]) -> Vec<&'a str> {
    #[allow(unreachable_patterns)]
    let mut strings: Vec<_> = objects
        .iter()
        .map(|x| match *x {
            Term::NamedNode(nn) => nn.as_str(),
            Term::BlankNode(bl) => bl.as_str(),
            Term::Literal(l) => l.value(),
            _ => panic!("Invalid state"),
        })
        .collect();
    strings.sort();
    strings
}

/// The row ranges `(offset, len)` that may hold the targets, from the sparse index of a sorted
/// column of `height` rows. Overlapping ranges are merged.
///
/// For a target, the range starts at the indexed row of the last key before the target (or at
/// the start), and ends at the indexed row of the first key after the target (or at the end),
/// inclusive. The targets MUST be sorted. With few targets, each one seeks in the index, at a
/// cost logarithmic in the size of the index. With many, the targets and the keys of the index
/// are walked together, at a cost linear in the number of targets plus the size of the index.
fn get_lookup_offsets(
    trgs: &[&str],
    sparse_map: &BTreeMap<Arc<String>, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    if lookup_offsets_should_seek(trgs.len(), sparse_map.len()) {
        fast_path::hit(FastPath::LookupSeek);
        lookup_offsets_seek(trgs, sparse_map, height)
    } else {
        fast_path::hit(FastPath::LookupWalk);
        lookup_offsets_walk(trgs, sparse_map, height)
    }
}

/// Seeking costs about log2(keys) comparisons per target, walking about one per key.
fn lookup_offsets_should_seek(n_trgs: usize, n_keys: usize) -> bool {
    let log2_keys = (usize::BITS - n_keys.leading_zeros()) as usize;
    n_trgs.saturating_mul(log2_keys) <= n_keys
}

/// [get_lookup_offsets] by two seeks in the index per target.
fn lookup_offsets_seek(
    trgs: &[&str],
    sparse_map: &BTreeMap<Arc<String>, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    use std::ops::Bound::{Excluded, Unbounded};
    let mut out_offsets: Vec<(usize, usize)> = vec![];
    // The keys are `Arc<String>`, which borrows as `String` but not as `str`.
    let mut trg_buf = String::new();
    for trg in trgs {
        trg_buf.clear();
        trg_buf.push_str(trg);
        let from = sparse_map
            .range::<String, _>(..&trg_buf)
            .next_back()
            .map(|(_, offset)| cmp::min(*offset, height))
            .unwrap_or(0);
        let to = sparse_map
            .range::<String, _>((Excluded(&trg_buf), Unbounded))
            .next()
            .map(|(_, offset)| *offset)
            .unwrap_or(height - 1);
        push_lookup_range(&mut out_offsets, from, to);
    }
    finish_lookup_ranges(out_offsets)
}

/// [get_lookup_offsets] by walking the targets and the keys of the index together.
fn lookup_offsets_walk(
    trgs: &[&str],
    sparse_map: &BTreeMap<Arc<String>, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    let mut out_offsets: Vec<(usize, usize)> = vec![];
    let mut keys = sparse_map.iter().peekable();
    // The offset of the last key before the current target.
    let mut before: Option<usize> = None;
    for trg in trgs {
        while let Some((k, offset)) = keys.peek() {
            if k.as_str() < *trg {
                before = Some(**offset);
                keys.next();
            } else {
                break;
            }
        }
        let from = before.map(|x| cmp::min(x, height)).unwrap_or(0);
        // The first key after the target (skipping a key equal to the target).
        let mut after = keys.clone();
        let to = loop {
            match after.next() {
                Some((k, _)) if k.as_str() == *trg => continue,
                Some((_, offset)) => break *offset,
                None => break height - 1,
            }
        };
        push_lookup_range(&mut out_offsets, from, to);
    }
    finish_lookup_ranges(out_offsets)
}

/// Adds the inclusive row range `from..=to`, merged into the last range if they overlap.
fn push_lookup_range(out_offsets: &mut Vec<(usize, usize)>, from: usize, to: usize) {
    // Ranges are exclusive in Polars slice.
    let exclusive_to = to + 1;
    assert!(from <= exclusive_to, "Out of order should never happen");
    if let Some(last) = out_offsets.last_mut() {
        if from <= last.1 {
            last.1 = last.1.max(exclusive_to);
            return;
        }
    }
    out_offsets.push((from, exclusive_to));
}

fn finish_lookup_ranges(out_offsets: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    out_offsets
        .into_iter()
        .map(|(from, to)| (from, to - from))
        .collect()
}

#[cfg(test)]
mod lookup_offsets_tests;

/// Reading more than this many separate ranges of a table is slower than reading ranges with
/// small gaps between them merged.
const MAX_LOOKUP_RANGES: usize = 256;

/// Prepares lookup ranges for reading: returns None if the ranges cover more than half of the
/// `height` rows (reading everything is then cheaper), and otherwise merges the ranges separated
/// by the smallest gaps until there are at most [MAX_LOOKUP_RANGES].
fn coalesce_lookup_ranges(
    mut offsets: Vec<(usize, usize)>,
    height: usize,
) -> Option<Vec<(usize, usize)>> {
    let rows: usize = offsets.iter().map(|(_, len)| len).sum();
    if rows * 2 > height {
        return None;
    }
    if offsets.len() > MAX_LOOKUP_RANGES {
        let mut gaps: Vec<usize> = offsets
            .windows(2)
            .map(|w| w[1].0 - (w[0].0 + w[0].1))
            .collect();
        let k = offsets.len() - MAX_LOOKUP_RANGES;
        let (_, max_merged_gap, _) = gaps.select_nth_unstable(k - 1);
        let max_merged_gap = *max_merged_gap;
        let mut merged: Vec<(usize, usize)> = Vec::with_capacity(MAX_LOOKUP_RANGES + 1);
        let mut n_merged = 0;
        for (offset, len) in offsets.drain(..) {
            if let Some(last) = merged.last_mut() {
                let gap = offset - (last.0 + last.1);
                if gap <= max_merged_gap && n_merged < k {
                    last.1 = offset + len - last.0;
                    n_merged += 1;
                    continue;
                }
            }
            merged.push((offset, len));
        }
        offsets = merged;
    }
    Some(offsets)
}

fn get_u32_at_offset(u32_chunked: &UInt32Chunked, offset: usize) -> Option<u32> {
    let u = u32_chunked.get(offset);
    u
}

fn update_string_index_at_offset(
    u32_chunked: &StringChunked,
    offset: usize,
    sparse_map: &mut BTreeMap<Arc<String>, usize>,
) {
    let u = u32_chunked.get(offset);
    if let Some(s) = u {
        let e = sparse_map.entry(Arc::new(s.to_string()));
        e.or_insert(offset);
    }
}

fn create_sparse_index(ser: &Series, cat_encs: Option<&CatEncs>) -> SparseIndex {
    match cat_encs {
        None => create_sparse_string_index(ser),
        Some(rev_map) => create_sparse_cat_index(ser, rev_map),
    }
}

fn create_sparse_cat_index(ser: &Series, encs: &CatEncs) -> SparseIndex {
    assert!(!ser.is_empty());
    let strch = ser.u32().unwrap();
    let mut sparse_map = BTreeMap::new();
    let mut current_offset = 0;
    let mut offsets = vec![];
    let mut u32s = vec![];
    while current_offset < ser.len() {
        let u = get_u32_at_offset(strch, current_offset);
        offsets.push(current_offset);
        u32s.push(u);
        current_offset += OFFSET_STEP;
    }
    //Ensure that we have both ends
    let final_offset = ser.len() - 1;
    if current_offset != final_offset {
        let u = get_u32_at_offset(strch, final_offset);
        offsets.push(final_offset);
        u32s.push(u);
    }
    if !offsets.is_empty() {
        let strs = encs.maps.decode_batch(&u32s);
        for (offset, s) in offsets.into_iter().zip(strs) {
            let e = sparse_map.entry(Arc::new(s.unwrap().to_string()));
            e.or_insert(offset);
        }
    }

    SparseIndex { map: sparse_map }
}
fn create_sparse_string_index(ser: &Series) -> SparseIndex {
    assert!(!ser.is_empty());
    let strch = ser.str().unwrap();
    let mut sparse_map = BTreeMap::new();
    let mut current_offset = 0;
    while current_offset < ser.len() {
        update_string_index_at_offset(strch, current_offset, &mut sparse_map);
        current_offset += OFFSET_STEP;
    }
    //Ensure that we have both ends
    let final_offset = ser.len() - 1;
    if current_offset != final_offset {
        update_string_index_at_offset(strch, final_offset, &mut sparse_map);
    }
    SparseIndex { map: sparse_map }
}

/// Merges segments into one. The segments hold disjoint sets of triples (new triples are
/// deduplicated against the stored ones before they become a segment), so merging only needs to
/// sort them. `dfs` holds the subject and object columns of each segment. The object sort is
/// created from the merged triples rather than by merging the object sorts of the segments, so
/// these never have to be read.
fn compact_segments(
    dfs: Vec<DataFrame>,
    cats: &Cats,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    object_indexing_enabled: bool,
    triples_folder: Option<&Path>,
) -> Result<TriplesSegment, TriplestoreError> {
    let rank_maps = cats.local_rank_maps(dfs.iter().collect(), subj_type, obj_type);
    let mut dfs = dfs.into_iter();
    let mut df = dfs.next().unwrap();
    for other in dfs {
        df.vstack_mut_owned(other)
            .map_err(TriplestoreError::LazyLoadError)?;
    }
    df.rechunk_mut_par();
    let subject_ranks = rank_maps.get(subj_type).map(|m| {
        map_ranks(
            df.column(SUBJECT_COL_NAME).unwrap(),
            m,
            SUBJECT_RANK_COL_NAME,
        )
    });
    let mut sort_by = vec![];
    if obj_type.is_lang_string() {
        if let Some(subject_ranks) = subject_ranks {
            df.with_column(subject_ranks.into_column()).unwrap();
            sort_by.push(col(SUBJECT_RANK_COL_NAME));
        } else {
            sort_by.push(col(SUBJECT_COL_NAME));
        }
        let o = df
            .column(OBJECT_COL_NAME)
            .unwrap()
            .struct_()
            .unwrap()
            .clone();
        for (field, rank_col) in [
            (LANG_STRING_VALUE_FIELD, LANG_VALUE_RANK_COL_NAME),
            (LANG_STRING_LANG_FIELD, LANG_LANG_RANK_COL_NAME),
        ] {
            let f = o.field_by_name(field).unwrap().into_column();
            if let Some(m) = rank_maps.get(obj_type) {
                df.with_column(map_ranks(&f, m, rank_col).into_column())
                    .unwrap();
            } else {
                df.with_column(f.with_name(rank_col.into())).unwrap();
            }
            sort_by.push(col(rank_col));
        }
    } else {
        let object_ranks = rank_maps
            .get(obj_type)
            .map(|m| map_ranks(df.column(OBJECT_COL_NAME).unwrap(), m, OBJECT_RANK_COL_NAME));
        match (subject_ranks, object_ranks) {
            (Some(s), Some(o)) => {
                // A single packed key sorts much faster than two columns.
                let key: Vec<u64> = s
                    .into_no_null_iter()
                    .zip(o.into_no_null_iter())
                    .map(|(s, o)| ((s as u64) << 32) | o as u64)
                    .collect();
                df.with_column(
                    UInt64Chunked::from_vec(SO_RANK_KEY_COL_NAME.into(), key).into_column(),
                )
                .unwrap();
                df.with_column(s.into_column()).unwrap();
                df.with_column(o.into_column()).unwrap();
                sort_by.push(col(SO_RANK_KEY_COL_NAME));
            }
            (s, o) => {
                if let Some(s) = s {
                    df.with_column(s.into_column()).unwrap();
                    sort_by.push(col(SUBJECT_RANK_COL_NAME));
                } else {
                    sort_by.push(col(SUBJECT_COL_NAME));
                }
                if let Some(o) = o {
                    df.with_column(o.into_column()).unwrap();
                    sort_by.push(col(OBJECT_RANK_COL_NAME));
                } else {
                    sort_by.push(col(OBJECT_COL_NAME));
                }
            }
        }
    }
    let n_sort = sort_by.len();
    let df = df
        .lazy()
        .sort_by_exprs(
            sort_by,
            SortMultipleOptions {
                descending: vec![false; n_sort],
                nulls_last: vec![false; n_sort],
                multithreaded: true,
                maintain_order: false,
                limit: None,
            },
        )
        .collect()
        .map_err(TriplestoreError::LazyLoadError)?;
    // The object sort is created from the rank columns.
    if object_indexing_enabled {
        assert!(
            df.column(SUBJECT_RANK_COL_NAME).is_ok() && df.column(OBJECT_RANK_COL_NAME).is_ok()
        );
    }
    let sketch = crate::statistics::sketch_segment(&df, subj_type, obj_type);
    let indexed = create_indices(
        df,
        object_indexing_enabled,
        subj_type,
        obj_type,
        cats,
        false,
        triples_folder,
    )?;
    let mut segment = indexed.into_segment();
    segment.sketch = OnceLock::from(Arc::new(sketch));
    Ok(segment)
}

const SO_RANK_KEY_COL_NAME: &str = "so_rank_key";
const LANG_VALUE_RANK_COL_NAME: &str = "lang_value_rank";
const LANG_LANG_RANK_COL_NAME: &str = "lang_lang_rank";

fn map_ranks(c: &polars_core::prelude::Column, rank_map: &RankMap, name: &str) -> UInt32Chunked {
    let ranks: Vec<u32> = c
        .u32()
        .unwrap()
        .into_no_null_iter()
        .map(|u| rank_map.get(u).unwrap())
        .collect();
    UInt32Chunked::from_vec(name.into(), ranks)
}

/// The rank of every code, null where the code is null or has no rank.
pub(crate) fn codes_to_ranks(ch: &UInt32Chunked, rank_map: &RankMap) -> UInt32Chunked {
    let mut out: UInt32Chunked = if ch.null_count() == 0 {
        ch.into_no_null_iter().map(|u| rank_map.get(u)).collect()
    } else {
        ch.iter().map(|u| u.and_then(|u| rank_map.get(u))).collect()
    };
    out.rename(ch.name().clone());
    out
}

/// Drops the validity bitmaps of columns without nulls (e.g. codes built from iterators of
/// options), so that the stored triples take the null-free fast paths of Polars kernels.
fn without_all_valid_validity(df: DataFrame) -> DataFrame {
    let height = df.height();
    let columns: Vec<_> = df
        .columns()
        .iter()
        .map(|c| {
            let s = c.as_materialized_series();
            let has_validity = s.chunks().iter().any(|a| a.validity().is_some());
            if s.null_count() == 0 && has_validity && !matches!(s.dtype(), DataType::Struct(_)) {
                let chunks = s.chunks().iter().map(|a| a.with_validity(None)).collect();
                // Safety: the chunks have the dtype of the series, only without validity.
                unsafe {
                    Series::from_chunks_and_dtype_unchecked(s.name().clone(), chunks, s.dtype())
                }
                .into_column()
            } else {
                c.clone()
            }
        })
        .collect();
    DataFrame::new(height, columns).unwrap()
}
