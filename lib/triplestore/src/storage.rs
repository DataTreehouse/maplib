mod deduplication;
pub mod so_index;

use crate::errors::TriplestoreError;
use crate::storage::so_index::SubjectObjectIndex;
use crate::IndexingOptions;
use file_io::{scan_parquet, FileIOError};
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use polars::prelude::{
    as_struct, col, concat, IdxSize, IntoLazy, LazyFrame, PlSmallStr, UnionArgs,
};
use polars::prelude::{ParquetCompression, ParquetWriter};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    IntoColumn, Series, SortMultipleOptions, StringChunked, StringChunkedBuilder, UInt32Chunked,
    UInt64Chunked,
};
use polars_core::utils::Container;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{
    CatEncs, Cats, LockedCats, OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME,
};
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
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument, trace, warn};
use uuid::Uuid;

const OFFSET_STEP: usize = 100;
/// Row group size of triple tables stored on disk. Lookups through the sparse indices read whole
/// row groups, so smaller groups make point lookups cheaper and full scans slightly slower.
pub(crate) const DISK_ROW_GROUP_SIZE: usize = 16_384;
const SPARSE_INDEX_KEY_COLUMN_NAME: &str = "key";
const SPARSE_INDEX_VALUE_COLUMN_NAME: &str = "value";

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
}

impl Triples {
    /// Creates triples from serialized parquet files. In disk storage mode (`triples_folder` set)
    /// the files are copied into the triples folder, otherwise they are read into memory.
    pub fn from_serialization(
        subject_object_file: &Path,
        subject_object_index_df: DataFrame,
        object_subject_file: Option<&Path>,
        object_subject_index_df: Option<DataFrame>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        triples_folder: Option<&Path>,
    ) -> Result<Self, TriplestoreError> {
        let object_indexing_enabled = object_subject_file.is_some();
        let subject_sort = StoredTriples::from_parquet_file(subject_object_file, triples_folder)?;
        let object_sort = object_subject_file
            .map(|f| StoredTriples::from_parquet_file(f, triples_folder))
            .transpose()?;
        let height = subject_sort.height();
        let segment = TriplesSegment {
            height,
            subject_sort: Some(subject_sort),
            subject_sparse_index: Some(SparseIndex::from_data_frame(subject_object_index_df)),
            object_sort,
            object_sparse_index: object_subject_index_df.map(SparseIndex::from_data_frame),
        };

        Ok(Self {
            segments: vec![segment],
            height,
            subject_type,
            object_type,
            object_indexing_enabled,
            subject_object_index: None,
            triples_folder: triples_folder.map(|x| x.to_owned()),
        })
    }

    pub fn remove_overlapping_triples_from_dataframe(
        &mut self,
        df: &DataFrame,
        cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        if self.subject_object_index.is_none() && self.triples_folder.is_some() {
            return Ok(self
                .subject_lookup_index(df, cats)?
                .maybe_insert_deduplicate(df, false));
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
        let t_u32s = self.get_u32s()?;

        let mut subject_sorts = Vec::new();
        for seg in &self.segments {
            subject_sorts.push(seg.subject_sort.as_ref().unwrap().get_lazy_frame()?);
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
            let rank_map = Arc::new(cats.rank_map(&u32s, &t));

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
                                        let rank_map = rank_map_cloned1.clone();
                                        let u32s = UInt32Chunked::from_iter(
                                            x.u32()?
                                                .iter()
                                                .map(|x| rank_map.get(&x.unwrap()).copied()),
                                        );
                                        Ok(u32s.into_column())
                                    },
                                    |_, y| Ok(y.clone()),
                                )
                                .alias(LANG_STRING_VALUE_FIELD),
                            col(use_col)
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD)
                                .map(
                                    move |x| {
                                        let rank_map = rank_map_cloned2.clone();
                                        let u32s = UInt32Chunked::from_iter(
                                            x.u32()?
                                                .iter()
                                                .map(|x| rank_map.get(&x.unwrap()).copied()),
                                        );
                                        Ok(u32s.into_column())
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
                                    let rank_map = rank_map_cloned.clone();
                                    let u32s = UInt32Chunked::from_iter(
                                        x.u32()?.iter().map(|x| rank_map.get(&x.unwrap()).copied()),
                                    );
                                    Ok(u32s.into_column())
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
        let df = lf.collect().unwrap();
        let st = StoredTriples::TriplesInMemory(Box::new(TriplesInMemory::new(df)));
        Ok(st)
    }

    pub fn get_height(&self) -> usize {
        let mut h = 0;
        for s in &self.segments {
            h += s.height;
        }
        h
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
        let mut subject_object_index = SubjectObjectIndex::new(&subject_type, &object_type);
        let maybe_df = subject_object_index.maybe_insert_deduplicate(&df, true);
        df = maybe_df.unwrap();
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
            subject_object_index: Some(subject_object_index),
            triples_folder: triples_folder.map(|x| x.to_owned()),
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
        // In disk storage mode the deduplication index is only built when triples are added.
        let subject_object_index = if triples_folder.is_none() {
            let mut subject_object_index = SubjectObjectIndex::new(&subject_type, &object_type);
            subject_object_index.insert(&df);
            Some(subject_object_index)
        } else {
            None
        };
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
        })
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&NamedOrBlankNode>>,
        objects: &Option<Vec<&Term>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        let mut all_sms = vec![];
        for s in &self.segments {
            all_sms.extend(s.get_lazy_frames(
                subjects,
                objects,
                &self.subject_type,
                &self.object_type,
            )?);
        }
        Ok(all_sms)
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
        Ok(all_lfs)
    }

    pub(crate) fn get_lazy_frame_slices(&self) -> Result<Option<Vec<LazyFrame>>, TriplestoreError> {
        let mut all_lfs = vec![];
        for s in &self.segments {
            let lf_is = s.get_lazy_frames(&None, &None, &self.subject_type, &self.object_type)?;
            for (lf, _) in lf_is {
                all_lfs.push(lf);
            }
        }
        if all_lfs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(all_lfs))
        }
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
}

impl TriplesSegment {
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
                    lf_subj
                        .clone()
                        .slice(from_i as i64, (OFFSET_STEP * 2) as u32)
                        .collect()
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
                    lf_subj
                        .slice(to_i as i64, (OFFSET_STEP * 2 + 1) as u32)
                        .collect()
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

            let ret = lf.slice(from_i as i64, height as u32).collect().unwrap();
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
        let IndexedTriples {
            subject_sort,
            subject_sparse_index,
            object_sort,
            object_sparse_index,
            height,
        } = create_indices(
            df,
            object_indexing_enabled,
            subject_type,
            object_type,
            cats,
            compacted,
            triples_folder,
        )?;
        let triples = TriplesSegment {
            height,
            subject_sort: Some(subject_sort),
            subject_sparse_index: Some(subject_sparse_index),
            object_sort,
            object_sparse_index,
        };
        Ok(triples)
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&NamedOrBlankNode>>,
        objects: &Option<Vec<&Term>>,
        _subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if let Some(subjects) = subjects {
            let strings = get_subject_strings(subjects);
            let offsets = get_lookup_offsets(
                &strings,
                &self.subject_sparse_index.as_ref().unwrap().map,
                self.height,
            );
            return self
                .subject_sort
                .as_ref()
                .unwrap()
                .get_lazy_frames(Some(offsets));
        } else if let Some(objects) = objects {
            if let Some(sorted) = &self.object_sort {
                let allow_pushdown = if let BaseRDFNodeType::Literal(t, _) = object_type {
                    t.as_ref() == xsd::STRING
                } else {
                    true
                };

                let offsets = if allow_pushdown {
                    let strings = get_object_strings(objects);
                    let offsets = get_lookup_offsets(
                        strings.as_slice(),
                        &self.object_sparse_index.as_ref().unwrap().map,
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
    height: usize,
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
        df = df
            .lazy()
            .sort_by_exprs(
                sort_by,
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![false],
                    multithreaded: true,
                    maintain_order: false,
                    limit: None,
                },
            )
            .collect()
            .unwrap();
        let object_encs = maybe_get_cat_encs(cats, obj_type);
        let sparse = create_sparse_index(
            df.column(OBJECT_COL_NAME).unwrap().as_materialized_series(),
            object_encs,
        );

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
}

impl StoredTriples {
    pub fn get_lazy_frame(&self) -> Result<LazyFrame, TriplestoreError> {
        Ok(self.get_lazy_frame_and_height()?.0)
    }

    fn get_lazy_frame_and_height(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        match self {
            StoredTriples::TriplesInMemory(mem) => mem.get_lazy_frame(),
            StoredTriples::TriplesOnDisk(disk) => disk.get_lazy_frame(),
        }
    }

    fn height(&self) -> usize {
        match self {
            StoredTriples::TriplesInMemory(mem) => mem.df.as_ref().unwrap().height(),
            StoredTriples::TriplesOnDisk(disk) => disk.height,
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
        file: &Path,
        triples_folder: Option<&Path>,
    ) -> Result<Self, TriplestoreError> {
        if let Some(triples_folder) = triples_folder {
            Ok(StoredTriples::TriplesOnDisk(Arc::new(
                TriplesOnDisk::copy_from(file, triples_folder)?,
            )))
        } else {
            let df = scan_parquet(file)
                .map_err(TriplestoreError::FileIOError)?
                .collect()
                .map_err(TriplestoreError::LazyLoadError)?;
            Ok(StoredTriples::TriplesInMemory(Box::new(
                TriplesInMemory::new(df),
            )))
        }
    }

    pub(crate) fn get_lazy_frames(
        &self,
        offsets: Option<Vec<(usize, usize)>>,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        let (lf, height) = self.get_lazy_frame_and_height()?;
        let out = if let Some(offsets) = offsets {
            let output: Result<Vec<_>, TriplestoreError> = offsets
                .into_par_iter()
                .map(|(offset, len)| {
                    let lf = lf
                        .clone()
                        .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                        .slice(offset as i64, len as IdxSize);
                    Ok((lf, len))
                })
                .collect();
            output?
        } else {
            vec![(lf, height)]
        };
        Ok(out)
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
/// triplestore; the file is removed when the last reference is dropped.
pub struct TriplesOnDisk {
    path: PathBuf,
    height: usize,
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
        Ok(Self { path, height })
    }

    fn copy_from(file: &Path, triples_folder: &Path) -> Result<Self, TriplestoreError> {
        let path = Self::new_path(triples_folder);
        std::fs::copy(file, &path)
            .map_err(|e| TriplestoreError::FileIOError(FileIOError::FileCreateIOError(e)))?;
        let mut on_disk = Self { path, height: 0 };
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

    fn scan(&self) -> Result<LazyFrame, TriplestoreError> {
        scan_parquet(&self.path).map_err(TriplestoreError::FileIOError)
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        Ok((self.scan()?, self.height))
    }
}

impl Drop for TriplesOnDisk {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            warn!("Could not remove triples file {:?}: {}", self.path, e);
        }
    }
}

fn get_lookup_interval(
    trg: &Arc<String>,
    sparse_map: &BTreeMap<Arc<String>, usize>,
    height: usize,
) -> (usize, usize) {
    let mut from = 0;
    //Todo: remove this clone..
    let mut range_backwards = sparse_map.range(..trg.clone());
    while let Some((s, prev)) = range_backwards.next_back() {
        if s != trg {
            from = cmp::min(*prev, height);
            break;
        }
    }
    //Todo: remove this clone..
    let range_forwards = sparse_map.range(trg.clone()..);
    let mut to = height - 1;
    for (s, next) in range_forwards {
        if *s != *trg {
            to = *next;
            break;
        }
    }
    // We correct here since ranges are exclusive in Polars slice
    let exclusive_to = to + 1;
    if from > exclusive_to {
        panic!(
            "Out of order should never happen: trg {}, map {:?}, from {}, to {}",
            trg, sparse_map, from, to
        );
    }
    (from, exclusive_to)
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

fn get_lookup_offsets(
    trgs: &[&str],
    sparse_map: &BTreeMap<Arc<String>, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    //Trgs MUST be sorted
    let offsets = trgs
        .iter()
        .map(|trg| get_lookup_interval(&Arc::new(trg.to_string()), sparse_map, height));

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
    let IndexedTriples {
        subject_sort,
        subject_sparse_index,
        object_sort,
        object_sparse_index,
        height,
    } = create_indices(
        df,
        object_indexing_enabled,
        subj_type,
        obj_type,
        cats,
        false,
        triples_folder,
    )?;
    Ok(TriplesSegment {
        height,
        subject_sort: Some(subject_sort),
        subject_sparse_index: Some(subject_sparse_index),
        object_sort,
        object_sparse_index,
    })
}

const SO_RANK_KEY_COL_NAME: &str = "so_rank_key";
const LANG_VALUE_RANK_COL_NAME: &str = "lang_value_rank";
const LANG_LANG_RANK_COL_NAME: &str = "lang_lang_rank";

fn map_ranks(
    c: &polars_core::prelude::Column,
    rank_map: &HashMap<u32, u32>,
    name: &str,
) -> UInt32Chunked {
    let ranks: Vec<u32> = c
        .u32()
        .unwrap()
        .into_no_null_iter()
        .map(|u| *rank_map.get(&u).unwrap())
        .collect();
    UInt32Chunked::from_vec(name.into(), ranks)
}
