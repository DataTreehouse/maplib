use crate::errors::TriplestoreError;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{
    col, concat, lit, Expr, IdxSize, IntoLazy, IpcWriter, JoinArgs, JoinType, LazyFrame,
    PlSmallStr, ScanArgsIpc, UnionArgs,
};
use polars_core::datatypes::{AnyValue, CategoricalChunked, LogicalType};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    CategoricalOrdering, CompatLevel, DataType, Series, SortMultipleOptions, SortOptions,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp;

use crate::IndexingOptions;
use log::debug;
use oxrdf::vocab::xsd;
use polars::io::SerWriter;
use polars_core::utils::Container;
use representation::multitype::{lf_column_from_categorical, lf_column_to_categorical};
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::{BTreeMap, HashMap};
use std::fs::{remove_file, File};
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;

const OFFSET_STEP: usize = 100;
const MIN_SIZE_CACHING: usize = 100_000_000; //100MB

const EXISTING_COL: &str = "existing";

#[derive(Clone)]
pub(crate) struct Triples {
    // Valid states:
    // - One or more, non-overlapping indexed segments.
    // - Single, unindexed segment
    segments: Vec<TriplesSegment>,
    height: usize,
    unindexed: Vec<StoredTriples>,
    object_indexing_enabled: bool,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
}

impl Triples {
    pub(crate) fn new(
        df: DataFrame,
        call_uuid: &str,
        storage_folder: Option<&PathBuf>,
        subject_type: BaseRDFNodeType,
        object_type: BaseRDFNodeType,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
        delay_index: bool,
    ) -> Result<Self, TriplestoreError> {
        let height = df.height();
        let mut unindexed = vec![];
        let mut segments = vec![];
        let object_indexing_enabled = can_and_should_index_object(&object_type, verb_iri, indexing);

        if delay_index {
            unindexed.push(StoredTriples::new(
                df,
                &subject_type,
                &object_type,
                storage_folder,
            )?);
        } else {
            let (segment, _) = TriplesSegment::new(
                df.lazy(),
                false,
                call_uuid,
                storage_folder,
                &subject_type,
                &object_type,
                object_indexing_enabled,
            )?;
            segments.push(segment);
        }
        Ok(Triples {
            segments,
            height,
            unindexed,
            subject_type,
            object_type,
            object_indexing_enabled,
        })
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
        allow_duplicates: bool,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        assert!(allow_duplicates || self.unindexed.is_empty());
        let mut all_sms = vec![];
        for s in &self.segments {
            all_sms.extend(s.get_lazy_frames(subjects, objects, &self.object_type)?);
        }
        for t in &self.unindexed {
            all_sms.extend(t.get_lazy_frames(None)?)
        }
        Ok(all_sms)
    }

    pub fn index_unindexed_maybe_segments(
        &mut self,
        df: Option<DataFrame>,
        storage_folder: Option<&PathBuf>,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let return_new = df.is_some();
        let mut compacting_indexing_time = 0f32;
        let mut subjects_time = 0f32;
        let mut nonoverlapping_time = 0f32;
        let total_now = Instant::now();

        let df = if !self.unindexed.is_empty() {
            let mut all_lfs = vec![];
            for u in &self.unindexed {
                all_lfs.extend(u.get_lazy_frames(None)?.into_iter().map(|(lf, ..)| lf));
            }
            if let Some(df) = df {
                all_lfs.push(df.lazy());
            }
            let df = concat(
                all_lfs,
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
            .collect()
            .unwrap();
            for mut u in self.unindexed.drain(..) {
                u.wipe()?
            }
            df
        } else if let Some(df) = df {
            df
        } else {
            return Ok(None);
        };

        let should_compact = if self.height < df.height() * 2 {
            // Compact if amount of existing triples is similar to amount of new triples
            true
        } else if self.segments.len() > 1 {
            // Compact if we are getting too fragmented
            let first_height = self.segments.get(0).unwrap().height;
            let others_height: usize = self.segments[1..].iter().map(|x| x.height).sum();
            others_height * 2 > first_height
        } else {
            // Do not compact otherwise
            false
        };
        let out = if !should_compact {
            debug!(
                "Deduping incoming {} triples, existing {}",
                df.height(),
                self.height
            );
            let mut incoming_df = df;
            let mut remaining_height = self.height;
            let mut i = 0;

            let now_subjects = Instant::now();
            let subjects_col;
            let subjects_str = if OFFSET_STEP / 10 * incoming_df.height() < remaining_height {
                let new_subjects_col = incoming_df
                    .column(SUBJECT_COL_NAME)
                    .unwrap()
                    .unique()
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap()
                    .sort(SortOptions {
                        descending: false,
                        nulls_last: false,
                        multithreaded: true,
                        maintain_order: false,
                        limit: None,
                    })
                    .unwrap();
                subjects_col = Some(new_subjects_col);
                let subjects =
                    create_string_vec(subjects_col.as_ref().unwrap().as_materialized_series());
                Some(subjects)
            } else {
                None
            };
            subjects_time += now_subjects.elapsed().as_secs_f32();

            while i < self.segments.len() {
                debug!(
                    "Iter {} remaining height {} df height {}",
                    i,
                    remaining_height,
                    incoming_df.height()
                );
                let segment = self.segments.get(i).unwrap();
                if i > 0 && segment.height < (remaining_height - segment.height) * 2 {
                    // Compact if we reach a segment that is too similar in size to the remaining segments
                    break;
                }
                if i > 0 && remaining_height < incoming_df.height() * 2 {
                    // We compact if the incoming triples is too similar to the remaining triples
                    break;
                }
                let non_overlap_now = Instant::now();
                incoming_df = segment.non_overlapping(subjects_str.as_ref(), incoming_df)?;
                nonoverlapping_time += non_overlap_now.elapsed().as_secs_f32();

                if incoming_df.height() == 0 {
                    break;
                }
                remaining_height -= segment.height;
                i += 1;
            }
            if i < self.segments.len() && incoming_df.height() > 0 {
                debug!("Stopped dedupe early with something to add");
                // We stopped early, compact the latest segments and the incoming.
                let compact_now = Instant::now();
                let ts: Vec<_> = self.segments.drain(i..).collect();
                let mut triples = Triples {
                    segments: ts,
                    height: 0,
                    unindexed: vec![],
                    subject_type: self.subject_type.clone(),
                    object_type: self.object_type.clone(),
                    object_indexing_enabled: self.object_indexing_enabled,
                };
                let new_triples =
                    triples.compact_all(Some(incoming_df), storage_folder, return_new)?;
                self.height = self.height - remaining_height + triples.height;
                self.segments.extend(triples.segments.drain(..));
                compacting_indexing_time += compact_now.elapsed().as_secs_f32();
                Ok(new_triples)
            } else if incoming_df.height() > 0 {
                debug!("Dedupe finished, adding remaining");
                let compacting_now = Instant::now();
                let (new_segment, new_triples) = TriplesSegment::new(
                    incoming_df
                        .lazy()
                        .with_column(lit(false).alias(EXISTING_COL)),
                    return_new,
                    &Uuid::new_v4().to_string(),
                    storage_folder,
                    &self.subject_type,
                    &self.object_type,
                    self.object_indexing_enabled,
                )?;
                self.height = self.height + new_segment.height;
                self.segments.push(new_segment);
                compacting_indexing_time += compacting_now.elapsed().as_secs_f32();
                Ok(new_triples)
            } else {
                debug!("Nothing to add");
                Ok(None)
            }
        } else {
            debug!("Creating compacted segment");
            let compacting_now = Instant::now();
            let new_triples = self.compact_all(Some(df), storage_folder, return_new)?;
            compacting_indexing_time += compacting_now.elapsed().as_secs_f32();
            Ok(new_triples)
        };

        let total_time = total_now.elapsed().as_secs_f32();
        debug!(
            "Took {} compacting:  {}, subjects {}, nonoverlaps {}",
            total_time,
            compacting_indexing_time / total_time,
            subjects_time / total_time,
            nonoverlapping_time / total_time
        );
        out
    }

    fn index_unindexed_no_segments(
        &mut self,
        storage_folder: Option<&PathBuf>,
        df: Option<DataFrame>,
        return_new: bool,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        assert!(self.segments.is_empty());
        if self.unindexed.is_empty() && df.is_none() {
            Ok(None)
        } else {
            let mut lfs = vec![];
            for st in &self.unindexed {
                let new_lfs = st.get_lazy_frames(None)?;
                for (mut lf, _) in new_lfs {
                    if df.is_some() {
                        lf = lf.with_column(lit(true).alias(EXISTING_COL));
                    }
                    lfs.push(lf);
                }
            }
            if let Some(df) = df {
                lfs.push(df.lazy().with_column(lit(false).alias(EXISTING_COL)));
            }
            let lf = concat(
                lfs,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    diagonal: false,
                    from_partitioned_ds: false,
                    maintain_order: false,
                },
            )
            .unwrap();
            let (ts, df) = TriplesSegment::new(
                lf,
                return_new,
                &Uuid::new_v4().to_string(),
                storage_folder,
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
            )?;
            self.height = ts.height;
            self.segments.push(ts);
            for mut st in self.unindexed.drain(..) {
                st.wipe()?;
            }
            Ok(df)
        }
    }

    pub(crate) fn add_index(
        &mut self,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
        storage_folder: Option<&PathBuf>,
    ) -> Result<(), TriplestoreError> {
        let object_indexing_enabled =
            can_and_should_index_object(&self.object_type, verb_iri, indexing);
        for segment in &mut self.segments {
            segment.add_index(
                self.object_indexing_enabled,
                &self.subject_type,
                &self.object_type,
                storage_folder,
            )?;
        }
        self.object_indexing_enabled = object_indexing_enabled;
        Ok(())
    }

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        storage_folder: Option<&PathBuf>,
        delay_index: bool,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        debug!(
            "Adding incoming {} triples, existing {} delay index {}",
            df.height(),
            self.height,
            delay_index
        );
        let out = if delay_index {
            let st = StoredTriples::new(df, &self.subject_type, &self.object_type, storage_folder)?;
            self.unindexed.push(st);
            Ok(None)
        } else {
            self.index_unindexed_maybe_segments(Some(df), storage_folder)
        };
        out
    }

    fn compact_all(
        &mut self,
        df: Option<DataFrame>,
        storage_folder: Option<&PathBuf>,
        return_new: bool,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        for seg in self.segments.drain(..) {
            self.unindexed.push(seg.subject_sort.unwrap());
        }
        let df = self.index_unindexed_no_segments(storage_folder, df, return_new)?;
        Ok(df)
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct TriplesSegment {
    height: usize,
    call_uuid: String, //TODO: For uniqueness of blank nodes, an optimization
    subject_sort: Option<StoredTriples>,
    subject_sparse_index: Option<BTreeMap<String, usize>>,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<BTreeMap<String, usize>>,
}

impl TriplesSegment {
    pub(crate) fn new(
        lf: LazyFrame,
        return_new: bool,
        call_uuid: &str,
        storage_folder: Option<&PathBuf>,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        object_indexing_enabled: bool,
    ) -> Result<(Self, Option<DataFrame>), TriplestoreError> {
        let (
            IndexedTriples {
                subject_sort,
                subject_sparse_index,
                object_sort,
                object_sparse_index,
                height,
            },
            new_df,
        ) = create_indices(
            lf.lazy(),
            return_new,
            storage_folder,
            object_indexing_enabled,
            &subject_type,
            &object_type,
        )?;
        let triples = TriplesSegment {
            height,
            call_uuid: call_uuid.to_owned(),
            subject_sort: Some(subject_sort),
            subject_sparse_index: Some(subject_sparse_index),
            object_sort,
            object_sparse_index,
        };
        Ok((triples, new_df))
    }

    pub(crate) fn add_index(
        &mut self,
        object_indexing_enabled: bool,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        storage_folder: Option<&PathBuf>,
    ) -> Result<(), TriplestoreError> {
        self.maybe_do_object_indexing(
            object_indexing_enabled,
            subject_type,
            object_type,
            storage_folder,
        )?;
        Ok(())
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
        object_type: &BaseRDFNodeType,
    ) -> Result<Vec<(LazyFrame, usize)>, TriplestoreError> {
        if let Some(subjects) = subjects {
            let mut strings: Vec<_> = subjects
                .iter()
                .map(|x| match *x {
                    Subject::NamedNode(nn) => nn.as_str(),
                    Subject::BlankNode(bl) => bl.as_str(),
                })
                .collect();
            strings.sort();
            let offsets = get_lookup_offsets(
                strings.as_slice(),
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
                let allow_pushdown = if let BaseRDFNodeType::Literal(t) = object_type {
                    t.as_ref() == xsd::STRING
                } else {
                    true
                };

                let offsets = if allow_pushdown {
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
                    let offsets = get_lookup_offsets(
                        strings.as_slice(),
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

    fn maybe_do_object_indexing(
        &mut self,
        object_indexing_enabled: bool,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
        storage_folder: Option<&PathBuf>,
    ) -> Result<(), TriplestoreError> {
        if object_indexing_enabled && self.object_sort.is_none() {
            let mut lfs = self.get_lazy_frames(&None, &None, object_type)?;
            assert_eq!(lfs.len(), 1);
            let (lf, _) = lfs.pop().unwrap();
            let (object_sort, obj_map) =
                create_object_index(lf, subject_type, object_type, storage_folder)?;
            self.object_sort = Some(object_sort);
            self.object_sparse_index = Some(obj_map);
        }
        Ok(())
    }

    pub(crate) fn non_overlapping(
        &self,
        subjects: Option<&Vec<&str>>,
        df: DataFrame,
    ) -> Result<DataFrame, TriplestoreError> {
        let offsets = if let Some(subjects) = subjects {
            Some(get_lookup_offsets(
                subjects,
                self.subject_sparse_index.as_ref().unwrap(),
                self.height,
            ))
        } else {
            None
        };
        debug!("offsets {:?}", offsets);
        let lfs: Vec<_> = self
            .subject_sort
            .as_ref()
            .unwrap()
            .get_lazy_frames(offsets)?
            .into_iter()
            .map(|(lf, ..)| lf)
            .collect();
        let mut lf = concat(
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
        .unwrap();
        // //Work around issue in Polars
        // lf = lf.with_column(col(SUBJECT_COL_NAME).cast(DataType::String));
        // if object_type.is_lang_string() {
        //     lf = lf.with_column(
        //         as_struct(vec![
        //             col(OBJECT_COL_NAME)
        //                 .struct_()
        //                 .field_by_name(LANG_STRING_VALUE_FIELD)
        //                 .cast(DataType::String)
        //                 .alias(LANG_STRING_VALUE_FIELD),
        //             col(OBJECT_COL_NAME)
        //                 .struct_()
        //                 .field_by_name(LANG_STRING_LANG_FIELD)
        //                 .cast(DataType::String)
        //                 .alias(LANG_STRING_LANG_FIELD),
        //         ])
        //             .alias(OBJECT_COL_NAME),
        //     )
        // } else if object_type.polars_data_type() == DataType::String {
        //     lf = lf
        //         .with_column(col(OBJECT_COL_NAME).cast(DataType::String));
        // }
        //
        // let existing_df = lf.collect().unwrap();
        // lf = existing_df.lazy();
        // let mut rdf_node_types = HashMap::new();
        // rdf_node_types.insert(
        //         SUBJECT_COL_NAME.to_string(),
        //         subject_type.as_rdf_node_type(),
        //     );
        //     rdf_node_types.insert(
        //         OBJECT_COL_NAME.to_string(),
        //         object_type.as_rdf_node_type(),
        //     );
        // lf = lf_columns_to_categorical(
        //     lf,
        //     &rdf_node_types,
        //     CategoricalOrdering::Physical,
        // );
        lf = df.lazy().join(
            lf,
            [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
            [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
            JoinArgs {
                how: JoinType::Anti,
                validation: Default::default(),
                suffix: None,
                slice: None,
                nulls_equal: false,
                coalesce: Default::default(),
                maintain_order: Default::default(),
            },
        );
        Ok(lf.collect().unwrap())
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
    return_new: bool,
    storage_folder: Option<&PathBuf>,
    should_index_by_objects: bool,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
) -> Result<(IndexedTriples, Option<DataFrame>), TriplestoreError> {
    let now = Instant::now();
    let (mut df, subject_sparse_index) =
        create_unique_df_and_sparse_map(lf, true, true, return_new);
    debug!(
        "Creating subject sparse map took {} seconds",
        now.elapsed().as_secs_f32()
    );
    let out_new = if return_new {
        let out_new = df
            .clone()
            .lazy()
            .filter(col(EXISTING_COL).not())
            .drop([col(EXISTING_COL)])
            .collect()
            .unwrap();
        df = df.drop(EXISTING_COL).unwrap();
        if out_new.height() > 0 {
            Some(out_new)
        } else {
            None
        }
    } else {
        None
    };
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
    Ok((
        IndexedTriples {
            subject_sort,
            subject_sparse_index,
            object_sort,
            object_sparse_index,
            height,
        },
        out_new,
    ))
}

fn create_object_index(
    df: LazyFrame,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    storage_folder: Option<&PathBuf>,
) -> Result<(StoredTriples, BTreeMap<String, usize>), TriplestoreError> {
    let lf = sort_indexed_lf(df.lazy(), false, false, false);

    // No need to deduplicate as subject index creation has deduplicated
    let (df, obj_sparse_map) = create_unique_df_and_sparse_map(lf, false, false, false);
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
        storage_folder: Option<&PathBuf>,
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
        storage_folder: &Path,
    ) -> Result<Self, TriplestoreError> {
        let height = df.height();
        let file_name = format!("tmp_{}.ipc", Uuid::new_v4());
        let mut file_path_buf = storage_folder.to_owned();
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
    trgs: &[&str],
    sparse_map: &BTreeMap<String, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    //Trgs MUST be sorted
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

fn sort_indexed_lf(
    lf: LazyFrame,
    is_subject: bool,
    also_other: bool,
    sort_on_existing: bool,
) -> LazyFrame {
    let c = get_col(is_subject);
    let mut lf = cast_col_to_cat(lf, c, true);

    let mut by = vec![PlSmallStr::from_str(c)];
    let mut descending = vec![false];
    if also_other {
        let other_c = get_col(!is_subject);
        by.push(PlSmallStr::from_str(other_c));
        descending.push(false);
    }
    if sort_on_existing {
        by.push(PlSmallStr::from_str(EXISTING_COL));
        descending.push(true);
    }

    lf = lf.sort(
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
) -> (DataFrame, BTreeMap<String, usize>) {
    let deduplicate_now = Instant::now();
    let c = get_col(is_subject);
    if deduplicate {
        lf = sort_indexed_lf(lf, is_subject, true, sort_on_existing);
        let other_c = get_col(!is_subject);
        lf = lf.with_column(
            repeated_from_last_row_expr(c)
                .and(repeated_from_last_row_expr(other_c))
                .alias("is_duplicated"),
        );
        lf = lf.filter(col("is_duplicated").not());
        lf = cast_col_to_cat(lf, c, false);
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
    let sparse_map = create_sparse_map(ser);
    debug!(
        "Creating sparse map took {} seconds",
        sparse_now.elapsed().as_secs_f32()
    );
    (df, sparse_map)
}

fn create_sparse_map(ser: &Series) -> BTreeMap<String, usize> {
    assert!(!ser.is_empty());
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

fn create_string_vec(ser: &Series) -> Vec<&str> {
    let mut sparse = Vec::with_capacity(ser.len());
    for any in ser.iter() {
        match any {
            AnyValue::String(s) => {
                sparse.push(s);
            }
            _ => panic!(),
        }
    }
    sparse
}
