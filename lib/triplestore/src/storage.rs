use crate::errors::TriplestoreError;
use crate::{IndexingOptions, StoredBaseRDFNodeType};
use log::trace;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::{
    col, concat, lit, Expr, IdxSize, IntoLazy, JoinArgs, JoinType, LazyFrame, PlSmallStr,
    StaticArray, UnionArgs,
};
use polars_core::datatypes::AnyValue;
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    IntoColumn, PolarsIterator, Series, SortMultipleOptions, UInt32Chunked,
};
use polars_core::series::SeriesIter;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{rdf_split_iri_str, CatEncs, Cats, OBJECT_ARG_SORT_COL_NAME};
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::cmp;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::path::{Path, PathBuf};
use std::time::Instant;

const OFFSET_STEP: usize = 100;
const MIN_SIZE_CACHING: usize = 100_000_000; //100MB

const EXISTING_COL: &str = "existing";

#[derive(Clone)]
struct SparseIndex {
    map: BTreeMap<String, usize>,
}

#[derive(Clone)]
pub(crate) struct Triples {
    segments: Vec<TriplesSegment>,
    height: usize,
    subject_type: StoredBaseRDFNodeType,
    object_type: StoredBaseRDFNodeType,
    object_indexing_enabled: bool,
}

impl Triples {
    pub fn new(
        df: DataFrame,
        storage_folder: Option<&PathBuf>,
        subject_type: StoredBaseRDFNodeType,
        object_type: StoredBaseRDFNodeType,
        verb_iri: &NamedNode,
        indexing: &IndexingOptions,
        cats: &Cats,
    ) -> Result<Self, TriplestoreError> {
        let object_indexing_enabled =
            can_and_should_index_object(&object_type.as_base_rdf_node_type(), verb_iri, indexing);
        let height = df.height();
        let mut segments = vec![];

        let segment = TriplesSegment::new(
            df,
            storage_folder,
            &subject_type,
            &object_type,
            object_indexing_enabled,
            cats,
        )?;
        segments.push(segment);

        Ok(Self {
            segments,
            height,
            subject_type,
            object_type,
            object_indexing_enabled,
        })
    }

    pub(crate) fn get_lazy_frames(
        &self,
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
        global_cats: &Cats,
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

    pub(crate) fn add_triples(
        &mut self,
        df: DataFrame,
        storage_folder: Option<&PathBuf>,
        global_cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let mut compacting_indexing_time = 0f32;
        let mut subjects_time = 0f32;
        let mut nonoverlapping_time = 0f32;
        let total_now = Instant::now();

        // Here we decide if segments should be compacted and so on.
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
            trace!(
                "Deduping incoming {} triples, existing {}",
                df.height(),
                self.height
            );
            let mut incoming_df = df;
            let mut remaining_height = self.height;
            let mut i = 0;

            let now_subjects = Instant::now();
            let subjects_str = if OFFSET_STEP / 10 * incoming_df.height() < remaining_height {
                let new_subjects_col = incoming_df.column(SUBJECT_COL_NAME).unwrap();
                let ct = self.subject_type.as_cat_type().unwrap();
                let encs = global_cats.cat_map.get(&ct).unwrap();
                let subjects =
                    create_deduplicated_string_vec(new_subjects_col.as_materialized_series(), encs);
                Some(subjects)
            } else {
                None
            };
            subjects_time += now_subjects.elapsed().as_secs_f32();

            while i < self.segments.len() {
                trace!(
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
                trace!("Stopped dedupe early with something to add");
                // We stopped early, compact the latest segments and the incoming.
                let compact_now = Instant::now();
                let mut ts: Vec<_> = self.segments.drain(i..).map(|x| (x, false)).collect();
                let incoming_segment = TriplesSegment::new(
                    incoming_df,
                    storage_folder,
                    &self.subject_type,
                    &self.object_type,
                    self.object_indexing_enabled,
                    global_cats,
                )?;
                ts.push((incoming_segment, true));
                let (segment, new_triples) = compact_segments(
                    ts,
                    global_cats,
                    &self.subject_type,
                    &self.object_type,
                    storage_folder,
                )?;
                self.height =
                    self.height - remaining_height + new_triples.as_ref().unwrap().height();
                self.segments.push(segment);
                compacting_indexing_time += compact_now.elapsed().as_secs_f32();
                Ok(new_triples)
            } else if incoming_df.height() > 0 {
                trace!("Dedupe finished, adding remaining");
                let compacting_now = Instant::now();
                let new_segment = TriplesSegment::new(
                    incoming_df.clone(),
                    storage_folder,
                    &self.subject_type,
                    &self.object_type,
                    self.object_indexing_enabled,
                    global_cats,
                )?;
                self.height = self.height + new_segment.height;
                self.segments.push(new_segment);
                compacting_indexing_time += compacting_now.elapsed().as_secs_f32();
                Ok(Some(incoming_df))
            } else {
                trace!("Nothing to add");
                Ok(None)
            }
        } else {
            trace!("Creating compacted segment");
            let compacting_now = Instant::now();
            let mut ts: Vec<_> = self.segments.drain(..).map(|x| (x, false)).collect();
            let incoming_segment = TriplesSegment::new(
                df,
                storage_folder,
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
                global_cats,
            )?;
            ts.push((incoming_segment, true));
            let (segment, new_triples) = compact_segments(
                ts,
                global_cats,
                &self.subject_type,
                &self.object_type,
                storage_folder,
            )?;
            self.segments.push(segment);
            compacting_indexing_time += compacting_now.elapsed().as_secs_f32();
            Ok(new_triples)
        };

        let total_time = total_now.elapsed().as_secs_f32();
        trace!(
            "Took {} compacting:  {}, subjects {}, nonoverlaps {}",
            total_time,
            compacting_indexing_time / total_time,
            subjects_time / total_time,
            nonoverlapping_time / total_time
        );
        out
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
    pub(crate) fn get_size(&self) -> usize {
        let mut size = 0;
        if let Some(t) = &self.subject_sort {
            size += t.get_size();
        }
        if let Some(t) = &self.object_sort {
            size += t.get_size();
        }
        size
    }
}

impl TriplesSegment {
    pub(crate) fn new(
        df: DataFrame,
        storage_folder: Option<&PathBuf>,
        subject_type: &StoredBaseRDFNodeType,
        object_type: &StoredBaseRDFNodeType,
        object_indexing_enabled: bool,
        cats: &Cats,
    ) -> Result<Self, TriplestoreError> {
        let IndexedTriples {
            subject_sort,
            subject_sparse_index,
            object_sort,
            object_sparse_index,
            height,
        } = create_indices(
            df,
            storage_folder,
            object_indexing_enabled,
            &subject_type,
            &object_type,
            cats,
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
        subjects: &Option<Vec<&Subject>>,
        objects: &Option<Vec<&Term>>,
        _subject_type: &StoredBaseRDFNodeType,
        object_type: &StoredBaseRDFNodeType,
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
                let allow_pushdown = if let StoredBaseRDFNodeType::Literal(t) = object_type {
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

    pub(crate) fn non_overlapping(
        &self,
        subjects: Option<&Vec<&str>>,
        df: DataFrame,
    ) -> Result<DataFrame, TriplestoreError> {
        let offsets = if let Some(subjects) = subjects {
            Some(get_lookup_offsets(
                subjects,
                &self.subject_sparse_index.as_ref().unwrap().map,
                self.height,
            ))
        } else {
            None
        };
        trace!("offsets {:?}", offsets);
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
    subject_sparse_index: SparseIndex,
    object_sort: Option<StoredTriples>,
    object_sparse_index: Option<SparseIndex>,
    height: usize,
}

// LF should always be sorted by subjects underlying string
// When object is cat or a string, the LF should have an u32-col that can sort the objects by the (underlying) string.
fn create_indices(
    mut df: DataFrame,
    storage_folder: Option<&PathBuf>,
    should_index_by_objects: bool,
    subj_type: &StoredBaseRDFNodeType,
    obj_type: &StoredBaseRDFNodeType,
    cats: &Cats,
) -> Result<IndexedTriples, TriplestoreError> {
    assert!(df.height() > 0);
    let now = Instant::now();
    let subject_encs = get_encs(cats, subj_type);
    let subject_sparse_index = create_sparse_index(
        df.column(SUBJECT_COL_NAME)
            .unwrap()
            .as_materialized_series(),
        subject_encs.unwrap(),
    );
    trace!(
        "Creating subject sparse map took {} seconds",
        now.elapsed().as_secs_f32()
    );
    let store_now = Instant::now();
    let height = df.height();
    let subject_sort = StoredTriples::new(df.clone(), subj_type, obj_type, storage_folder)?;
    trace!("Storing triples took {}", store_now.elapsed().as_secs_f32());
    let mut object_sort = None;
    let mut object_sparse_index = None;

    if should_index_by_objects {
        let object_now = Instant::now();
        df = df
            .sort(
                vec![PlSmallStr::from_str(OBJECT_ARG_SORT_COL_NAME)],
                SortMultipleOptions {
                    descending: vec![false, false],
                    nulls_last: vec![false, false],
                    multithreaded: false,
                    maintain_order: false,
                    limit: None,
                },
            )
            .unwrap();
        let object_encs = get_encs(cats, obj_type);
        let sparse = create_sparse_index(
            df.column(OBJECT_COL_NAME).unwrap().as_materialized_series(),
            object_encs.unwrap(),
        );
        object_sort = Some(StoredTriples::new(
            df.clone(),
            subj_type,
            obj_type,
            storage_folder,
        )?);
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

fn get_encs<'a>(c: &'a Cats, t: &StoredBaseRDFNodeType) -> Option<&'a CatEncs> {
    let ct = t.as_cat_type();
    if let Some(ct) = ct {
        Some(c.cat_map.get(&ct).unwrap())
    } else {
        None
    }
}

pub fn can_and_should_index_object(
    object_type: &BaseRDFNodeType,
    verb_iri: &NamedNode,
    indexing: &IndexingOptions,
) -> bool {
    let can_index_object = if matches!(
        object_type,
        BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode
    ) {
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
    pub(crate) fn get_size(&self) -> usize {
        match self {
            StoredTriples::TriplesOnDisk(_) => {
                todo!()
            }
            StoredTriples::TriplesInMemory(m) => m.get_size(),
        }
    }
}

impl StoredTriples {
    fn new(
        df: DataFrame,
        subj_type: &StoredBaseRDFNodeType,
        obj_type: &StoredBaseRDFNodeType,
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
                    let lf = lf
                        .clone()
                        .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                        .slice(offset as i64, len as IdxSize);
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
    subj_type: StoredBaseRDFNodeType,
    obj_type: StoredBaseRDFNodeType,
}

impl TriplesOnDisk {
    fn new(
        mut df: DataFrame,
        subj_type: &StoredBaseRDFNodeType,
        obj_type: &StoredBaseRDFNodeType,
        storage_folder: &Path,
    ) -> Result<Self, TriplestoreError> {
        todo!()
        // let height = df.height();
        // let file_name = format!("tmp_{}.ipc", Uuid::new_v4());
        // let mut file_path_buf = storage_folder.to_owned();
        // file_path_buf.push(file_name);
        // let file_path = file_path_buf.as_path();
        //
        // write_ipc(&mut df, file_path, subj_type, obj_type)?;
        // Ok(Self {
        //     height,
        //     df_path: file_path.to_str().unwrap().to_string(),
        //     subj_type: subj_type.clone(),
        //     obj_type: obj_type.clone(),
        // })
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        todo!()
        // let p = Path::new(&self.df_path);
        // Ok((scan_ipc(p, &self.subj_type, &self.obj_type)?, self.height))
    }

    pub(crate) fn wipe(&mut self) -> Result<(), TriplestoreError> {
        todo!()
        // let p = Path::new(&self.df_path);
        // remove_file(p).map_err(TriplestoreError::RemoveFileError)?;
        // Ok(())
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

    pub(crate) fn get_size(&self) -> usize {
        self.df.as_ref().unwrap().estimated_size()
    }
}

fn get_lookup_interval(
    trg: &String,
    sparse_map: &BTreeMap<String, usize>,
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
    (from, exclusive_to)
}

fn get_subject_strings<'a>(subjects: &[&'a Subject]) -> Vec<&'a str> {
    let mut strings: Vec<_> = subjects
        .iter()
        .map(|x| match *x {
            Subject::NamedNode(nn) => rdf_split_iri_str(nn.as_str()).1,
            Subject::BlankNode(bl) => bl.as_str(),
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
            Term::NamedNode(nn) => rdf_split_iri_str(nn.as_str()).1,
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
    sparse_map: &BTreeMap<String, usize>,
    height: usize,
) -> Vec<(usize, usize)> {
    //Trgs MUST be sorted
    let offsets = trgs
        .iter()
        .map(|trg| get_lookup_interval(&trg.to_string(), sparse_map, height));

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

fn update_index_at_offset(
    u32_chunked: &UInt32Chunked,
    offset: usize,
    sparse_map: &mut BTreeMap<String, usize>,
    encs: &CatEncs,
) {
    let u = u32_chunked.get(offset);
    if let Some(u) = u {
        let s = encs.rev_map.get(&u).unwrap();
        let e = sparse_map.entry(s.clone());
        e.or_insert(offset);
    }
}

pub(crate) fn repeated_from_last_row_expr(c: &str) -> Expr {
    col(c)
        .shift(lit(1))
        .is_not_null()
        .and(col(c).shift(lit(1)).eq(col(c)))
}

fn create_sparse_index(ser: &Series, encs: &CatEncs) -> SparseIndex {
    assert!(!ser.is_empty());
    let strch = ser.u32().unwrap();
    let mut sparse_map = BTreeMap::new();
    let mut current_offset = 0;
    while current_offset < ser.len() {
        update_index_at_offset(strch, current_offset, &mut sparse_map, encs);
        current_offset += OFFSET_STEP;
    }
    //Ensure that we have both ends
    let final_offset = ser.len() - 1;
    if current_offset != final_offset {
        update_index_at_offset(strch, final_offset, &mut sparse_map, encs);
    }
    SparseIndex { map: sparse_map }
}

//Assumes sorted.
fn create_deduplicated_string_vec<'a>(ser: &Series, cat_encs: &'a CatEncs) -> Vec<&'a str> {
    let mut v = Vec::with_capacity(ser.len());
    let mut last = None;
    for any in ser.iter() {
        match any {
            AnyValue::UInt32(u) => {
                if let Some(last) = last {
                    if last == u {
                        continue;
                    }
                }
                last = Some(u);
                let s = cat_encs.rev_map.get(&u).unwrap().as_str();
                v.push(s);
            }
            _ => unreachable!("Should never happen"),
        }
    }
    v
}

fn compact_segments(
    segments: Vec<(TriplesSegment, bool)>,
    cats: &Cats,
    subj_type: &StoredBaseRDFNodeType,
    obj_type: &StoredBaseRDFNodeType,
    storage_folder: Option<&PathBuf>,
) -> Result<(TriplesSegment, Option<DataFrame>), TriplestoreError> {
    let mut subject_segments = Vec::with_capacity(segments.len());
    let mut objects_segments = Vec::with_capacity(segments.len());
    for (
        TriplesSegment {
            subject_sort,
            object_sort,
            ..
        },
        new,
    ) in segments
    {
        for (lf, _) in subject_sort.unwrap().get_lazy_frames(None)? {
            subject_segments.push((lf.collect().unwrap(), new));
        }
        if let Some(object_sort) = object_sort {
            for (lf, _) in object_sort.get_lazy_frames(None)? {
                objects_segments.push((lf.collect().unwrap(), false));
            }
        }
    }
    let subject_encs = get_encs(cats, subj_type);
    let object_encs = get_encs(cats, obj_type);

    let (compact_subjects, new_df) = compact_dataframe_segments(
        subject_segments,
        SUBJECT_COL_NAME,
        OBJECT_COL_NAME,
        subject_encs,
        object_encs,
    )?;
    let height = compact_subjects.height();
    let subject_index = create_sparse_index(
        compact_subjects
            .column(SUBJECT_COL_NAME)
            .unwrap()
            .as_materialized_series(),
        subject_encs.unwrap(),
    );
    let stored_subjects =
        StoredTriples::new(compact_subjects, subj_type, obj_type, storage_folder)?;

    let (stored_objects, object_index) = if !objects_segments.is_empty() {
        let (compact_objects, _) = compact_dataframe_segments(
            objects_segments,
            OBJECT_COL_NAME,
            SUBJECT_COL_NAME,
            object_encs,
            subject_encs,
        )?;
        let object_index = create_sparse_index(
            compact_objects
                .column(OBJECT_COL_NAME)
                .unwrap()
                .as_materialized_series(),
            object_encs.unwrap(),
        );
        let stored_objects =
            StoredTriples::new(compact_objects, subj_type, obj_type, storage_folder)?;
        (Some(stored_objects), Some(object_index))
    } else {
        (None, None)
    };
    Ok((
        TriplesSegment {
            height,
            subject_sort: Some(stored_subjects),
            subject_sparse_index: Some(subject_index),
            object_sort: stored_objects,
            object_sparse_index: object_index,
        },
        new_df,
    ))
}

fn compact_dataframe_segments(
    mut segments: Vec<(DataFrame, bool)>,
    col_a: &str,
    col_b: &str,
    a_cat_encs: Option<&CatEncs>,
    b_cat_encs: Option<&CatEncs>,
) -> Result<(DataFrame, Option<DataFrame>), TriplestoreError> {
    for (df, _) in &mut segments {
        df.as_single_chunk();
    }
    let existing_segments_borrow = &segments;

    let mut queue = BinaryHeap::new();

    for (df, new) in existing_segments_borrow {
        let col_a_iter = df.column(col_a).unwrap().as_materialized_series().iter();
        let col_b_iter = df.column(col_b).unwrap().as_materialized_series().iter();
        if let Some(it) = TripleIterator::new(col_a_iter, col_b_iter, a_cat_encs, b_cat_encs, *new)
        {
            queue.push(Reverse(it));
        }
    }

    let mut last_a: Option<AnyValue> = None;
    let mut last_b: Option<AnyValue> = None;
    let mut last_new = false;
    let mut a_vec = vec![];
    let mut b_vec = vec![];

    let mut a_new_vec = vec![];
    let mut b_new_vec = vec![];
    while !queue.is_empty() {
        let mut it = queue.pop().unwrap().0;
        let dupe = if let (Some(last_a), Some(last_b)) = (&last_a, &last_b) {
            last_a == &it.a_original && last_b == &it.b_original
        } else {
            false
        };

        if !dupe {
            if last_new {
                a_new_vec.push(last_a.unwrap().clone());
                b_new_vec.push(last_b.unwrap().clone());
            }
            last_a = Some(it.a_original.clone());
            last_b = Some(it.b_original.clone());
            last_new = it.new;
            a_vec.push(it.a_original.clone());
            b_vec.push(it.b_original.clone());
        } else {
            if !it.new {
                last_new = false;
            }
        }

        if let Some(it) = it.next(a_cat_encs, b_cat_encs) {
            queue.push(Reverse(it));
        }
    }
    let a_ser =
        Series::from_any_values(PlSmallStr::from_str(col_a), a_vec.as_slice(), false).unwrap();
    let b_ser =
        Series::from_any_values(PlSmallStr::from_str(col_b), b_vec.as_slice(), false).unwrap();
    let df = DataFrame::new(vec![a_ser.into_column(), b_ser.into_column()]).unwrap();

    if last_new {
        a_new_vec.push(last_a.unwrap().clone());
        b_new_vec.push(last_b.unwrap().clone());
    }

    let new_df = if !a_new_vec.is_empty() {
        let a_new_ser =
            Series::from_any_values(PlSmallStr::from_str(col_a), a_new_vec.as_slice(), false)
                .unwrap();
        let b_new_ser =
            Series::from_any_values(PlSmallStr::from_str(col_b), b_new_vec.as_slice(), false)
                .unwrap();
        let new_df =
            DataFrame::new(vec![a_new_ser.into_column(), b_new_ser.into_column()]).unwrap();
        Some(new_df)
    } else {
        None
    };

    Ok((df, new_df))
}

struct TripleIterator<'a> {
    a_original: AnyValue<'a>,
    a_decode: Option<AnyValue<'a>>,
    a_iterator: SeriesIter<'a>,
    b_original: AnyValue<'a>,
    b_decode: Option<AnyValue<'a>>,
    b_iterator: SeriesIter<'a>,
    new: bool,
}

impl<'a> TripleIterator<'a> {
    pub fn new(
        mut a_iterator: SeriesIter<'a>,
        mut b_iterator: SeriesIter<'a>,
        a_cat_encs: Option<&'a CatEncs>,
        b_cat_encs: Option<&'a CatEncs>,
        new: bool,
    ) -> Option<Self> {
        if let Some(a) = a_iterator.next() {
            let a_original = a;
            let b_original = b_iterator.next().unwrap();
            let a_decode = decode_elem(&a_original, a_cat_encs);
            let b_decode = decode_elem(&b_original, b_cat_encs);
            Some(TripleIterator {
                a_original,
                a_decode,
                a_iterator,
                b_original,
                b_decode,
                b_iterator,
                new,
            })
        } else {
            None
        }
    }

    pub fn next(
        self,
        a_cat_encs: Option<&'a CatEncs>,
        b_cat_encs: Option<&'a CatEncs>,
    ) -> Option<Self> {
        let Self {
            mut a_iterator,
            mut b_iterator,
            new,
            ..
        } = self;

        if let Some(a) = a_iterator.next() {
            let a_original = a;
            let b_original = b_iterator.next().unwrap();
            let a_decode = decode_elem(&a_original, a_cat_encs);
            let b_decode = decode_elem(&b_original, b_cat_encs);
            Some(TripleIterator {
                a_original,
                a_decode,
                a_iterator,
                b_original,
                b_decode,
                b_iterator,
                new,
            })
        } else {
            None
        }
    }
}

fn decode_elem<'a>(
    any_value: &AnyValue<'a>,
    cat_encs: Option<&'a CatEncs>,
) -> Option<AnyValue<'a>> {
    if let Some(cat_encs) = cat_encs {
        if let AnyValue::UInt32(u) = any_value {
            Some(AnyValue::String(cat_encs.rev_map.get(u).unwrap()))
        } else {
            unreachable!("Should never happen")
        }
    } else {
        None
    }
}

impl Eq for TripleIterator<'_> {}

impl PartialEq<Self> for TripleIterator<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd<Self> for TripleIterator<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TripleIterator<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        let use_self_a = if let Some(decoded) = &self.a_decode {
            decoded
        } else {
            &self.a_original
        };
        let use_self_b = if let Some(decoded) = &self.b_decode {
            decoded
        } else {
            &self.b_original
        };
        let use_other_a = if let Some(decoded) = &other.a_decode {
            decoded
        } else {
            &other.a_original
        };
        let use_other_b = if let Some(decoded) = &other.b_decode {
            decoded
        } else {
            &other.b_original
        };

        let c = cmp_any(use_self_a, use_other_a);
        if !c.is_eq() {
            c
        } else {
            cmp_any(use_self_b, use_other_b)
        }
    }
}

fn cmp_any(lhs: &AnyValue, rhs: &AnyValue) -> Ordering {
    match (lhs, rhs) {
        (AnyValue::Boolean(l), AnyValue::Boolean(r)) => l.cmp(r),
        (AnyValue::Null, _) => {
            unreachable!()
        }
        (AnyValue::String(l), AnyValue::String(r)) => l.cmp(r),
        (AnyValue::UInt8(l), AnyValue::UInt8(r)) => l.cmp(r),
        (AnyValue::UInt16(l), AnyValue::UInt16(r)) => l.cmp(r),
        (AnyValue::UInt32(l), AnyValue::UInt32(r)) => l.cmp(r),
        (AnyValue::UInt64(l), AnyValue::UInt64(r)) => l.cmp(r),
        (AnyValue::Int8(l), AnyValue::Int8(r)) => l.cmp(r),
        (AnyValue::Int16(l), AnyValue::Int16(r)) => l.cmp(r),
        (AnyValue::Int32(l), AnyValue::Int32(r)) => l.cmp(r),
        (AnyValue::Int64(l), AnyValue::Int64(r)) => l.cmp(r),
        (AnyValue::Int128(l), AnyValue::Int128(r)) => l.cmp(r),
        (AnyValue::Float32(l), AnyValue::Float32(r)) => {
            if l == r {
                Ordering::Equal
            } else if l < r {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (AnyValue::Float64(l), AnyValue::Float64(r)) => {
            if l == r {
                Ordering::Equal
            } else if l < r {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (AnyValue::Date(l), AnyValue::Date(r)) => l.cmp(r),
        (AnyValue::Datetime(l, ..), AnyValue::Datetime(r, ..)) => l.cmp(r),
        (AnyValue::DatetimeOwned(l, ..), AnyValue::DatetimeOwned(r, ..)) => l.cmp(r),
        (AnyValue::Duration(l, ..), AnyValue::Duration(r, ..)) => l.cmp(r),
        (AnyValue::Time(l), AnyValue::Time(r)) => l.cmp(r),
        (AnyValue::Categorical(_, _), _) => {
            unreachable!()
        }
        (AnyValue::CategoricalOwned(_, _), _) => {
            unreachable!()
        }
        (AnyValue::Enum(_, _), _) => {
            unreachable!()
        }
        (AnyValue::EnumOwned(_, _), _) => {
            unreachable!()
        }
        (AnyValue::List(_), _) => {
            todo!()
        }
        (AnyValue::Array(_, _), _) => {
            unreachable!()
        }
        (AnyValue::Struct(..), AnyValue::Struct(..)) => {
            todo!()
        }
        (AnyValue::StructOwned(l), AnyValue::StructOwned(r)) => {
            let (lvs, _) = l.as_ref();
            let (rvs, _) = r.as_ref();
            for (lv, rv) in lvs.into_iter().zip(rvs) {
                let c = cmp_any(lv, rv);
                if !c.is_eq() {
                    return c;
                }
            }
            Ordering::Equal
        }
        (AnyValue::StringOwned(l), AnyValue::StringOwned(r)) => l.cmp(r),
        (AnyValue::Binary(_), _) => {
            todo!()
        }
        (AnyValue::BinaryOwned(_), _) => {
            todo!()
        }
        (AnyValue::Decimal(..), AnyValue::Decimal(..)) => {
            todo!()
        }
        (_, _) => unreachable!(),
    }
}
