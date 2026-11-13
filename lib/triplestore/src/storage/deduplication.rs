use super::{
    compact_segments, get_lookup_offsets, maybe_get_cat_encs, Triples, TriplesSegment,
    DISK_ROW_GROUP_SIZE,
};
use crate::errors::TriplestoreError;
use crate::storage::so_index::SubjectObjectIndex;
use polars::prelude::{col, concat, LazyFrame, UnionArgs};
use polars_core::prelude::DataFrame;
use representation::cats::Cats;
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;
use tracing::trace;

/// A trailing run of segments is merged when the segment before it is at most this many times
/// larger than the run. Segment sizes then shrink geometrically towards the end, so a table has
/// O(log n) segments, every triple is rewritten O(log n) times, and a large segment is only
/// rewritten when the triples added after it are of comparable size.
const SEGMENT_SIZE_RATIO: usize = 2;
/// Upper bound on the number of segments of a table, which each query reads separately.
const MAX_SEGMENTS: usize = 16;
/// Disk storage mode: segments added after the first one stay in memory until they are merged
/// into a segment of at least this many triples, so that small inserts (e.g. datalog
/// iterations) do not write and rewrite small parquet files.
const DISK_MIN_SEGMENT_HEIGHT: usize = DISK_ROW_GROUP_SIZE;
/// Disk storage mode: deduplication against a stored segment reads only the row groups that may
/// hold the subjects of the new triples, unless that is more than this many row groups or covers
/// more than half of the segment, in which case the whole segment is read.
const MAX_LOOKUP_ROW_GROUPS: usize = 256;

impl Triples {
    pub fn deduplicate_and_insert(
        &mut self,
        df: DataFrame,
        global_cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let new_df = if self.subject_object_index.is_none() && self.triples_folder.is_some() {
            // Disk storage mode without a resident index: deduplicate against the stored triples
            // with the same subjects only.
            let mut index = self.subject_lookup_index(&df, global_cats)?;
            index.maybe_insert_deduplicate(&df, true)
        } else {
            self.ensure_subject_object_index()?;
            self.subject_object_index
                .as_mut()
                .unwrap()
                .maybe_insert_deduplicate(&df, true)
        };
        let Some(new_df) = new_df else {
            return Ok(None);
        };
        let new_height = new_df.height();
        self.height += new_height;

        // The trailing segments to merge with the new triples: segments[first..].
        let n = self.segments.len();
        let mut first = n;
        let mut merged_height = new_height;
        while first > 0
            && (self.segments[first - 1].height <= SEGMENT_SIZE_RATIO * merged_height
                || first + 1 > MAX_SEGMENTS)
        {
            first -= 1;
            merged_height += self.segments[first].height;
        }
        let triples_folder = self
            .segment_folder(first == 0, merged_height)
            .map(|x| x.to_owned());

        let segment = if first == n {
            TriplesSegment::new(
                new_df.clone(),
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
                global_cats,
                false,
                triples_folder.as_deref(),
            )?
        } else {
            let compacting_now = Instant::now();
            let mut dfs = Vec::with_capacity(n - first + 1);
            for seg in self.segments.drain(first..) {
                dfs.push(
                    seg.get_subject_sort_lazy_frame()?
                        .collect()
                        .map_err(TriplestoreError::LazyLoadError)?,
                );
            }
            dfs.push(new_df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap());
            let segment = compact_segments(
                dfs,
                global_cats,
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
                triples_folder.as_deref(),
            )?;
            trace!(
                "Compacting {} segments into {} triples took: {}",
                n - first + 1,
                merged_height,
                compacting_now.elapsed().as_secs_f32()
            );
            segment
        };
        self.segments.push(segment);
        Ok(Some(new_df))
    }

    /// The folder to store a segment in: in disk storage mode the first segment and large
    /// segments are stored on disk, small trailing segments are kept in memory.
    fn segment_folder(&self, first_segment: bool, height: usize) -> Option<&Path> {
        if first_segment || height >= DISK_MIN_SEGMENT_HEIGHT {
            self.triples_folder.as_deref()
        } else {
            None
        }
    }

    pub(crate) fn ensure_subject_object_index(&mut self) -> Result<(), TriplestoreError> {
        if self.subject_object_index.is_none() {
            let mut subject_object_index =
                SubjectObjectIndex::new(&self.subject_type, &self.object_type);
            // In disk storage mode the index is rebuilt from all stored segments.
            for lf in self.get_all_triples_lazy_frames(true)? {
                let df = lf.collect().map_err(TriplestoreError::LazyLoadError)?;
                subject_object_index.insert(&df);
            }
            self.subject_object_index = Some(subject_object_index);
        }
        Ok(())
    }

    /// An index of (at least) the stored triples whose subject occurs in `df`. Uses the sparse
    /// subject indices of the segments, so that only the relevant row groups are read.
    pub(crate) fn subject_lookup_index(
        &self,
        df: &DataFrame,
        cats: &Cats,
    ) -> Result<SubjectObjectIndex, TriplestoreError> {
        let mut index = SubjectObjectIndex::new(&self.subject_type, &self.object_type);
        let subjects = subject_strings(df, cats, &self.subject_type);
        let mut lfs = vec![];
        for seg in &self.segments {
            if let Some(subjects) = &subjects {
                let strs: Vec<&str> = subjects.iter().map(|x| x.as_str()).collect();
                let offsets = get_lookup_offsets(
                    &strs,
                    &seg.subject_sparse_index.as_ref().unwrap().map,
                    seg.height,
                );
                if let Some(ranges) = row_group_ranges(&offsets, seg.height) {
                    for (lf, _) in seg
                        .subject_sort
                        .as_ref()
                        .unwrap()
                        .get_lazy_frames(Some(ranges))?
                    {
                        lfs.push(lf);
                    }
                    continue;
                }
            }
            lfs.push(seg.get_subject_sort_lazy_frame()?);
        }
        if !lfs.is_empty() {
            let lf = concat_lfs(lfs);
            let stored = lf
                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                .collect()
                .map_err(TriplestoreError::LazyLoadError)?;
            index.insert(&stored);
        }
        Ok(index)
    }

    /// In disk storage mode, drops the in-memory deduplication index, which holds every triple
    /// of the table. Later inserts deduplicate against the stored segments.
    pub(crate) fn release_subject_object_index_if_on_disk(&mut self) {
        if self.triples_folder.is_some() {
            self.subject_object_index = None;
        }
    }
}

/// The sorted, distinct subject strings of `df`, or None if subjects are not categorical.
fn subject_strings(
    df: &DataFrame,
    cats: &Cats,
    subject_type: &representation::BaseRDFNodeType,
) -> Option<Vec<String>> {
    let encs = maybe_get_cat_encs(cats, subject_type)?;
    let us: HashSet<u32> = df
        .column(SUBJECT_COL_NAME)
        .unwrap()
        .u32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    let us: Vec<_> = us.into_iter().map(Some).collect();
    let mut strings: Vec<String> = encs
        .maps
        .decode_batch(&us)
        .into_iter()
        .map(|x| x.unwrap().into_owned())
        .collect();
    strings.sort_unstable();
    Some(strings)
}

/// Widens lookup offsets to whole row groups and merges the ones that touch, or returns None if
/// reading the whole segment is cheaper.
fn row_group_ranges(offsets: &[(usize, usize)], height: usize) -> Option<Vec<(usize, usize)>> {
    let mut ranges: Vec<(usize, usize)> = vec![];
    for (offset, len) in offsets {
        if *len == 0 {
            continue;
        }
        let from = (offset / DISK_ROW_GROUP_SIZE) * DISK_ROW_GROUP_SIZE;
        let to = ((offset + len).div_ceil(DISK_ROW_GROUP_SIZE) * DISK_ROW_GROUP_SIZE).min(height);
        if let Some(last) = ranges.last_mut() {
            if from <= last.1 {
                last.1 = last.1.max(to);
                continue;
            }
        }
        ranges.push((from, to));
    }
    let rows: usize = ranges.iter().map(|(from, to)| to - from).sum();
    if rows.div_ceil(DISK_ROW_GROUP_SIZE) > MAX_LOOKUP_ROW_GROUPS || rows * 2 > height {
        None
    } else {
        Some(
            ranges
                .into_iter()
                .map(|(from, to)| (from, to - from))
                .collect(),
        )
    }
}

fn concat_lfs(mut lfs: Vec<LazyFrame>) -> LazyFrame {
    if lfs.len() == 1 {
        lfs.pop().unwrap()
    } else {
        concat(
            lfs,
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
    }
}
