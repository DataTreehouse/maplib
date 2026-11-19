use super::{
    compact_segments, get_lookup_offsets, maybe_get_cat_encs, Triples, TriplesSegment,
    DISK_ROW_GROUP_SIZE,
};
use crate::errors::TriplestoreError;
use crate::storage::so_index::SubjectObjectIndex;
use polars::prelude::{col, concat, LazyFrame, UnionArgs};
use polars_core::prelude::DataFrame;
use representation::cats::{distinct_codes, extend_codes, Cats};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::path::Path;
use std::time::Instant;
use tracing::trace;
use utils::polars::collect_in_memory;

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
/// Inserts of at most this many new triples go to the unsorted write buffer of the table, which
/// is merged into a sorted segment when it holds more than this many triples. Reads include the
/// whole buffer, so it is kept small.
pub(crate) const MAX_BUFFER_HEIGHT: usize = 4096;

impl Triples {
    pub fn deduplicate_and_insert(
        &mut self,
        df: DataFrame,
        global_cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let lookup_index = if self.subject_object_index.is_none() {
            self.temporary_lookup_index(&df, global_cats)?
        } else {
            None
        };
        let new_df = if let Some(mut index) = lookup_index {
            // No resident index: deduplicate against the stored triples with the same subjects
            // only.
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
        self.generation = super::next_generation();

        // A small insert goes to the buffer: no sort, no sparse index, no compaction.
        let buffered = self.buffer_height() + new_height;
        if !self.segments.is_empty() && buffered <= MAX_BUFFER_HEIGHT {
            let so = new_df.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
            match &mut self.buffer {
                Some(buffer) => {
                    buffer
                        .vstack_mut_owned(so)
                        .map_err(TriplestoreError::LazyLoadError)?;
                    if buffer.max_n_chunks() > 16 {
                        buffer.rechunk_mut();
                    }
                }
                None => self.buffer = Some(so),
            }
            return Ok(Some(new_df));
        }
        // The buffer is merged with the new triples (and possibly trailing segments).
        let buffer = self.buffer.take();
        let new_height = buffered;

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

        let segment = if first == n && buffer.is_none() {
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
                    collect_in_memory(seg.get_subject_sort_lazy_frame()?)
                        .map_err(TriplestoreError::LazyLoadError)?,
                );
            }
            dfs.extend(buffer);
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

    /// Turns the object index (the triples sorted by object) of the stored triples on or off.
    /// Turning it on sorts every segment by object, which takes about as long as adding the
    /// triples did; turning it off drops the object sorts.
    pub(crate) fn set_object_indexing(
        &mut self,
        enabled: bool,
        global_cats: &Cats,
    ) -> Result<(), TriplestoreError> {
        if enabled == self.object_indexing_enabled {
            return Ok(());
        }
        if !enabled {
            for seg in self.segments.iter_mut() {
                seg.object_sort = None;
                seg.object_sparse_index = None;
                seg.object_filter = None;
            }
            self.object_indexing_enabled = false;
            return Ok(());
        }
        let now = Instant::now();
        let segments = std::mem::take(&mut self.segments);
        let mut rebuilt = Vec::with_capacity(segments.len());
        for (i, seg) in segments.into_iter().enumerate() {
            let triples_folder = self
                .segment_folder(i == 0, seg.height)
                .map(|x| x.to_owned());
            let df = collect_in_memory(seg.get_subject_sort_lazy_frame()?)
                .map_err(TriplestoreError::LazyLoadError)?;
            // The old segment (and its files in disk storage mode) is dropped before the new one
            // is written.
            drop(seg);
            rebuilt.push(compact_segments(
                vec![df],
                global_cats,
                &self.subject_type,
                &self.object_type,
                true,
                triples_folder.as_deref(),
            )?);
        }
        self.segments = rebuilt;
        self.object_indexing_enabled = true;
        trace!(
            "Creating the object index of {} triples took {}",
            self.height,
            now.elapsed().as_secs_f32()
        );
        Ok(())
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
                SubjectObjectIndex::new(&self.subject_type, &self.object_type)?;
            // In disk storage mode the index is rebuilt from all stored segments.
            for lf in self.get_all_triples_lazy_frames(true)? {
                let df = collect_in_memory(lf).map_err(TriplestoreError::LazyLoadError)?;
                subject_object_index.insert(&df);
            }
            self.subject_object_index = Some(subject_object_index);
        }
        Ok(())
    }

    /// When the table has no resident deduplication index: an index of (at least) the stored
    /// triples whose subject occurs in `df`, or None if building the full index is preferable.
    ///
    /// In disk storage mode the full index is not kept, so the lookup index is always used. In
    /// memory, the lookup index is used when the rows that may hold the subjects of `df` are at
    /// most half of the table, so that a small first insert into a large table does not index
    /// every stored triple. A large insert builds the full index, which is then kept for later
    /// inserts.
    pub(crate) fn temporary_lookup_index(
        &self,
        df: &DataFrame,
        cats: &Cats,
    ) -> Result<Option<SubjectObjectIndex>, TriplestoreError> {
        if self.triples_folder.is_some() {
            return Ok(Some(self.subject_lookup_index(df, cats)?));
        }
        let Some(subjects) = self.subject_targets(df, cats) else {
            return Ok(None);
        };
        let mut ranges = Vec::with_capacity(self.segments.len());
        let mut rows = 0;
        for seg in &self.segments {
            let strs = segment_strings(seg, &subjects);
            if strs.is_empty() {
                ranges.push(vec![]);
                continue;
            }
            let offsets = get_lookup_offsets(
                &strs,
                &seg.subject_sparse_index.as_ref().unwrap().map,
                seg.height,
            );
            rows += offsets.iter().map(|(_, len)| len).sum::<usize>();
            ranges.push(offsets);
        }
        if rows * 2 > self.height {
            return Ok(None);
        }
        let mut index = SubjectObjectIndex::new(&self.subject_type, &self.object_type)?;
        for (seg, offsets) in self.segments.iter().zip(ranges) {
            if offsets.is_empty() {
                continue;
            }
            for (lf, _) in seg
                .subject_sort
                .as_ref()
                .unwrap()
                .get_lazy_frames(Some(offsets))?
            {
                let stored = collect_in_memory(lf).map_err(TriplestoreError::LazyLoadError)?;
                index.insert(&stored);
            }
        }
        if let Some(buffer) = &self.buffer {
            index.insert(buffer);
        }
        Ok(Some(index))
    }

    /// An index of (at least) the stored triples whose subject occurs in `df`. Uses the sparse
    /// subject indices of the segments, so that only the relevant row groups are read.
    pub(crate) fn subject_lookup_index(
        &self,
        df: &DataFrame,
        cats: &Cats,
    ) -> Result<SubjectObjectIndex, TriplestoreError> {
        let mut index = SubjectObjectIndex::new(&self.subject_type, &self.object_type)?;
        let subjects = self.subject_targets(df, cats);
        let mut lfs = vec![];
        for seg in &self.segments {
            if let Some(subjects) = &subjects {
                let strs = segment_strings(seg, subjects);
                if strs.is_empty() {
                    // The filter of the segment holds none of the subjects.
                    continue;
                }
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
        if let Some((lf, _)) = self.buffer_lazy_frame() {
            lfs.push(lf);
        }
        if !lfs.is_empty() {
            let lf = concat_lfs(lfs);
            let stored =
                collect_in_memory(lf.select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]))
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

impl Triples {
    /// The distinct subjects of `df` that the filter of some segment may hold, as (string,
    /// code) sorted by string, or None if subjects are not categorical. Subjects that no
    /// segment holds are not decoded. The write buffer is deduplicated against as a whole.
    fn subject_targets(&self, df: &DataFrame, cats: &Cats) -> Option<Vec<(String, u32)>> {
        let encs = maybe_get_cat_encs(cats, &self.subject_type)?;
        let mut codes = Vec::new();
        extend_codes(&mut codes, df.column(SUBJECT_COL_NAME).unwrap());
        let codes: Vec<u32> = distinct_codes(codes)
            .into_iter()
            .filter(|c| self.segments_may_hold_code(*c, false))
            .collect();
        if codes.is_empty() {
            return Some(vec![]);
        }
        let us: Vec<_> = codes.iter().map(|c| Some(*c)).collect();
        let mut targets: Vec<(String, u32)> = encs
            .maps
            .decode_batch(&us)
            .into_iter()
            .zip(codes)
            .map(|(x, c)| (x.unwrap().into_owned(), c))
            .collect();
        targets.sort_unstable();
        Some(targets)
    }
}

/// The strings of the `targets` (sorted by string) that the subject filter of `seg` may hold.
fn segment_strings<'a>(seg: &TriplesSegment, targets: &'a [(String, u32)]) -> Vec<&'a str> {
    targets
        .iter()
        .filter(|(_, c)| seg.may_hold_code(*c, false))
        .map(|(s, _)| s.as_str())
        .collect()
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
