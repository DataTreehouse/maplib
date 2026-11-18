//! Deleting triples from a table in place.
//!
//! The rows to delete are found through the sparse indices of the segments, so only the rows
//! that may hold the subjects (and, for the object sort, the objects) of the deleted triples are
//! read. The rows are then cut out of the stored frames: in memory the remaining slices share the
//! buffers of the old frame, and on disk the remaining rows of the parquet file are kept as a
//! list of row ranges ([`DiskView`]). The sort orders stay valid, so nothing is sorted or
//! indexed again, and the cost depends on the number of deleted triples rather than on the
//! size of the table.

use super::{
    get_lookup_offsets, maybe_get_cat_encs, SparseIndex, StoredTriples, Triples, TriplesOnDisk,
    TriplesSegment,
};
use crate::errors::TriplestoreError;
use polars::prelude::{
    col, concat, DataFrame, IntoLazy, JoinArgs, JoinType, LazyFrame, MaintainOrderJoin,
    UnionArgs,
};
use polars_core::prelude::{BooleanChunked, DataType, IntoColumn, NewChunkedArray};
use polars_core::series::IsSorted;
use representation::cats::{distinct_codes, extend_codes, Cats};
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::sync::Arc;
use utils::polars::collect_in_memory;

/// A frame in memory is cut into at most this many slices by one deletion; deleting more rows
/// filters the frame instead.
const MAX_MEMORY_SLICES: usize = 16;
/// A frame in memory with more chunks than this, after slices were cut out of it, is copied
/// into one chunk.
const MAX_MEMORY_CHUNKS: usize = 32;
/// A table on disk is kept as at most this many row ranges of its file; with more, the remaining
/// rows are written to a new file.
const MAX_VIEW_RANGES: usize = 64;
const ROW_COL_NAME: &str = "maplib_delete_row";

/// The rows of a table on disk that remain after triples were deleted from it: row ranges
/// `(offset, len)` of its parquet file, sorted and disjoint. The file is shared with the
/// stored triples it was cut from, which other clones of the triplestore may still hold.
#[derive(Clone)]
pub struct DiskView {
    disk: Arc<TriplesOnDisk>,
    kept: Vec<(usize, usize)>,
    height: usize,
}

impl DiskView {
    pub(crate) fn height(&self) -> usize {
        self.height
    }

    pub(crate) fn get_lazy_frame(&self) -> Result<(LazyFrame, usize), TriplestoreError> {
        let mut lfs = Vec::with_capacity(self.kept.len());
        for (offset, len) in &self.kept {
            lfs.push(self.disk.scan()?.slice(*offset as i64, *len as u32));
        }
        let lf = if lfs.len() == 1 {
            lfs.pop().unwrap()
        } else {
            concat(
                lfs,
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
            .unwrap()
        };
        Ok((lf, self.height))
    }

    /// Reads the rows in `ranges` (`(offset, len)` of the remaining rows, sorted and not
    /// overlapping).
    pub(crate) fn read_ranges(&self, ranges: &[(usize, usize)]) -> Result<DataFrame, TriplestoreError> {
        let physical = to_physical(&self.kept, ranges);
        if physical.is_empty() {
            return self
                .disk
                .scan()?
                .slice(0, 0)
                .collect()
                .map_err(TriplestoreError::LazyLoadError);
        }
        self.disk.read_ranges(&physical)
    }
}

/// Maps row ranges of the remaining rows to row ranges of the file. Both are sorted.
fn to_physical(kept: &[(usize, usize)], logical: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = vec![];
    let mut k = 0;
    // The first remaining row of kept[k].
    let mut k_start = 0;
    for &(offset, len) in logical {
        let mut at = offset;
        let end = offset + len;
        while at < end {
            while k < kept.len() && k_start + kept[k].1 <= at {
                k_start += kept[k].1;
                k += 1;
            }
            if k == kept.len() {
                return out;
            }
            let within = at - k_start;
            let take = (kept[k].1 - within).min(end - at);
            let p = kept[k].0 + within;
            match out.last_mut() {
                Some(last) if last.0 + last.1 == p => last.1 += take,
                _ => out.push((p, take)),
            }
            at += take;
        }
    }
    out
}

/// The row ranges that remain of `kept` when the rows `positions` (sorted, distinct, counted in
/// the rows of `kept`) are removed.
fn remove_positions(kept: &[(usize, usize)], positions: &[usize]) -> Vec<(usize, usize)> {
    let mut out = Vec::with_capacity(kept.len() + positions.len());
    let mut i = 0;
    let mut start = 0;
    for &(physical, len) in kept {
        let end = start + len;
        let mut at = start;
        while i < positions.len() && positions[i] < end {
            let pos = positions[i];
            if pos > at {
                out.push((physical + (at - start), pos - at));
            }
            at = pos + 1;
            i += 1;
        }
        if at < end {
            out.push((physical + (at - start), end - at));
        }
        start = end;
    }
    out
}

/// Removes the rows `positions` (sorted and distinct) from a frame. Keeps the sorted flag of
/// the subjects, as removing rows keeps them sorted.
fn remove_rows_df(df: DataFrame, positions: &[usize]) -> DataFrame {
    let height = df.height();
    let subjects_sorted = df
        .column(SUBJECT_COL_NAME)
        .map(|c| c.as_materialized_series().is_sorted_flag() == IsSorted::Ascending)
        .unwrap_or(false);
    let kept = remove_positions(&[(0, height)], positions);
    let mut out = if kept.len() <= MAX_MEMORY_SLICES {
        let mut slices = kept.into_iter();
        if let Some((offset, len)) = slices.next() {
            let mut out = df.slice(offset as i64, len);
            for (offset, len) in slices {
                out.vstack_mut_owned(df.slice(offset as i64, len)).unwrap();
            }
            if out.max_n_chunks() > MAX_MEMORY_CHUNKS {
                out.rechunk_mut();
            }
            out
        } else {
            df.clear()
        }
    } else {
        let mut keep = vec![true; height];
        for p in positions {
            keep[*p] = false;
        }
        let mask = BooleanChunked::from_slice("".into(), &keep);
        let mut out = df.filter(&mask).unwrap();
        out.rechunk_mut();
        out
    };
    if subjects_sorted && out.height() > 0 {
        let mut subjects = out
            .column(SUBJECT_COL_NAME)
            .unwrap()
            .as_materialized_series()
            .clone();
        subjects.set_sorted_flag(IsSorted::Ascending);
        out.with_column(subjects.into_column()).unwrap();
    }
    out
}

impl StoredTriples {
    /// Removes the rows `positions` (sorted and distinct, fewer than the rows stored).
    fn remove_rows(&mut self, positions: &[usize]) -> Result<(), TriplestoreError> {
        if positions.is_empty() {
            return Ok(());
        }
        let (disk, kept) = match self {
            StoredTriples::TriplesInMemory(mem) => {
                let df = mem.df.take().unwrap();
                mem.df = Some(remove_rows_df(df, positions));
                return Ok(());
            }
            StoredTriples::TriplesOnDisk(disk) => {
                (disk.clone(), remove_positions(&[(0, disk.height)], positions))
            }
            StoredTriples::TriplesOnDiskView(view) => {
                (view.disk.clone(), remove_positions(&view.kept, positions))
            }
        };
        let height = kept.iter().map(|(_, len)| len).sum();
        let view = DiskView { disk, kept, height };
        if view.kept.len() <= MAX_VIEW_RANGES {
            *self = StoredTriples::TriplesOnDiskView(Box::new(view));
        } else {
            // Too many ranges make every read slower: the remaining rows get a file of their own.
            let df = collect_in_memory(view.get_lazy_frame()?.0)
                .map_err(TriplestoreError::LazyLoadError)?;
            let folder = view
                .disk
                .path
                .parent()
                .expect("Triple files are in a folder")
                .to_owned();
            *self = StoredTriples::new(df, Some(&folder))?;
        }
        Ok(())
    }
}

impl SparseIndex {
    /// Shifts the offsets of the index after the rows `positions` (sorted) were removed, leaving
    /// `height` rows. A key at a removed row then points at the next remaining row, which keeps
    /// the index valid: the rows before a key's offset are at most the key, and the rows from
    /// it are at least the key. Offsets past the end point at the last row; lookups read up to
    /// and including the offset of the next key, so this only widens them.
    fn remove_rows(&mut self, positions: &[usize], height: usize) {
        for offset in self.map.values_mut() {
            let removed_before = positions.partition_point(|p| *p < *offset);
            *offset = (*offset - removed_before).min(height - 1);
        }
    }
}

/// The rows to delete from a table, found by [`Triples::find_rows_to_delete`].
pub(crate) struct RowsToDelete {
    /// By segment: the segment, its rows in the subject sort and in the object sort.
    segments: Vec<(usize, Vec<usize>, Option<Vec<usize>>)>,
    /// The subject and object columns of the triples that are deleted.
    triples: DataFrame,
}

impl RowsToDelete {
    /// The number of triples that are deleted.
    pub(crate) fn count(&self) -> usize {
        self.triples.height()
    }

    /// The subject and object columns of the triples that are deleted.
    pub(crate) fn triples(&self) -> &DataFrame {
        &self.triples
    }
}

impl Triples {
    /// Finds the stored triples among `to_delete` (subject and object columns, encoded with the
    /// global cats). Returns None if none of them are stored.
    pub(crate) fn find_rows_to_delete(
        &self,
        to_delete: &DataFrame,
        cats: &Cats,
    ) -> Result<Option<RowsToDelete>, TriplestoreError> {
        let to_delete = to_delete
            .select([SUBJECT_COL_NAME, OBJECT_COL_NAME])
            .unwrap();
        let subjects = match sorted_strings(&to_delete, SUBJECT_COL_NAME, &self.subject_type, cats) {
            // None of the subjects are known, so none of the triples are stored.
            Some(s) if s.is_empty() => return Ok(None),
            s => s,
        };
        let subjects: Option<Vec<&str>> = subjects
            .as_ref()
            .map(|s| s.iter().map(|x| x.as_str()).collect());
        let mut segments = vec![];
        let mut found = vec![];
        for (i, seg) in self.segments.iter().enumerate() {
            let (subject_rows, triples) = matching_rows(
                seg.subject_sort.as_ref().unwrap(),
                seg.subject_sparse_index.as_ref().unwrap(),
                seg.height,
                subjects.as_deref(),
                &to_delete,
            )?;
            if subject_rows.is_empty() {
                continue;
            }
            let object_rows = if let Some(object_sort) = &seg.object_sort {
                let objects = sorted_strings(&triples, OBJECT_COL_NAME, &self.object_type, cats);
                let objects: Option<Vec<&str>> = objects
                    .as_ref()
                    .map(|s| s.iter().map(|x| x.as_str()).collect());
                let (object_rows, _) = matching_rows(
                    object_sort,
                    seg.object_sparse_index.as_ref().unwrap(),
                    seg.height,
                    objects.as_deref(),
                    &triples,
                )?;
                if object_rows.len() != subject_rows.len() {
                    return Err(TriplestoreError::IndexingError(format!(
                        "The object sort of a table holds {} of {} triples to delete",
                        object_rows.len(),
                        subject_rows.len()
                    )));
                }
                Some(object_rows)
            } else {
                None
            };
            segments.push((i, subject_rows, object_rows));
            found.push(triples);
        }
        let mut found = found.into_iter();
        let Some(mut triples) = found.next() else {
            return Ok(None);
        };
        for other in found {
            triples
                .vstack_mut_owned(other)
                .map_err(TriplestoreError::LazyLoadError)?;
        }
        Ok(Some(RowsToDelete { segments, triples }))
    }

    /// Deletes the rows found by [`Triples::find_rows_to_delete`].
    pub(crate) fn delete_rows(&mut self, rows: RowsToDelete) -> Result<(), TriplestoreError> {
        let RowsToDelete { segments, triples } = rows;
        for (i, subject_rows, object_rows) in segments.into_iter().rev() {
            let seg: &mut TriplesSegment = &mut self.segments[i];
            let height = seg.height - subject_rows.len();
            if height == 0 {
                self.segments.remove(i);
                continue;
            }
            seg.subject_sort
                .as_mut()
                .unwrap()
                .remove_rows(&subject_rows)?;
            seg.subject_sparse_index
                .as_mut()
                .unwrap()
                .remove_rows(&subject_rows, height);
            if let (Some(object_sort), Some(object_rows)) = (&mut seg.object_sort, object_rows) {
                object_sort.remove_rows(&object_rows)?;
                seg.object_sparse_index
                    .as_mut()
                    .unwrap()
                    .remove_rows(&object_rows, height);
            }
            seg.height = height;
        }
        self.height -= triples.height();
        self.generation = super::next_generation();
        if let Some(index) = &mut self.subject_object_index {
            index.delete(&triples);
        }
        Ok(())
    }
}

/// The sorted, distinct strings of a column of codes (the codes not in the cats are left out), or
/// of strings. None for other columns.
fn sorted_strings(
    df: &DataFrame,
    c: &str,
    t: &BaseRDFNodeType,
    cats: &Cats,
) -> Option<Vec<String>> {
    let column = df.column(c).unwrap();
    let mut strings: Vec<String> = if let Some(encs) = maybe_get_cat_encs(cats, t) {
        if !matches!(column.dtype(), DataType::UInt32) {
            return None;
        }
        let mut codes = Vec::new();
        extend_codes(&mut codes, column);
        let us: Vec<_> = distinct_codes(codes).into_iter().map(Some).collect();
        encs.maps
            .decode_batch(&us)
            .into_iter()
            .flatten()
            .map(|x| x.into_owned())
            .collect()
    } else if let Ok(s) = column.str() {
        s.iter().flatten().map(|x| x.to_string()).collect()
    } else {
        return None;
    };
    strings.sort_unstable();
    strings.dedup();
    Some(strings)
}

/// The rows (sorted) of `stored`, a sort of `height` rows with the sparse index `sparse`, that
/// hold triples of `to_delete`, and those triples. With `targets` (the sorted strings of the
/// sorted column of the triples to delete) only the rows that may hold them are read.
fn matching_rows(
    stored: &StoredTriples,
    sparse: &SparseIndex,
    height: usize,
    targets: Option<&[&str]>,
    to_delete: &DataFrame,
) -> Result<(Vec<usize>, DataFrame), TriplestoreError> {
    let ranges = targets
        .map(|t| get_lookup_offsets(t, &sparse.map, height))
        .filter(|r| r.iter().map(|(_, len)| len).sum::<usize>() * 2 <= height);
    let (lf, ranges) = match ranges {
        Some(ranges) if ranges.is_empty() => {
            return Ok((vec![], to_delete.clear()));
        }
        Some(ranges) => {
            let mut lfs: Vec<_> = stored
                .get_lazy_frames(Some(ranges.clone()))?
                .into_iter()
                .map(|(lf, _)| lf)
                .collect();
            let lf = if lfs.len() == 1 {
                lfs.pop().unwrap()
            } else {
                concat(
                    lfs,
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
                .unwrap()
            };
            (lf, ranges)
        }
        None => (stored.get_lazy_frame()?, vec![(0, height)]),
    };
    let on = [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
    let found = collect_in_memory(
        lf.select(on.clone())
            .with_row_index(ROW_COL_NAME, None)
            .join(
                to_delete.clone().lazy(),
                on.clone(),
                on,
                JoinArgs {
                    how: JoinType::Semi,
                    validation: Default::default(),
                    suffix: None,
                    slice: None,
                    nulls_equal: false,
                    coalesce: Default::default(),
                    maintain_order: MaintainOrderJoin::Left,
                    build_side: None,
                },
            )
            .map_err(TriplestoreError::LazyLoadError)?,
    )
    .map_err(TriplestoreError::LazyLoadError)?;
    // Row numbers in the rows read, to rows of the sort.
    let mut read: Vec<usize> = found
        .column(ROW_COL_NAME)
        .unwrap()
        .as_materialized_series()
        .cast(&DataType::UInt64)
        .unwrap()
        .u64()
        .unwrap()
        .into_no_null_iter()
        .map(|x| x as usize)
        .collect();
    read.sort_unstable();
    read.dedup();
    let mut rows = Vec::with_capacity(read.len());
    let mut r = ranges.iter();
    let mut current = r.next().copied();
    let mut start = 0;
    for i in read {
        while let Some((_, len)) = current {
            if i < start + len {
                break;
            }
            start += len;
            current = r.next().copied();
        }
        let (offset, _) = current.expect("Rows read are in the ranges");
        rows.push(offset + (i - start));
    }
    let triples = found.drop(ROW_COL_NAME).unwrap();
    Ok((rows, triples))
}

#[cfg(test)]
mod tests {
    use super::{remove_positions, to_physical};

    #[test]
    fn test_remove_positions_and_to_physical() {
        let kept = remove_positions(&[(0, 10)], &[0, 3, 4, 9]);
        assert_eq!(kept, vec![(1, 2), (5, 4)]);
        // Remaining rows: 1 2 5 6 7 8
        let kept2 = remove_positions(&kept, &[1, 2]);
        assert_eq!(kept2, vec![(1, 1), (6, 3)]);
        assert_eq!(to_physical(&kept, &[(0, 3), (4, 2)]), vec![(1, 2), (5, 1), (7, 2)]);
        assert_eq!(to_physical(&kept, &[(1, 10)]), vec![(2, 1), (5, 4)]);
    }
}
