use super::{
    compact_segments, create_deduplicated_string_vec, maybe_get_cat_encs, Triples, TriplesSegment,
    OFFSET_STEP,
};
use crate::errors::TriplestoreError;
use polars_core::prelude::DataFrame;
use representation::cats::Cats;
use representation::SUBJECT_COL_NAME;
use std::path::PathBuf;
use std::time::Instant;
use tracing::trace;

impl Triples {
    pub fn deduplicate_and_insert(
        &mut self,
        df: DataFrame,
        global_cats: &Cats,
        storage_folder: Option<&PathBuf>,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let start_deduplication = Instant::now();
        let new_df = if let Some(so_index) = &mut self.subject_object_index {
            let new_df = so_index.update_so_index(df, &self.object_type);
            trace!(
                "Deduplication using subject object index took: {}",
                start_deduplication.elapsed().as_secs_f32()
            );
            new_df
        } else {
            let new_df = self.deduplicate_segment_no_index(df, global_cats)?;
            trace!(
                "Deduplication using scan took: {} for datatype {}",
                start_deduplication.elapsed().as_secs_f32(),
                &self.object_type
            );
            new_df
        };

        if let Some(new_df) = new_df {
            // Here we decide if segments should be compacted
            let mut should_compact = self.segments.len() > 10 && self
                .height
                .saturating_mul(self.segments.len().saturating_mul(self.segments.len()))
                > 100_000;
            let new_segment = TriplesSegment::new(
                new_df.clone(),
                storage_folder,
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
                &global_cats,
            )?;
            self.height = self.height + new_segment.height;

            if should_compact {
                trace!("Creating compacted segment");
                let compacting_now = Instant::now();
                let mut ts: Vec<_> = self.segments.drain(..).map(|x| (x, false)).collect();

                ts.push((new_segment, true));
                let (segment, _) = compact_segments(
                    ts,
                    &global_cats,
                    &self.subject_type,
                    &self.object_type,
                    storage_folder,
                )?;
                self.segments.push(segment);
                trace!(
                    "Compacting took: {}",
                    compacting_now.elapsed().as_secs_f32()
                );
            } else {
                self.segments.push(new_segment);
            }
            Ok(Some(new_df))
        } else {
            Ok(None)
        }
    }

    pub fn deduplicate_segment_no_index(
        &self,
        df: DataFrame,
        global_cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        let mut subjects_time = 0f32;
        let mut nonoverlapping_time = 0f32;

        let mut incoming_df = df;
        let mut remaining_height = self.height;
        let mut i = 0;

        let now_subjects = Instant::now();
        let maybe_subject_encs = maybe_get_cat_encs(global_cats, &self.subject_type);

        let subjects_str = if OFFSET_STEP / 10 * incoming_df.height() < remaining_height {
            let new_subjects_col = incoming_df.column(SUBJECT_COL_NAME).unwrap();
            let subjects = create_deduplicated_string_vec(
                new_subjects_col.as_materialized_series(),
                maybe_subject_encs.as_ref().unwrap(),
            );
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

            let non_overlap_now = Instant::now();
            let subjects_str_non_cow = if let Some(subjects_str) = &subjects_str {
                let v: Vec<&str> = subjects_str.iter().map(|x| x.as_str()).collect();
                Some(v)
            } else {
                None
            };
            incoming_df = segment.non_overlapping(subjects_str_non_cow.as_ref(), incoming_df)?;
            nonoverlapping_time += non_overlap_now.elapsed().as_secs_f32();

            if incoming_df.height() == 0 {
                break;
            }
            remaining_height -= segment.height;
            i += 1;
        }
        trace!(
            subject = subjects_time,
            nonoverlapping = nonoverlapping_time
        );
        let res = if incoming_df.height() > 0 {
            Some(incoming_df)
        } else {
            None
        };
        Ok(res)
    }
}
