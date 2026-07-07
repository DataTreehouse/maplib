use super::{compact_segments, Triples, TriplesSegment};
use crate::errors::TriplestoreError;
use crate::storage::so_index::SubjectObjectIndex;
use polars_core::prelude::DataFrame;
use representation::cats::Cats;
use std::time::Instant;
use tracing::trace;

impl Triples {
    pub fn deduplicate_and_insert(
        &mut self,
        df: DataFrame,
        global_cats: &Cats,
    ) -> Result<Option<DataFrame>, TriplestoreError> {
        self.ensure_subject_object_index()?;

        let new_df = self
            .subject_object_index
            .as_mut()
            .unwrap()
            .maybe_insert_deduplicate(&df, true);
        if let Some(new_df) = new_df {
            // Here we decide if segments should be compacted
            let should_compact = self.segments.len() > 10
                && self
                    .height
                    .saturating_mul(self.segments.len().saturating_mul(self.segments.len()))
                    > 100_000;
            let new_segment = TriplesSegment::new(
                new_df.clone(),
                &self.subject_type,
                &self.object_type,
                self.object_indexing_enabled,
                &global_cats,
                false,
            )?;
            self.height = self.height + new_segment.height;

            if should_compact {
                trace!("Creating compacted segment");
                let compacting_now = Instant::now();
                let mut ts: Vec<_> = self.segments.drain(..).map(|x| (x, false)).collect();

                ts.push((new_segment, true));
                let (segment, _) =
                    compact_segments(ts, &global_cats, &self.subject_type, &self.object_type)?;
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

    pub(crate) fn ensure_subject_object_index(&mut self) -> Result<(), TriplestoreError> {
        if self.subject_object_index.is_none() {
            let mut subject_object_index =
                SubjectObjectIndex::new(&self.subject_type, &self.object_type);
            let mut lfs = self.get_all_triples_lazy_frames(true)?;
            assert_eq!(lfs.len(), 1);
            let mut df = lfs
                .pop()
                .unwrap()
                .collect()
                .map_err(|x| TriplestoreError::LazyLoadError(x))?;
            subject_object_index.insert(&mut df);
            self.subject_object_index = Some(subject_object_index);
        }
        Ok(())
    }
}
