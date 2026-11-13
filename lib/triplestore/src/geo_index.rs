use crate::errors::TriplestoreError;
use crate::{NewTriples, Triplestore};
use geo::is_geo_literal_type;
use std::time::Instant;
use tracing::trace;

impl Triplestore {
    /// Rebuilds the geo index from the stored triples.
    ///
    /// The index refers to geometries by their cats, so it must be rebuilt when the cats are
    /// renumbered (compaction), when the triplestore is deserialized, and when it is created for
    /// a triplestore that already holds triples.
    pub(crate) fn rebuild_geo_index(&mut self) -> Result<(), TriplestoreError> {
        let Some(index) = &self.geo_index else {
            return Ok(());
        };
        let now = Instant::now();
        index.clear().map_err(TriplestoreError::GeoError)?;
        for map in self.graph_triples_map.values() {
            for ((_, object_type), ts) in map.values().flat_map(|m| m.iter()) {
                if !is_geo_literal_type(object_type) {
                    continue;
                }
                for (lf, _) in ts.get_lazy_frames(&None, &None)? {
                    index
                        .add_geometries(
                            &lf.collect().unwrap(),
                            object_type,
                            &object_type.default_stored_cat_state(),
                            self.global_cats.clone(),
                        )
                        .map_err(TriplestoreError::GeoError)?;
                }
            }
        }
        trace!(
            "Rebuilding the geo index took {} seconds",
            now.elapsed().as_secs_f32()
        );
        Ok(())
    }

    /// Adds the geometries among newly added triples to the geo index.
    pub(crate) fn add_to_geo_index(
        &self,
        new_triples: &[NewTriples],
    ) -> Result<(), TriplestoreError> {
        let Some(index) = &self.geo_index else {
            return Ok(());
        };
        for nt in new_triples {
            if !is_geo_literal_type(&nt.object_type) {
                continue;
            }
            if let Some(df) = &nt.df {
                index
                    .add_geometries(
                        df,
                        &nt.object_type,
                        &nt.object_type.default_stored_cat_state(),
                        self.global_cats.clone(),
                    )
                    .map_err(TriplestoreError::GeoError)?;
            }
        }
        Ok(())
    }
}
