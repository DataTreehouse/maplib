use crate::errors::TriplestoreError;
use crate::Triplestore;

impl Triplestore {
    /// Rebuilds the full text search indexes from the stored triples.
    ///
    /// The indexes refer to triples by their cats, so they must be rebuilt when the cats are
    /// renumbered (compaction) or when the triplestore is deserialized.
    pub(crate) fn rebuild_fts_indexes(&mut self) -> Result<(), TriplestoreError> {
        let graphs: Vec<_> = self.fts_index.keys().cloned().collect();
        for graph in graphs {
            let mut index = self.fts_index.remove(&graph).unwrap();
            index
                .clear(self.global_cats.clone())
                .map_err(TriplestoreError::FtsError)?;
            if let Some(map) = self.graph_triples_map.get(&graph) {
                for (predicate, map) in map {
                    for ((subject_type, object_type), ts) in map {
                        if !object_type.is_lit_type(oxrdf::vocab::xsd::STRING)
                            && !object_type.is_lang_string()
                        {
                            continue;
                        }
                        for (lf, _) in ts.get_lazy_frames(&None, &None)? {
                            index
                                .add_literal_string(
                                    &lf.collect().unwrap(),
                                    predicate,
                                    subject_type,
                                    &subject_type.default_stored_cat_state(),
                                    object_type,
                                    &object_type.default_stored_cat_state(),
                                    self.global_cats.clone(),
                                )
                                .map_err(TriplestoreError::FtsError)?;
                        }
                    }
                }
            }
            index.commit(true).map_err(TriplestoreError::FtsError)?;
            self.fts_index.insert(graph, index);
        }
        Ok(())
    }
}
