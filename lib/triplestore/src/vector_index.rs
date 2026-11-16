use crate::errors::TriplestoreError;
use crate::{NewTriples, Triplestore};
use representation::dataset::NamedGraph;
use std::time::Instant;
use tracing::trace;
use vector::{is_vector_literal_type, VectorIndex, VectorIndexOptions};

impl Triplestore {
    fn vector_index_options(&self) -> VectorIndexOptions {
        VectorIndexOptions {
            approximate: self.indexing.vector_ann,
        }
    }

    /// Creates the vector index of a graph from its stored triples.
    fn build_vector_index(&self, graph: &NamedGraph) -> Result<VectorIndex, TriplestoreError> {
        let now = Instant::now();
        let index =
            VectorIndex::new(&self.vector_index_options()).map_err(TriplestoreError::VectorError)?;
        if let Some(map) = self.graph_triples_map.get(graph) {
            for (predicate, map) in map {
                for ((subject_type, object_type), ts) in map {
                    if !is_vector_literal_type(object_type) {
                        continue;
                    }
                    for (lf, _) in ts.get_lazy_frames(&None, &None)? {
                        index
                            .add_vectors(
                                &lf.collect().unwrap(),
                                predicate,
                                subject_type,
                                object_type,
                                self.global_cats.clone(),
                            )
                            .map_err(TriplestoreError::VectorError)?;
                    }
                }
            }
        }
        index.finish().map_err(TriplestoreError::VectorError)?;
        trace!(
            "Building the vector index took {} seconds",
            now.elapsed().as_secs_f32()
        );
        Ok(index)
    }

    /// Rebuilds the vector indexes of all graphs from the stored triples.
    ///
    /// The indexes refer to triples by their cats, so they must be rebuilt when the cats are
    /// renumbered (compaction), when the triplestore is deserialized, and when they are created
    /// for a triplestore that already holds triples.
    pub(crate) fn rebuild_vector_indexes(&mut self) -> Result<(), TriplestoreError> {
        self.vector_index.clear();
        if !self.indexing.vectors {
            return Ok(());
        }
        let graphs: Vec<_> = self.graph_triples_map.keys().cloned().collect();
        for graph in graphs {
            let index = self.build_vector_index(&graph)?;
            self.vector_index.insert(graph, index);
        }
        Ok(())
    }

    /// Adds the vectors among newly added triples to the vector indexes.
    pub(crate) fn add_to_vector_index(
        &mut self,
        new_triples: &[NewTriples],
    ) -> Result<(), TriplestoreError> {
        if !self.indexing.vectors {
            return Ok(());
        }
        let mut touched = vec![];
        for nt in new_triples {
            if !is_vector_literal_type(&nt.object_type) {
                continue;
            }
            let Some(df) = &nt.df else {
                continue;
            };
            if !self.vector_index.contains_key(&nt.graph) {
                let index = VectorIndex::new(&self.vector_index_options())
                    .map_err(TriplestoreError::VectorError)?;
                self.vector_index.insert(nt.graph.clone(), index);
            }
            let index = self.vector_index.get(&nt.graph).unwrap();
            index
                .add_vectors(
                    df,
                    &nt.predicate,
                    &nt.subject_type,
                    &nt.object_type,
                    self.global_cats.clone(),
                )
                .map_err(TriplestoreError::VectorError)?;
            if !touched.contains(&nt.graph) {
                touched.push(nt.graph.clone());
            }
        }
        for g in touched {
            self.vector_index
                .get(&g)
                .unwrap()
                .finish()
                .map_err(TriplestoreError::VectorError)?;
        }
        Ok(())
    }
}
