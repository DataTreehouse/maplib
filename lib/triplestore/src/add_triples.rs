use super::{
    add_rank_sort_triples, can_encode_globally, prepare_add_triples_global,
    prepare_add_triples_par, NewTriples, TriplesToAdd, Triplestore,
};
use crate::errors::TriplestoreError;
use crate::storage::Triples;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::CatTriples;
use representation::dataset::NamedGraph;
use polars::prelude::{col, IntoLazy};
use representation::rdf_to_polars::normalized_datetime_dtype;
use representation::constants::normalize_legacy_chrontext_named_node;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{instrument, trace};

impl Triplestore {
    #[instrument(skip_all)]
    pub fn add_triples_vec(
        &mut self,
        ts: Vec<TriplesToAdd>,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        self.add_triples_vec_excluding(ts, None)
    }

    /// Adds the triples, except those that the graph `exclude` already has (with the same
    /// predicate, subject and object). The triples are compared in their encoded form, as when
    /// they are deduplicated against the graph they are added to, so no query is needed. The
    /// excluded graph is not changed.
    #[instrument(skip_all)]
    pub fn add_triples_vec_excluding(
        &mut self,
        ts: Vec<TriplesToAdd>,
        exclude: Option<&NamedGraph>,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let ts: Vec<TriplesToAdd> = ts.into_iter().map(normalize_datetime_objects).collect();
        let prepare_triples_now = Instant::now();
        let dfs_to_add = if can_encode_globally(&ts, &self.global_cats) {
            prepare_add_triples_global(ts, self.global_cats.clone())?
        } else {
            prepare_add_triples_par(ts, self.global_cats.clone())?
        };
        trace!(
            "Preparing triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();
        let new_triples = self.add_local_cat_triples_excluding(dfs_to_add, exclude)?;
        trace!(
            "Adding triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(new_triples)
    }

    #[instrument(skip_all)]
    pub(crate) fn add_local_cat_triples(
        &mut self,
        local_cat_triples: Vec<CatTriples>,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        self.add_local_cat_triples_excluding(local_cat_triples, None)
    }

    pub(crate) fn add_local_cat_triples_excluding(
        &mut self,
        local_cat_triples: Vec<CatTriples>,
        exclude: Option<&NamedGraph>,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_globalize = Instant::now();
        let cat_triples = self.globalize(local_cat_triples);
        trace!(
            "Globalizing took: {} seconds",
            start_globalize.elapsed().as_secs_f32()
        );
        let start_sort_rank = Instant::now();
        // Collects in memory, see IN_MEMORY_COLLECT_LOOPS in lib.rs.
        let cat_triples: Result<_, TriplestoreError> = cat_triples
            .into_par_iter()
            .map(|mut t| {
                t.encoded_triples = add_rank_sort_triples(
                    t.encoded_triples,
                    &t.subject_type,
                    &t.object_type,
                    self.global_cats.clone(),
                )?;
                Ok(t)
            })
            .collect();
        let cat_triples = cat_triples?;
        trace!(
            "Sorting and adding rank took {} seconds",
            start_sort_rank.elapsed().as_secs_f32()
        );
        let cat_triples = if let Some(exclude) = exclude {
            self.without_triples_in_graph(cat_triples, exclude)?
        } else {
            cat_triples
        };
        let start_add_global = Instant::now();
        let res = self.add_global_cat_triples(cat_triples);
        trace!(
            "Add global took: {} seconds",
            start_add_global.elapsed().as_secs_f32()
        );
        res
    }

    /// Removes the globally encoded triples that the graph already has.
    fn without_triples_in_graph(
        &mut self,
        cat_triples: Vec<CatTriples>,
        graph: &NamedGraph,
    ) -> Result<Vec<CatTriples>, TriplestoreError> {
        let start = Instant::now();
        let cats = self.global_cats.clone();
        let cats = cats.read()?;
        let mut out = Vec::with_capacity(cat_triples.len());
        for mut t in cat_triples {
            let predicate = normalize_legacy_chrontext_named_node(t.predicate.clone());
            let stored = self
                .graph_triples_map
                .tables_for_indexing()
                .get_mut(graph)
                .and_then(|m| m.get_mut(&predicate))
                .and_then(|m| m.get_mut(&(t.subject_type.clone(), t.object_type.clone())));
            if let Some(stored) = stored {
                match stored.remove_overlapping_triples_from_dataframe(&t.encoded_triples.df, &cats)? {
                    Some(df) => t.encoded_triples.df = df,
                    None => continue,
                }
                stored.release_subject_object_index_if_on_disk();
            }
            out.push(t);
        }
        trace!(
            "Removing the triples of graph {graph} took {} seconds",
            start.elapsed().as_secs_f32()
        );
        Ok(out)
    }

    #[instrument(skip_all)]
    pub(crate) fn add_global_cat_triples(
        &mut self,
        global_cat_triples: Vec<CatTriples>,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_prep_add = Instant::now();
        let mut add_map: HashMap<_, (_, Vec<_>)> = HashMap::new();

        for CatTriples {
            encoded_triples,
            predicate,
            graph,
            subject_type,
            object_type,
            local_cats: _,
        } in global_cat_triples
        {
            // Deprecated predicates in the former chrontext namespace are stored as the
            // predicates of the maplib vocabulary that replaced them.
            let predicate = normalize_legacy_chrontext_named_node(predicate);
            self.add_graph_if_not_exists(&graph)?;
            let k = (subject_type.clone(), object_type.clone());

            let triples = self
                .graph_triples_map
                .predicate_mut(&graph, &predicate)
                .and_then(|m| m.remove(&k));

            let (s, o) = k;

            let k2 = (graph.clone(), predicate.clone(), s, o);

            if !add_map.contains_key(&k2) {
                add_map.insert(k2.clone(), (triples, vec![]));
            }
            let (_, v) = add_map.get_mut(&k2).unwrap();
            v.push(encoded_triples);
        }
        let cats = self.global_cats.clone();
        let mut add_vec = vec![];
        for ((graph, p, s, o), (t1, t2)) in add_map.into_iter() {
            add_vec.push((graph, p, s, o, t1, t2));
        }
        trace!(
            "Preparing for add took {}",
            start_prep_add.elapsed().as_secs_f32()
        );
        let indexing = self.indexing.clone();
        let triples_folder = self.triples_folder.clone();
        // Tables are independent, so they are built in parallel. Every collect under this loop is
        // in memory, see IN_MEMORY_COLLECT_LOOPS in lib.rs.
        let r: Result<Vec<_>, TriplestoreError> = add_vec
            .into_par_iter()
            .map(|(graph, p, s, o, triples, v)| {
                let mut new_triples_vec = vec![];
                let mut v_iter = v.into_iter();
                let mut triples = if let Some(triples) = triples {
                    triples
                } else {
                    let et = v_iter.next().unwrap();
                    let new_triples = NewTriples {
                        df: Some(
                            et.df
                                .clone()
                                .select([SUBJECT_COL_NAME, OBJECT_COL_NAME])
                                .unwrap(),
                        ),
                        graph: graph.clone(),
                        predicate: p.clone(),
                        subject_type: s.clone(),
                        object_type: o.clone(),
                    };
                    new_triples_vec.push(new_triples);
                    Triples::new(
                        et.df,
                        s.clone(),
                        o.clone(),
                        &p,
                        &indexing,
                        cats.clone(),
                        triples_folder.as_deref(),
                    )?
                };
                for enc in v_iter {
                    let new_triples_opt = triples.add_triples(enc.df.clone(), cats.clone())?;
                    let new_triples = NewTriples {
                        df: new_triples_opt
                            .map(|x| x.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap()),
                        graph: graph.clone(),
                        predicate: p.clone(),
                        subject_type: s.clone(),
                        object_type: o.clone(),
                    };
                    new_triples_vec.push(new_triples);
                }
                triples.release_subject_object_index_if_on_disk();
                let k = (graph, p, s, o);
                Ok(((k, triples), new_triples_vec))
            })
            .collect();
        let (to_add_map, out_new_triples): (HashMap<_, _>, Vec<_>) = r?.into_iter().unzip();

        let out_new_triples: Vec<_> = out_new_triples.into_iter().flatten().collect();
        for ((graph, p, s, o), v) in to_add_map {
            self.graph_triples_map
                .predicate_entry(&graph, &p)
                .insert((s, o), v);
        }
        let fts_iris = self.indexing.fts_iris;
        for nt in &out_new_triples {
            let fts_object = match nt.object_type {
                BaseRDFNodeType::Literal(..) => true,
                BaseRDFNodeType::IRI => fts_iris,
                _ => false,
            };
            if fts_object {
                if let Some(df) = &nt.df {
                    if let Some(fts_index) = self.fts_index.get_mut(&nt.graph) {
                        let fts_now = Instant::now();
                        fts_index
                            .add_literal_string(
                                df,
                                &nt.predicate,
                                &nt.subject_type,
                                &nt.subject_type.default_stored_cat_state(),
                                &nt.object_type,
                                &nt.object_type.default_stored_cat_state(),
                                self.global_cats.clone(),
                            )
                            .map_err(TriplestoreError::FtsError)?;
                        trace!(
                            "Adding to fts index took {} seconds",
                            fts_now.elapsed().as_secs_f32()
                        );
                        fts_index.commit(true).map_err(TriplestoreError::FtsError)?;
                    }
                }
            }
        }

        self.add_to_geo_index(&out_new_triples)?;
        self.add_to_vector_index(&out_new_triples)?;
        Ok(out_new_triples)
    }
}

/// Stores xsd:dateTime objects as UTC instants even when they come from naive or non-UTC
/// datetime columns, so that all stored dateTimes compare and union with each other.
pub(crate) fn normalize_datetime_objects(mut t: TriplesToAdd) -> TriplesToAdd {
    if let Ok(c) = t.df.column(OBJECT_COL_NAME) {
        if let Some(dt) = normalized_datetime_dtype(c.dtype()) {
            t.df = t
                .df
                .lazy()
                .with_column(col(OBJECT_COL_NAME).cast(dt))
                .collect()
                .unwrap();
        }
    }
    t
}
