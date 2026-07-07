use super::{
    add_rank_sort_triples, prepare_add_triples_par, NewTriples, TriplesToAdd, Triplestore,
};
use crate::errors::TriplestoreError;
use crate::storage::Triples;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::CatTriples;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{instrument, trace};

impl Triplestore {
    #[instrument(skip_all)]
    pub fn add_triples_vec(
        &mut self,
        ts: Vec<TriplesToAdd>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let prepare_triples_now = Instant::now();
        let dfs_to_add = prepare_add_triples_par(ts, self.global_cats.clone())?;
        trace!(
            "Preparing triples took {} seconds",
            prepare_triples_now.elapsed().as_secs_f32()
        );
        let add_triples_now = Instant::now();
        let new_triples = self.add_local_cat_triples(dfs_to_add, transient)?;
        trace!(
            "Adding triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(new_triples)
    }

    #[instrument(skip_all)]
    fn add_local_cat_triples(
        &mut self,
        local_cat_triples: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_globalize = Instant::now();
        let cat_triples = self.globalize(local_cat_triples);
        trace!(
            "Globalizing took: {} seconds",
            start_globalize.elapsed().as_secs_f32()
        );
        let start_sort_rank = Instant::now();
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
        let start_add_global = Instant::now();
        let res = self.add_global_cat_triples(cat_triples, transient);
        trace!(
            "Add global took: {} seconds",
            start_add_global.elapsed().as_secs_f32()
        );
        res
    }

    #[instrument(skip_all)]
    pub(crate) fn add_global_cat_triples(
        &mut self,
        global_cat_triples: Vec<CatTriples>,
        transient: bool,
    ) -> Result<Vec<NewTriples>, TriplestoreError> {
        let start_prep_add = Instant::now();
        let mut add_map: HashMap<_, (_, Vec<_>)> = HashMap::new();

        for CatTriples {
            mut encoded_triples,
            predicate,
            graph,
            subject_type,
            object_type,
            local_cats: _,
        } in global_cat_triples
        {
            self.add_graph_if_not_exists(&graph, transient);
            let use_map = if transient {
                &mut self.graph_transient_triples_map.get_mut(&graph).unwrap()
            } else {
                &mut self.graph_triples_map.get_mut(&graph).unwrap()
            };
            let mut map = HashMap::new();
            map.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.clone().into_default_stored_rdf_node_state(),
            );
            map.insert(
                OBJECT_COL_NAME.to_string(),
                object_type.clone().into_default_stored_rdf_node_state(),
            );

            let k = (subject_type.clone(), object_type.clone());
            let triples = if let Some(m) = use_map.get_mut(&predicate) {
                m.remove(&k)
            } else {
                None
            };

            // Remove triples that are already inserted non-transiently
            if transient {
                if let Some(map) = self.graph_triples_map.get_mut(&graph) {
                    if let Some(m) = map.get_mut(&predicate) {
                        if let Some(non_transient_triples) = m.get_mut(&k) {
                            let nondup_df = non_transient_triples.remove_overlapping_triples_from_dataframe(&encoded_triples.df)?;
                            if let Some(nondup_df) = nondup_df {
                                encoded_triples.df = nondup_df;
                            } else {
                                continue;
                            }
                        }
                    }
                }
            };

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
        //Why does not par iter work?
        let r: Result<Vec<_>, TriplestoreError> = add_vec
            .into_iter()
            .map(move |(graph, p, s, o, triples, v)| {
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
                    let triples =
                        Triples::new(et.df, s.clone(), o.clone(), &p, &indexing, cats.clone())?;
                    triples
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
                let k = (graph, p, s, o);
                Ok(((k, triples), new_triples_vec))
            })
            .collect();
        let (to_add_map, out_new_triples): (HashMap<_, _>, Vec<_>) = r?.into_iter().unzip();

        let out_new_triples: Vec<_> = out_new_triples.into_iter().flatten().collect();
        for ((graph, p, s, o), v) in to_add_map {
            let use_map = if transient {
                self.graph_transient_triples_map.get_mut(&graph).unwrap()
            } else {
                self.graph_triples_map.get_mut(&graph).unwrap()
            };
            if !use_map.contains_key(&p) {
                use_map.insert(p.clone(), HashMap::new());
            }
            let k = (s, o);
            let m = use_map.get_mut(&p).unwrap();
            m.insert(k, v);
        }
        for nt in &out_new_triples {
            if matches!(nt.object_type, BaseRDFNodeType::Literal(..)) {
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

        Ok(out_new_triples)
    }
}
