use utils::polars::CollectAuto;
use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::changes::Deleted;
use crate::{NewTriples, Triplestore};
use oxrdf::NamedNode;
use polars::prelude::{col, IntoLazy};
use polars_core::datatypes::AnyValue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{cat_encode_triples, CatTriples, LockedCats};
use representation::dataset::NamedGraph;
use representation::multitype::{
    known_convert_lf_multicol_to_single, set_struct_all_null_to_null_row, split_df_multicols,
};
use representation::solution_mapping::EagerSolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME,
};
use std::time::Instant;
use tracing::trace;
use utils::polars::collect_in_memory;

impl Triplestore {
    /// Deletes the triples from the graph. Returns the number of triples that were really
    /// deleted, i.e. that existed, and with `keep_triples` also those triples.
    pub fn delete_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        graph: &NamedGraph,
        keep_triples: bool,
    ) -> Result<Deleted, SparqlError> {
        // Nothing can be deleted from a graph that does not exist.
        if !self.does_graph_exist(graph) {
            return Ok(Deleted::default());
        }
        let sms: Vec<_> = sms
            .into_par_iter()
            .map(|(x, y)| partition_by_global_predicate_col(x, y, self.global_cats.clone()))
            .flatten()
            .collect();
        let global_cat_triples =
            triples_solution_mappings_to_global_cat_triples(sms, self.global_cats.clone(), graph)?;
        if !global_cat_triples.is_empty() {
            self.delete_triples_vec(global_cat_triples, graph, keep_triples)
                .map_err(SparqlError::TriplestoreError)
        } else {
            Ok(Deleted::default())
        }
    }

    pub fn delete_triples_vec(
        &mut self,
        ts: Vec<CatTriples>,
        graph: &NamedGraph,
        keep_triples: bool,
    ) -> Result<Deleted, TriplestoreError> {
        let add_triples_now = Instant::now();
        let deleted = self.delete_global_cat_triples(ts, graph, keep_triples)?;
        self.n_triples_deleted += deleted.count;
        trace!(
            "Deleting triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(deleted)
    }

    /// Deletes the triples, and returns the number of triples that existed and were deleted, and
    /// with `keep_triples` those triples. The triples are deleted from the tables in place, so
    /// the cost depends on the number of triples deleted, not on the size of the tables. Tables
    /// that contain none of the triples are left as they are.
    fn delete_global_cat_triples(
        &mut self,
        gcts: Vec<CatTriples>,
        graph: &NamedGraph,
        keep_triples: bool,
    ) -> Result<Deleted, TriplestoreError> {
        self.check_graph_exists(graph)?;
        let mut deleted = Deleted::default();
        for gct in gcts {
            let types = (gct.subject_type.clone(), gct.object_type.clone());
            let Some(triples) = self
                .graph_triples_map
                .get(graph)
                .and_then(|m| m.get(&gct.predicate))
                .and_then(|m| m.get(&types))
            else {
                continue;
            };
            let rows = {
                let cats = self.global_cats.read()?;
                triples.find_rows_to_delete(&gct.encoded_triples.df, &cats)?
            };
            let Some(rows) = rows else {
                // None of the triples existed, so the table is left as it is.
                continue;
            };
            deleted.count += rows.count();
            if keep_triples {
                deleted.triples.push(NewTriples {
                    df: Some(rows.triples().clone()),
                    graph: graph.clone(),
                    predicate: gct.predicate.clone(),
                    subject_type: gct.subject_type.clone(),
                    object_type: gct.object_type.clone(),
                });
            }
            let map = self
                .graph_triples_map
                .predicate_mut(graph, &gct.predicate)
                .expect("The table exists");
            let table = map.get_mut(&types).expect("The table exists");
            table.delete_rows(rows)?;
            if table.is_empty() {
                map.remove(&types);
                if map.is_empty() {
                    self.graph_triples_map
                        .remove_predicate(graph, &gct.predicate);
                }
            }
        }
        Ok(deleted)
    }
}

fn partition_by_global_predicate_col(
    mut sm: EagerSolutionMappings,
    predicate: Option<NamedNode>,
    global_cats: LockedCats,
) -> Vec<(EagerSolutionMappings, NamedNode)> {
    if let Some(predicate) = predicate {
        vec![(sm, predicate)]
    } else {
        let mut pred_col_t = sm.rdf_node_types.get(PREDICATE_COL_NAME).unwrap().clone();
        if pred_col_t.is_multi() {
            if !pred_col_t.map.contains_key(&BaseRDFNodeType::IRI) {
                unreachable!("Predicates should always be IRI")
            }
            sm.mappings = known_convert_lf_multicol_to_single(
                sm.mappings.lazy(),
                PREDICATE_COL_NAME,
                &BaseRDFNodeType::IRI,
            )
            .collect_auto()
            .unwrap();
            sm.rdf_node_types.insert(
                PREDICATE_COL_NAME.to_string(),
                RDFNodeState::from_bases(
                    BaseRDFNodeType::IRI,
                    pred_col_t.map.remove(&BaseRDFNodeType::IRI).unwrap(),
                ),
            );
        }
        let partitions = sm
            .mappings
            .partition_by([PREDICATE_COL_NAME], true)
            .unwrap();
        let mut predicates_u32 = vec![];
        let mut sms = vec![];
        sm.rdf_node_types.remove(PREDICATE_COL_NAME).unwrap();
        for mut part in partitions {
            {
                let any_predicate = part.column(PREDICATE_COL_NAME).unwrap().get(0);
                if let Ok(AnyValue::UInt32(u)) = any_predicate {
                    predicates_u32.push(u);
                } else {
                    panic!("Predicate: {any_predicate:?}");
                }
            }
            part = part.select([SUBJECT_COL_NAME, OBJECT_COL_NAME]).unwrap();
            sms.push(EagerSolutionMappings::new(part, sm.rdf_node_types.clone()));
        }
        let predicates = global_cats
            .read()
            .unwrap()
            .decode_iri_u32s(&predicates_u32, None);
        sms.into_iter().zip(predicates).collect()
    }
}

fn triples_solution_mappings_to_global_cat_triples(
    sm_preds: Vec<(EagerSolutionMappings, NamedNode)>,
    global_cats: LockedCats,
    graph: &NamedGraph,
) -> Result<Vec<CatTriples>, TriplestoreError> {
    // Collects in memory, see IN_MEMORY_COLLECT_LOOPS in lib.rs.
    let mappings_maps_preds: Vec<_> = sm_preds
        .into_par_iter()
        .map(|(sm, predicate)| {
            let EagerSolutionMappings {
                mappings,
                rdf_node_types,
            } = sm;

            let dfs_maps = split_df_multicols(mappings, &rdf_node_types);
            let mut dfs_maps_preds = Vec::with_capacity(dfs_maps.len());
            for (mut df, map) in dfs_maps {
                let mut lf = df.lazy();
                for (k, v) in &map {
                    //Important to work around null cols
                    lf = lf.with_column(set_struct_all_null_to_null_row(col(k), v));
                }
                lf = lf.drop_nulls(None);
                df = collect_in_memory(lf).unwrap();
                if df.height() > 0 {
                    dfs_maps_preds.push((df, map, predicate.clone()));
                }
            }
            dfs_maps_preds
        })
        .flatten()
        .collect();
    let cat_triples: Vec<_> = mappings_maps_preds
        .into_par_iter()
        .map(|(mappings, mut rdf_node_types, predicate)| {
            let (subject_type, subject_state) = rdf_node_types
                .remove(SUBJECT_COL_NAME)
                .unwrap()
                .map
                .drain()
                .next()
                .unwrap();
            let (object_type, object_state) = rdf_node_types
                .remove(OBJECT_COL_NAME)
                .unwrap()
                .map
                .drain()
                .next()
                .unwrap();
            let e = cat_encode_triples(
                mappings,
                subject_type,
                object_type,
                predicate,
                graph.clone(),
                subject_state,
                object_state,
                &global_cats.read().unwrap(),
            );
            e
        })
        .collect();
    Ok(cat_triples)
}
