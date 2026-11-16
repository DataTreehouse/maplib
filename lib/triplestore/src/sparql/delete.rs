use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::{PredicateTriples, Triples};
use crate::changes::{concat_lfs, Deleted};
use crate::{add_rank_sort_triples, NewTriples, Triplestore};
use oxrdf::NamedNode;
use polars::prelude::{
    col, concat, DataFrame, IntoLazy, JoinArgs, JoinType, MaintainOrderJoin, UnionArgs,
};
use polars_core::datatypes::AnyValue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{cat_encode_triples, CatTriples, EncodedTriples, LockedCats};
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
    /// with `keep_triples` those triples. Tables that contain none of the triples are left as
    /// they are.
    fn delete_global_cat_triples(
        &mut self,
        gcts: Vec<CatTriples>,
        graph: &NamedGraph,
        keep_triples: bool,
    ) -> Result<Deleted, TriplestoreError> {
        self.check_graph_exists(graph)?;
        let mut deleted = Deleted::default();
        for gct in gcts {
            let Some(map) = self.graph_triples_map.get(graph) else {
                continue;
            };
            let Some(existing_len) = map
                .get(&gct.predicate)
                .and_then(|m| m.get(&(gct.subject_type.clone(), gct.object_type.clone())))
                .map(|t| t.len())
            else {
                continue;
            };
            let remaining_gct = get_triples_after_deletion(&gct, map)?;
            let remaining_len = remaining_gct
                .as_ref()
                .map(|x| x.encoded_triples.df.height())
                .unwrap_or(0);
            let n = existing_len.saturating_sub(remaining_len);
            if n == 0 {
                // None of the triples existed, so the table is left as it is.
                continue;
            }
            deleted.count += n;
            if keep_triples {
                // The table exists, since triples were deleted from it.
                let types = (gct.subject_type.clone(), gct.object_type.clone());
                let existing = &map[&gct.predicate][&types];
                deleted.triples.push(NewTriples {
                    df: Some(deleted_triples_df(&gct, existing)?),
                    graph: graph.clone(),
                    predicate: gct.predicate.clone(),
                    subject_type: gct.subject_type.clone(),
                    object_type: gct.object_type.clone(),
                });
            }
            self.delete_if_exists(&gct, graph)?;
            if let Some(mut gct) = remaining_gct {
                gct.encoded_triples = add_rank_sort_triples(
                    gct.encoded_triples,
                    &gct.subject_type,
                    &gct.object_type,
                    self.global_cats.clone(),
                )?;
                self.add_global_cat_triples(vec![gct])?;
            }
        }
        Ok(deleted)
    }

    fn delete_if_exists(
        &mut self,
        gct: &CatTriples,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        self.check_graph_exists(graph)?;
        let map_empty = if let Some(m1) = self.graph_triples_map.predicate_mut(graph, &gct.predicate) {
            let k = (gct.subject_type.clone(), gct.object_type.clone());
            m1.remove(&k);
            m1.is_empty()
        } else {
            false
        };
        if map_empty {
            self.graph_triples_map.remove_predicate(graph, &gct.predicate);
        }
        Ok(())
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
            .collect()
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

fn get_triples_after_deletion(
    gct: &CatTriples,
    map: &PredicateTriples,
) -> Result<Option<CatTriples>, TriplestoreError> {
    if let Some(m) = map.get(&gct.predicate) {
        let type_ = (gct.subject_type.clone(), gct.object_type.clone());
        let encoded: Result<_, TriplestoreError> = if let Some(triples) = m.get(&type_) {
            let lfs = triples.get_lazy_frames(&None, &None)?;
            let lfs_only: Vec<_> = lfs.into_iter().map(|(lf, _)| lf).collect();
            let mut lf = concat(
                lfs_only,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    diagonal: false,
                    from_partitioned_ds: false,
                    maintain_order: false,
                    strict: false,
                },
            )
            .unwrap();
            let to_delete = gct.encoded_triples.df.clone().lazy();

            lf = lf
                .join(
                    to_delete,
                    [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
                    [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)],
                    JoinArgs {
                        how: JoinType::Anti,
                        validation: Default::default(),
                        suffix: None,
                        slice: None,
                        nulls_equal: false,
                        coalesce: Default::default(),
                        maintain_order: MaintainOrderJoin::None,
                        build_side: None,
                    },
                )
                .unwrap();
            let df = lf.collect().unwrap();
            if df.height() > 0 {
                Ok(Some(EncodedTriples {
                    df,
                    subject: gct.encoded_triples.subject.clone(),
                    subject_local_cat_uuid: None,
                    object: gct.encoded_triples.object.clone(),
                    object_local_cat_uuid: None,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        };
        let encoded = encoded?;
        if let Some(encoded) = encoded {
            return Ok(Some(CatTriples {
                encoded_triples: encoded,
                predicate: gct.predicate.clone(),
                graph: gct.graph.clone(),
                subject_type: gct.subject_type.clone(),
                object_type: gct.object_type.clone(),
                local_cats: vec![],
            }));
        }
    }
    Ok(None)
}

/// The triples of the table that are deleted (subject and object), for reporting what an update
/// deleted.
fn deleted_triples_df(gct: &CatTriples, triples: &Triples) -> Result<DataFrame, TriplestoreError> {
    let lfs: Vec<_> = triples
        .get_lazy_frames(&None, &None)?
        .into_iter()
        .map(|(lf, _)| lf)
        .collect();
    let on = [col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
    let df = concat_lfs(lfs)
        .join(
            gct.encoded_triples.df.clone().lazy(),
            on.clone(),
            on.clone(),
            JoinArgs {
                how: JoinType::Semi,
                validation: Default::default(),
                suffix: None,
                slice: None,
                nulls_equal: false,
                coalesce: Default::default(),
                maintain_order: MaintainOrderJoin::None,
                build_side: None,
            },
        )
        .unwrap()
        .select(on)
        .collect()
        .unwrap();
    Ok(df)
}
