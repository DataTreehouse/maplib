use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::Triples;
use crate::{add_rank_sort_triples, Triplestore};
use oxrdf::NamedNode;
use polars::prelude::{col, concat, IntoLazy, JoinArgs, JoinType, MaintainOrderJoin, UnionArgs};
use polars_core::datatypes::AnyValue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::{cat_encode_triples, CatTriples, EncodedTriples, LockedCats};
use representation::dataset::NamedGraph;
use representation::multitype::{set_struct_all_null_to_null_row, split_df_multicols};
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::time::Instant;
use tracing::trace;

impl Triplestore {
    pub fn delete_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        graph: &NamedGraph,
    ) -> Result<(), SparqlError> {
        let sms: Vec<_> = sms
            .into_par_iter()
            .map(|(x, y)| {
                partition_by_global_predicate_col(x, y, self.global_cats.clone())
            })
            .flatten()
            .collect();
        let global_cat_triples = triples_solution_mappings_to_global_cat_triples(
            sms,
            self.global_cats.clone(),
            graph,
        )?;
        if !global_cat_triples.is_empty() {
            self.delete_triples_vec(global_cat_triples, graph)
                .map_err(SparqlError::TriplestoreError)?;
        };
        Ok(())
    }

    pub fn delete_triples_vec(
        &mut self,
        ts: Vec<CatTriples>,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        let add_triples_now = Instant::now();
        self.delete_global_cat_triples(ts, graph)?;
        trace!(
            "Deleting triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(())
    }

    fn delete_global_cat_triples(
        &mut self,
        gcts: Vec<CatTriples>,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        self.check_graph_exists(graph)?;
        for gct in gcts {
            if self.graph_triples_map.contains_key(graph) {
                let remaining_gct =
                    get_triples_after_deletion(&gct, self.graph_triples_map.get(graph).unwrap())?;

                if let Some(mut gct) = remaining_gct {
                    self.delete_if_exists(&gct, false, graph)?;
                    gct.encoded_triples = add_rank_sort_triples(
                                gct.encoded_triples,
                                &gct.subject_type,
                                &gct.object_type,
                                self.global_cats.clone(),
                            )?;
                    self.add_global_cat_triples(vec![gct], false)?;
                } else {
                    self.delete_if_exists(&gct, false, graph)?;
                }
            }

            if self.graph_transient_triples_map.contains_key(graph) {
                let remaining_gct = get_triples_after_deletion(
                    &gct,
                    self.graph_transient_triples_map.get(graph).unwrap(),
                )?;
                if let Some(mut gct) = remaining_gct {
                    self.delete_if_exists(&gct, true, graph)?;
                    gct.encoded_triples = add_rank_sort_triples(
                        gct.encoded_triples,
                        &gct.subject_type,
                        &gct.object_type,
                        self.global_cats.clone(),
                    )?;
                    self.add_global_cat_triples(vec![gct], true)?;
                } else {
                    self.delete_if_exists(&gct, true, graph)?;
                }
            }
        }
        Ok(())
    }

    fn delete_if_exists(
        &mut self,
        gct: &CatTriples,
        transient: bool,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        self.check_graph_exists(graph)?;
        let use_map = if transient {
            self.graph_transient_triples_map.get_mut(graph).unwrap()
        } else {
            self.graph_triples_map.get_mut(graph).unwrap()
        };
        let map_empty = if let Some(m1) = use_map.get_mut(&gct.predicate) {
            let k = (gct.subject_type.clone(), gct.object_type.clone());
            m1.remove(&k);
            m1.is_empty()
        } else {
            false
        };
        if map_empty {
            use_map.remove(&gct.predicate);
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
        let predicates = global_cats.read().unwrap().decode_iri_u32s(&predicates_u32, None);
        sms.into_iter().zip(predicates.into_iter()).collect()
    }
}

fn triples_solution_mappings_to_global_cat_triples(
    sm_preds: Vec<(EagerSolutionMappings, NamedNode)>,
    global_cats: LockedCats,
    graph: &NamedGraph,
) -> Result<Vec<CatTriples>, TriplestoreError> {
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
                df = lf.collect().unwrap();
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
    map: &HashMap<NamedNode, HashMap<(BaseRDFNodeType, BaseRDFNodeType), Triples>>,
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

            lf = lf.join(
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
            );
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
