use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::Triples;
use crate::StoredBaseRDFNodeType;
use crate::Triplestore;
use log::trace;
use oxrdf::NamedNode;
use polars::prelude::{col, concat, IntoLazy, JoinArgs, JoinType, MaintainOrderJoin, UnionArgs};
use polars_core::datatypes::AnyValue;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use representation::cats::{cat_encode_triples, CatTriples, Cats, EncodedTriples};
use representation::multitype::split_df_multicols;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::time::Instant;

impl Triplestore {
    pub fn delete_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
    ) -> Result<(), SparqlError> {
        let sms: Vec<_> = sms
            .into_par_iter()
            .map(|(x, y)| partition_by_global_predicate_col(x, y, &self.cats))
            .flatten()
            .collect();
        let global_cat_triples = triples_solution_mappings_to_global_cat_triples(sms, &self.cats);
        if !global_cat_triples.is_empty() {
            self.delete_triples_vec(global_cat_triples)
                .map_err(SparqlError::StoreTriplesError)?;
        };
        Ok(())
    }

    pub fn delete_triples_vec(
        &mut self,
        ts: Vec<CatTriples>,
    ) -> Result<(), TriplestoreError> {
        let add_triples_now = Instant::now();
        self.delete_global_cat_triples(ts)?;
        trace!(
            "Deleting triples df took {} seconds",
            add_triples_now.elapsed().as_secs_f32()
        );
        Ok(())
    }

    fn delete_global_cat_triples(
        &mut self,
        gcts: Vec<CatTriples>,
    ) -> Result<(), TriplestoreError> {
        for gct in gcts {
            let remaining_gct = get_triples_after_deletion(&gct, &self.triples_map, &self.cats)?;

            if let Some(gct) = remaining_gct {
                self.delete_if_exists(&gct, false);
                self.add_global_cat_triples(vec![gct], false)?;
            } else {
                self.delete_if_exists(&gct, false)
            }

            let remaining_gct =
                get_triples_after_deletion(&gct, &self.transient_triples_map, &self.cats)?;
            if let Some(gct) = remaining_gct {
                self.delete_if_exists(&gct, true);
                self.add_global_cat_triples(vec![gct], true)?;
            } else {
                self.delete_if_exists(&gct, true)
            }
        }
        Ok(())
    }

    fn delete_if_exists(&mut self, gct: &CatTriples, transient: bool) {
        let use_map = if transient {
            &mut self.transient_triples_map
        } else {
            &mut self.triples_map
        };
        let map_empty = if let Some(m1) = use_map.get_mut(&gct.predicate) {
            for e in &gct.encoded_triples {
                let k = (
                    StoredBaseRDFNodeType::from_base_and_prefix(&gct.subject_type, &e.subject),
                    StoredBaseRDFNodeType::from_base_and_prefix(&gct.object_type, &e.object),
                );
                m1.remove(&k);
            }
            m1.is_empty()
        } else {
            false
        };
        if map_empty {
            use_map.remove(&gct.predicate);
        }
    }
}

fn partition_by_global_predicate_col(
    sm: EagerSolutionMappings,
    predicate: Option<NamedNode>,
    global_cats: &Cats,
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
        let predicates = global_cats.decode_iri_u32s(&predicates_u32);
        sms.into_iter().zip(predicates.into_iter()).collect()
    }
}

fn triples_solution_mappings_to_global_cat_triples(
    sm_preds: Vec<(EagerSolutionMappings, NamedNode)>,
    global_cats: &Cats,
) -> Vec<CatTriples> {
    let mappings_maps_preds: Vec<_> = sm_preds
        .into_par_iter()
        .map(|(mut sm, predicate)| {
            let EagerSolutionMappings {
                mappings,
                rdf_node_types,
            } = sm;
            let dfs_maps = split_df_multicols(mappings, &rdf_node_types);
            let mut dfs_maps_preds = Vec::with_capacity(dfs_maps.len());
            for (df, map) in dfs_maps {
                dfs_maps_preds.push((df, map, predicate.clone()));
            }
            dfs_maps_preds
        })
        .flatten()
        .collect();
    mappings_maps_preds
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
                subject_state,
                object_state,
                global_cats,
            );
            e
        })
        .collect()
}

fn get_triples_after_deletion(
    gct: &CatTriples,
    map: &HashMap<NamedNode, HashMap<(StoredBaseRDFNodeType, StoredBaseRDFNodeType), Triples>>,
    global_cats: &Cats,
) -> Result<Option<CatTriples>, TriplestoreError> {
    if let Some(m) = map.get(&gct.predicate) {
        let out_encoded: Result<Vec<_>, TriplestoreError> = gct
            .encoded_triples
            .par_iter()
            .map(|e| {
                let subj_type =
                    StoredBaseRDFNodeType::from_base_and_prefix(&gct.subject_type, &e.subject);
                let obj_type =
                    StoredBaseRDFNodeType::from_base_and_prefix(&gct.object_type, &e.object);

                let type_ = (subj_type, obj_type);
                if let Some(triples) = m.get(&type_) {
                    let lfs = triples.get_lazy_frames(&None, &None,  global_cats)?;
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
                        },
                    )
                    .unwrap();
                    let to_delete = e.df.clone().lazy();

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
                        },
                    );
                    let df = lf.collect().unwrap();
                    if df.height() > 0 {
                        Ok(Some(EncodedTriples {
                            df,
                            subject: e.subject.clone(),
                            subject_local_cat_uuid: None,
                            object: e.object.clone(),
                            object_local_cat_uuid: None,
                        }))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })
            .collect();
        let out_encoded = out_encoded?;
        let out_encoded: Vec<_> = out_encoded
            .into_iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        if out_encoded.len() > 0 {
            return Ok(Some(CatTriples {
                encoded_triples: out_encoded,
                predicate: gct.predicate.clone(),
                subject_type: gct.subject_type.clone(),
                object_type: gct.object_type.clone(),
                local_cats: vec![],
            }));
        }
    }
    Ok(None)
}
