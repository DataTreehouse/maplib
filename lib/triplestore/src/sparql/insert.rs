use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::{NewTriples, TriplesToAdd};
use oxrdf::NamedNode;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::dataset::NamedGraph;
use representation::multitype::split_df_multicols;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::time::Instant;

impl Triplestore {
    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        transient: bool,
        graph: &NamedGraph,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let start_create_triples = Instant::now();
        let all_triples_to_add = construct_result_as_triples_to_add(sms);
        println!("Finished creating triples to add {}", start_create_triples.elapsed().as_secs_f32());
        let new_triples = if !all_triples_to_add.is_empty() {
            self.add_triples_vec(all_triples_to_add, transient, graph)
                .map_err(SparqlError::TriplestoreError)?
        } else {
            vec![]
        };
        Ok(new_triples)
    }
}

fn construct_result_as_triples_to_add(
    sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
) -> Vec<TriplesToAdd> {
    let tta: Vec<Vec<_>> = sms
        .into_par_iter()
        .map(
            |(
                EagerSolutionMappings {
                    mappings,
                    mut rdf_node_types,
                },
                predicate,
            )| {
                if mappings.height() == 0 {
                    vec![]
                } else {
                    let mut all_triples_to_add = vec![];

                    let mut multicols = HashMap::new();
                    let subj_dt = rdf_node_types.get(SUBJECT_COL_NAME).unwrap();
                    if subj_dt.is_multi() {
                        multicols.insert(SUBJECT_COL_NAME.to_string(), subj_dt.clone());
                    }
                    let obj_dt = rdf_node_types.get(OBJECT_COL_NAME).unwrap();
                    if obj_dt.is_multi() {
                        multicols.insert(OBJECT_COL_NAME.to_string(), obj_dt.clone());
                    }
                    if !multicols.is_empty() {
                        let dfs_dts = split_df_multicols(mappings, &multicols);
                        for (df, mut map) in dfs_dts {
                            let mut subject_type =
                                map.remove(SUBJECT_COL_NAME).unwrap_or(subj_dt.clone());
                            let (new_subj_dt, new_subj_state) =
                                subject_type.map.drain().next().unwrap();
                            //Predicate never multi col
                            let new_pred_state =
                                if let Some(s) = rdf_node_types.get(PREDICATE_COL_NAME) {
                                    let (new_pred_dt, new_pred_state) =
                                        s.clone().map.drain().next().unwrap();
                                    assert!(new_pred_dt.is_iri());
                                    Some(new_pred_state)
                                } else {
                                    None
                                };
                            let mut object_type =
                                map.remove(OBJECT_COL_NAME).unwrap_or(obj_dt.clone());
                            let (new_obj_dt, new_obj_state) =
                                object_type.map.drain().next().unwrap();

                            all_triples_to_add.push(TriplesToAdd {
                                df,
                                subject_type: new_subj_dt,
                                object_type: new_obj_dt,
                                predicate: predicate.clone(),
                                subject_cat_state: new_subj_state,
                                object_cat_state: new_obj_state,
                                predicate_cat_state: new_pred_state,
                            });
                        }
                    } else {
                        let (subject_type, subject_state) = rdf_node_types
                            .remove(SUBJECT_COL_NAME)
                            .unwrap()
                            .map
                            .drain()
                            .next()
                            .unwrap();
                        let predicate_state =
                            if let Some(mut s) = rdf_node_types.remove(PREDICATE_COL_NAME) {
                                Some(s.map.drain().map(|(_, y)| y).next().unwrap())
                            } else {
                                None
                            };

                        let (object_type, object_state) = rdf_node_types
                            .remove(OBJECT_COL_NAME)
                            .unwrap()
                            .map
                            .drain()
                            .next()
                            .unwrap();
                        all_triples_to_add.push(TriplesToAdd {
                            df: mappings,
                            subject_type,
                            object_type,
                            predicate,
                            subject_cat_state: subject_state,
                            predicate_cat_state: predicate_state,
                            object_cat_state: object_state,
                        });
                    }
                    all_triples_to_add
                }
            },
        )
        .collect();
    let tta: Vec<_> = tta.into_iter().flatten().collect();
    tta
}
