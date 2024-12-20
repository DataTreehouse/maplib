// use super::Triplestore;
// use crate::errors::TriplestoreError;
// use crate::sparql::errors::SparqlError;
// use log::debug;
// use oxrdf::{NamedNode, Term, Variable};
// use polars_core::datatypes::{AnyValue, CategoricalOrdering};
// use polars_core::prelude::{CategoricalChunked, LogicalType, Series};
// use polars_core::utils::Container;
// use query_processing::graph_patterns::{order_by, union};
// use representation::multitype::{
//     force_convert_multicol_to_single_col, lf_columns_to_categorical, non_multi_type_string,
// };
// use representation::query_context::Context;
// use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
// use representation::{
//     BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME,
// };
// use spargebra::algebra::{Expression, GraphPattern};
// use std::collections::{BTreeMap, HashMap};
//
// const OFFSET_STEP: usize = 1_000;
//
// #[derive(Clone)]
// pub struct TriplestoreIndex {
//     spo: EagerSolutionMappings,
//     spo_sparse: BTreeMap<String, usize>,
//     ops: Option<EagerSolutionMappings>,
//     ops_sparse: Option<BTreeMap<String, usize>>,
// }
//
// impl Triplestore {
//     pub fn create_index(&mut self, ops: bool) -> Result<(), TriplestoreError> {
//         self.deduplicate()?;
//         let mut keys_sorted = vec![];
//         for k in self.triples_map.keys() {
//             keys_sorted.push(k.clone());
//         }
//         keys_sorted.sort();
//         let mut to_concat = vec![];
//         for k in keys_sorted {
//             let map = self.triples_map.get(&k).unwrap();
//             for ((subject_type, object_type), v) in map {
//                 let sm = v
//                     .get_solution_mappings(subject_type, object_type, Some(&k))
//                     .unwrap();
//                 to_concat.push(sm);
//             }
//         }
//         let mut sm = union(to_concat, false).unwrap();
//         sm.mappings = lf_columns_to_categorical(
//             sm.mappings,
//             &sm.rdf_node_types,
//             CategoricalOrdering::Lexical,
//         );
//         sm = sm.as_eager().as_lazy();
//
//         let spo_by = vec![
//             SUBJECT_COL_NAME.to_string(),
//             VERB_COL_NAME.to_string(),
//             OBJECT_COL_NAME.to_string(),
//         ];
//         let spo_sm = order_by(sm.clone(), &spo_by, vec![true, true, true]).unwrap();
//         let eager_spo_sm = spo_sm.as_eager();
//         let subj_ser = eager_spo_sm
//             .mappings
//             .column(SUBJECT_COL_NAME)
//             .unwrap()
//             .as_materialized_series();
//         let subj_type = eager_spo_sm.rdf_node_types.get(SUBJECT_COL_NAME).unwrap();
//         let subj_sparse_map = create_sparse_map(subj_ser, subj_type);
//
//         let (eager_ops_sm, obj_sparse_map) = if ops {
//             let ops_by = vec![
//                 OBJECT_COL_NAME.to_string(),
//                 VERB_COL_NAME.to_string(),
//                 SUBJECT_COL_NAME.to_string(),
//             ];
//             let ops_sm = order_by(sm.clone(), &ops_by, vec![true, true, true]).unwrap();
//             let eager_ops_sm = ops_sm.as_eager();
//             let obj_ser = eager_ops_sm
//                 .mappings
//                 .column(OBJECT_COL_NAME)
//                 .unwrap()
//                 .as_materialized_series();
//             let obj_type = eager_ops_sm.rdf_node_types.get(OBJECT_COL_NAME).unwrap();
//             let obj_sparse_map = create_sparse_map(obj_ser, obj_type);
//             (Some(eager_ops_sm), Some(obj_sparse_map))
//         } else {
//             (None, None)
//         };
//         Ok(())
//     }
//
//     pub fn get_index_lf(
//         &self,
//         subject_keep_rename: &Option<String>,
//         verb_keep_rename: &Option<String>,
//         object_keep_rename: &Option<String>,
//         subject_term: Option<Term>,
//         object_term: Option<Term>,
//     ) -> Result<SolutionMappings, SparqlError> {
//         if let Some(index) = &self.index {
//             let mut sm = if let Some(Term::NamedNode(subject_iri)) = &subject_term {
//                 get_exact_lookup(subject_iri, &index.spo, &index.spo_sparse, SUBJECT_COL_NAME)
//             } else if self.index.as_ref().unwrap().ops.is_some()
//                 && matches!(&object_term, Some(Term::NamedNode(_)))
//             {
//                 if let Some(Term::NamedNode(object_iri)) = &object_term {
//                     get_exact_lookup(
//                         object_iri,
//                         index.ops.as_ref().unwrap(),
//                         index.ops_sparse.as_ref().unwrap(),
//                         OBJECT_COL_NAME,
//                     )
//                 } else {
//                     panic!("Should never happen")
//                 }
//             } else {
//                 index.spo.clone().as_lazy()
//             };
//             let dummy_gp = GraphPattern::Bgp { patterns: vec![] };
//             if let Some(subject_term) = &subject_term {
//                 let expression = match subject_term {
//                     Term::NamedNode(nn) => Expression::NamedNode(nn.clone()),
//                     Term::Literal(l) => Expression::Literal(l.clone()),
//                     _ => panic!(),
//                 };
//                 sm = self.lazy_filter(
//                     &dummy_gp,
//                     &Expression::Equal(
//                         Box::new(Expression::Variable(Variable::new_unchecked(
//                             SUBJECT_COL_NAME,
//                         ))),
//                         Box::new(expression),
//                     ),
//                     Some(sm),
//                     &Context::new(),
//                     &None,
//                 )?;
//             }
//             if let Some(object_term) = &object_term {
//                 let expression = match object_term {
//                     Term::NamedNode(nn) => Expression::NamedNode(nn.clone()),
//                     Term::Literal(l) => Expression::Literal(l.clone()),
//                     _ => panic!(),
//                 };
//                 sm = self.lazy_filter(
//                     &dummy_gp,
//                     &Expression::Equal(
//                         Box::new(Expression::Variable(Variable::new_unchecked(
//                             OBJECT_COL_NAME,
//                         ))),
//                         Box::new(expression),
//                     ),
//                     Some(sm),
//                     &Context::new(),
//                     &None,
//                 )?;
//             }
//             let mut out_datatypes = HashMap::new();
//             let use_subject_col_name = uuid::Uuid::new_v4().to_string();
//             let use_verb_col_name = uuid::Uuid::new_v4().to_string();
//             let use_object_col_name = uuid::Uuid::new_v4().to_string();
//             sm.mappings = sm.mappings.rename(
//                 [SUBJECT_COL_NAME, VERB_COL_NAME, OBJECT_COL_NAME],
//                 [
//                     &use_subject_col_name,
//                     &use_verb_col_name,
//                     &use_object_col_name,
//                 ],
//                 true,
//             );
//
//             let mut drop = vec![];
//             if let Some(renamed) = subject_keep_rename {
//                 sm.mappings = sm.mappings.rename([&use_subject_col_name], [renamed], true);
//                 out_datatypes.insert(
//                     renamed.to_string(),
//                     sm.rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
//                 );
//             } else {
//                 drop.push(use_subject_col_name);
//             }
//             if let Some(renamed) = object_keep_rename {
//                 sm.mappings = sm.mappings.rename([&use_object_col_name], [renamed], true);
//                 out_datatypes.insert(
//                     renamed.to_string(),
//                     sm.rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
//                 );
//             } else {
//                 drop.push(use_object_col_name);
//             }
//             if let Some(renamed) = verb_keep_rename {
//                 sm.mappings = sm.mappings.rename([&use_verb_col_name], [renamed], true);
//                 out_datatypes.insert(
//                     renamed.to_string(),
//                     sm.rdf_node_types.remove(VERB_COL_NAME).unwrap(),
//                 );
//             } else {
//                 drop.push(use_verb_col_name);
//             }
//             sm.mappings = sm.mappings.drop(drop);
//             sm.rdf_node_types = out_datatypes;
//             Ok(sm)
//         } else {
//             panic!()
//         }
//     }
// }
//
// fn get_exact_lookup(
//     iri: &NamedNode,
//     eager_sm: &EagerSolutionMappings,
//     sparse_map: &BTreeMap<String, usize>,
//     col_name: &str,
// ) -> SolutionMappings {
//     debug!("Getting exact lookup for {}", iri);
//     let iri_str = iri.as_str();
//     let mut from = 0;
//     let mut range_backwards = sparse_map.range(..iri_str.to_string());
//     while let Some((s, prev)) = range_backwards.next_back() {
//         if s != iri_str {
//             from = *prev;
//             break;
//         }
//     }
//     let range_forwards = sparse_map.range(iri_str.to_string()..);
//     let height = eager_sm.mappings.height();
//     let mut offset = height - from;
//     for (s, next) in range_forwards {
//         if s != iri_str {
//             offset = *next - from;
//             break;
//         }
//     }
//     debug!("Len {} from {} offset {}", height, from, offset);
//     let (_, aft) = eager_sm.mappings.split_at(from as i64);
//     let (bef, _) = aft.split_at(offset as i64);
//     let eager_sm = EagerSolutionMappings::new(bef, eager_sm.rdf_node_types.clone());
//     let mut sm = eager_sm.as_lazy();
//     if matches!(
//         sm.rdf_node_types.get(col_name).unwrap(),
//         RDFNodeType::MultiType(..)
//     ) {
//         sm.mappings =
//             force_convert_multicol_to_single_col(sm.mappings, col_name, &BaseRDFNodeType::IRI);
//         sm.rdf_node_types
//             .insert(col_name.to_string(), RDFNodeType::IRI);
//     }
//     sm
// }
//
// fn get_iri_ser(series: &Series, is_rdf_node_type: &RDFNodeType) -> Option<Series> {
//     if matches!(is_rdf_node_type, RDFNodeType::MultiType(..)) {
//         let subj_struct = series.struct_().unwrap();
//         let subj_iri_ser = subj_struct
//             .field_by_name(&non_multi_type_string(&BaseRDFNodeType::IRI))
//             .unwrap();
//         Some(subj_iri_ser)
//     } else if is_rdf_node_type == &RDFNodeType::IRI {
//         Some(series.clone())
//     } else {
//         None
//     }
// }
//
// fn update_at_offset(
//     cat_chunked: &CategoricalChunked,
//     offset: usize,
//     sparse_map: &mut BTreeMap<String, usize>,
// ) {
//     let any = cat_chunked.get_any_value(offset).unwrap();
//     let s = match any {
//         AnyValue::Null => None,
//         AnyValue::Categorical(c, rev, _) => Some(rev.get(c).to_string()),
//         AnyValue::CategoricalOwned(c, rev, _) => Some(rev.get(c).to_string()),
//         _ => panic!(),
//     };
//     if let Some(s) = s {
//         let e = sparse_map.entry(s);
//         e.or_insert(offset);
//     }
// }
//
// fn create_sparse_map(ser: &Series, rdf_node_type: &RDFNodeType) -> BTreeMap<String, usize> {
//     let iri_ser = get_iri_ser(ser, rdf_node_type);
//     let iri_cat_ser = iri_ser
//         .as_ref()
//         .map(|iri_ser| iri_ser.categorical().unwrap());
//     let mut sparse_map = BTreeMap::new();
//     if let Some(iri_cat_ser) = iri_cat_ser {
//         let mut current_offset = 0;
//         while current_offset < ser.len() {
//             update_at_offset(iri_cat_ser, current_offset, &mut sparse_map);
//             current_offset += OFFSET_STEP;
//         }
//     }
//     sparse_map
// }
