use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use oxrdf::{NamedNode, NamedOrBlankNode, Term, Variable};
use polars::prelude::Expr;
use polars_core::datatypes::{AnyValue, CategoricalOrdering};
use polars_core::prelude::{CategoricalChunked, LogicalType, Series};
use polars_core::utils::Container;
use query_processing::graph_patterns::{filter, order_by, union};
use representation::multitype::{force_convert_multicol_to_single_col, lf_columns_to_categorical, non_multi_type_string};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME,
};
use std::collections::{BTreeMap, HashMap};
use representation::query_context::Context;
use spargebra::algebra::{Expression, GraphPattern};

#[derive(Clone)]
pub struct TriplestoreIndex {
    sop: EagerSolutionMappings,
    sop_sparse: BTreeMap<String, usize>,
}

impl Triplestore {
    pub fn create_index(&mut self) -> Result<(), TriplestoreError> {
        let mut keys_sorted = vec![];
        for k in self.df_map.keys() {
            keys_sorted.push(k.clone());
        }
        keys_sorted.sort();
        let mut to_concat = vec![];
        for k in keys_sorted {
            let map = self.df_map.get(&k).unwrap();
            for ((subject_type, object_type), v) in map {
                let sm = v
                    .get_solution_mappings(subject_type, object_type, Some(&k))
                    .unwrap();
                to_concat.push(sm);
            }
        }
        let mut sm = union(to_concat, false).unwrap();
        sm.mappings = lf_columns_to_categorical(
            sm.mappings,
            &sm.rdf_node_types,
            CategoricalOrdering::Lexical,
        );

        let by = vec![
            SUBJECT_COL_NAME.to_string(),
            OBJECT_COL_NAME.to_string(),
            VERB_COL_NAME.to_string(),
        ];
        let sm = order_by(sm, &by, vec![true, true, true]).unwrap();
        let eager_sm = sm.as_eager();
        let subj_ser = eager_sm
            .mappings
            .column(SUBJECT_COL_NAME)
            .unwrap()
            .as_materialized_series();
        let subj_type = eager_sm.rdf_node_types.get(SUBJECT_COL_NAME).unwrap();
        let subj_iri_ser = get_iri_ser(subj_ser, subj_type);
        let subj_iri_cat_ser = if let Some(subj_iri_ser) = &subj_iri_ser {
            Some(subj_iri_ser.categorical().unwrap())
        } else {
            None
        };

        let mut sparse_map = BTreeMap::new();
        let mut current_offset = 0;
        while current_offset < subj_ser.len() {
            if let Some(subj_iri_cat_ser) = subj_iri_cat_ser {
                update_at_offset(subj_iri_cat_ser, current_offset, &mut sparse_map);
            }
            current_offset = current_offset + 1_000;
        }
        self.index = Some(TriplestoreIndex {
            sop: eager_sm,
            sop_sparse: sparse_map,
        });
        Ok(())
    }

    pub fn get_index_lf(
        &self,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_term: Option<Term>,
        object_term: Option<Term>,
    ) -> Result<SolutionMappings, SparqlError> {
        if let Some(index) = &self.index {
            let mut sm = if let Some(Term::NamedNode(subject_iri)) = &subject_term {
                let subject_str = subject_iri.as_str();
                let mut range = index.sop_sparse.range(subject_str.to_string()..);
                let mut from = 0;
                while let Some((s, prev)) = range.next_back() {
                    if s != s {
                        from = prev.clone();
                    } else {
                        break;
                    }
                }
                let mut offset = index.sop.mappings.height();
                while let Some((s, next)) = range.next() {
                    if s != s {
                        offset = next.clone() - from;
                    } else {
                        break;
                    }
                }
                let (_, aft) = index.sop.mappings.split_at(from as i64);
                let (bef, _) = aft.split_at(offset as i64);
                let eager_sm = EagerSolutionMappings::new(bef, index.sop.rdf_node_types.clone());
                let mut sm = eager_sm.as_lazy();
                if matches!(sm.rdf_node_types.get(SUBJECT_COL_NAME).unwrap(), RDFNodeType::MultiType(..)) {
                    sm.mappings = force_convert_multicol_to_single_col(sm.mappings, SUBJECT_COL_NAME, &BaseRDFNodeType::IRI);
                }
                sm
            } else {
                index.sop.clone().as_lazy()
            };
            let dummy_gp = GraphPattern::Bgp { patterns: vec![] };
            if let Some(subject_term) = &subject_term {
                let expression = match subject_term {
                    Term::NamedNode(nn) => {
                        Expression::NamedNode(nn.clone())
                    }
                    Term::Literal(l) => {
                        Expression::Literal(l.clone())
                    }
                    _ => panic!()
                };
                sm = self.lazy_filter(
                    &dummy_gp,
                    &Expression::Equal(Box::new(Expression::Variable(Variable::new_unchecked(SUBJECT_COL_NAME))), Box::new(expression)),
                    Some(sm),
                    &Context::new(),
                    &None
                )?;
            }
            if let Some(object_term) = &object_term {
                let expression = match object_term {
                    Term::NamedNode(nn) => {
                        Expression::NamedNode(nn.clone())
                    }
                    Term::Literal(l) => {
                        Expression::Literal(l.clone())
                    }
                    _ => panic!()
                };
                sm = self.lazy_filter(
                    &dummy_gp,
                    &Expression::Equal(Box::new(Expression::Variable(Variable::new_unchecked(OBJECT_COL_NAME))), Box::new(expression)),
                    Some(sm),
                    &Context::new(),
                    &None,
                )?;
            }
            let mut out_datatypes = HashMap::new();
            let use_subject_col_name = uuid::Uuid::new_v4().to_string();
            let use_verb_col_name = uuid::Uuid::new_v4().to_string();
            let use_object_col_name = uuid::Uuid::new_v4().to_string();
            sm.mappings = sm.mappings.rename(
                [SUBJECT_COL_NAME, VERB_COL_NAME, OBJECT_COL_NAME],
                [&use_subject_col_name, &use_verb_col_name, &use_object_col_name],
                true,
            );

            let mut drop = vec![];
            if let Some(renamed) = subject_keep_rename {
                sm.mappings = sm.mappings.rename([&use_subject_col_name], [renamed], true);
                out_datatypes.insert(
                    renamed.to_string(),
                    sm.rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                );
            } else {
                drop.push(use_subject_col_name);
            }
            if let Some(renamed) = object_keep_rename {
                sm.mappings = sm.mappings.rename([&use_object_col_name], [renamed], true);
                out_datatypes.insert(
                    renamed.to_string(),
                    sm.rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                );
            } else {
                drop.push(use_object_col_name);
            }
            if let Some(renamed) = verb_keep_rename {
                sm.mappings = sm.mappings.rename([&use_verb_col_name], [renamed], true);
                out_datatypes.insert(
                    renamed.to_string(),
                    sm.rdf_node_types.remove(VERB_COL_NAME).unwrap(),
                );
            } else {
                drop.push(use_verb_col_name);
            }
            sm.mappings = sm.mappings.drop(drop);
            sm.rdf_node_types = out_datatypes;
            Ok(sm)
        } else {
            panic!()
        }
    }
}

fn get_iri_ser(
    series: &Series,
    is_rdf_node_type: &RDFNodeType,
) -> Option<Series> {
    if matches!(is_rdf_node_type, RDFNodeType::MultiType(..)) {
        let subj_struct = series.struct_().unwrap();
        let subj_iri_ser = subj_struct
            .field_by_name(&non_multi_type_string(&BaseRDFNodeType::IRI))
            .unwrap();
        Some(subj_iri_ser)
    } else if is_rdf_node_type == &RDFNodeType::IRI {
        Some(series.clone())
    } else {
        None
    }
}

fn update_at_offset(
    cat_chunked: &CategoricalChunked,
    offset: usize,
    sparse_map: &mut BTreeMap<String, usize>,
) {
    let any = cat_chunked.get_any_value(offset).unwrap();
    let s = match any {
        AnyValue::Null => None,
        AnyValue::Categorical(c, rev, _) => Some(rev.get(c).to_string()),
        AnyValue::CategoricalOwned(c, rev, _) => Some(rev.get(c).to_string()),
        _ => panic!(),
    };
    if let Some(s) = s {
        let e = sparse_map.entry(s);
        e.or_insert(offset);
    }
}
