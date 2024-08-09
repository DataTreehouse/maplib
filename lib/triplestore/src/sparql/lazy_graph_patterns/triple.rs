use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::SolutionMappings;

use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use log::debug;
use oxrdf::NamedNode;
use polars::prelude::IntoLazy;
use polars::prelude::{col, lit, AnyValue, DataFrame, DataType, Expr, JoinType, Series};
use query_processing::graph_patterns::{join, union};
use representation::multitype::convert_lf_col_to_multitype;
use representation::{literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    pub fn lazy_triple_pattern(
        &self,
        mut solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!(
            "Processing triple pattern {:?} at {}",
            triple_pattern,
            context.as_str()
        );
        let subject_filter = create_term_pattern_filter(&triple_pattern.subject, SUBJECT_COL_NAME);
        let object_filter = create_term_pattern_filter(&triple_pattern.object, OBJECT_COL_NAME);
        let object_datatype_req = match &triple_pattern.object {
            TermPattern::NamedNode(_nn) => Some(RDFNodeType::IRI),
            TermPattern::BlankNode(_) => None,
            TermPattern::Literal(l) => match l.datatype() {
                _ => Some(RDFNodeType::Literal(l.datatype().into_owned())),
            },
            TermPattern::Variable(_) => None,
        };
        let subject_rename = get_keep_rename_term_pattern(&triple_pattern.subject);
        let verb_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);

        let (
            SolutionMappings {
                mappings: lf,
                rdf_node_types: dts,
            },
            height_0,
        ) = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_predicate_lf(
                n,
                &subject_rename,
                &verb_rename,
                &object_rename,
                subject_filter,
                object_filter,
                None, //TODO!
                object_datatype_req.as_ref(),
            )?,
            NamedNodePattern::Variable(v) => {
                let predicates: HashSet<NamedNode>;
                if let Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                }) = solution_mappings
                {
                    if let Some(dt) = rdf_node_types.get(v.as_str()) {
                        if let RDFNodeType::IRI = dt {
                            let mappings_df = mappings.collect().unwrap();
                            let predicates_series = mappings_df
                                .column(v.as_str())
                                .unwrap()
                                .cast(&DataType::String)
                                .unwrap();
                            let predicates_iter = predicates_series.iter();
                            predicates = predicates_iter
                                .filter_map(|x| match x {
                                    AnyValue::Null => None,
                                    AnyValue::String(s) => Some(literal_iri_to_namednode(s)),
                                    AnyValue::StringOwned(s) => Some(literal_iri_to_namednode(&s)),
                                    x => panic!("Should never happen: {}", x),
                                })
                                .collect();
                            solution_mappings = Some(SolutionMappings {
                                mappings: mappings_df.lazy(),
                                rdf_node_types,
                            })
                        } else {
                            predicates = HashSet::new();
                            solution_mappings = Some(SolutionMappings {
                                mappings,
                                rdf_node_types,
                            })
                        };
                    } else {
                        solution_mappings = Some(SolutionMappings {
                            mappings,
                            rdf_node_types,
                        });
                        predicates = self.all_predicates();
                    }
                } else {
                    predicates = self.all_predicates();
                }
                self.get_predicates_lf(
                    predicates.into_iter().collect(),
                    &subject_rename,
                    &verb_rename,
                    &object_rename,
                    subject_filter,
                    object_filter,
                    object_datatype_req.as_ref(),
                )?
            }
        };
        let colnames: Vec<_> = dts.keys().cloned().collect();
        if let Some(SolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) = solution_mappings
        {
            let overlap: Vec<_> = colnames
                .iter()
                .filter(|x| rdf_node_types.contains_key(*x))
                .cloned()
                .collect();
            if height_0 {
                // Important that overlapping cols are dropped from mappings and not from lf,
                // since we also overwrite rdf_node_types with dts correspondingly below.
                mappings = mappings.drop(overlap.as_slice());
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(lf, [], [], JoinType::Cross.into());
                }
                rdf_node_types.extend(dts);
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                });
            } else {
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                });
                let new_solution_mappings = SolutionMappings {
                    mappings: lf,
                    rdf_node_types: dts,
                };
                solution_mappings = Some(join(
                    solution_mappings.unwrap(),
                    new_solution_mappings,
                    JoinType::Inner,
                )?);
            }
        } else {
            solution_mappings = Some(SolutionMappings {
                mappings: lf,
                rdf_node_types: dts,
            })
        }
        Ok(solution_mappings.unwrap())
    }

    pub fn get_predicate_lf(
        &self,
        verb_uri: &NamedNode,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        subject_datatype_req: Option<&RDFNodeType>,
        object_datatype_req: Option<&RDFNodeType>,
    ) -> Result<(SolutionMappings, bool), SparqlError> {
        if let Some(m) = self.df_map.get(verb_uri) {
            if m.is_empty() {
                panic!("Empty map should never happen");
            }
            if let Some(SolutionMappings {
                mappings: mut lf,
                mut rdf_node_types,
            }) = multiple_tt_to_lf(
                m,
                self.transient_df_map.get(verb_uri),
                subject_datatype_req,
                object_datatype_req,
                subject_filter,
                object_filter,
            )? {
                let mut out_datatypes = HashMap::new();
                let use_subject_col_name = uuid::Uuid::new_v4().to_string();
                let use_object_col_name = uuid::Uuid::new_v4().to_string();
                lf = lf.rename(
                    [SUBJECT_COL_NAME, OBJECT_COL_NAME],
                    [&use_subject_col_name, &use_object_col_name],
                );

                let mut drop = vec![];
                if let Some(renamed) = subject_keep_rename {
                    lf = lf.rename([&use_subject_col_name], [renamed]);
                    out_datatypes.insert(
                        renamed.to_string(),
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                    );
                } else {
                    drop.push(use_subject_col_name);
                }
                if let Some(renamed) = object_keep_rename {
                    lf = lf.rename([&use_object_col_name], [renamed]);
                    out_datatypes.insert(
                        renamed.to_string(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    );
                } else {
                    drop.push(use_object_col_name)
                }
                if let Some(renamed) = verb_keep_rename {
                    lf = lf.with_column(
                        lit(rdf_named_node_to_polars_literal_value(verb_uri)).alias(renamed),
                    );
                    out_datatypes.insert(renamed.clone(), RDFNodeType::IRI);
                }
                lf = lf.drop(drop);
                Ok((SolutionMappings::new(lf, out_datatypes), false))
            } else {
                Ok(create_empty_lf_datatypes(
                    subject_keep_rename,
                    verb_keep_rename,
                    object_keep_rename,
                    object_datatype_req,
                ))
            }
        } else {
            Ok(create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            ))
        }
    }

    fn get_predicates_lf(
        &self,
        predicate_uris: Vec<NamedNode>,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        object_datatype_req: Option<&RDFNodeType>,
    ) -> Result<(SolutionMappings, bool), SparqlError> {
        let mut solution_mappings = vec![];

        let need_multi_subject =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, true);
        let need_multi_object =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, false);

        for v in predicate_uris {
            let (
                SolutionMappings {
                    mappings: mut lf,
                    rdf_node_types: mut datatypes_map,
                },
                height_0,
            ) = self.get_predicate_lf(
                &v,
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                subject_filter.clone(),
                object_filter.clone(),
                None, //TODO!
                object_datatype_req,
            )?;
            if let Some(subj_col) = subject_keep_rename {
                if !height_0
                    && need_multi_subject
                    && !matches!(
                        datatypes_map.get(subj_col).unwrap(),
                        &RDFNodeType::MultiType(..)
                    )
                {
                    lf = lf.with_column(convert_lf_col_to_multitype(
                        subj_col,
                        datatypes_map.get(subj_col).unwrap(),
                    ));
                    let existing_type =
                        BaseRDFNodeType::from_rdf_node_type(datatypes_map.get(subj_col).unwrap());
                    datatypes_map.insert(
                        subj_col.clone(),
                        RDFNodeType::MultiType(vec![existing_type]),
                    );
                }
            }

            if let Some(obj_col) = object_keep_rename {
                if !height_0
                    && need_multi_object
                    && !matches!(
                        datatypes_map.get(obj_col).unwrap(),
                        &RDFNodeType::MultiType(..)
                    )
                {
                    lf = lf.with_column(convert_lf_col_to_multitype(
                        obj_col,
                        datatypes_map.get(obj_col).unwrap(),
                    ));
                    let existing_type =
                        BaseRDFNodeType::from_rdf_node_type(datatypes_map.get(obj_col).unwrap());
                    datatypes_map
                        .insert(obj_col.clone(), RDFNodeType::MultiType(vec![existing_type]));
                }
            }

            if !height_0 {
                solution_mappings.push(SolutionMappings::new(lf, datatypes_map));
            }
        }
        Ok(if !solution_mappings.is_empty() {
            let sm = union(solution_mappings, false)?;
            (sm, false)
        } else {
            create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            )
        })
    }

    fn all_predicates(&self) -> HashSet<NamedNode> {
        let mut strs = HashSet::new();
        for k in self.df_map.keys() {
            strs.insert(k.clone());
        }
        strs
    }
    fn partial_check_need_multi(
        &self,
        predicates: &Vec<NamedNode>,
        object_datatype_req: Option<&RDFNodeType>,
        subject: bool,
    ) -> bool {
        if !subject && object_datatype_req.is_some() {
            return false;
        }

        let mut first_datatype = None;
        for p in predicates {
            if let Some(tt_map) = self.df_map.get(p) {
                for (subject_dt, object_dt) in tt_map.keys() {
                    let use_dt = if subject { subject_dt } else { object_dt };
                    if let Some(first) = &first_datatype {
                        if first != &use_dt {
                            return true;
                        }
                    } else {
                        first_datatype = Some(use_dt);
                    }
                }
            }
        }
        false
    }
}

pub fn create_empty_lf_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype_req: Option<&RDFNodeType>,
) -> (SolutionMappings, bool) {
    let mut series_vec = vec![];
    let mut out_datatypes = HashMap::new();

    if let Some(subject_rename) = subject_keep_rename {
        out_datatypes.insert(subject_rename.to_string(), RDFNodeType::None);
        series_vec.push(Series::new_empty(
            subject_rename,
            &BaseRDFNodeType::None.polars_data_type(),
        ))
    }
    if let Some(verb_rename) = verb_keep_rename {
        out_datatypes.insert(verb_rename.to_string(), RDFNodeType::None);
        series_vec.push(Series::new_empty(
            verb_rename,
            &BaseRDFNodeType::None.polars_data_type(),
        ))
    }
    if let Some(object_rename) = object_keep_rename {
        let (use_datatype, use_polars_datatype) = if let Some(dt) = object_datatype_req {
            let polars_dt = BaseRDFNodeType::from_rdf_node_type(dt).polars_data_type();
            (dt.clone(), polars_dt)
        } else {
            let dt = BaseRDFNodeType::None;
            let polars_dt = dt.polars_data_type();
            (dt.as_rdf_node_type(), polars_dt)
        };
        out_datatypes.insert(object_rename.to_string(), use_datatype);
        series_vec.push(Series::new_empty(object_rename, &use_polars_datatype))
    }
    (
        SolutionMappings::new(DataFrame::new(series_vec).unwrap().lazy(), out_datatypes),
        true,
    )
}

pub fn create_term_pattern_filter(term_pattern: &TermPattern, target_col: &str) -> Option<Expr> {
    if let TermPattern::Literal(l) = term_pattern {
        return Some(col(target_col).eq(lit(rdf_literal_to_polars_literal_value(l))));
    } else if let TermPattern::NamedNode(nn) = term_pattern {
        return Some(col(target_col).eq(lit(rdf_named_node_to_polars_literal_value(nn))));
    }
    None
}

pub fn get_keep_rename_term_pattern(term_pattern: &TermPattern) -> Option<String> {
    if let TermPattern::Variable(v) = term_pattern {
        return Some(v.as_str().to_string());
    } else if let TermPattern::BlankNode(b) = term_pattern {
        return Some(b.as_str().to_string());
    }
    None
}

fn get_keep_rename_named_node_pattern(named_node_pattern: &NamedNodePattern) -> Option<String> {
    if let NamedNodePattern::Variable(v) = named_node_pattern {
        return Some(v.as_str().to_string());
    }
    None
}
