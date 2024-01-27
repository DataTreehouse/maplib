use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::solution_mapping::{is_string_col, SolutionMappings};
use representation::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};

use crate::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{col, concat, lit, Expr, JoinType, LazyFrame};
use polars::prelude::{IntoLazy, UnionArgs};
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::DataFrame;
use polars_core::series::Series;
use representation::multitype::{
    convert_lf_col_to_multitype, create_join_compatible_solution_mappings, join_workaround,
};
use representation::{literal_iri_to_namednode, RDFNodeType};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashMap;

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
                xsd::ANY_URI => Some(RDFNodeType::IRI),
                _ => Some(RDFNodeType::Literal(l.datatype().into_owned())),
            },
            TermPattern::Variable(_) => None,
        };
        let subject_rename = get_keep_rename_term_pattern(&triple_pattern.subject);
        let verb_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);

        let (lf, mut dts, height_0) = match &triple_pattern.predicate {
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
                let predicates: Vec<NamedNode>;
                if let Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                }) = solution_mappings
                {
                    if let Some(dt) = rdf_node_types.get(v.as_str()) {
                        if let RDFNodeType::IRI = dt {
                            let mappings_df = mappings.collect().unwrap();
                            let predicates_iter = mappings_df.column(v.as_str()).unwrap().iter();
                            predicates = predicates_iter
                                .filter_map(|x| match x {
                                    AnyValue::Null => None,
                                    AnyValue::Utf8(s) => Some(literal_iri_to_namednode(s)),
                                    _ => panic!("Should never happen"),
                                })
                                .collect();
                            solution_mappings = Some(SolutionMappings {
                                mappings: mappings_df.lazy(),
                                rdf_node_types,
                            })
                        } else {
                            predicates = vec![];
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
                    predicates,
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
                mappings = mappings.drop_columns(overlap.as_slice());
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(lf, [], [], JoinType::Cross.into());
                }
            } else if !overlap.is_empty() {
                let (new_mappings, new_rdf_node_types, mut lf, new_dts) =
                    create_join_compatible_solution_mappings(
                        mappings,
                        rdf_node_types,
                        lf,
                        dts,
                        true,
                    );

                dts = new_dts;
                rdf_node_types = new_rdf_node_types;
                mappings = new_mappings;

                let _join_on: Vec<Expr> = overlap.iter().map(|x| col(x)).collect();
                let mut strcol = vec![];
                for c in &overlap {
                    let dt = rdf_node_types.get(c).unwrap();
                    if is_string_col(dt) {
                        strcol.push(c);
                    }
                }
                for c in strcol {
                    lf = lf.with_column(col(c).cast(DataType::Categorical(None)));
                    mappings = mappings.with_column(col(c).cast(DataType::Categorical(None)));
                }

                mappings =
                    join_workaround(mappings, &rdf_node_types, lf, &dts, JoinType::Inner.into());
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
    ) -> Result<(LazyFrame, HashMap<String, RDFNodeType>, bool), SparqlError> {
        if let Some(m) = self.df_map.get(verb_uri) {
            if m.is_empty() {
                panic!("Empty map should never happen");
            }
            if let Some((subj_dt, obj_dt, mut lf)) = multiple_tt_to_lf(
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
                    out_datatypes.insert(renamed.to_string(), subj_dt.clone());
                } else {
                    drop.push(use_subject_col_name);
                }
                if let Some(renamed) = object_keep_rename {
                    lf = lf.rename([&use_object_col_name], [renamed]);
                    out_datatypes.insert(renamed.to_string(), obj_dt.clone());
                } else {
                    drop.push(use_object_col_name)
                }
                if let Some(renamed) = verb_keep_rename {
                    lf = lf.with_column(lit(verb_uri.to_string()).alias(renamed));
                    out_datatypes.insert(renamed.clone(), RDFNodeType::IRI);
                }
                lf = lf.drop_columns(drop);
                Ok((lf, out_datatypes, false))
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
    ) -> Result<(LazyFrame, HashMap<String, RDFNodeType>, bool), SparqlError> {
        let mut out_datatypes = HashMap::new();
        let mut lfs = vec![];

        let need_multi_subject =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, true);
        let need_multi_object =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, false);

        for v in predicate_uris {
            let (mut lf, datatypes_map, height_0) = self.get_predicate_lf(
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
                    && datatypes_map.get(subj_col).unwrap() != &RDFNodeType::MultiType
                {
                    lf = convert_lf_col_to_multitype(
                        lf,
                        subj_col,
                        datatypes_map.get(subj_col).unwrap(),
                    );
                    out_datatypes.insert(subj_col.clone(), RDFNodeType::MultiType);
                } else {
                    out_datatypes.insert(
                        subj_col.clone(),
                        datatypes_map.get(subj_col).unwrap().clone(),
                    );
                }
            }

            if let Some(obj_col) = object_keep_rename {
                if !height_0
                    && need_multi_object
                    && datatypes_map.get(obj_col).unwrap() != &RDFNodeType::MultiType
                {
                    lf = convert_lf_col_to_multitype(
                        lf,
                        obj_col,
                        datatypes_map.get(obj_col).unwrap(),
                    );
                    out_datatypes.insert(obj_col.clone(), RDFNodeType::MultiType);
                } else {
                    out_datatypes
                        .insert(obj_col.clone(), datatypes_map.get(obj_col).unwrap().clone());
                }
            }

            if !height_0 {
                if let Some(verb_col) = verb_keep_rename {
                    out_datatypes.insert(
                        verb_col.clone(),
                        datatypes_map.get(verb_col).unwrap().clone(),
                    );
                }
                lfs.push(lf);
            }
        }
        Ok(if !lfs.is_empty() {
            (
                concat(lfs, UnionArgs::default()).unwrap(),
                out_datatypes,
                false,
            )
        } else {
            create_empty_lf_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            )
        })
    }

    fn all_predicates(&self) -> Vec<NamedNode> {
        let mut strs = vec![];
        for k in self.df_map.keys() {
            strs.push(k.clone())
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
            for (subject_dt, object_dt) in self.df_map.get(p).unwrap().keys() {
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
        false
    }
}

pub fn create_empty_lf_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype_req: Option<&RDFNodeType>,
) -> (LazyFrame, HashMap<String, RDFNodeType>, bool) {
    let mut series_vec = vec![];
    let mut out_datatypes = HashMap::new();

    if let Some(subject_rename) = subject_keep_rename {
        out_datatypes.insert(subject_rename.to_string(), RDFNodeType::IRI);
        series_vec.push(Series::new_empty(
            subject_rename,
            &RDFNodeType::IRI.polars_data_type(),
        ))
    }
    if let Some(verb_rename) = verb_keep_rename {
        out_datatypes.insert(verb_rename.to_string(), RDFNodeType::IRI);
        series_vec.push(Series::new_empty(
            verb_rename,
            &RDFNodeType::IRI.polars_data_type(),
        ))
    }
    if let Some(object_rename) = object_keep_rename {
        let (use_datatype, use_polars_datatype) = if let Some(dt) = object_datatype_req {
            let polars_dt = dt.polars_data_type();
            (dt.clone(), polars_dt)
        } else {
            let dt = RDFNodeType::IRI;
            let polars_dt = DataType::Utf8;
            (dt, polars_dt)
        };
        out_datatypes.insert(object_rename.to_string(), use_datatype);
        series_vec.push(Series::new_empty(object_rename, &use_polars_datatype))
    }
    (
        DataFrame::new(series_vec).unwrap().lazy(),
        out_datatypes,
        true,
    )
}

pub fn create_term_pattern_filter(term_pattern: &TermPattern, target_col: &str) -> Option<Expr> {
    if let TermPattern::Literal(l) = term_pattern {
        return Some(col(target_col).eq(lit(sparql_literal_to_polars_literal_value(l))));
    } else if let TermPattern::NamedNode(nn) = term_pattern {
        return Some(col(target_col).eq(lit(sparql_named_node_to_polars_literal_value(nn))));
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
