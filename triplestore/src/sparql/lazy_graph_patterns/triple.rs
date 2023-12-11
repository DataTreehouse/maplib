use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::Context;
use crate::sparql::solution_mapping::{is_string_col, SolutionMappings};
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};

use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use crate::sparql::multitype::{clean_up_after_join_workaround, convert_df_col_to_multitype, create_compatible_solution_mappings, create_join_compatible_solution_mappings, helper_cols_join_workaround_polars_object_series_bug, unitype_to_multitype};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{col, concat, lit, Expr, JoinType};
use polars::prelude::{IntoLazy, UnionArgs};
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::DataFrame;
use polars_core::series::Series;
use representation::{literal_iri_to_namednode, RDFNodeType};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashMap;

impl Triplestore {
    pub fn lazy_triple_pattern(
        &self,
        mut solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        _context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let subject_filter = create_term_pattern_filter(&triple_pattern.subject, "subject");
        let object_filter = create_term_pattern_filter(&triple_pattern.object, "object");
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

        let (mut df, mut dts) = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_predicate_df(
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
                    columns,
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
                                columns,
                                rdf_node_types,
                            })
                        } else {
                            predicates = vec![];
                            solution_mappings = Some(SolutionMappings {
                                mappings,
                                columns,
                                rdf_node_types,
                            })
                        };
                    } else {
                        solution_mappings = Some(SolutionMappings {
                            mappings,
                            columns,
                            rdf_node_types,
                        });
                        predicates = self.all_predicates();
                    }
                } else {
                    predicates = self.all_predicates();
                }
                self.get_predicates_df(
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

        let colnames: Vec<_> = df
            .get_column_names()
            .iter()
            .map(|x| x.to_string())
            .collect();
        if let Some(SolutionMappings {
            mut mappings,
            mut columns,
            mut rdf_node_types,
        }) = solution_mappings
        {
            let overlap: Vec<_> = colnames
                .iter()
                .filter(|x| columns.contains(*x))
                .map(|x| x.clone())
                .collect();
            if df.height() == 0 {
                df = df.drop_many(overlap.as_slice());
                //workaround for object registry bug
                let mut multicols = vec![];
                for (k, v) in &rdf_node_types {
                    if v == &RDFNodeType::MultiType {
                        multicols.push(k.clone())
                    }
                }
                for c in &multicols {
                    mappings = mappings.with_column(lit("").alias(c));
                }
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(df.lazy(), [], [], JoinType::Cross.into());
                    for m in &multicols {
                        let mut df = mappings.collect().unwrap();
                        convert_df_col_to_multitype(&mut df, m, &RDFNodeType::IRI);
                        dts.insert(m.clone(), RDFNodeType::MultiType);
                        mappings = df.lazy();
                    }
                }
            } else {
                if !overlap.is_empty() {
                    let (new_mappings, new_rdf_node_types, lf, new_dts) =
                        create_join_compatible_solution_mappings(
                            mappings,
                            rdf_node_types,
                            df.lazy(),
                            dts,
                            true
                        );
                    dts = new_dts;
                    rdf_node_types = new_rdf_node_types;
                    mappings = new_mappings;

                    let join_on: Vec<Expr> = overlap.iter().map(|x| col(x)).collect();
                    let (mut existing_mappings, mut lf, left_original_map, right_original_map) =
                        helper_cols_join_workaround_polars_object_series_bug(
                            mappings,
                            lf,
                            &overlap,
                            &rdf_node_types,
                        );
                    let mut strcol = vec![];
                    for c in &overlap {
                        let dt = rdf_node_types.get(c).unwrap();
                        if dt == &RDFNodeType::MultiType || is_string_col(dt) {
                            strcol.push(c);
                        }
                    }
                    for c in strcol {
                        lf = lf.with_column(col(c).cast(DataType::Categorical(None)));
                        existing_mappings =
                            existing_mappings.with_column(col(c).cast(DataType::Categorical(None)));
                    }

                    existing_mappings = existing_mappings.join(
                        lf,
                        join_on.as_slice(),
                        join_on.as_slice(),
                        JoinType::Inner.into(),
                    );
                    mappings = clean_up_after_join_workaround(
                        existing_mappings,
                        left_original_map,
                        right_original_map,
                    );
                } else {
                    mappings = mappings.join(df.lazy(), [], [], JoinType::Cross.into());
                }
            }
            columns.extend(colnames);
            rdf_node_types.extend(dts);
            solution_mappings = Some(SolutionMappings {
                mappings,
                columns,
                rdf_node_types,
            });
        } else {
            solution_mappings = Some(SolutionMappings {
                mappings: df.lazy(),
                columns: colnames.into_iter().collect(),
                rdf_node_types: dts,
            })
        }
        Ok(solution_mappings.unwrap())
    }

    pub fn get_predicate_df(
        &self,
        verb_uri: &NamedNode,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        subject_datatype_req: Option<&RDFNodeType>,
        object_datatype_req: Option<&RDFNodeType>,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), SparqlError> {
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

                let mut drop = vec![];
                if let Some(renamed) = subject_keep_rename {
                    lf = lf.rename(["subject"], [renamed]);
                    out_datatypes.insert(renamed.to_string(), subj_dt.clone());
                } else {
                    drop.push("subject");
                }
                if let Some(renamed) = object_keep_rename {
                    lf = lf.rename(["object"], [renamed]);
                    out_datatypes.insert(renamed.to_string(), obj_dt.clone());
                } else {
                    drop.push("object")
                }
                if let Some(renamed) = verb_keep_rename {
                    lf = lf.with_column(lit(verb_uri.to_string()).alias(renamed));
                    out_datatypes.insert(renamed.clone(), RDFNodeType::IRI);
                }
                lf = lf.drop_columns(drop);
                let df = lf.collect().unwrap();
                Ok((df, out_datatypes))
            } else {
                Ok(create_empty_df_datatypes(
                    subject_keep_rename,
                    verb_keep_rename,
                    object_keep_rename,
                    object_datatype_req,
                ))
            }
        } else {
            Ok(create_empty_df_datatypes(
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                object_datatype_req,
            ))
        }
    }

    fn get_predicates_df(
        &self,
        predicate_uris: Vec<NamedNode>,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        object_datatype_req: Option<&RDFNodeType>,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), SparqlError> {
        let mut out_datatypes = HashMap::new();
        let mut lfs = vec![];

        let need_multi_subject =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, true);
        let need_multi_object =
            self.partial_check_need_multi(&predicate_uris, object_datatype_req, false);

        for v in predicate_uris {
            let (mut df, datatypes_map) = self.get_predicate_df(
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
                if df.height() > 0
                    && need_multi_subject
                    && datatypes_map.get(subj_col).unwrap() != &RDFNodeType::MultiType
                {
                    df.with_column(unitype_to_multitype(
                        df.column(subj_col).unwrap(),
                        datatypes_map.get(subj_col).unwrap(),
                    ))
                    .unwrap();
                    out_datatypes.insert(subj_col.clone(), RDFNodeType::MultiType);
                } else {
                    out_datatypes.insert(
                        subj_col.clone(),
                        datatypes_map.get(subj_col).unwrap().clone(),
                    );
                }
            }

            if let Some(obj_col) = object_keep_rename {
                if df.height() > 0
                    && need_multi_object
                    && datatypes_map.get(obj_col).unwrap() != &RDFNodeType::MultiType
                {
                    df.with_column(unitype_to_multitype(
                        df.column(obj_col).unwrap(),
                        datatypes_map.get(obj_col).unwrap(),
                    ))
                    .unwrap();
                    out_datatypes.insert(obj_col.clone(), RDFNodeType::MultiType);
                } else {
                    out_datatypes
                        .insert(obj_col.clone(), datatypes_map.get(obj_col).unwrap().clone());
                }
            }

            if df.height() > 0 {
                if let Some(verb_col) = verb_keep_rename {
                    out_datatypes.insert(
                        verb_col.clone(),
                        datatypes_map.get(verb_col).unwrap().clone(),
                    );
                }
                lfs.push(df.lazy());
            }
        }
        Ok(if !lfs.is_empty() {
            (
                concat(lfs, UnionArgs::default())
                    .unwrap()
                    .collect()
                    .unwrap(),
                out_datatypes,
            )
        } else {
            create_empty_df_datatypes(
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

pub fn create_empty_df_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype_req: Option<&RDFNodeType>,
) -> (DataFrame, HashMap<String, RDFNodeType>) {
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
    (DataFrame::new(series_vec).unwrap(), out_datatypes)
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
