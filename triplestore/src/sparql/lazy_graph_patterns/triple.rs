use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::Context;
use crate::sparql::solution_mapping::{is_string_col, SolutionMappings};
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};

use oxrdf::vocab::xsd;
use polars::prelude::{col, concat, lit, Expr};
use polars::prelude::{IntoLazy, UnionArgs};
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::DataFrame;
use polars_core::prelude::JoinType;
use polars_core::series::Series;
use representation::RDFNodeType;
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
            _ => None,
        };
        let subject_rename = get_keep_rename_term_pattern(&triple_pattern.subject);
        let verb_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);

        let (mut df, mut dts) = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_predicate_df(
                n.as_str(),
                &subject_rename,
                &verb_rename,
                &object_rename,
                subject_filter,
                object_filter,
                &object_datatype_req,
            )?,
            NamedNodePattern::Variable(v) => {
                let predicates: Vec<String>;
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
                                    AnyValue::Utf8(s) => Some(s.to_string()),
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
                    &predicates,
                    &subject_rename,
                    &verb_rename,
                    &object_rename,
                    subject_filter,
                    object_filter,
                    &object_datatype_req,
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
            let overlap: Vec<_> = colnames.iter().filter(|x| columns.contains(*x)).collect();

            if df.height() == 0 {
                df = df.drop_many(overlap.as_slice());
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(df.lazy(), [], [], JoinType::Cross.into());
                    for c in overlap {
                        columns.insert(c.to_string());
                        rdf_node_types.insert(c.to_string(), dts.remove(c).unwrap());
                    }
                }
            } else {
                if !overlap.is_empty() {
                    //TODO: Introduce data type sensitivity here.
                    let join_on: Vec<Expr> = overlap.iter().map(|x| col(x)).collect();
                    let mut strcol = vec![];
                    for c in overlap {
                        if is_string_col(rdf_node_types.get(c).unwrap()) {
                            strcol.push(c);
                        }
                    }
                    let mut lf = df.lazy();
                    for c in strcol {
                        lf = lf.with_column(col(c).cast(DataType::Categorical(None)));
                        mappings = mappings.with_column(col(c).cast(DataType::Categorical(None)));
                    }

                    mappings = mappings.join(
                        lf,
                        join_on.as_slice(),
                        join_on.as_slice(),
                        JoinType::Inner.into(),
                    );
                } else {
                    mappings = mappings.join(df.lazy(), [], [], JoinType::Cross.into());
                }

                columns.extend(colnames);
                rdf_node_types.extend(dts);
            }
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

    fn get_predicate_df(
        &self,
        verb_uri: &str,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        object_datatype_req: &Option<RDFNodeType>,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), SparqlError> {
        if let Some(m) = self.df_map.get(verb_uri) {
            if m.is_empty() {
                panic!("Empty map should never happen");
            }
            let (dt, tt) = if let Some(object_datatype) = object_datatype_req {
                if let Some(tt) = m.get(object_datatype) {
                    (object_datatype, tt)
                } else {
                    return Ok(create_empty_df_datatypes(
                        subject_keep_rename,
                        verb_keep_rename,
                        object_keep_rename,
                        object_datatype_req,
                    ));
                }
            } else if m.len() > 1 {
                let dts: Vec<&RDFNodeType> = m.keys().collect();
                todo!("Multiple datatypes not supported yet {:?}", dts);
            } else {
                m.iter().next().unwrap()
            };

            assert!(tt.unique, "Should be deduplicated");
            let mut out_datatypes = HashMap::new();
            let mut lf = concat(
                tt.get_lazy_frames()
                    .map_err(SparqlError::TripleTableReadError)?,
                UnionArgs::default(),
            )
            .unwrap()
            .select(vec![col("subject"), col("object")]);

            let mut drop = vec![];
            if let Some(renamed) = subject_keep_rename {
                lf = lf.rename(["subject"], [renamed]);
                out_datatypes.insert(renamed.to_string(), RDFNodeType::IRI);
            } else {
                drop.push("select");
            }
            if let Some(renamed) = object_keep_rename {
                lf = lf.rename(["object"], [renamed]);
                out_datatypes.insert(renamed.to_string(), dt.clone());
            } else {
                drop.push("object")
            }
            if let Some(f) = subject_filter {
                lf = lf.filter(f);
            }
            if let Some(f) = object_filter {
                lf = lf.filter(f);
            }
            lf = lf.drop_columns(drop);
            Ok((lf.collect().unwrap(), out_datatypes))
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
        predicate_uris: &Vec<String>,
        subject_keep_rename: &Option<String>,
        verb_keep_rename: &Option<String>,
        object_keep_rename: &Option<String>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
        object_datatype_req: &Option<RDFNodeType>,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), SparqlError> {
        let mut out_datatypes = HashMap::new();
        let mut lfs = vec![];
        for v in predicate_uris {
            let (mut df, datatypes_map) = self.get_predicate_df(
                v,
                subject_keep_rename,
                verb_keep_rename,
                object_keep_rename,
                subject_filter.clone(),
                object_filter.clone(),
                object_datatype_req,
            )?;

            //Should be possible to skip around multiple datatypes requirement when df is empty
            if df.height() > 0 {
                if let Some(renamed) = verb_keep_rename {
                    if let Some(others_dt) = out_datatypes.get(renamed) {
                        if others_dt != &RDFNodeType::IRI {
                            todo!("Multiple datatypes not implemented yet");
                        }
                    }
                    out_datatypes.insert(renamed.to_string(), RDFNodeType::IRI);
                    df = df
                        .lazy()
                        .with_column(lit(v.to_string()).alias(renamed))
                        .collect()
                        .unwrap();
                }
                for (col, dt) in datatypes_map.into_iter() {
                    if let Some(others_dt) = out_datatypes.get(&col) {
                        if others_dt != &dt {
                            todo!("Multiple datatypes not implemented yet");
                        }
                    }
                    out_datatypes.insert(col, dt);
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

    fn all_predicates(&self) -> Vec<String> {
        let mut strs = vec![];
        for k in self.df_map.keys() {
            strs.push(k.clone())
        }
        strs
    }
}

fn create_empty_df_datatypes(
    subject_keep_rename: &Option<String>,
    verb_keep_rename: &Option<String>,
    object_keep_rename: &Option<String>,
    object_datatype_req: &Option<RDFNodeType>,
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
            let dt = RDFNodeType::None;
            let polars_dt = dt.polars_data_type();
            (dt, polars_dt)
        };
        out_datatypes.insert(object_rename.to_string(), use_datatype);
        series_vec.push(Series::new_empty(object_rename, &use_polars_datatype))
    }
    (DataFrame::new(series_vec).unwrap(), out_datatypes)
}

fn create_term_pattern_filter(term_pattern: &TermPattern, target_col: &str) -> Option<Expr> {
    if let TermPattern::Literal(l) = term_pattern {
        return Some(col(target_col).eq(lit(sparql_literal_to_polars_literal_value(l))));
    } else if let TermPattern::NamedNode(nn) = term_pattern {
        return Some(col(target_col).eq(lit(sparql_named_node_to_polars_literal_value(nn))));
    }
    None
}

fn get_keep_rename_term_pattern(term_pattern: &TermPattern) -> Option<String> {
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
