pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub mod multitype;
pub mod query_context;
pub mod solution_mapping;
pub mod sparql_to_polars;

use crate::sparql::query_context::Context;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use std::collections::HashMap;

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::multitype::split_df_multicols;
use crate::sparql::solution_mapping::SolutionMappings;
use crate::TriplesToAdd;
use polars::frame::DataFrame;
use polars::prelude::{col, IntoLazy};
use polars_core::enable_string_cache;
use polars_core::prelude::{DataType, Series, UniqueKeepStrategy};
use representation::literals::sparql_literal_to_any_value;
use representation::RDFNodeType;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use uuid::Uuid;

pub enum QueryResult {
    Select(DataFrame, HashMap<String, RDFNodeType>),
    Construct(Vec<(DataFrame, RDFNodeType, RDFNodeType)>),
}

impl Triplestore {
    pub fn query(&mut self, query: &str) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_parsed(&query)
    }

    fn query_parsed(&mut self, query: &Query) -> Result<QueryResult, SparqlError> {
        if !self.deduplicated {
            self.deduplicate()
                .map_err(SparqlError::DeduplicationError)?;
        }
        enable_string_cache();
        let context = Context::new();
        match query {
            Query::Select {
                dataset: _,
                pattern,
                base_iri: _,
            } => {
                let SolutionMappings {
                    mappings,
                    columns: _,
                    rdf_node_types: types,
                } = self.lazy_graph_pattern(pattern, None, &context)?;
                let mut df = mappings.collect().unwrap();
                df = cats_to_utf8s(df);

                Ok(QueryResult::Select(df, types))
            }
            Query::Construct {
                template,
                dataset: _,
                pattern,
                base_iri: _,
            } => {
                let SolutionMappings {
                    mappings,
                    columns: _,
                    rdf_node_types,
                } = self.lazy_graph_pattern(pattern, None, &context)?;
                let mut df = mappings.collect().unwrap();
                df = cats_to_utf8s(df);
                let mut dfs = vec![];
                for t in template {
                    dfs.push(triple_to_df(&df, &rdf_node_types, t)?);
                }
                Ok(QueryResult::Construct(dfs))
            }
            _ => Err(SparqlError::QueryTypeNotSupported),
        }
    }

    pub fn insert(&mut self, query: &str, transient: bool) -> Result<(), SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        if let Query::Construct { .. } = &query {
            let res = self.query_parsed(&query)?;
            match res {
                QueryResult::Select(_, _) => {
                    panic!("Should never happen")
                }
                QueryResult::Construct(dfs) => {
                    let mut all_triples_to_add = vec![];

                    for (df, subj_dt, obj_dt) in dfs {
                        if df.height() == 0 {
                            continue;
                        }
                        let mut multicols = vec![];
                        if subj_dt == RDFNodeType::MultiType {
                            multicols.push("subject");
                        }
                        if obj_dt == RDFNodeType::MultiType {
                            multicols.push("object");
                        }
                        if !multicols.is_empty() {
                            let dfs_dts = split_df_multicols(df, multicols);
                            for (df, mut map) in dfs_dts {
                                let new_subj_dt = map.remove("subject").unwrap_or(subj_dt.clone());
                                let new_obj_dt = map.remove("object").unwrap_or(obj_dt.clone());
                                all_triples_to_add.push(TriplesToAdd {
                                    df,
                                    subject_type: new_subj_dt,
                                    object_type: new_obj_dt,
                                    language_tag: None,
                                    static_verb_column: None,
                                    has_unique_subset: false,
                                });
                            }
                        } else {
                            all_triples_to_add.push(TriplesToAdd {
                                df,
                                subject_type: subj_dt,
                                object_type: obj_dt,
                                language_tag: None,
                                static_verb_column: None,
                                has_unique_subset: false,
                            });
                        }
                    }
                    if !all_triples_to_add.is_empty() {
                        self.add_triples_vec(all_triples_to_add, &call_uuid, transient)
                            .map_err(SparqlError::StoreTriplesError)?;
                    }
                    Ok(())
                }
            }
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }
}

fn triple_to_df(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    t: &TriplePattern,
) -> Result<(DataFrame, RDFNodeType, RDFNodeType), SparqlError> {
    let len = if triple_has_variable(t) {
        df.height()
    } else {
        1
    };
    let (subj_ser, subj_dt) = term_pattern_series(df, rdf_node_types, &t.subject, "subject", len);
    let (verb_ser, _) = named_node_pattern_series(df, rdf_node_types, &t.predicate, "verb", len);
    let (obj_ser, obj_dt) = term_pattern_series(df, rdf_node_types, &t.object, "object", len);
    let mut unique_subset = vec![];
    if subj_ser.dtype() != &DataType::Null {
        unique_subset.push("subject".to_string());
    }
    if verb_ser.dtype() != &DataType::Null {
        unique_subset.push("verb".to_string());
    }
    if obj_ser.dtype() != &DataType::Null {
        unique_subset.push("object".to_string());
    }
    let df = DataFrame::new(vec![subj_ser, verb_ser, obj_ser])
        .unwrap()
        .unique(
            Some(unique_subset.as_slice()),
            UniqueKeepStrategy::First,
            None,
        )
        .unwrap();
    Ok((df, subj_dt, obj_dt))
}

fn triple_has_variable(t: &TriplePattern) -> bool {
    if let TermPattern::Variable(_) = t.subject {
        return true;
    }
    if let TermPattern::Variable(_) = t.object {
        return true;
    }
    false
}

fn term_pattern_series(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    tp: &TermPattern,
    name: &str,
    len: usize,
) -> (Series, RDFNodeType) {
    match tp {
        TermPattern::NamedNode(nn) => named_node_series(nn, name, len),
        TermPattern::BlankNode(_) => {
            unimplemented!("Blank node term pattern not supported")
        }
        TermPattern::Literal(lit) => {
            if lit.datatype() == xsd::ANY_URI {
                named_node_series(&NamedNode::new(lit.to_string()).unwrap(), name, len)
            } else {
                let (anyvalue, dt) =
                    sparql_literal_to_any_value(&lit.value().to_string(), &Some(lit.datatype()));
                let mut any_values = vec![];
                for _ in 0..len {
                    any_values.push(anyvalue.clone())
                }
                (
                    Series::from_any_values(name, &any_values, false).unwrap(),
                    RDFNodeType::Literal(dt.into_owned()),
                )
            }
        }
        TermPattern::Variable(v) => variable_series(df, rdf_node_types, v, name),
    }
}

fn named_node_pattern_series(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    nnp: &NamedNodePattern,
    name: &str,
    len: usize,
) -> (Series, RDFNodeType) {
    match nnp {
        NamedNodePattern::NamedNode(nn) => named_node_series(nn, name, len),
        NamedNodePattern::Variable(v) => variable_series(df, rdf_node_types, v, name),
    }
}

fn named_node_series(nn: &NamedNode, name: &str, len: usize) -> (Series, RDFNodeType) {
    let mut ser = Series::from_iter([nn.to_string().as_str()].repeat(len));
    ser.rename(name);
    (ser, RDFNodeType::IRI)
}

fn variable_series(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    v: &Variable,
    name: &str,
) -> (Series, RDFNodeType) {
    let mut ser = df.column(v.as_str()).unwrap().clone();
    ser.rename(name);
    (ser, rdf_node_types.get(v.as_str()).unwrap().clone())
}

fn cats_to_utf8s(df: DataFrame) -> DataFrame {
    let mut cats = vec![];
    for c in df.columns(df.get_column_names()).unwrap() {
        if let DataType::Categorical(_) = c.dtype() {
            cats.push(c.name().to_string());
        }
    }
    let mut lf = df.lazy();
    for c in cats {
        lf = lf.with_column(col(&c).cast(DataType::Utf8))
    }
    lf.collect().unwrap()
}
