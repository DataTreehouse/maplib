pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;

use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use representation::query_context::Context;
use std::collections::HashMap;

use super::Triplestore;
use crate::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use crate::sparql::errors::SparqlError;
use crate::TriplesToAdd;
use polars::frame::DataFrame;
use polars::prelude::{col, IntoLazy};
use polars_core::enable_string_cache;
use polars_core::prelude::{DataType, Series, UniqueKeepStrategy};
use representation::literals::sparql_literal_to_any_value;
use representation::multitype::split_df_multicols;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeType;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use uuid::Uuid;

pub enum QueryResult {
    Select(DataFrame, HashMap<String, RDFNodeType>),
    Construct(Vec<(DataFrame, RDFNodeType, RDFNodeType)>),
}

impl Triplestore {
    pub fn query_deduplicated(
        &self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_deduplicated_impl(&query, parameters)
    }

    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_impl(&query, parameters)
    }

    fn query_impl(
        &mut self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<QueryResult, SparqlError> {
        if !self.deduplicated {
            self.deduplicate()
                .map_err(SparqlError::DeduplicationError)?;
        }
        self.query_deduplicated_impl(query, parameters)
    }

    fn query_deduplicated_impl(
        &self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<QueryResult, SparqlError> {
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
                    rdf_node_types: types,
                } = self.lazy_graph_pattern(pattern, None, &context, parameters)?;
                let mut df = mappings.collect().unwrap();
                df = cats_to_strings(df);

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
                    rdf_node_types,
                } = self.lazy_graph_pattern(pattern, None, &context, parameters)?;
                let mut df = mappings.collect().unwrap();
                df = cats_to_strings(df);
                let mut dfs = vec![];
                for t in template {
                    if let Some(df_and_types) = triple_to_df(&df, &rdf_node_types, t)? {
                        dfs.push(df_and_types);
                    }
                }
                Ok(QueryResult::Construct(dfs))
            }
            _ => Err(SparqlError::QueryTypeNotSupported),
        }
    }

    pub fn insert(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        transient: bool,
    ) -> Result<(), SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        if let Query::Construct { .. } = &query {
            let res = self.query_impl(&query, parameters)?;
            match res {
                QueryResult::Select(_, _) => {
                    panic!("Should never happen")
                }
                QueryResult::Construct(dfs) => self.insert_construct_result(dfs, transient),
            }
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }
    pub fn insert_construct_result(
        &mut self,
        dfs: Vec<(DataFrame, RDFNodeType, RDFNodeType)>,
        transient: bool,
    ) -> Result<(), SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let mut all_triples_to_add = vec![];

        for (df, subj_dt, obj_dt) in dfs {
            if df.height() == 0 {
                continue;
            }
            let mut multicols = vec![];
            if subj_dt == RDFNodeType::MultiType {
                multicols.push(SUBJECT_COL_NAME);
            }
            if obj_dt == RDFNodeType::MultiType {
                multicols.push(OBJECT_COL_NAME);
            }
            if !multicols.is_empty() {
                let lfs_dts = split_df_multicols(df.lazy(), multicols);
                for (lf, mut map) in lfs_dts {
                    let df = lf.collect().unwrap();
                    let new_subj_dt = map.remove(SUBJECT_COL_NAME).unwrap_or(subj_dt.clone());
                    let new_obj_dt = map.remove(OBJECT_COL_NAME).unwrap_or(obj_dt.clone());
                    all_triples_to_add.push(TriplesToAdd {
                        df,
                        subject_type: new_subj_dt,
                        object_type: new_obj_dt,
                        static_verb_column: None,
                        has_unique_subset: false,
                    });
                }
            } else {
                all_triples_to_add.push(TriplesToAdd {
                    df,
                    subject_type: subj_dt,
                    object_type: obj_dt,
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

fn triple_to_df(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    t: &TriplePattern,
) -> Result<Option<(DataFrame, RDFNodeType, RDFNodeType)>, SparqlError> {
    let len = if triple_has_variable(t) {
        df.height()
    } else {
        1
    };
    let (subj_ser, subj_dt) =
        term_pattern_series(df, rdf_node_types, &t.subject, SUBJECT_COL_NAME, len);
    let (verb_ser, _) =
        named_node_pattern_series(df, rdf_node_types, &t.predicate, VERB_COL_NAME, len);
    let (obj_ser, obj_dt) =
        term_pattern_series(df, rdf_node_types, &t.object, OBJECT_COL_NAME, len);
    let mut unique_subset = vec![];
    if subj_ser.dtype() != &DataType::Null {
        unique_subset.push(SUBJECT_COL_NAME.to_string());
    }
    if verb_ser.dtype() != &DataType::Null {
        unique_subset.push(VERB_COL_NAME.to_string());
    }
    if obj_ser.dtype() != &DataType::Null {
        unique_subset.push(OBJECT_COL_NAME.to_string());
    }
    let df = DataFrame::new(vec![subj_ser, verb_ser, obj_ser])
        .unwrap()
        .lazy()
        .drop_nulls(None)
        .unique(Some(unique_subset), UniqueKeepStrategy::First)
        .collect()
        .unwrap();
    if df.height() > 0 {
        Ok(Some((df, subj_dt, obj_dt)))
    } else {
        Ok(None)
    }
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
                    sparql_literal_to_any_value(lit.value(), lit.language(), &Some(lit.datatype()));
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

fn cats_to_strings(df: DataFrame) -> DataFrame {
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
