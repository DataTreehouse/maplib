pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;

use oxrdf::{NamedNode, Variable};
use representation::query_context::Context;
use std::collections::HashMap;

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::TriplesToAdd;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, Expr, IntoLazy};
use polars_core::frame::UniqueKeepStrategy;
use query_processing::expressions::col_null_expr;
use representation::multitype::{split_df_multicols, unique_workaround};
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeType;
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use uuid::Uuid;

#[derive(Debug)]
pub enum QueryResult {
    Select(DataFrame, HashMap<String, RDFNodeType>),
    Construct(Vec<(DataFrame, HashMap<String, RDFNodeType>)>),
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
                let df = mappings.collect().unwrap();
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
                let df = mappings.collect().unwrap();
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
        dfs: Vec<(DataFrame, HashMap<String, RDFNodeType>)>,
        transient: bool,
    ) -> Result<(), SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let mut all_triples_to_add = vec![];

        for (df, map) in dfs {
            if df.height() == 0 {
                continue;
            }
            let mut multicols = HashMap::new();
            let subj_dt = map.get(SUBJECT_COL_NAME).unwrap();
            if matches!(subj_dt, RDFNodeType::MultiType(..)) {
                multicols.insert(SUBJECT_COL_NAME.to_string(), subj_dt.clone());
            }
            let obj_dt = map.get(OBJECT_COL_NAME).unwrap();
            if matches!(obj_dt, RDFNodeType::MultiType(..)) {
                multicols.insert(OBJECT_COL_NAME.to_string(), obj_dt.clone());
            }
            if !multicols.is_empty() {
                let dfs_dts = split_df_multicols(df, &multicols);
                for (df, mut map) in dfs_dts {
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
                    subject_type: subj_dt.clone(),
                    object_type: obj_dt.clone(),
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
) -> Result<Option<(DataFrame, HashMap<String, RDFNodeType>)>, SparqlError> {
    let mut triple_types = HashMap::new();
    let (subj_expr, subj_dt) =
        term_pattern_expression(rdf_node_types, &t.subject, SUBJECT_COL_NAME);
    triple_types.insert(SUBJECT_COL_NAME.to_string(), subj_dt);
    let (verb_expr, verb_dt) = named_node_pattern_expr(rdf_node_types, &t.predicate, VERB_COL_NAME);
    triple_types.insert(VERB_COL_NAME.to_string(), verb_dt);
    let (obj_expr, obj_dt) = term_pattern_expression(rdf_node_types, &t.object, OBJECT_COL_NAME);
    triple_types.insert(OBJECT_COL_NAME.to_string(), obj_dt);

    let mut lf = df
        .clone()
        .lazy()
        .select(vec![subj_expr, verb_expr, obj_expr])
        .filter(
            col_null_expr(SUBJECT_COL_NAME, &triple_types)
                .not()
                .and(col_null_expr(VERB_COL_NAME, &triple_types).not())
                .and(col_null_expr(OBJECT_COL_NAME, &triple_types).not()),
        );
    lf = unique_workaround(lf, &triple_types, None, false, UniqueKeepStrategy::Any);

    let df = lf.collect().unwrap();
    if df.height() > 0 {
        Ok(Some((df, triple_types)))
    } else {
        Ok(None)
    }
}

fn term_pattern_expression(
    rdf_node_types: &HashMap<String, RDFNodeType>,
    tp: &TermPattern,
    name: &str,
) -> (Expr, RDFNodeType) {
    match tp {
        TermPattern::NamedNode(nn) => named_node_lit(nn, name),
        TermPattern::BlankNode(_) => {
            unimplemented!("Blank node term pattern not supported")
        }
        TermPattern::Literal(thelit) => {
            let l = lit(rdf_literal_to_polars_literal_value(thelit)).alias(name);
            (l, RDFNodeType::Literal(thelit.datatype().into_owned()))
        }
        TermPattern::Variable(v) => variable_expression(rdf_node_types, v, name),
    }
}

fn named_node_pattern_expr(
    rdf_node_types: &HashMap<String, RDFNodeType>,
    nnp: &NamedNodePattern,
    name: &str,
) -> (Expr, RDFNodeType) {
    match nnp {
        NamedNodePattern::NamedNode(nn) => named_node_lit(nn, name),
        NamedNodePattern::Variable(v) => variable_expression(rdf_node_types, v, name),
    }
}

fn named_node_lit(nn: &NamedNode, name: &str) -> (Expr, RDFNodeType) {
    (
        lit(rdf_named_node_to_polars_literal_value(nn)).alias(name),
        RDFNodeType::IRI,
    )
}

fn variable_expression(
    rdf_node_types: &HashMap<String, RDFNodeType>,
    v: &Variable,
    name: &str,
) -> (Expr, RDFNodeType) {
    (
        col(v.as_str()).alias(name),
        rdf_node_types.get(v.as_str()).unwrap().clone(),
    )
}
