pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;

use oxrdf::{NamedNode, Variable};
use representation::query_context::Context;
use std::collections::HashMap;

use super::Triplestore;
use crate::constants::{OBJECT_COL_NAME, OTTR_IRI, SUBJECT_COL_NAME, VERB_COL_NAME};
use crate::sparql::errors::SparqlError;
use crate::TriplesToAdd;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, DataType, Expr, IntoLazy};
use polars_core::enable_string_cache;
use query_processing::expressions::col_null_expr;
use representation::multitype::{split_df_multicols, unique_workaround};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use representation::RDFNodeType;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use uuid::Uuid;

#[derive(Debug)]
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
        dfs: Vec<(DataFrame, RDFNodeType, RDFNodeType)>,
        transient: bool,
    ) -> Result<(), SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let mut all_triples_to_add = vec![];

        for (df, subj_dt, obj_dt) in dfs {
            if df.height() == 0 {
                continue;
            }
            let mut multicols = HashMap::new();
            if matches!(subj_dt, RDFNodeType::MultiType(..)) {
                multicols.insert(SUBJECT_COL_NAME.to_string(), subj_dt.clone());
            }
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
    lf = unique_workaround(lf, &triple_types, None, false);

    let df = lf.collect().unwrap();
    if df.height() > 0 {
        Ok(Some((
            df,
            triple_types.remove(SUBJECT_COL_NAME).unwrap(),
            triple_types.remove(OBJECT_COL_NAME).unwrap(),
        )))
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
            if thelit.datatype().as_str() == OTTR_IRI {
                named_node_lit(&NamedNode::new(thelit.to_string()).unwrap(), name)
            } else {
                let l = lit(sparql_literal_to_polars_literal_value(thelit)).alias(name);
                (l, RDFNodeType::Literal(thelit.datatype().into_owned()))
            }
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
        lit(sparql_named_node_to_polars_literal_value(nn)).alias(name),
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

fn cats_to_strings(df: DataFrame) -> DataFrame {
    let mut cats = vec![];
    for c in df.columns(df.get_column_names()).unwrap() {
        if let DataType::Categorical(_, _) = c.dtype() {
            cats.push(c.name().to_string());
        }
    }
    let mut lf = df.lazy();
    for c in cats {
        lf = lf.with_column(col(&c).cast(DataType::String))
    }
    lf.collect().unwrap()
}
