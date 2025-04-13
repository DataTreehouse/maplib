pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;

use super::{NewTriples, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::TriplesToAdd;
use oxrdf::{NamedNode, Subject, Term, Triple, Variable};
use oxttl::TurtleSerializer;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, Expr, IntoLazy};
use polars_core::frame::UniqueKeepStrategy;
use query_processing::expressions::expr_is_null_workaround;
use query_processing::pushdowns::Pushdowns;
use representation::multitype::{split_df_multicols, unique_workaround};
use representation::polars_to_rdf::{df_as_result, QuerySolutions};
use representation::query_context::Context;
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeType;
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub enum QueryResult {
    Select(EagerSolutionMappings),
    Construct(Vec<(EagerSolutionMappings, Option<NamedNode>)>),
}

impl QueryResult {
    pub fn json(&self) -> String {
        match self {
            QueryResult::Select(EagerSolutionMappings {
                mappings,
                rdf_node_types,
            }) => {
                let QuerySolutions {
                    variables,
                    solutions,
                } = df_as_result(mappings.clone(), rdf_node_types);
                let json_serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json);
                let mut buffer = vec![];
                let mut serializer = json_serializer
                    .serialize_solutions_to_writer(&mut buffer, variables.clone())
                    .unwrap();
                for s in solutions {
                    let mut solvec = vec![];
                    for (i, t) in s.iter().enumerate() {
                        if let Some(t) = t {
                            solvec.push((variables.get(i).unwrap(), t.as_ref()));
                        }
                    }
                    serializer.serialize(solvec).unwrap()
                }
                serializer.finish().unwrap();
                String::from_utf8(buffer).unwrap()
            }
            QueryResult::Construct(c) => {
                let mut ser = TurtleSerializer::new().for_writer(vec![]);
                for (
                    EagerSolutionMappings {
                        mappings,
                        rdf_node_types,
                    },
                    verb,
                ) in c
                {
                    let mut mappings = mappings.clone();
                    let mut rdf_node_types = rdf_node_types.clone();
                    if let Some(verb) = verb {
                        mappings = mappings
                            .lazy()
                            .with_column(
                                lit(rdf_named_node_to_polars_literal_value(verb))
                                    .alias(VERB_COL_NAME),
                            )
                            .select([
                                col(SUBJECT_COL_NAME),
                                col(VERB_COL_NAME),
                                col(OBJECT_COL_NAME),
                            ])
                            .collect()
                            .unwrap();
                        rdf_node_types.insert(VERB_COL_NAME.to_string(), RDFNodeType::IRI);
                    }
                    let QuerySolutions {
                        variables,
                        solutions,
                    } = df_as_result(mappings, &rdf_node_types);
                    for s in solutions {
                        let mut subject = None;
                        let mut verb = None;
                        let mut object = None;
                        for (i, t) in s.into_iter().enumerate() {
                            let t = t.unwrap();
                            let v = variables.get(i).unwrap();
                            if v.as_str() == SUBJECT_COL_NAME {
                                subject = Some(match t {
                                    Term::NamedNode(nn) => Subject::NamedNode(nn),
                                    Term::BlankNode(bl) => Subject::BlankNode(bl),
                                    _ => todo!(),
                                });
                            } else if v.as_str() == VERB_COL_NAME {
                                verb = Some(match t {
                                    Term::NamedNode(nn) => nn,
                                    _ => panic!("Should never happen"),
                                });
                            } else if v.as_str() == OBJECT_COL_NAME {
                                object = Some(t);
                            } else {
                                panic!("Should never happen");
                            }
                        }
                        let t = Triple::new(subject.unwrap(), verb.unwrap(), object.unwrap());
                        ser.serialize_triple(t.as_ref()).unwrap();
                    }
                }
                let res = ser.finish().unwrap();
                String::from_utf8(res).unwrap()
            }
        }
    }
}

impl Triplestore {
    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
    ) -> Result<QueryResult, SparqlError> {
        if streaming {
            unimplemented!("Streaming is currently disabled due to an unresolved bug in Polarsq")
        }
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_parsed(&query, parameters, streaming, include_transient)
    }

    pub fn query_parsed(
        &mut self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
    ) -> Result<QueryResult, SparqlError> {
        if self.has_unindexed {
            self.index_unindexed().map_err(SparqlError::IndexingError)?;
        }

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
                    ..
                } = self.lazy_graph_pattern(
                    pattern,
                    None,
                    &context,
                    parameters,
                    Pushdowns::new(),
                    include_transient,
                )?;
                let df = mappings.with_streaming(streaming).collect().unwrap();
                Ok(QueryResult::Select(EagerSolutionMappings::new(df, types)))
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
                    ..
                } = self.lazy_graph_pattern(
                    pattern,
                    None,
                    &context,
                    parameters,
                    Pushdowns::new(),
                    include_transient,
                )?;
                let df = mappings.collect().unwrap();
                let mut solutions = vec![];
                for t in template {
                    if let Some((sm, verb)) = triple_to_solution_mappings(&df, &rdf_node_types, t)?
                    {
                        solutions.push((sm, verb));
                    }
                }
                Ok(QueryResult::Construct(solutions))
            }
            _ => Err(SparqlError::QueryTypeNotSupported),
        }
    }

    pub fn insert(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        transient: bool,
        streaming: bool,
        delay_index: bool,
        include_transient: bool,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        if let Query::Construct { .. } = &query {
            let res = self.query_parsed(&query, parameters, streaming, include_transient)?;
            match res {
                QueryResult::Select(_) => {
                    panic!("Should never happen")
                }
                QueryResult::Construct(dfs) => {
                    self.insert_construct_result(dfs, transient, delay_index)
                }
            }
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }
    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        transient: bool,
        delay_index: bool,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let mut all_triples_to_add = vec![];

        for (
            EagerSolutionMappings {
                mappings,
                rdf_node_types,
            },
            verb,
        ) in sms
        {
            if mappings.height() == 0 {
                continue;
            }
            let mut multicols = HashMap::new();
            let subj_dt = rdf_node_types.get(SUBJECT_COL_NAME).unwrap();
            if matches!(subj_dt, RDFNodeType::MultiType(..)) {
                multicols.insert(SUBJECT_COL_NAME.to_string(), subj_dt.clone());
            }
            let obj_dt = rdf_node_types.get(OBJECT_COL_NAME).unwrap();
            if matches!(obj_dt, RDFNodeType::MultiType(..)) {
                multicols.insert(OBJECT_COL_NAME.to_string(), obj_dt.clone());
            }
            if !multicols.is_empty() {
                let dfs_dts = split_df_multicols(mappings, &multicols);
                for (df, mut map) in dfs_dts {
                    let new_subj_dt = map.remove(SUBJECT_COL_NAME).unwrap_or(subj_dt.clone());
                    let new_obj_dt = map.remove(OBJECT_COL_NAME).unwrap_or(obj_dt.clone());
                    all_triples_to_add.push(TriplesToAdd {
                        df,
                        subject_type: new_subj_dt,
                        object_type: new_obj_dt,
                        static_verb_column: verb.clone(),
                    });
                }
            } else {
                all_triples_to_add.push(TriplesToAdd {
                    df: mappings,
                    subject_type: subj_dt.clone(),
                    object_type: obj_dt.clone(),
                    static_verb_column: verb,
                });
            }
        }
        let new_triples = if !all_triples_to_add.is_empty() {
            self.add_triples_vec(all_triples_to_add, &call_uuid, transient, delay_index)
                .map_err(SparqlError::StoreTriplesError)?
        } else {
            vec![]
        };
        Ok(new_triples)
    }
}

fn triple_to_solution_mappings(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    t: &TriplePattern,
) -> Result<Option<(EagerSolutionMappings, Option<NamedNode>)>, SparqlError> {
    let mut triple_types = HashMap::new();
    let (subj_expr, subj_dt) =
        term_pattern_expression(rdf_node_types, &t.subject, SUBJECT_COL_NAME)?;
    triple_types.insert(SUBJECT_COL_NAME.to_string(), subj_dt);
    let mut filter_expr = expr_is_null_workaround(
        col(SUBJECT_COL_NAME),
        triple_types.get(SUBJECT_COL_NAME).unwrap(),
    )
    .not();
    let mut select_expr = vec![subj_expr];
    let verb = match &t.predicate {
        NamedNodePattern::NamedNode(verb) => Some(verb.clone()),
        NamedNodePattern::Variable(_) => {
            let (verb_expr, verb_dt) =
                named_node_pattern_expr(rdf_node_types, &t.predicate, VERB_COL_NAME)?;
            triple_types.insert(VERB_COL_NAME.to_string(), verb_dt);
            filter_expr = filter_expr.and(
                expr_is_null_workaround(
                    col(VERB_COL_NAME),
                    triple_types.get(VERB_COL_NAME).unwrap(),
                )
                .not(),
            );
            select_expr.push(verb_expr);
            None
        }
    };

    let (obj_expr, obj_dt) = term_pattern_expression(rdf_node_types, &t.object, OBJECT_COL_NAME)?;
    select_expr.push(obj_expr);
    triple_types.insert(OBJECT_COL_NAME.to_string(), obj_dt);
    filter_expr = filter_expr.and(
        expr_is_null_workaround(
            col(OBJECT_COL_NAME),
            triple_types.get(OBJECT_COL_NAME).unwrap(),
        )
        .not(),
    );
    let mut lf = df.clone().lazy().select(select_expr).filter(filter_expr);
    lf = unique_workaround(lf, &triple_types, None, false, UniqueKeepStrategy::Any);

    let df = lf.collect().unwrap();
    if df.height() > 0 {
        Ok(Some((EagerSolutionMappings::new(df, triple_types), verb)))
    } else {
        Ok(None)
    }
}

fn term_pattern_expression(
    rdf_node_types: &HashMap<String, RDFNodeType>,
    tp: &TermPattern,
    name: &str,
) -> Result<(Expr, RDFNodeType), SparqlError> {
    match tp {
        TermPattern::NamedNode(nn) => Ok(named_node_lit(nn, name)),
        TermPattern::BlankNode(_) => {
            unimplemented!("Blank node term pattern not supported")
        }
        TermPattern::Literal(thelit) => {
            let l = lit(rdf_literal_to_polars_literal_value(thelit)).alias(name);
            Ok((l, RDFNodeType::Literal(thelit.datatype().into_owned())))
        }
        TermPattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name)?),
    }
}

fn named_node_pattern_expr(
    rdf_node_types: &HashMap<String, RDFNodeType>,
    nnp: &NamedNodePattern,
    name: &str,
) -> Result<(Expr, RDFNodeType), SparqlError> {
    match nnp {
        NamedNodePattern::NamedNode(nn) => Ok(named_node_lit(nn, name)),
        NamedNodePattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name)?),
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
) -> Result<(Expr, RDFNodeType), SparqlError> {
    let t = if let Some(t) = rdf_node_types.get(v.as_str()) {
        t.clone()
    } else {
        return Err(SparqlError::ConstructWithUndefinedVariable(
            v.as_str().to_string(),
        ));
    };

    Ok((col(v.as_str()).alias(name), t))
}
