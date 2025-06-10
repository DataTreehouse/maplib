pub mod errors;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;

use utils::polars::{pl_interruptable_collect, InterruptableCollectError};

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
use spargebra::term::{
    GraphNamePattern, GroundQuadPattern, GroundTermPattern, NamedNodePattern, QuadPattern,
    TermPattern, TriplePattern,
};
use spargebra::{GraphUpdateOperation, Query, Update};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[cfg(feature = "pyo3")]
use pyo3::Python;
use spargebra::algebra::GraphPattern;

#[derive(Debug)]
pub enum QueryResult {
    Select(EagerSolutionMappings),
    Construct(Vec<(EagerSolutionMappings, Option<NamedNode>)>),
}

pub struct QuerySettings {
    pub include_transient: bool,
    pub allow_duplicates: bool,
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
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_parsed(
            &query,
            parameters,
            streaming,
            include_transient,
            #[cfg(feature = "pyo3")]
            py,
        )
    }

    pub fn query_indexed(
        &self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
        allow_duplicates: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_parsed_indexed(
            &query,
            parameters,
            streaming,
            include_transient,
            allow_duplicates,
            true,
            #[cfg(feature = "pyo3")]
            py,
        )
    }

    pub fn query_indexed_uninterruptable(
        &self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
        allow_duplicates: bool,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        self.query_parsed_indexed_uninterruptable(
            &query,
            parameters,
            streaming,
            include_transient,
            allow_duplicates,
            true,
        )
    }

    pub fn query_parsed(
        &mut self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<QueryResult, SparqlError> {
        if self.has_unindexed {
            self.index_unindexed().map_err(SparqlError::IndexingError)?;
        }
        self.query_parsed_indexed(
            query,
            parameters,
            streaming,
            include_transient,
            false,
            true,
            #[cfg(feature = "pyo3")]
            py,
        )
    }

    pub fn query_parsed_indexed(
        &self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
        allow_duplicates: bool,
        deduplicate_triples: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<QueryResult, SparqlError> {
        let qs = QuerySettings {
            include_transient,
            allow_duplicates,
        };
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
                    &qs,
                )?;

                match pl_interruptable_collect(
                    mappings.with_new_streaming(streaming),
                    #[cfg(feature = "pyo3")]
                    py,
                ) {
                    Ok(df) => Ok(QueryResult::Select(EagerSolutionMappings::new(df, types))),
                    Err(InterruptableCollectError::Interrupted) => {
                        Err(SparqlError::InterruptSignal)
                    }
                    Err(e) => {
                        panic!("Error {e}");
                    }
                }
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
                    &qs,
                )?;
                match pl_interruptable_collect(
                    mappings.with_new_streaming(streaming),
                    #[cfg(feature = "pyo3")]
                    py,
                ) {
                    Ok(df) => {
                        let mut solutions = vec![];
                        for t in template {
                            if let Some((sm, verb)) = triple_to_solution_mappings(
                                &df,
                                &rdf_node_types,
                                t,
                                None,
                                deduplicate_triples,
                            )? {
                                solutions.push((sm, verb));
                            }
                        }
                        Ok(QueryResult::Construct(solutions))
                    }
                    Err(InterruptableCollectError::Interrupted) => {
                        Err(SparqlError::InterruptSignal)
                    }
                    Err(e) => {
                        panic!("Error {e}");
                    }
                }
            }
            _ => Err(SparqlError::QueryTypeNotSupported),
        }
    }

    pub fn query_parsed_indexed_uninterruptable(
        &self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        include_transient: bool,
        allow_duplicates: bool,
        deduplicate_triples: bool,
    ) -> Result<QueryResult, SparqlError> {
        let qs = QuerySettings {
            include_transient,
            allow_duplicates,
        };
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
                    &qs,
                )?;

                let df = mappings.with_new_streaming(streaming).collect().unwrap();
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
                    &qs,
                )?;
                let df = mappings.with_new_streaming(streaming).collect().unwrap();

                let mut solutions = vec![];
                for t in template {
                    if let Some((sm, verb)) = triple_to_solution_mappings(
                        &df,
                        &rdf_node_types,
                        t,
                        None,
                        deduplicate_triples,
                    )? {
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
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let query = Query::parse(query, None).map_err(SparqlError::ParseError)?;
        if let Query::Construct { .. } = &query {
            let res = self.query_parsed(
                &query,
                parameters,
                streaming,
                include_transient,
                #[cfg(feature = "pyo3")]
                py,
            )?;
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
        let all_triples_to_add = construct_result_as_triples_to_add(sms);
        let new_triples = if !all_triples_to_add.is_empty() {
            self.add_triples_vec(all_triples_to_add, &call_uuid, transient, delay_index)
                .map_err(SparqlError::StoreTriplesError)?
        } else {
            vec![]
        };
        Ok(new_triples)
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        delay_index: bool,
        include_transient: bool,
        allow_duplicates: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<(), SparqlError> {
        let update = Update::parse(update, None).map_err(SparqlError::ParseError)?;
        self.update_parsed(
            &update,
            parameters,
            streaming,
            delay_index,
            include_transient,
            allow_duplicates,
            #[cfg(feature = "pyo3")]
            py,
        )?;
        Ok(())
    }

    pub fn update_parsed(
        &mut self,
        update: &Update,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        delay_index: bool,
        include_transient: bool,
        allow_duplicates: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<(), SparqlError> {
        if self.has_unindexed {
            self.index_unindexed().map_err(SparqlError::IndexingError)?;
        }
        self.update_parsed_indexed(
            update,
            parameters,
            streaming,
            delay_index,
            include_transient,
            allow_duplicates,
            #[cfg(feature = "pyo3")]
            py,
        )?;
        Ok(())
    }

    pub fn update_parsed_indexed(
        &mut self,
        update: &Update,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        delay_index: bool,
        include_transient: bool,
        allow_duplicates: bool,
        #[cfg(feature = "pyo3")] py: Python<'_>,
    ) -> Result<(), SparqlError> {
        for u in &update.operations {
            match u {
                GraphUpdateOperation::InsertData { .. } => {
                    todo!()
                }
                GraphUpdateOperation::DeleteData { .. } => {
                    todo!()
                }
                GraphUpdateOperation::DeleteInsert {
                    delete,
                    insert,
                    using,
                    pattern,
                } => {
                    if using.is_some() {
                        todo!()
                    }
                    let mut variables = HashSet::new();
                    for d in delete {
                        if let GroundTermPattern::Variable(v) = &d.subject {
                            variables.insert(v.clone());
                        }
                        if let GroundTermPattern::Variable(v) = &d.object {
                            variables.insert(v.clone());
                        }
                        if let NamedNodePattern::Variable(v) = &d.predicate {
                            variables.insert(v.clone());
                        }
                    }
                    for i in insert {
                        if let TermPattern::Variable(v) = &i.subject {
                            variables.insert(v.clone());
                        }
                        if let TermPattern::Variable(v) = &i.object {
                            variables.insert(v.clone());
                        }
                        if let NamedNodePattern::Variable(v) = &i.predicate {
                            variables.insert(v.clone());
                        }
                    }
                    let mut variables: Vec<_> = variables.into_iter().collect();
                    variables.sort();
                    let p = GraphPattern::Project {
                        variables,
                        inner: pattern.clone(),
                    };
                    let q = Query::Select {
                        dataset: None,
                        pattern: p,
                        base_iri: None,
                    };
                    let r = self.query_parsed_indexed(
                        &q,
                        parameters,
                        streaming,
                        include_transient,
                        allow_duplicates,
                        delay_index, //TODO! Check
                        #[cfg(feature = "pyo3")]
                        py,
                    )?;
                    let EagerSolutionMappings {
                        mappings,
                        rdf_node_types,
                    } = if let QueryResult::Select(sm) = r {
                        sm
                    } else {
                        unreachable!("Should never happen")
                    };
                    let mut delete_solutions = vec![];
                    for d in delete {
                        let del = triple_to_solution_mappings(
                            &mappings,
                            &rdf_node_types,
                            &ground_quad_pattern_to_triple_pattern(d),
                            None,
                            false,
                        )?;
                        if let Some(sol) = del {
                            delete_solutions.push(sol);
                        }
                    }
                    if !delete_solutions.is_empty() {
                        self.delete_construct_result(delete_solutions, delay_index)?;
                    }
                    let mut insert_solutions = vec![];
                    for i in insert {
                        let insert = triple_to_solution_mappings(
                            &mappings,
                            &rdf_node_types,
                            &quad_pattern_to_triple_pattern(i),
                            None,
                            true,
                        )?;
                        if let Some(insert) = insert {
                            insert_solutions.push(insert);
                        }
                    }
                    if !insert_solutions.is_empty() {
                        self.insert_construct_result(insert_solutions, false, delay_index)?;
                    }
                }
                GraphUpdateOperation::Load { .. } => {}
                GraphUpdateOperation::Clear { .. } => {}
                GraphUpdateOperation::Create { .. } => {}
                GraphUpdateOperation::Drop { .. } => {}
            }
        }
        Ok(())
    }

    pub fn delete_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        delay_index: bool,
    ) -> Result<(), SparqlError> {
        let call_uuid = Uuid::new_v4().to_string();
        let all_triples_to_add = construct_result_as_triples_to_add(sms);
        if !all_triples_to_add.is_empty() {
            self.delete_triples_vec(all_triples_to_add, &call_uuid, delay_index)
                .map_err(SparqlError::StoreTriplesError)?;
        };
        Ok(())
    }
}

fn quad_pattern_to_triple_pattern(qp: &QuadPattern) -> TriplePattern {
    match &qp.graph_name {
        GraphNamePattern::NamedNode(_) => {
            todo!()
        }
        GraphNamePattern::DefaultGraph => {}
        GraphNamePattern::Variable(_) => {
            todo!()
        }
    }
    TriplePattern {
        subject: qp.subject.clone(),
        predicate: qp.predicate.clone(),
        object: qp.object.clone(),
    }
}

fn ground_quad_pattern_to_triple_pattern(qp: &GroundQuadPattern) -> TriplePattern {
    match qp.graph_name {
        GraphNamePattern::NamedNode(_) => {
            todo!()
        }
        GraphNamePattern::DefaultGraph => {}
        GraphNamePattern::Variable(_) => {
            todo!()
        }
    }
    let subject = TermPattern::from(qp.subject.clone());
    let predicate = qp.predicate.clone();
    let object = TermPattern::from(qp.object.clone());
    TriplePattern {
        subject,
        predicate,
        object,
    }
}

fn construct_result_as_triples_to_add(
    sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
) -> Vec<TriplesToAdd> {
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
    all_triples_to_add
}

pub fn triple_to_solution_mappings(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    t: &TriplePattern,
    preserve_column: Option<&str>,
    unique: bool,
) -> Result<Option<(EagerSolutionMappings, Option<NamedNode>)>, SparqlError> {
    let mut select_expr = vec![];
    let mut triple_types = HashMap::new();
    if let Some(preserve_column) = preserve_column {
        select_expr.push(col(preserve_column));
        triple_types.insert(
            preserve_column.to_string(),
            rdf_node_types.get(preserve_column).unwrap().clone(),
        );
    }
    let (subj_expr, subj_dt) =
        term_pattern_expression(rdf_node_types, &t.subject, SUBJECT_COL_NAME)?;
    triple_types.insert(SUBJECT_COL_NAME.to_string(), subj_dt);
    let mut filter_expr = expr_is_null_workaround(
        col(SUBJECT_COL_NAME),
        triple_types.get(SUBJECT_COL_NAME).unwrap(),
    )
    .not();
    select_expr.push(subj_expr);
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
    if unique {
        lf = unique_workaround(lf, &triple_types, None, false, UniqueKeepStrategy::Any);
    }
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
