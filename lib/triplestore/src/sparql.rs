pub mod delete;
pub mod errors;
mod insert;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
mod rewrite;
//mod rewrite;

use tracing::{instrument, trace, warn};
use utils::polars::{pl_interruptable_collect, InterruptableCollectError};

use super::{NewTriples, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::rewrite::rewrite;
use oxrdf::{NamedNode, Subject, Term, Triple, Variable};
use oxttl::TurtleSerializer;
use polars::frame::DataFrame;
use polars::prelude::{as_struct, col, lit, Expr, IntoLazy, LiteralValue};
use polars_core::frame::UniqueKeepStrategy;
use polars_core::prelude::DataType;
use query_processing::expressions::expr_is_null_workaround;
use query_processing::graph_patterns::unique_workaround;
use query_processing::pushdowns::Pushdowns;
use representation::cats::{Cats, LockedCats};
use representation::dataset::{NamedGraph, QueryGraph};
use representation::polars_to_rdf::{df_as_result, QuerySolutions};
use representation::query_context::Context;
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use representation::{OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spargebra::algebra::{GraphPattern, QueryDataset};
use spargebra::term::{
    GraphNamePattern, GroundQuadPattern, GroundTermPattern, NamedNodePattern, QuadPattern,
    TermPattern, TriplePattern,
};
use spargebra::{GraphUpdateOperation, Query, Update};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub enum QueryResult {
    Select(EagerSolutionMappings),
    Construct(Vec<(EagerSolutionMappings, Option<NamedNode>)>),
}

#[derive(Debug)]
pub struct QuerySettings {
    pub include_transient: bool,
    pub max_rows: Option<usize>,
    pub strict_project: bool,
}

impl QueryResult {
    pub fn json(&self, global_cats: LockedCats) -> String {
        match self {
            QueryResult::Select(sm) => {
                let QuerySolutions {
                    variables,
                    solutions,
                } = df_as_result(sm, global_cats);
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
                for (sm, predicate) in c {
                    let mut sm = sm.clone();
                    if let Some(predicate) = predicate {
                        sm.mappings = sm
                            .mappings
                            .lazy()
                            .with_column(
                                lit(rdf_named_node_to_polars_literal_value(predicate))
                                    .alias(PREDICATE_COL_NAME),
                            )
                            .select([
                                col(SUBJECT_COL_NAME),
                                col(PREDICATE_COL_NAME),
                                col(OBJECT_COL_NAME),
                            ])
                            .collect()
                            .unwrap();
                        sm.rdf_node_types.insert(
                            PREDICATE_COL_NAME.to_string(),
                            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                        );
                    }
                    let QuerySolutions {
                        variables,
                        solutions,
                    } = df_as_result(&sm, global_cats.clone());
                    for s in solutions {
                        let mut subject = None;
                        let mut predicate = None;
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
                            } else if v.as_str() == PREDICATE_COL_NAME {
                                predicate = Some(match t {
                                    Term::NamedNode(nn) => nn,
                                    _ => panic!("Should never happen"),
                                });
                            } else if v.as_str() == OBJECT_COL_NAME {
                                object = Some(t);
                            } else {
                                panic!("Should never happen");
                            }
                        }
                        let t = Triple::new(subject.unwrap(), predicate.unwrap(), object.unwrap());
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
    #[instrument(skip_all)]
    pub fn query(
        &self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        trace!(?query);
        self.query_parsed(&query, parameters, streaming, query_settings, graph)
    }

    pub fn query_uninterruptable(
        &self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        self.query_parsed_uninterruptable(&query, parameters, streaming, query_settings, graph)
    }

    pub fn query_parsed(
        &self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
    ) -> Result<QueryResult, SparqlError> {
        let query = rewrite(query.clone());
        let context = Context::new();
        match &query {
            Query::Select {
                dataset,
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
                    query_settings,
                    &dataset_or_named_graph(dataset, graph),
                )?;

                match pl_interruptable_collect(mappings.with_new_streaming(streaming)) {
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
                dataset,
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
                    query_settings,
                    &dataset_or_named_graph(dataset, graph),
                )?;
                match pl_interruptable_collect(mappings.with_new_streaming(streaming)) {
                    Ok(df) => {
                        let mut solutions = vec![];
                        for t in template {
                            let cats = self.global_cats.read()?;
                            if let Some((sm, predicate)) = triple_to_solution_mappings(
                                &df,
                                &rdf_node_types,
                                t,
                                None,
                                true,
                                &cats,
                            )? {
                                solutions.push((sm, predicate));
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

    pub fn query_parsed_uninterruptable(
        &self,
        query: &Query,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
    ) -> Result<QueryResult, SparqlError> {
        let query = rewrite(query.clone());
        let context = Context::new();
        match &query {
            Query::Select {
                dataset,
                pattern,
                base_iri: _,
            } => {
                let qg = dataset_or_named_graph(dataset, graph);

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
                    query_settings,
                    &qg,
                )?;

                let df = mappings.with_new_streaming(streaming).collect().unwrap();
                Ok(QueryResult::Select(EagerSolutionMappings::new(df, types)))
            }
            Query::Construct {
                template,
                dataset,
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
                    query_settings,
                    &dataset_or_named_graph(dataset, graph),
                )?;
                let df = mappings.with_new_streaming(streaming).collect().unwrap();

                let mut solutions = vec![];
                for t in template {
                    let cats = self.global_cats.read()?;
                    if let Some((sm, predicate)) =
                        triple_to_solution_mappings(&df, &rdf_node_types, t, None, true, &cats)?
                    {
                        solutions.push((sm, predicate));
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
        query_settings: &QuerySettings,
        graph: &NamedGraph,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        if let Query::Construct { .. } = &query {
            let res =
                self.query_parsed(&query, parameters, streaming, query_settings, Some(graph))?;
            let r = match res {
                QueryResult::Select(_) => {
                    panic!("Should never happen")
                }
                QueryResult::Construct(dfs) => self.insert_construct_result(dfs, transient, graph),
            };
            r
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<(), SparqlError> {
        let update = Update::parse(update, None, prefixes).map_err(SparqlError::ParseError)?;
        self.update_parsed(&update, parameters, streaming, query_settings, graph)?;
        Ok(())
    }

    pub fn update_parsed(
        &mut self,
        update: &Update,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
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
                        dataset: using.clone(),
                        pattern: p,
                        base_iri: None,
                    };
                    let r = self.query_parsed(&q, parameters, streaming, query_settings, graph)?;
                    let EagerSolutionMappings {
                        mappings,
                        rdf_node_types,
                    } = if let QueryResult::Select(sm) = r {
                        sm
                    } else {
                        unreachable!("Should never happen")
                    };
                    let mut delete_solutions: HashMap<_, Vec<_>> = HashMap::new();
                    for d in delete {
                        let cats = self.global_cats.read()?;
                        let del = triple_to_solution_mappings(
                            &mappings,
                            &rdf_node_types,
                            &ground_quad_pattern_to_triple_pattern(d),
                            None,
                            false,
                            &cats,
                        )?;
                        if let Some(sol) = del {
                            match &d.graph_name {
                                GraphNamePattern::NamedNode(nn) => {
                                    let k = NamedGraph::NamedGraph(nn.clone());
                                    if let Some(existing) = delete_solutions.get_mut(&k) {
                                        existing.push(sol)
                                    } else {
                                        delete_solutions.insert(k, vec![sol]);
                                    }
                                }
                                GraphNamePattern::DefaultGraph => {
                                    let ng = NamedGraph::DefaultGraph;
                                    if let Some(existing) = delete_solutions.get_mut(&ng) {
                                        existing.push(sol)
                                    } else {
                                        delete_solutions.insert(ng, vec![sol]);
                                    }
                                }
                                GraphNamePattern::Variable(_) => {
                                    todo!()
                                }
                            }
                        }
                    }
                    for (g, delete_solutions) in delete_solutions {
                        self.delete_construct_result(delete_solutions, &g)?;
                    }
                    let mut insert_solutions: HashMap<_, Vec<_>> = HashMap::new();
                    for i in insert {
                        let cats = self.global_cats.read()?;
                        let insert = triple_to_solution_mappings(
                            &mappings,
                            &rdf_node_types,
                            &quad_pattern_to_triple_pattern(i),
                            None,
                            true,
                            &cats,
                        )?;
                        if let Some(insert) = insert {
                            match &i.graph_name {
                                GraphNamePattern::NamedNode(nn) => {
                                    let k = NamedGraph::NamedGraph(nn.clone());
                                    if let Some(existing) = insert_solutions.get_mut(&k) {
                                        existing.push(insert);
                                    } else {
                                        insert_solutions.insert(k, vec![insert]);
                                    }
                                }
                                GraphNamePattern::DefaultGraph => {
                                    let k = NamedGraph::DefaultGraph;
                                    if let Some(existing) = insert_solutions.get_mut(&k) {
                                        existing.push(insert);
                                    } else {
                                        insert_solutions.insert(k, vec![insert]);
                                    }
                                }
                                GraphNamePattern::Variable(_) => {
                                    todo!()
                                }
                            }
                        }
                    }
                    for (g, s) in insert_solutions {
                        self.insert_construct_result(s, false, &g)?;
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

pub fn triple_to_solution_mappings(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
    t: &TriplePattern,
    preserve_column: Option<&str>,
    unique: bool,
    global_cats: &Cats,
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
    let (subj_expr, subj_dt) = term_pattern_expression(
        rdf_node_types,
        &t.subject,
        SUBJECT_COL_NAME,
        global_cats,
        true,
    )?;
    triple_types.insert(SUBJECT_COL_NAME.to_string(), subj_dt);
    let mut filter_expr = expr_is_null_workaround(
        col(SUBJECT_COL_NAME),
        triple_types.get(SUBJECT_COL_NAME).unwrap(),
    )
    .not();
    select_expr.push(subj_expr);
    let predicate = match &t.predicate {
        NamedNodePattern::NamedNode(predicate) => Some(predicate.clone()),
        NamedNodePattern::Variable(_) => {
            let (predicate_expr, predicate_dt) = named_node_pattern_expr(
                rdf_node_types,
                &t.predicate,
                PREDICATE_COL_NAME,
                global_cats,
            )?;
            triple_types.insert(PREDICATE_COL_NAME.to_string(), predicate_dt);
            filter_expr = filter_expr.and(
                expr_is_null_workaround(
                    col(PREDICATE_COL_NAME),
                    triple_types.get(PREDICATE_COL_NAME).unwrap(),
                )
                .not(),
            );
            select_expr.push(predicate_expr);
            None
        }
    };

    let (obj_expr, obj_dt) = term_pattern_expression(
        rdf_node_types,
        &t.object,
        OBJECT_COL_NAME,
        global_cats,
        false,
    )?;
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
        Ok(Some((
            EagerSolutionMappings::new(df, triple_types),
            predicate,
        )))
    } else {
        Ok(None)
    }
}

fn term_pattern_expression(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    tp: &TermPattern,
    name: &str,
    global_cats: &Cats,
    subject: bool,
) -> Result<(Expr, RDFNodeState), SparqlError> {
    match tp {
        TermPattern::NamedNode(nn) => Ok(named_node_u32_lit(nn, name, global_cats)),
        TermPattern::BlankNode(_) => {
            unimplemented!("Blank node term pattern not supported")
        }
        TermPattern::Literal(thelit) => {
            let l = lit(rdf_literal_to_polars_literal_value(thelit)).alias(name);
            Ok((
                l,
                BaseRDFNodeType::Literal(thelit.datatype().into_owned())
                    .into_default_input_rdf_node_state(),
            ))
        }
        TermPattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name, subject)?),
    }
}

fn named_node_pattern_expr(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    nnp: &NamedNodePattern,
    name: &str,
    global_cats: &Cats,
) -> Result<(Expr, RDFNodeState), SparqlError> {
    match nnp {
        NamedNodePattern::NamedNode(nn) => Ok(named_node_u32_lit(nn, name, global_cats)),
        NamedNodePattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name, false)?),
    }
}

fn named_node_u32_lit(nn: &NamedNode, name: &str, global_cats: &Cats) -> (Expr, RDFNodeState) {
    let (u, state) = global_cats.encode_iri_or_local_cat(nn.as_str());
    (lit(u).cast(DataType::UInt32).alias(name), state)
}

fn variable_expression(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    v: &Variable,
    name: &str,
    subject: bool,
) -> Result<(Expr, RDFNodeState), SparqlError> {
    if let Some(t) = rdf_node_types.get(v.as_str()) {
        if subject {
            let mut new_map = HashMap::new();
            let mut exprs = vec![];
            for (bt, bs) in &t.map {
                if matches!(bt, BaseRDFNodeType::BlankNode | BaseRDFNodeType::IRI) {
                    new_map.insert(bt.clone(), bs.clone());
                    if t.is_multi() {
                        exprs.push(
                            col(v.as_str())
                                .struct_()
                                .field_by_name(&bt.field_col_name()),
                        );
                    } else {
                        exprs.push(col(v.as_str()))
                    }
                }
            }
            if exprs.is_empty() {
                let bt = BaseRDFNodeType::None;
                let bs = bt.default_input_cat_state();
                let e = lit(LiteralValue::untyped_null())
                    .cast(bt.polars_data_type(&bs, true))
                    .alias(name);
                let s = RDFNodeState::from_bases(bt, bs);
                Ok((e, s))
            } else if exprs.len() == 1 {
                Ok((
                    exprs.pop().unwrap().alias(name),
                    RDFNodeState::from_map(new_map),
                ))
            } else {
                Ok((
                    as_struct(exprs).alias(name),
                    RDFNodeState::from_map(new_map),
                ))
            }
        } else {
            Ok((col(v.as_str()).alias(name), t.clone()))
        }
    } else {
        Err(SparqlError::ConstructWithUndefinedVariable(
            v.as_str().to_string(),
        ))
    }
}

fn dataset_or_named_graph(
    dataset: &Option<QueryDataset>,
    graph: Option<&NamedGraph>,
) -> QueryGraph {
    if let Some(dataset) = dataset {
        QueryGraph::QueryDataset(dataset.clone())
    } else if let Some(graph) = graph {
        QueryGraph::from_named_graph(graph)
    } else {
        QueryGraph::NamedGraph(NamedGraph::DefaultGraph)
    }
}
