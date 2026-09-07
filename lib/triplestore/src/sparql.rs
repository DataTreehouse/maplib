pub mod debug;
pub mod delete;
pub mod errors;
mod insert;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
mod rewrite;

use tracing::{instrument, trace, warn};
use utils::polars::{pl_interruptable_collect, InterruptableCollectError};

use super::{NewTriples, Triplestore};
use crate::sparql::debug::DebugOutputs;
use crate::sparql::errors::SparqlError;
use crate::sparql::rewrite::rewrite;
use oxrdf::NamedNode;
use query_processing::bindings::maybe_replace_bindings;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::debug::DebugOutput;
use representation::query_context::Context;
use representation::result::{
    ground_quad_pattern_to_quad_pattern, quad_patterns_to_solution_mappings,
    template_to_solution_mappings, QueryResult, QueryResultKind,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{GraphPattern, QueryDataset};
use spargebra::term::{
    GraphNamePattern, GroundTerm, GroundTermPattern, NamedNodePattern, QuadPattern, TermPattern,
};
use spargebra::{GraphUpdateOperation, Query, Update};
use std::collections::{HashMap, HashSet};

pub struct InsertResult {
    pub new_triples: Vec<NewTriples>,
    pub debug: Option<DebugOutputs>,
}

pub struct UpdateResult {
    pub debug: Option<DebugOutputs>,
}

#[derive(Debug, Clone)]
pub struct QuerySettings {
    pub include_transient: bool,
    pub max_rows: Option<usize>,
    pub strict_project: bool,
}

impl Triplestore {
    #[instrument(skip_all)]
    pub fn query(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
        debug_no_results: bool,
        bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> Result<QueryResult, SparqlError> {
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        let query =
            maybe_replace_bindings(query, bindings).map_err(SparqlError::ReplaceBindingsError)?;
        trace!(?query);
        self.query_parsed(
            &query,
            parameters,
            streaming,
            query_settings,
            graph,
            debug_no_results,
        )
    }

    pub fn query_parsed(
        &self,
        query: &Query,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
    ) -> Result<QueryResult, SparqlError> {
        let query = rewrite(query.clone());
        let context = Context::new();
        let mut no_results = false;
        let res = match &query {
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

                match pl_interruptable_collect(mappings.with_streaming(streaming)) {
                    Ok(df) => {
                        if df.height() == 0 {
                            no_results = true;
                        }
                        QueryResultKind::Select(EagerSolutionMappings::new(df, types))
                    }
                    Err(InterruptableCollectError::Interrupted) => {
                        return Err(SparqlError::InterruptSignal)
                    }
                    Err(e) => {
                        return Err(SparqlError::QueryExecutionError(e.to_string()));
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
                match pl_interruptable_collect(mappings.with_streaming(streaming)) {
                    Ok(df) => {
                        if df.height() == 0 {
                            no_results = true;
                        }
                        let quad_template: Vec<_> = template
                            .iter()
                            .map(|x| QuadPattern {
                                subject: x.subject.clone(),
                                predicate: x.predicate.clone(),
                                object: x.object.clone(),
                                graph_name: GraphNamePattern::DefaultGraph,
                            })
                            .collect();

                        let mut solutions_map = template_to_solution_mappings(
                            df,
                            rdf_node_types,
                            &quad_template,
                            self.global_cats.clone(),
                        )
                        .map_err(|x| SparqlError::QuadPatternsError(x.to_string()))?;
                        let solutions = solutions_map
                            .remove(&NamedGraph::DefaultGraph)
                            .unwrap_or(Vec::new());
                        QueryResultKind::Construct(solutions)
                    }
                    Err(InterruptableCollectError::Interrupted) => {
                        return Err(SparqlError::InterruptSignal)
                    }
                    Err(e) => {
                        return Err(SparqlError::QueryExecutionError(e.to_string()));
                    }
                }
            }
            _ => return Err(SparqlError::QueryTypeNotSupported),
        };
        let debug_output = if debug_no_results && no_results {
            Some(self.debug(&query, parameters, query_settings, graph)?)
        } else if debug_no_results {
            Some(DebugOutputs::new(vec![DebugOutput::HasResults]))
        } else {
            None
        };
        Ok(QueryResult {
            kind: res,
            debug: debug_output,
            pushdown_paths: Vec::new(),
        })
    }

    pub fn insert(
        &mut self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        transient: bool,
        streaming: bool,
        query_settings: &QuerySettings,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
        prefixes: Option<&HashMap<String, NamedNode>>,
        debug_no_results: bool,
    ) -> Result<InsertResult, SparqlError> {
        let query = Query::parse(query, None, prefixes).map_err(SparqlError::ParseError)?;
        self.insert_parsed(
            &query,
            parameters,
            transient,
            streaming,
            query_settings,
            source_graph,
            target_graph,
            debug_no_results,
        )
    }

    pub fn insert_parsed(
        &mut self,
        query: &Query,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        transient: bool,
        streaming: bool,
        query_settings: &QuerySettings,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
        debug_no_results: bool,
    ) -> Result<InsertResult, SparqlError> {
        if let Query::Construct { .. } = &query {
            let res = self.query_parsed(
                query,
                parameters,
                streaming,
                query_settings,
                Some(source_graph),
                debug_no_results,
            )?;
            let new_triples = match res.kind {
                QueryResultKind::Select(_) => {
                    return Err(SparqlError::QueryExecutionError(
                        "Got SELECT query when CONSTRUCT was expected".to_string(),
                    ))
                }
                QueryResultKind::Construct(dfs) => {
                    self.insert_construct_result(dfs, transient, target_graph)
                }
            };
            Ok(InsertResult {
                new_triples: new_triples?,
                debug: res.debug,
            })
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, SparqlError> {
        let update = Update::parse(update, None, prefixes).map_err(SparqlError::ParseError)?;
        let res = self.update_parsed(
            &update,
            parameters,
            streaming,
            query_settings,
            graph,
            debug_no_results,
        )?;
        Ok(res)
    }

    pub fn update_parsed(
        &mut self,
        update: &Update,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        streaming: bool,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, SparqlError> {
        let mut debug_output = if debug_no_results { Some(vec![]) } else { None };
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
                    let QueryResult { kind, debug, .. } = self.query_parsed(
                        &q,
                        parameters,
                        streaming,
                        query_settings,
                        graph,
                        debug_no_results,
                    )?;
                    if debug_no_results {
                        if let Some(debug) = debug {
                            debug_output.as_mut().unwrap().extend(debug.debug_outputs);
                        }
                    }
                    let EagerSolutionMappings {
                        mappings,
                        rdf_node_types,
                    } = if let QueryResultKind::Select(sm) = kind {
                        sm
                    } else {
                        unreachable!("Should never happen")
                    };
                    let mut delete_solutions: HashMap<_, Vec<_>> = HashMap::new();
                    for d in delete {
                        let cats = self.global_cats.read()?;
                        let del = quad_patterns_to_solution_mappings(
                            &mappings,
                            &rdf_node_types,
                            &ground_quad_pattern_to_quad_pattern(d),
                            None,
                            false,
                            &cats,
                        )
                        .map_err(|e| SparqlError::QuadPatternsError(e.to_string()))?;
                        if let Some((df, pred, graph)) = del {
                            let sol = (df, pred);
                            if let Some(existing) = delete_solutions.get_mut(&graph) {
                                existing.push(sol)
                            } else {
                                delete_solutions.insert(graph, vec![sol]);
                            }
                        }
                    }
                    for (g, delete_solutions) in delete_solutions {
                        self.delete_construct_result(delete_solutions, &g)?;
                    }
                    let insert_solutions = template_to_solution_mappings(
                        mappings,
                        rdf_node_types,
                        insert,
                        self.global_cats.clone(),
                    )
                    .map_err(|x| SparqlError::QuadPatternsError(x.to_string()))?;
                    for (g, s) in insert_solutions {
                        self.insert_construct_result(s, false, &g)?;
                    }
                    self.maybe_garbage_collect()?;
                }
                GraphUpdateOperation::Load { .. } => {}
                GraphUpdateOperation::Clear { .. } => {}
                GraphUpdateOperation::Create { .. } => {}
                GraphUpdateOperation::Drop { .. } => {}
            }
        }
        let debug = if let Some(debug_output) = debug_output {
            Some(DebugOutputs::new(debug_output))
        } else if debug_no_results {
            Some(DebugOutputs::new(vec![DebugOutput::HasResults]))
        } else {
            None
        };
        let res = UpdateResult { debug };
        Ok(res)
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
