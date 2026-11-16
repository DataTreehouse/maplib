pub mod debug;
pub mod delete;
pub mod errors;
pub mod evaluation_hook;
mod insert;
pub(crate) mod lazy_aggregate;
mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
mod lazy_window;
mod load;
mod rewrite;

use tracing::{instrument, trace, warn};
use utils::polars::{pl_interruptable_collect_estimated, InterruptableCollectError};

use super::{NewTriples, Triplestore};
use crate::changes::{tables_as_new_triples, Changes, Deleted};
use crate::sparql::debug::DebugOutputs;
use crate::sparql::errors::SparqlError;
use crate::sparql::rewrite::rewrite;
use crate::triples_read::{
    subject_to_oxrdf_subject, term_to_oxrdf_term, triples_to_triples_to_add,
};
use external_sparql::federation::FederationSettings;
use oxrdf::NamedNode;
use oxrdf::{Term, Triple};
use query_processing::bindings::maybe_replace_bindings;
use query_processing::pushdowns::Pushdowns;
use representation::constants::DEFAULT_GRAPH_IRI;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::debug::DebugOutput;
use representation::query_context::Context;
use representation::result::{
    expand_graph_variable, ground_quad_pattern_to_quad_pattern, quad_patterns_to_solution_mappings,
    template_to_solution_mappings, QueryResult, QueryResultKind,
};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{GraphPattern, GraphTarget, QueryDataset};
use spargebra::term::{
    GraphName, GraphNamePattern, GroundQuadPattern, GroundTerm, GroundTermPattern,
    NamedNodePattern, QuadPattern, TermPattern,
};
use spargebra::{GraphUpdateOperation, Query, Update};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct InsertResult {
    pub new_triples: Vec<NewTriples>,
    pub debug: Option<DebugOutputs>,
}

pub struct UpdateResult {
    pub debug: Option<DebugOutputs>,
    /// The triples really inserted and deleted, per graph.
    pub changes: Changes,
}

#[derive(Debug, Clone)]
pub struct QuerySettings {
    /// Graphs whose triples are merged into the default graph of the query, e.g. the transient
    /// graph. Graphs that do not exist or are empty are skipped.
    pub include_graphs: Vec<NamedGraph>,
    pub max_rows: Option<usize>,
    pub strict_project: bool,
    /// Overrides the federation settings of the triplestore (SERVICE and LOAD), e.g. for
    /// queries to a served model.
    pub federation: Option<Arc<FederationSettings>>,
    /// Called before each graph pattern is evaluated, see [evaluation_hook].
    pub evaluation_hook: Option<evaluation_hook::EvaluationHookRef>,
}

impl Triplestore {
    #[instrument(skip_all)]
    pub fn query(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
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
        self.query_parsed(&query, parameters, query_settings, graph, debug_no_results)
    }

    pub fn query_parsed(
        &self,
        query: &Query,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
    ) -> Result<QueryResult, SparqlError> {
        // A hook addresses graph patterns by their position in the query as given, so a query
        // evaluated with a hook is not rewritten (its static parts are rewritten when the hook
        // evaluates them as queries of their own).
        let query = if query_settings.evaluation_hook.is_some() {
            query.clone()
        } else {
            rewrite(query.clone())
        };
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
                    height_estimate,
                } = self.lazy_graph_pattern(
                    pattern,
                    None,
                    &context,
                    parameters,
                    Pushdowns::new(),
                    query_settings,
                    &self.query_graph(dataset, graph, query_settings),
                )?;

                match pl_interruptable_collect_estimated(mappings, height_estimate) {
                    Ok(df) => {
                        if df.height() == 0 {
                            no_results = true;
                        }
                        QueryResultKind::Select(EagerSolutionMappings::new(df, types))
                    }
                    Err(InterruptableCollectError::Cancelled(c)) => {
                        return Err(SparqlError::Cancelled(c))
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
                graph_template,
                dataset,
                pattern,
                base_iri: _,
            } => {
                let SolutionMappings {
                    mappings,
                    rdf_node_types,
                    height_estimate,
                } = self.lazy_graph_pattern(
                    pattern,
                    None,
                    &context,
                    parameters,
                    Pushdowns::new(),
                    query_settings,
                    &self.query_graph(dataset, graph, query_settings),
                )?;
                match pl_interruptable_collect_estimated(mappings, height_estimate) {
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
                            .chain(graph_template.iter().cloned())
                            .collect();

                        let solutions = template_to_solution_mappings(
                            df,
                            rdf_node_types,
                            &quad_template,
                            self.global_cats.clone(),
                        )
                        .map_err(|x| SparqlError::QuadPatternsError(x.to_string()))?;
                        QueryResultKind::Construct(solutions)
                    }
                    Err(InterruptableCollectError::Cancelled(c)) => {
                        return Err(SparqlError::Cancelled(c))
                    }
                    Err(InterruptableCollectError::Interrupted) => {
                        return Err(SparqlError::InterruptSignal)
                    }
                    Err(e) => {
                        return Err(SparqlError::QueryExecutionError(e.to_string()));
                    }
                }
            }
            Query::Ask {
                dataset,
                pattern,
                base_iri: _,
            } => {
                let SolutionMappings { mappings, .. } = self.lazy_graph_pattern(
                    pattern,
                    None,
                    &context,
                    parameters,
                    Pushdowns::new(),
                    query_settings,
                    &self.query_graph(dataset, graph, query_settings),
                )?;
                match pl_interruptable_collect_estimated(mappings.limit(1), 1) {
                    Ok(df) => {
                        if df.height() == 0 {
                            no_results = true;
                        }
                        QueryResultKind::Ask(df.height() > 0)
                    }
                    Err(InterruptableCollectError::Cancelled(c)) => {
                        return Err(SparqlError::Cancelled(c))
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
            // The query itself ran, so a failing explanation should not fail it:
            // return the results without debug output instead.
            match self.debug(&query, parameters, query_settings, graph) {
                Ok(d) => Some(d),
                Err(SparqlError::InterruptSignal) => return Err(SparqlError::InterruptSignal),
                Err(_) => None,
            }
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
        query_settings: &QuerySettings,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
        debug_no_results: bool,
    ) -> Result<InsertResult, SparqlError> {
        if let Query::Construct { .. } = &query {
            let res = self.query_parsed(
                query,
                parameters,
                query_settings,
                Some(source_graph),
                debug_no_results,
            )?;
            let new_triples = match res.kind {
                QueryResultKind::Select(_) | QueryResultKind::Ask(_) => {
                    return Err(SparqlError::QueryExecutionError(
                        "Got SELECT or ASK query when CONSTRUCT was expected".to_string(),
                    ))
                }
                QueryResultKind::Construct(dfs) => {
                    self.insert_constructed_triples(dfs, target_graph)
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
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, SparqlError> {
        self.update_keep_triples(
            update,
            parameters,
            query_settings,
            graph,
            prefixes,
            debug_no_results,
            false,
        )
    }

    /// As `update`, and with `keep_triples` the changes also hold the triples that were
    /// inserted and deleted, not only their counts.
    #[allow(clippy::too_many_arguments)]
    pub fn update_keep_triples(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        prefixes: Option<&HashMap<String, NamedNode>>,
        debug_no_results: bool,
        keep_triples: bool,
    ) -> Result<UpdateResult, SparqlError> {
        let update = Update::parse(update, None, prefixes).map_err(SparqlError::ParseError)?;
        self.update_parsed_keep_triples(
            &update,
            parameters,
            query_settings,
            graph,
            debug_no_results,
            keep_triples,
        )
    }

    pub fn update_parsed(
        &mut self,
        update: &Update,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, SparqlError> {
        self.update_parsed_keep_triples(
            update,
            parameters,
            query_settings,
            graph,
            debug_no_results,
            false,
        )
    }

    /// As `update_parsed`, and with `keep_triples` the changes also hold the triples that were
    /// inserted and deleted, not only their counts.
    pub fn update_parsed_keep_triples(
        &mut self,
        update: &Update,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
        keep_triples: bool,
    ) -> Result<UpdateResult, SparqlError> {
        // The token of the update stops the evaluation of WHERE clauses, and is checked before
        // each operation, but writes run without it so that an operation is never left half
        // done. Operations that were done before a timeout stay done.
        let token = utils::cancel::current();
        utils::cancel::suspended(|| {
            self.update_parsed_with_token(
                update,
                parameters,
                query_settings,
                graph,
                debug_no_results,
                token,
                keep_triples,
            )
        })
    }

    fn update_parsed_with_token(
        &mut self,
        update: &Update,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        debug_no_results: bool,
        token: Option<utils::cancel::CancelToken>,
        keep_triples: bool,
    ) -> Result<UpdateResult, SparqlError> {
        let mut debug_output = if debug_no_results { Some(vec![]) } else { None };
        let mut changes = Changes::new(keep_triples);
        for u in &update.operations {
            if let Some(t) = &token {
                t.check()?;
            }
            match u {
                GraphUpdateOperation::InsertData { data } => {
                    // Blank nodes are scoped to this operation, as when parsing a document.
                    let parser_call = self.parser_call.to_string();
                    self.parser_call += 1;
                    let mut triples_by_graph: HashMap<NamedGraph, Vec<Triple>> = HashMap::new();
                    for q in data {
                        let g = graph_name_to_named_graph(&q.graph_name);
                        triples_by_graph.entry(g).or_default().push(Triple::new(
                            subject_to_oxrdf_subject(q.subject.clone(), &parser_call),
                            q.predicate.clone(),
                            term_to_oxrdf_term(q.object.clone(), &parser_call),
                        ));
                    }
                    for (g, triples) in triples_by_graph {
                        changes.touch(&g);
                        let new_triples = self
                            .add_triples(triples, &g)
                            .map_err(SparqlError::TriplestoreError)?;
                        changes.add_inserted(new_triples);
                    }
                    self.maybe_garbage_collect()?;
                }
                GraphUpdateOperation::DeleteData { data } => {
                    let mut triples_by_graph: HashMap<NamedGraph, Vec<Triple>> = HashMap::new();
                    for q in data {
                        triples_by_graph
                            .entry(graph_name_to_named_graph(&q.graph_name))
                            .or_default()
                            .push(Triple::new(
                                q.subject.clone(),
                                q.predicate.clone(),
                                Term::from(q.object.clone()),
                            ));
                    }
                    for (g, triples) in triples_by_graph {
                        changes.touch(&g);
                        if !self.does_graph_exist(&g) {
                            continue;
                        }
                        // Triples with values that cannot be parsed cannot be stored, so there is
                        // nothing to delete for them.
                        let (ttas, _dropped) = triples_to_triples_to_add(triples, &g);
                        let sms: Vec<_> = ttas
                            .into_iter()
                            .map(|tta| {
                                let rdf_node_types = HashMap::from([
                                    (
                                        SUBJECT_COL_NAME.to_string(),
                                        RDFNodeState::from_bases(
                                            tta.subject_type,
                                            tta.subject_cat_state,
                                        ),
                                    ),
                                    (
                                        OBJECT_COL_NAME.to_string(),
                                        RDFNodeState::from_bases(
                                            tta.object_type,
                                            tta.object_cat_state,
                                        ),
                                    ),
                                ]);
                                (
                                    EagerSolutionMappings::new(tta.df, rdf_node_types),
                                    tta.predicate,
                                )
                            })
                            .collect();
                        let deleted = self.delete_construct_result(sms, &g, keep_triples)?;
                        changes.add_deleted(&g, deleted);
                    }
                    self.maybe_garbage_collect()?;
                }
                GraphUpdateOperation::DeleteInsert {
                    delete,
                    insert,
                    using,
                    pattern,
                } => {
                    if !debug_no_results && insert.is_empty() && using.is_none() {
                        if let Some(deleted) = self.delete_whole_tables(
                            delete,
                            pattern,
                            query_settings,
                            graph,
                            keep_triples,
                        )? {
                            self.n_triples_deleted += deleted.count;
                            changes.add_deleted(&NamedGraph::DefaultGraph, deleted);
                            self.maybe_garbage_collect()?;
                            continue;
                        }
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
                        if let GraphNamePattern::Variable(v) = &d.graph_name {
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
                        if let GraphNamePattern::Variable(v) = &i.graph_name {
                            variables.insert(v.clone());
                        }
                    }
                    if variables.is_empty() {
                        // The number of solutions matters even without template variables,
                        // e.g. for blank nodes, so it must survive the projection.
                        pattern.on_in_scope_variable(|v| {
                            variables.insert(v.clone());
                        });
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
                    let QueryResult { kind, debug, .. } =
                        utils::cancel::with_token(token.clone(), || {
                            self.query_parsed(
                                &q,
                                parameters,
                                query_settings,
                                graph,
                                debug_no_results,
                            )
                        })?;
                    // Nothing is written by an operation whose WHERE clause took too long.
                    if let Some(t) = &token {
                        t.check()?;
                    }
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
                        for (df, qp) in expand_graph_variable(
                            &mappings,
                            &rdf_node_types,
                            &ground_quad_pattern_to_quad_pattern(d),
                            self.global_cats.clone(),
                        ) {
                            let cats = self.global_cats.read()?;
                            let del = quad_patterns_to_solution_mappings(
                                &df,
                                &rdf_node_types,
                                &qp,
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
                    }
                    for (g, delete_solutions) in delete_solutions {
                        let deleted =
                            self.delete_construct_result(delete_solutions, &g, keep_triples)?;
                        changes.add_deleted(&g, deleted);
                    }
                    let insert_solutions = template_to_solution_mappings(
                        mappings,
                        rdf_node_types,
                        insert,
                        self.global_cats.clone(),
                    )
                    .map_err(|x| SparqlError::QuadPatternsError(x.to_string()))?;
                    for (_, _, g) in &insert_solutions {
                        changes.touch(g);
                    }
                    let new_triples = self
                        .insert_constructed_triples(insert_solutions, &NamedGraph::DefaultGraph)?;
                    changes.add_inserted(new_triples);
                    self.maybe_garbage_collect()?;
                }
                GraphUpdateOperation::Load {
                    silent,
                    source,
                    destination,
                } => {
                    let target = match destination {
                        GraphName::NamedNode(nn) => NamedGraph::from_maybe_named_node(Some(nn)),
                        GraphName::DefaultGraph => {
                            graph.cloned().unwrap_or(NamedGraph::DefaultGraph)
                        }
                    };
                    // LOAD only inserts, and triples that are already present are not added again,
                    // so the growth of each graph is the number of triples it really inserted.
                    // Quads in the source go to their own graphs when the target is the default.
                    let sizes_before: HashMap<NamedGraph, usize> = self
                        .graph_triples_map
                        .keys()
                        .map(|g| (g.clone(), self.graph_size(g)))
                        .collect();
                    let mut new_triples = vec![];
                    self.load(
                        source.as_str(),
                        &target,
                        *silent,
                        query_settings,
                        keep_triples.then_some(&mut new_triples),
                    )?;
                    changes.touch(&target);
                    let graphs: Vec<NamedGraph> = self.graph_triples_map.keys().cloned().collect();
                    for g in graphs {
                        let before = sizes_before.get(&g).copied().unwrap_or(0);
                        let n = self.graph_size(&g).saturating_sub(before);
                        if n > 0 {
                            changes.add_inserted_count(&g, n);
                        }
                    }
                    changes.keep_inserted_triples(new_triples);
                }
                GraphUpdateOperation::Clear { graph: target, .. }
                | GraphUpdateOperation::Drop { graph: target, .. } => {
                    // Graphs are kept as empty graphs, so that they can still be referenced.
                    let default_graph = graph.cloned().unwrap_or(NamedGraph::DefaultGraph);
                    let to_clear: Vec<NamedGraph> = match target {
                        GraphTarget::NamedNode(nn) => {
                            vec![NamedGraph::from_maybe_named_node(Some(nn))]
                        }
                        GraphTarget::DefaultGraph => vec![default_graph],
                        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => {
                            let all = matches!(target, GraphTarget::AllGraphs);
                            let mut gs: HashSet<NamedGraph> = HashSet::new();
                            for g in self.graph_triples_map.keys() {
                                if all || g != &default_graph {
                                    gs.insert(g.clone());
                                }
                            }
                            gs.into_iter().collect()
                        }
                    };
                    for g in to_clear {
                        let deleted = self.graph_deleted_by_clear(&g, keep_triples)?;
                        self.clear_graph(&g)?;
                        changes.add_deleted(&g, deleted);
                    }
                }
                GraphUpdateOperation::Create { .. } => {}
            }
        }
        let debug = if let Some(debug_output) = debug_output {
            Some(DebugOutputs::new(debug_output))
        } else if debug_no_results {
            Some(DebugOutputs::new(vec![DebugOutput::HasResults]))
        } else {
            None
        };
        let res = UpdateResult { debug, changes };
        Ok(res)
    }
}

impl Triplestore {
    /// Fast path for deleting all triples of a graph, or all triples with a given predicate:
    /// `DELETE WHERE { ?s ?p ?o }` and `DELETE WHERE { ?s <p> ?o }` (or the same as
    /// `DELETE { .. } WHERE { .. }` without INSERT or USING). When the pattern is matched in the
    /// graph that the triples are deleted from, every triple of the matching tables is deleted,
    /// so the tables are dropped without evaluating the pattern. Returns what was deleted, or None
    /// if the fast path does not apply.
    fn delete_whole_tables(
        &mut self,
        delete: &[GroundQuadPattern],
        pattern: &GraphPattern,
        query_settings: &QuerySettings,
        graph: Option<&NamedGraph>,
        keep_triples: bool,
    ) -> Result<Option<Deleted>, SparqlError> {
        let none = Ok(None);
        let [d] = delete else {
            return none;
        };
        let GraphPattern::Bgp { patterns } = pattern else {
            return none;
        };
        let [tp] = patterns.as_slice() else {
            return none;
        };
        if !matches!(d.graph_name, GraphNamePattern::DefaultGraph) || d.predicate != tp.predicate {
            return none;
        }
        let (
            GroundTermPattern::Variable(s),
            GroundTermPattern::Variable(o),
            TermPattern::Variable(ts),
            TermPattern::Variable(to),
        ) = (&d.subject, &d.object, &tp.subject, &tp.object)
        else {
            return none;
        };
        if s != ts || o != to || s == o {
            return none;
        }
        if let NamedNodePattern::Variable(p) = &d.predicate {
            if p == s || p == o {
                return none;
            }
        }
        // The triples are deleted from the default graph of the update, so the pattern must be
        // matched in exactly that graph.
        let target = NamedGraph::DefaultGraph;
        match self.query_graph(&None, graph, query_settings) {
            QueryGraph::NamedGraph(g) if g == target => {}
            _ => return none,
        }
        if !self.graph_triples_map.contains_key(&target) {
            return Ok(Some(Deleted::default()));
        }
        let height = |m: &HashMap<_, crate::storage::Triples>| -> usize {
            m.values().map(|t| t.get_height()).sum()
        };
        // The removed tables are kept until their triples have been collected.
        let removed: Vec<_> = match &d.predicate {
            NamedNodePattern::Variable(_) => self
                .graph_triples_map
                .graph_mut(&target)
                .unwrap()
                .drain()
                .collect(),
            NamedNodePattern::NamedNode(p) => self
                .graph_triples_map
                .remove_predicate(&target, p)
                .map(|m| (p.clone(), m))
                .into_iter()
                .collect(),
        };
        let mut deleted = Deleted::default();
        for (p, m) in &removed {
            deleted.count += height(m);
            if keep_triples {
                deleted.triples.extend(
                    tables_as_new_triples(m, &target, p).map_err(SparqlError::TriplestoreError)?,
                );
            }
        }
        Ok(Some(deleted))
    }

    /// What clearing the graph deletes.
    fn graph_deleted_by_clear(
        &self,
        graph: &NamedGraph,
        keep_triples: bool,
    ) -> Result<Deleted, SparqlError> {
        let mut deleted = Deleted {
            count: self.graph_size(graph),
            triples: vec![],
        };
        if keep_triples {
            if let Some(tables) = self.graph_triples_map.get(graph) {
                for (p, m) in tables {
                    deleted.triples.extend(
                        tables_as_new_triples(m, graph, p)
                            .map_err(SparqlError::TriplestoreError)?,
                    );
                }
            }
        }
        Ok(deleted)
    }
}

fn graph_name_to_named_graph(graph_name: &GraphName) -> NamedGraph {
    match graph_name {
        GraphName::NamedNode(nn) => NamedGraph::from_maybe_named_node(Some(nn)),
        GraphName::DefaultGraph => NamedGraph::DefaultGraph,
    }
}

impl Triplestore {
    /// The graph or dataset a query runs against. The `include_graphs` of the query settings are
    /// merged into the default graph, as if they were given with FROM.
    pub fn query_graph(
        &self,
        dataset: &Option<QueryDataset>,
        graph: Option<&NamedGraph>,
        query_settings: &QuerySettings,
    ) -> QueryGraph {
        let include: Vec<NamedNode> = query_settings
            .include_graphs
            .iter()
            .filter(|g| self.graph_has_triples(g))
            .filter_map(named_graph_as_named_node)
            .collect();
        if let Some(dataset) = dataset {
            let mut dataset = dataset.clone();
            // A dataset without FROM graphs has an empty default graph, which stays empty.
            if !dataset.default.is_empty() {
                for nn in include {
                    if !dataset.default.contains(&nn) {
                        dataset.default.push(nn);
                    }
                }
            }
            return QueryGraph::QueryDataset(dataset);
        }
        let graph = graph.cloned().unwrap_or(NamedGraph::DefaultGraph);
        let include: Vec<NamedNode> = include
            .into_iter()
            .filter(|nn| NamedGraph::from_maybe_named_node(Some(nn)) != graph)
            .collect();
        if include.is_empty() {
            return QueryGraph::NamedGraph(graph);
        }
        if let Some(graph_nn) = named_graph_as_named_node(&graph) {
            let mut default = vec![graph_nn];
            default.extend(include);
            QueryGraph::QueryDataset(QueryDataset {
                default,
                named: None,
            })
        } else {
            // Blank node graphs cannot be given as FROM graphs.
            QueryGraph::NamedGraph(graph)
        }
    }

    fn graph_has_triples(&self, graph: &NamedGraph) -> bool {
        if let Some(m) = self.graph_triples_map.get(graph) {
            m.values().any(|m| !m.is_empty())
        } else {
            false
        }
    }
}

fn named_graph_as_named_node(graph: &NamedGraph) -> Option<NamedNode> {
    match graph {
        NamedGraph::DefaultGraph => Some(NamedNode::new_unchecked(DEFAULT_GRAPH_IRI)),
        NamedGraph::NamedGraph(nn) => Some(nn.clone()),
        NamedGraph::BlankNode(_) => None,
    }
}
