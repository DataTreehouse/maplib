//! SPARQL 1.1 Federated Query: `SERVICE <iri> { ... }`, `SERVICE SILENT` and `SERVICE ?var`.
//!
//! The pattern is sent to the endpoint as a SELECT query of its in-scope variables. When
//! solutions are bound before the SERVICE (it is joined after other patterns, see `join.rs`),
//! their distinct values of the shared variables are sent with the pattern, as VALUES blocks of
//! `batch_size` rows (a bind join, as Jena, RDF4J FedX and Comunica do), so that the endpoint
//! only returns compatible solutions. Variables that are unbound in some incoming row, or bound
//! to blank nodes, are not sent, and are joined locally. maplib endpoints get the bindings as
//! Arrow (a PVALUES parameter) in batches of `arrow_batch_size` rows instead. The results of the
//! batches are joined with the incoming solutions here, with the SPARQL join semantics.
//!
//! SILENT: when the endpoint can not be reached or fails, the SERVICE gives a single empty
//! solution (the incoming solutions are kept as they are). Errors from maplib's own settings,
//! a refused endpoint or too many rows, are not silenced.

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use external_sparql::federation::{
    is_known_maplib_endpoint, project_variables, run_service_calls, FederationSettings,
    ServiceCall, SERVICE_BINDINGS_PARAMETER,
};
use polars::prelude::{BooleanChunked, IntoLazy, JoinType, NewChunkedArray};
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::{distinct, join, union};
use representation::dataset::QueryGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::GraphPattern;
use spargebra::term::{GroundTerm, NamedNodePattern, Term, Variable};
use spargebra::Query;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};
use utils::polars::{pl_interruptable_collect, InterruptableCollectError};

/// The in-scope variables of the pattern, without duplicates, in order of appearance.
pub(crate) fn in_scope_variables(gp: &GraphPattern) -> Vec<Variable> {
    let mut vars: Vec<Variable> = vec![];
    gp.on_in_scope_variable(|v| {
        if !vars.contains(v) {
            vars.push(v.clone())
        }
    });
    vars
}

/// Does the pattern contain a SERVICE (outside of expressions)?
pub(crate) fn contains_service(gp: &GraphPattern) -> bool {
    match gp {
        GraphPattern::Service { .. } => true,
        GraphPattern::Bgp { .. }
        | GraphPattern::Path { .. }
        | GraphPattern::Values { .. }
        | GraphPattern::PValues { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Lateral { left, right } => contains_service(left) || contains_service(right),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => contains_service(inner),
    }
}

/// A single solution without variables, the identity of joins.
fn empty_solution() -> SolutionMappings {
    SolutionMappings::new(DataFrame::empty_with_height(1).lazy(), HashMap::new(), 1)
}

fn collect(sm: SolutionMappings) -> Result<EagerSolutionMappings, SparqlError> {
    let SolutionMappings {
        mappings,
        rdf_node_types,
        ..
    } = sm;
    match pl_interruptable_collect(mappings) {
        Ok(df) => Ok(EagerSolutionMappings::new(df, rdf_node_types)),
        Err(InterruptableCollectError::Interrupted) => Err(SparqlError::InterruptSignal),
        Err(e) => Err(SparqlError::QueryExecutionError(e.to_string())),
    }
}

/// The SELECT query of the pattern, with the bindings (VALUES or PVALUES) joined in first.
fn select_query(
    inner: &GraphPattern,
    variables: &[Variable],
    bindings: Option<GraphPattern>,
) -> String {
    let pattern = match bindings {
        Some(b) => GraphPattern::Join {
            left: Box::new(b),
            right: Box::new(inner.clone()),
        },
        None => inner.clone(),
    };
    let pattern = if variables.is_empty() {
        pattern
    } else {
        GraphPattern::Project {
            inner: Box::new(pattern),
            variables: variables.to_vec(),
        }
    };
    Query::Select {
        dataset: None,
        pattern,
        base_iri: None,
    }
    .to_string()
}

impl Triplestore {
    /// The federation settings of the query, or else of the triplestore.
    pub(crate) fn federation_settings(
        &self,
        query_settings: &QuerySettings,
    ) -> Arc<FederationSettings> {
        query_settings
            .federation
            .clone()
            .unwrap_or_else(|| self.federation.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_service(
        &self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        silent: bool,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let settings = self.federation_settings(query_settings);
        let variables = in_scope_variables(inner);
        let incoming = match solution_mappings {
            Some(sm) => Some(collect(sm)?),
            None => None,
        };
        match name {
            NamedNodePattern::NamedNode(nn) => {
                settings.check_allowed(nn.as_str(), false)?;
                self.service_on_endpoint(
                    nn.as_str(),
                    inner,
                    &variables,
                    incoming,
                    silent,
                    &settings,
                    query_settings,
                    context,
                    dataset,
                )
            }
            NamedNodePattern::Variable(v) => {
                let unbound = || {
                    SparqlError::ServiceEvaluationError(format!(
                        "SERVICE {v} needs {v} to be bound to the IRI of an endpoint by the patterns before it"
                    ))
                };
                let Some(incoming) = incoming else {
                    return if silent {
                        Ok(empty_solution())
                    } else {
                        Err(unbound())
                    };
                };
                let Some(state) = incoming.rdf_node_types.get(v.as_str()) else {
                    return if silent {
                        Ok(incoming.as_lazy())
                    } else {
                        Err(unbound())
                    };
                };
                let terms = column_as_terms(
                    incoming.mappings.column(v.as_str()).unwrap(),
                    state,
                    self.global_cats.clone(),
                    false,
                );
                let mut endpoints: Vec<String> = vec![];
                let mut index: HashMap<String, usize> = HashMap::new();
                let mut row_endpoint = Vec::with_capacity(terms.len());
                for t in terms {
                    match t {
                        Some(Term::NamedNode(nn)) => {
                            let i = *index.entry(nn.as_str().to_string()).or_insert_with(|| {
                                endpoints.push(nn.as_str().to_string());
                                endpoints.len() - 1
                            });
                            row_endpoint.push(Some(i));
                        }
                        _ => row_endpoint.push(None),
                    }
                }
                let n_not_iri = row_endpoint.iter().filter(|x| x.is_none()).count();
                if n_not_iri > 0 && !silent {
                    return Err(unbound());
                }
                // All endpoints are checked before any of them is queried.
                for e in &endpoints {
                    settings.check_allowed(e, false)?;
                }
                let mask = |f: &dyn Fn(&Option<usize>) -> bool| {
                    BooleanChunked::from_iter_values(
                        "mask".into(),
                        row_endpoint.iter().map(|x| f(x)),
                    )
                };
                let mut parts = vec![];
                for (i, e) in endpoints.iter().enumerate() {
                    let m = mask(&|x| *x == Some(i));
                    let sub = EagerSolutionMappings::new(
                        incoming
                            .mappings
                            .filter(&m)
                            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?,
                        incoming.rdf_node_types.clone(),
                    );
                    parts.push(self.service_on_endpoint(
                        e,
                        inner,
                        &variables,
                        Some(sub),
                        silent,
                        &settings,
                        query_settings,
                        context,
                        dataset,
                    )?);
                }
                if n_not_iri > 0 {
                    // SILENT: the rows without an endpoint are kept as they are.
                    let m = mask(&|x| x.is_none());
                    let sub = EagerSolutionMappings::new(
                        incoming
                            .mappings
                            .filter(&m)
                            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?,
                        incoming.rdf_node_types.clone(),
                    );
                    parts.push(sub.as_lazy());
                }
                if parts.is_empty() {
                    // No incoming rows.
                    return Ok(incoming.as_lazy());
                }
                Ok(union(parts, true, self.global_cats.clone())?)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn service_on_endpoint(
        &self,
        endpoint: &str,
        inner: &GraphPattern,
        variables: &[Variable],
        incoming: Option<EagerSolutionMappings>,
        silent: bool,
        settings: &Arc<FederationSettings>,
        query_settings: &QuerySettings,
        context: &Context,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        match self.service_results(endpoint, inner, variables, incoming.as_ref(), settings) {
            Ok(results) => {
                if let Some(incoming) = incoming {
                    self.compatible_join(incoming, results, query_settings, context, dataset)
                } else {
                    Ok(results)
                }
            }
            Err(SparqlError::ServiceError(e)) if silent && !e.is_policy_error() => {
                warn!("SERVICE SILENT <{endpoint}> failed, its results are left out: {e}");
                Ok(incoming.map(|x| x.as_lazy()).unwrap_or_else(empty_solution))
            }
            Err(e) => Err(e),
        }
    }

    /// The SPARQL join of the incoming solutions with the results of the endpoint. Shared
    /// variables that are unbound in some rows of either side are compatible with any value
    /// there, so they are joined by renaming them on the results side and merging them after an
    /// equi-join on the shared variables that are bound everywhere.
    fn compatible_join(
        &self,
        incoming: EagerSolutionMappings,
        results: SolutionMappings,
        query_settings: &QuerySettings,
        context: &Context,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut results = collect(results)?;
        let shared: Vec<String> = incoming
            .rdf_node_types
            .keys()
            .filter(|k| results.rdf_node_types.contains_key(*k))
            .cloned()
            .collect();
        let mut renamed = vec![];
        for v in shared {
            if self.fully_bound(&incoming, &v) && self.fully_bound(&results, &v) {
                continue;
            }
            let outer = format!("__maplib_service_{v}");
            results
                .mappings
                .rename(&v, outer.as_str().into())
                .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
            let state = results.rdf_node_types.remove(&v).unwrap();
            results.rdf_node_types.insert(outer.clone(), state);
            renamed.push((Variable::new_unchecked(v), Variable::new_unchecked(outer)));
        }
        let mut sm = join(
            incoming.as_lazy(),
            results.as_lazy(),
            JoinType::Inner,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?;
        for (i, (v, outer)) in renamed.iter().enumerate() {
            sm = self.merge_outer_binding(sm, v, outer, i, context, query_settings, dataset)?;
        }
        Ok(sm)
    }

    /// Whether the variable is bound to the same kind of term in every row.
    fn fully_bound(&self, sm: &EagerSolutionMappings, v: &str) -> bool {
        let Some(state) = sm.rdf_node_types.get(v) else {
            return false;
        };
        if state.is_multi() || state.map.keys().any(|t| matches!(t, BaseRDFNodeType::None)) {
            return false;
        }
        sm.mappings
            .column(v)
            .map(|c| c.null_count() == 0)
            .unwrap_or(false)
    }

    /// The solutions of the pattern at the endpoint, only those compatible with the incoming
    /// solutions when there are any (a bind join), else all of them.
    fn service_results(
        &self,
        endpoint: &str,
        inner: &GraphPattern,
        variables: &[Variable],
        incoming: Option<&EagerSolutionMappings>,
        settings: &Arc<FederationSettings>,
    ) -> Result<SolutionMappings, SparqlError> {
        let empty = || {
            project_variables(
                EagerSolutionMappings::new(DataFrame::empty(), HashMap::new()),
                variables,
            )
            .as_lazy()
        };
        if let Some(incoming) = incoming {
            if incoming.mappings.height() == 0 {
                return Ok(empty());
            }
        }
        let keys = match incoming {
            Some(i) if settings.bind_join => self.bind_keys(i, variables)?,
            _ => None,
        };
        let cats = self.global_cats.clone();
        let mut results = vec![];
        match keys {
            None => {
                debug!("SERVICE <{endpoint}>: sending the pattern without bindings");
                let query = select_query(inner, variables, None);
                results.extend(run_service_calls(
                    settings,
                    endpoint,
                    vec![ServiceCall::Sparql {
                        query,
                        variables: variables.to_vec(),
                    }],
                    cats,
                )?);
            }
            Some(keys) => {
                let n = keys.mappings.height();
                let key_vars: Vec<Variable> = keys
                    .mappings
                    .get_column_names()
                    .iter()
                    .map(|c| Variable::new_unchecked(c.as_str()))
                    .collect();
                let batch_size = settings.batch_size.max(1);
                let values_calls = |from: usize, to: usize| -> Vec<ServiceCall> {
                    (from..to)
                        .step_by(batch_size)
                        .map(|start| {
                            let len = batch_size.min(to - start);
                            let values = self.values_pattern(&keys, &key_vars, start, len);
                            ServiceCall::Sparql {
                                query: select_query(inner, variables, Some(values)),
                                variables: variables.to_vec(),
                            }
                        })
                        .collect()
                };
                let mut offset = 0;
                if !(settings.arrow && is_known_maplib_endpoint(endpoint)) {
                    // The first batch also tells whether the endpoint is a maplib server.
                    let first = batch_size.min(n);
                    debug!(
                        "SERVICE <{endpoint}>: bind join of {n} bindings of {key_vars:?}, in batches of {batch_size}"
                    );
                    results.extend(run_service_calls(
                        settings,
                        endpoint,
                        values_calls(0, first),
                        cats.clone(),
                    )?);
                    offset = first;
                    if offset < n && !(settings.arrow && is_known_maplib_endpoint(endpoint)) {
                        results.extend(run_service_calls(
                            settings,
                            endpoint,
                            values_calls(offset, n),
                            cats.clone(),
                        )?);
                        offset = n;
                    }
                }
                if offset < n {
                    let arrow_batch_size = settings.arrow_batch_size.max(1);
                    debug!(
                        "SERVICE <{endpoint}>: bind join of {} bindings of {key_vars:?} as Arrow, in batches of {arrow_batch_size}", n - offset
                    );
                    let pvalues = GraphPattern::PValues {
                        variables: key_vars.clone(),
                        bindings_parameter: SERVICE_BINDINGS_PARAMETER.to_string(),
                    };
                    let query = select_query(inner, variables, Some(pvalues));
                    let calls = (offset..n)
                        .step_by(arrow_batch_size)
                        .map(|start| ServiceCall::Maplib {
                            query: query.clone(),
                            variables: variables.to_vec(),
                            bindings: EagerSolutionMappings::new(
                                keys.mappings
                                    .slice(start as i64, arrow_batch_size.min(n - start)),
                                keys.rdf_node_types.clone(),
                            ),
                        })
                        .collect();
                    results.extend(run_service_calls(settings, endpoint, calls, cats.clone())?);
                }
            }
        }
        // Results parsed from SPARQL results have IRIs and strings as strings: they are encoded
        // like the local data, so that they can be joined with it.
        let results: Vec<SolutionMappings> = {
            let cats = self.global_cats.read()?;
            results
                .into_iter()
                .map(|x| cats.encode_solution_mappings(x).as_lazy())
                .collect()
        };
        if results.is_empty() {
            Ok(empty())
        } else {
            Ok(union(results, true, self.global_cats.clone())?)
        }
    }

    /// The distinct values of the variables that can be sent to an endpoint as bindings: those
    /// of the incoming solutions that are bound in every row to IRIs or literals. None if there
    /// are none.
    pub(crate) fn bind_keys(
        &self,
        incoming: &EagerSolutionMappings,
        variables: &[Variable],
    ) -> Result<Option<EagerSolutionMappings>, SparqlError> {
        let mut candidates: Vec<String> = vec![];
        for v in variables {
            let name = v.as_str();
            let Some(state) = incoming.rdf_node_types.get(name) else {
                continue;
            };
            if state
                .map
                .keys()
                .any(|t| matches!(t, BaseRDFNodeType::BlankNode | BaseRDFNodeType::None))
            {
                continue;
            }
            let c = incoming.mappings.column(name).unwrap();
            if !state.is_multi() && c.null_count() > 0 {
                continue;
            }
            candidates.push(name.to_string());
        }
        loop {
            if candidates.is_empty() {
                return Ok(None);
            }
            let df = incoming
                .mappings
                .select(candidates.iter().map(|x| x.as_str()))
                .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
            let types: HashMap<_, _> = candidates
                .iter()
                .map(|c| (c.clone(), incoming.rdf_node_types.get(c).unwrap().clone()))
                .collect();
            let keys = collect(distinct(EagerSolutionMappings::new(df, types).as_lazy())?)?;
            // Multi type columns are unbound where all their fields are null.
            let mut unbound = None;
            for c in &candidates {
                let state = keys.rdf_node_types.get(c).unwrap();
                if state.is_multi() {
                    let terms = column_as_terms(
                        keys.mappings.column(c).unwrap(),
                        state,
                        self.global_cats.clone(),
                        false,
                    );
                    if terms.iter().any(|t| t.is_none()) {
                        unbound = Some(c.clone());
                        break;
                    }
                }
            }
            if let Some(c) = unbound {
                candidates.retain(|x| x != &c);
            } else {
                return Ok(Some(keys));
            }
        }
    }

    /// A VALUES block with rows `start..start+len` of the keys.
    fn values_pattern(
        &self,
        keys: &EagerSolutionMappings,
        key_vars: &[Variable],
        start: usize,
        len: usize,
    ) -> GraphPattern {
        let slice = keys.mappings.slice(start as i64, len);
        let columns: Vec<Vec<Option<Term>>> = key_vars
            .iter()
            .map(|v| {
                column_as_terms(
                    slice.column(v.as_str()).unwrap(),
                    keys.rdf_node_types.get(v.as_str()).unwrap(),
                    self.global_cats.clone(),
                    false,
                )
            })
            .collect();
        let mut bindings = Vec::with_capacity(len);
        for i in 0..slice.height() {
            let row = columns
                .iter()
                .map(|c| match &c[i] {
                    Some(Term::NamedNode(nn)) => Some(GroundTerm::NamedNode(nn.clone())),
                    Some(Term::Literal(l)) => Some(GroundTerm::Literal(l.clone())),
                    _ => None,
                })
                .collect();
            bindings.push(row);
        }
        GraphPattern::Values {
            variables: key_vars.to_vec(),
            bindings,
        }
    }
}
