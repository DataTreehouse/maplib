use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use oxrdf::NamedNode;
use polars::prelude::{col, Expr, IntoLazy, JoinType};
use representation::null::expr_is_null_workaround;
use polars_core::frame::DataFrame;
use query_processing::expressions::named_node_local_enc;
use query_processing::graph_patterns::{join, union};
use query_processing::pushdowns::Pushdowns;
use representation::cats::LockedCats;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;
use std::collections::{HashMap, HashSet};

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(crate) fn lazy_graph(
        &self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GraphInner);
        match name {
            NamedNodePattern::NamedNode(nn) => {
                let allowed = match dataset {
                    QueryGraph::NamedGraph(_) => true,
                    QueryGraph::QueryDataset(qd) => qd
                        .named
                        .as_ref()
                        .map(|named| named.contains(nn))
                        // Only WITH was given, so all named graphs are available.
                        .unwrap_or(true),
                };
                let mut sm = self.lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    &inner_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    &QueryGraph::NamedGraph(NamedGraph::from_maybe_named_node(Some(nn))),
                )?;
                if !allowed {
                    sm.mappings = sm.mappings.limit(0);
                    sm.height_estimate = 0;
                }
                Ok(sm)
            }
            NamedNodePattern::Variable(v) => {
                let graphs = self.named_graphs_in_dataset(dataset, query_settings.include_transient);
                if graphs.is_empty() {
                    let mut sm = self.lazy_graph_pattern(
                        inner,
                        solution_mappings,
                        &inner_context,
                        parameters,
                        pushdowns,
                        query_settings,
                        &QueryGraph::NamedGraph(NamedGraph::DefaultGraph),
                    )?;
                    sm.mappings = sm.mappings.limit(0);
                    sm.height_estimate = 0;
                    return Ok(sm);
                }
                let mut sms = Vec::with_capacity(graphs.len());
                for g in graphs {
                    let sm = self.lazy_graph_pattern(
                        inner,
                        solution_mappings.clone(),
                        &inner_context,
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        &QueryGraph::NamedGraph(NamedGraph::NamedGraph(g.clone())),
                    )?;
                    let (enc, state) = self.graph_name_enc(&g)?;
                    if let Some(var_state) = sm.rdf_node_types.get(v.as_str()) {
                        // Solutions where the variable is bound must agree with the graph,
                        // solutions where it is unbound get it bound to the graph.
                        let is_null = expr_is_null_workaround(col(v.as_str()), var_state);
                        let mut bound = sm.clone();
                        bound.mappings = bound.mappings.filter(is_null.clone().not());
                        let graph_sm = SolutionMappings::new(
                            DataFrame::empty_with_height(1)
                                .lazy()
                                .with_column(enc.clone().alias(v.as_str())),
                            HashMap::from([(v.as_str().to_string(), state.clone())]),
                            1,
                        );
                        sms.push(join(
                            bound,
                            graph_sm,
                            JoinType::Inner,
                            self.global_cats.clone(),
                            query_settings.max_rows,
                        )?);
                        let SolutionMappings {
                            mappings,
                            mut rdf_node_types,
                            height_estimate,
                        } = sm;
                        rdf_node_types.insert(v.as_str().to_string(), state);
                        sms.push(SolutionMappings::new(
                            mappings
                                .filter(is_null)
                                .with_column(enc.alias(v.as_str())),
                            rdf_node_types,
                            height_estimate,
                        ));
                    } else {
                        let SolutionMappings {
                            mappings,
                            mut rdf_node_types,
                            height_estimate,
                        } = sm;
                        rdf_node_types.insert(v.as_str().to_string(), state);
                        sms.push(SolutionMappings::new(
                            mappings.with_column(enc.alias(v.as_str())),
                            rdf_node_types,
                            height_estimate,
                        ));
                    }
                }
                Ok(union(sms, false, self.global_cats.clone())?)
            }
        }
    }

    /// The named graphs of the dataset that GRAPH ?var ranges over, sorted for determinism.
    fn named_graphs_in_dataset(
        &self,
        dataset: &QueryGraph,
        include_transient: bool,
    ) -> Vec<NamedNode> {
        if let QueryGraph::QueryDataset(qd) = dataset {
            // FROM and USING always give the named graphs (possibly none),
            // whereas only WITH leaves all named graphs available.
            if let Some(named) = &qd.named {
                return named.clone();
            }
        }
        let mut graphs = HashSet::new();
        let mut maps = vec![&self.graph_triples_map];
        if include_transient {
            maps.push(&self.graph_transient_triples_map);
        }
        for map in maps {
            for (g, predicates) in map {
                if let NamedGraph::NamedGraph(nn) = g {
                    if predicates.values().any(|m| !m.is_empty()) {
                        graphs.insert(nn.clone());
                    }
                }
            }
        }
        let mut graphs: Vec<_> = graphs.into_iter().collect();
        graphs.sort();
        graphs
    }

    fn graph_name_enc(&self, nn: &NamedNode) -> Result<(Expr, RDFNodeState), SparqlError> {
        let cats = self.global_cats.read()?;
        let (enc, local) = named_node_local_enc(nn, &cats);
        let state = RDFNodeState::from_bases(
            BaseRDFNodeType::IRI,
            BaseCatState::CategoricalNative(local.map(LockedCats::new)),
        );
        Ok((enc, state))
    }
}
