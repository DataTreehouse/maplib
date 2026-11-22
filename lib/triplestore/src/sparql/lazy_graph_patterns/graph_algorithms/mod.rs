//! Graph algorithms as magic predicates: connected components (`maplib:component`), bridges
//! (`maplib:bridge`) and articulation points (`maplib:articulationPoint`).
//!
//! ```sparql
//! PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
//! SELECT ?node ?island ?size WHERE {
//!   ?node maplib:component ?c .
//!   ?c maplib:predicate cim:Terminal.ConnectivityNode, cim:Terminal.ConductingEquipment ;
//!      maplib:avoid ?open ;
//!      maplib:componentId ?island ;
//!      maplib:componentSize ?size .
//!   ?open cim:Switch.open true .
//! }
//! ```
//!
//! The graph is given by edge predicates (as for `maplib:shortestPath`: `maplib:predicate`,
//! `maplib:inversePredicate`, `maplib:anyPredicate`), or by the rows of other patterns of the
//! group: `maplib:edgeSource ?a ; maplib:edgeTarget ?b` make an edge of every solution of the
//! patterns that bind ?a and ?b, so edges can be derived (for instance between the substations
//! at the two ends of a line). Edges are undirected, but for `maplib:strong true` (strongly
//! connected components). Avoided nodes (`maplib:avoid`) are left out of the graph with their
//! edges, which is how a what-if (an opened switch, a line out of service) is asked without
//! changing the model.
//!
//! Components are numbered from 1 by decreasing size. A subject bound by the other patterns
//! restricts the output to those nodes; nodes without edges are components of their own.

pub mod algo;

use utils::polars::CollectAuto;
use super::magic_paths::{
    boolean_option, has_maplib_predicate, iri_option, literal_option, maplib_local_name,
    ARTICULATION_POINT, BRIDGE, COMPONENT,
};
use super::path::{LOOKUP_COLUMN, VALUE_COLUMN};
use super::path_search::algo::{Direction, SearchGraph};
use super::path_search::{
    add_endpoint, collect_once, connected_groups, constant_mappings, key_col, output_var, pattern_vars,
    u32_state, var_name, BuiltGraph, Endpoint,
};
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use crate::Triplestore;
use algo::{connected_components, cuts, strongly_connected_components, Components, NONE};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{by_name, col, DataType, IdxCa, IntoLazy, JoinArgs, JoinType, UniqueKeepStrategy};
use polars_core::frame::DataFrame;
use polars_core::prelude::Column;
use query_processing::graph_patterns::{distinct, join, union};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::multitype::compress_actual_multitypes;
use representation::null::expr_is_null_workaround;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

use super::magic_paths::path_error as err;

const VALUE_VAR: &str = "graph_algorithm_value";
const EDGE_ROW: &str = "graph_algorithm_edge";
const EDGE_END: &str = "graph_algorithm_end";

/// Whether a basic graph pattern uses one of the graph algorithm predicates.
pub fn has_graph_algorithm(patterns: &[TriplePattern]) -> bool {
    patterns.iter().any(|tp| {
        has_maplib_predicate(tp, COMPONENT)
            || has_maplib_predicate(tp, BRIDGE)
            || has_maplib_predicate(tp, ARTICULATION_POINT)
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Algorithm {
    Component,
    Bridge,
    ArticulationPoint,
}

impl Algorithm {
    fn name(&self) -> &'static str {
        match self {
            Algorithm::Component => COMPONENT,
            Algorithm::Bridge => BRIDGE,
            Algorithm::ArticulationPoint => ARTICULATION_POINT,
        }
    }
}

#[derive(Debug, Clone)]
struct GraphAlgorithmSpec {
    algorithm: Algorithm,
    /// The subject: the nodes (component, articulation point) or the first end of a bridge.
    node: Endpoint,
    /// The other end of a bridge.
    object_var: Option<String>,
    avoid: Option<Endpoint>,
    /// The nodes that count in the sizes and cut offs (all when not given).
    count: Option<Endpoint>,
    forward: Vec<NamedNode>,
    inverse: Vec<NamedNode>,
    any_predicate: Option<(bool, bool)>,
    exclude: Vec<NamedNode>,
    edge_source: Option<String>,
    edge_target: Option<String>,
    strong: bool,
    id_var: Option<String>,
    size_var: Option<String>,
    cut_off_var: Option<String>,
    predicate_var: Option<String>,
}

fn parse_spec(
    patterns: &[TriplePattern],
) -> Result<(GraphAlgorithmSpec, Vec<TriplePattern>), SparqlError> {
    let magic: Vec<(&TriplePattern, Algorithm)> = patterns
        .iter()
        .filter_map(|tp| {
            [
                Algorithm::Component,
                Algorithm::Bridge,
                Algorithm::ArticulationPoint,
            ]
            .into_iter()
            .find(|a| has_maplib_predicate(tp, a.name()))
            .map(|a| (tp, a))
        })
        .collect();
    if magic.len() != 1 {
        return Err(err(
            "use one of maplib:component, maplib:bridge or maplib:articulationPoint per group, put further ones in their own { }",
        ));
    }
    let (magic, algorithm) = magic[0];
    let name = algorithm.name();
    let options = &magic.object;
    if var_name(options).is_none() {
        return Err(err(format!(
            "the object of maplib:{name} must be a variable or a blank node holding its options"
        )));
    }
    let node = match var_name(&magic.subject) {
        Some(v) => Endpoint::Variable(v),
        None if algorithm == Algorithm::Bridge => {
            return Err(err("the subject of maplib:bridge must be a variable"))
        }
        None => Endpoint::Constants(vec![magic.subject.clone()]),
    };
    let mut spec = GraphAlgorithmSpec {
        algorithm,
        node,
        object_var: None,
        avoid: None,
        count: None,
        forward: vec![],
        inverse: vec![],
        any_predicate: None,
        exclude: vec![],
        edge_source: None,
        edge_target: None,
        strong: false,
        id_var: None,
        size_var: None,
        cut_off_var: None,
        predicate_var: None,
    };
    let mut rest = vec![];
    for p in patterns {
        if std::ptr::eq(p, magic) {
            continue;
        }
        let NamedNodePattern::NamedNode(pred) = &p.predicate else {
            rest.push(p.clone());
            continue;
        };
        let Some(option) = maplib_local_name(pred).filter(|_| &p.subject == options) else {
            rest.push(p.clone());
            continue;
        };
        let o = &p.object;
        match (option, algorithm) {
            ("object", Algorithm::Bridge) => spec.object_var = Some(output_var(option, o)?),
            ("avoid", _) => add_endpoint(option, &mut spec.avoid, o)?,
            ("countNodes", _) => add_endpoint(option, &mut spec.count, o)?,
            ("predicate", _) => spec.forward.push(iri_option(option, o)?.clone()),
            ("inversePredicate", _) => spec.inverse.push(iri_option(option, o)?.clone()),
            ("excludePredicate", _) => spec.exclude.push(iri_option(option, o)?.clone()),
            ("anyPredicate", _) => {
                let l = literal_option(option, o)?;
                spec.any_predicate = Some(match l.value().to_lowercase().as_str() {
                    "forward" => (true, false),
                    "backward" => (false, true),
                    "both" => (true, true),
                    v => {
                        return Err(err(format!(
                            "maplib:anyPredicate must be \"forward\", \"backward\" or \"both\", got \"{v}\""
                        )))
                    }
                });
            }
            ("edgeSource", _) => spec.edge_source = Some(output_var(option, o)?),
            ("edgeTarget", _) => spec.edge_target = Some(output_var(option, o)?),
            ("strong", Algorithm::Component) => spec.strong = boolean_option(option, o)?,
            ("componentId", _) => spec.id_var = Some(output_var(option, o)?),
            ("componentSize", _) => spec.size_var = Some(output_var(option, o)?),
            ("cutOff", Algorithm::Bridge | Algorithm::ArticulationPoint) => {
                spec.cut_off_var = Some(output_var(option, o)?)
            }
            ("edgePredicate", Algorithm::Bridge) => {
                spec.predicate_var = Some(output_var(option, o)?)
            }
            _ => {
                return Err(err(format!(
                    "unknown option maplib:{option} of maplib:{name}"
                )))
            }
        }
    }
    let pattern_edges = spec.edge_source.is_some() || spec.edge_target.is_some();
    let predicate_edges =
        !spec.forward.is_empty() || !spec.inverse.is_empty() || spec.any_predicate.is_some();
    if pattern_edges && (spec.edge_source.is_none() || spec.edge_target.is_none()) {
        return Err(err(
            "give both maplib:edgeSource and maplib:edgeTarget for edges from patterns",
        ));
    }
    if pattern_edges == predicate_edges {
        return Err(err(format!(
            "maplib:{name} needs its edges: maplib:predicate, maplib:inversePredicate or maplib:anyPredicate, or maplib:edgeSource and maplib:edgeTarget (not both kinds)"
        )));
    }
    if algorithm == Algorithm::Bridge && spec.object_var.is_none() {
        return Err(err(
            "maplib:bridge needs maplib:object, a variable for the other end of the bridge",
        ));
    }
    if pattern_edges && spec.predicate_var.is_some() {
        return Err(err(
            "maplib:edgePredicate needs edge predicates, not maplib:edgeSource/edgeTarget",
        ));
    }
    spec.forward.sort();
    spec.forward.dedup();
    spec.inverse.sort();
    spec.inverse.dedup();
    Ok((spec, rest))
}

/// The graph the algorithm runs on: nodes 0..n (the rows of `built.lookup`) and the edges.
struct EdgeList {
    built: Arc<BuiltGraph>,
    n: usize,
    from: Vec<u32>,
    to: Vec<u32>,
    /// The index (in `built.predicates`) of the predicate of each edge, for predicate edges.
    predicate: Option<Vec<u32>>,
    /// For edges from patterns: their solutions, with a row number (EDGE_ROW), and the row of
    /// each edge.
    rows: Option<(SolutionMappings, Vec<u32>)>,
}

impl Triplestore {
    /// Evaluates a basic graph pattern with a graph algorithm predicate.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_graph_algorithm(
        &self,
        patterns: &[TriplePattern],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: &Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let (spec, rest) = parse_spec(patterns)?;
        let name = spec.algorithm.name();

        // Group the other patterns (and the incoming solutions) by shared variables.
        let mut items: Vec<HashSet<String>> = rest.iter().map(pattern_vars).collect();
        let incoming_index = solution_mappings.as_ref().map(|sm| {
            items.push(sm.rdf_node_types.keys().cloned().collect());
            items.len() - 1
        });
        let groups = connected_groups(&items);
        let mut components: Vec<(HashSet<String>, SolutionMappings)> = vec![];
        let mut incoming = solution_mappings;
        for group in groups {
            let mut vars = HashSet::new();
            let mut tps = vec![];
            let mut with_incoming = false;
            for i in group {
                vars.extend(items[i].iter().cloned());
                if Some(i) == incoming_index {
                    with_incoming = true;
                } else {
                    tps.push(rest[i].clone());
                }
            }
            let initial = if with_incoming { incoming.take() } else { None };
            let sm = if tps.is_empty() {
                initial.unwrap()
            } else {
                self.lazy_graph_pattern(
                    &GraphPattern::Bgp { patterns: tps },
                    initial,
                    context,
                    parameters,
                    pushdowns.clone(),
                    query_settings,
                    dataset,
                )?
            };
            // Read more than once (edges, keys, restrictions and the final join): collected
            // once, so that every read gives the same rows (see collect_once).
            components.push((vars, collect_once(sm)?));
        }
        let component_of = |v: &str| components.iter().position(|(vars, _)| vars.contains(v));

        // The graph.
        let mut consumed: Vec<usize> = vec![];
        // the component of the edge patterns, when the output already holds its solutions
        let mut edge_component: Option<usize> = None;
        let mut edges_joined = false;
        let edges = if let (Some(a), Some(b)) = (&spec.edge_source, &spec.edge_target) {
            let ca = component_of(a);
            let cb = component_of(b);
            let c = match (ca, cb) {
                (Some(ca), Some(cb)) if ca == cb => ca,
                _ => {
                    return Err(err(format!(
                        "the edge ends ?{a} and ?{b} of maplib:{name} must be bound together by the other patterns of its group"
                    )))
                }
            };
            consumed.push(c);
            edge_component = Some(c);
            self.pattern_edges(components[c].1.clone(), a, b, spec.strong)?
        } else {
            self.predicate_edges(&spec, dataset)?
        };
        let n = edges.n;
        // the components are numbered by the node values (their dictionary codes), not by the
        // node numbers of this build of the graph, which may differ between builds
        let order = node_order(&edges.built);

        // The avoided nodes, and the nodes that count.
        let mut node_set = |endpoint: &Endpoint, what: &str| -> Result<Vec<bool>, SparqlError> {
            let keys = match endpoint {
                Endpoint::Variable(v) => {
                    let c = component_of(v).ok_or_else(|| {
                        err(format!(
                            "the {what} ?{v} must be bound by the other patterns of its group"
                        ))
                    })?;
                    consumed.push(c);
                    self.key_vec(components[c].1.clone(), v, &edges.built)?
                }
                Endpoint::Constants(c) => self.constant_keys(c, &edges.built)?,
            };
            Ok(key_mask(n, &keys))
        };
        let removed = match &spec.avoid {
            Some(e) => Some(node_set(e, "avoided nodes")?),
            None => None,
        };
        let weights: Option<Vec<u64>> = match &spec.count {
            Some(e) => Some(node_set(e, "counted nodes")?.into_iter().map(u64::from).collect()),
            None => None,
        };

        // The nodes the output is restricted to, and those of them that are not in the graph.
        let node_var = match &spec.node {
            Endpoint::Variable(v) => Some(v.clone()),
            Endpoint::Constants(_) => None,
        };
        let restriction = if spec.algorithm == Algorithm::Bridge {
            None
        } else {
            match &spec.node {
                Endpoint::Variable(v) => match component_of(v) {
                    Some(c) => Some(self.restriction(components[c].1.clone(), v, &edges.built)?),
                    None => None,
                },
                Endpoint::Constants(c) => {
                    let sm = constant_mappings(c, VALUE_VAR, &self.global_cats)?;
                    Some(self.restriction(sm, VALUE_VAR, &edges.built)?)
                }
            }
        };

        let out_var = node_var.clone().unwrap_or_else(|| VALUE_VAR.to_string());
        let out = match spec.algorithm {
            Algorithm::Component => {
                let comps = if spec.strong {
                    strongly_connected_components(n, &edges.from, &edges.to, removed.as_deref())
                } else {
                    connected_components(n, &edges.from, &edges.to, removed.as_deref())
                };
                let comps = weighted(comps.renumbered(&order), weights.as_deref());
                utils::cancel::check()?;
                debug!(
                    "maplib:component: {} components of a graph of {} nodes and {} edges",
                    comps.sizes.len(),
                    n,
                    edges.from.len()
                );
                self.component_output(&spec, &out_var, &edges, &comps, restriction)?
            }
            Algorithm::ArticulationPoint => {
                let c = cuts(n, &edges.from, &edges.to, removed.as_deref(), weights.as_deref());
                let c = algo::Cuts {
                    components: weighted(c.components.renumbered(&order), weights.as_deref()),
                    ..c
                };
                utils::cancel::check()?;
                let keep = restriction.as_ref().map(|(keys, _)| key_mask(n, keys));
                let rows: Vec<(u32, u64)> = c
                    .articulation_points
                    .iter()
                    .copied()
                    .filter(|(u, _)| keep.as_ref().is_none_or(|m| m[*u as usize]))
                    .collect();
                let nodes: Vec<u32> = rows.iter().map(|r| r.0).collect();
                let cut_off: Vec<i64> = rows.iter().map(|r| r.1 as i64).collect();
                let mut columns = vec![self.node_column(&out_var, &edges.built, &nodes)];
                let mut numbers = vec![];
                numbers.push((spec.cut_off_var.clone(), cut_off));
                numbers.extend(component_numbers(&spec, &c.components, &nodes));
                self.output(&mut columns, &edges.built, numbers, nodes.len())?
            }
            Algorithm::Bridge => {
                let c = cuts(n, &edges.from, &edges.to, removed.as_deref(), weights.as_deref());
                let c = algo::Cuts {
                    components: weighted(c.components.renumbered(&order), weights.as_deref()),
                    ..c
                };
                utils::cancel::check()?;
                let mut bridges = c.bridges.clone();
                bridges.sort_unstable();
                let object_var = spec.object_var.clone().unwrap();
                // bridges between the edge variables themselves: the solutions of the edges that
                // are bridges, picked by their row, so their values are those of the patterns
                let picked_rows = match (&edges.rows, &spec.edge_source, &spec.edge_target) {
                    (Some((rows_sm, edge_rows)), Some(source), Some(target))
                        if &out_var == source && &object_var == target =>
                    {
                        let from: Vec<u32> =
                            bridges.iter().map(|(e, _)| edges.from[*e as usize]).collect();
                        let mut numbers = vec![(
                            spec.cut_off_var.clone(),
                            bridges.iter().map(|b| b.1 as i64).collect::<Vec<i64>>(),
                        )];
                        numbers.extend(component_numbers(&spec, &c.components, &from));
                        let mut cols = vec![Column::new(
                            EDGE_ROW.into(),
                            bridges
                                .iter()
                                .map(|(e, _)| edge_rows[*e as usize])
                                .collect::<Vec<u32>>(),
                        )];
                        let integer = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                            .into_default_stored_rdf_node_state();
                        let mut types = rows_sm.rdf_node_types.clone();
                        for (name, values) in numbers {
                            if let Some(name) = name {
                                types.insert(name.clone(), integer.clone());
                                cols.push(Column::new(name.into(), values));
                            }
                        }
                        let picked = DataFrame::new(bridges.len(), cols).unwrap();
                        let mappings = rows_sm
                            .mappings
                            .clone()
                            .join(
                                picked.lazy(),
                                [col(EDGE_ROW)],
                                [col(EDGE_ROW)],
                                JoinArgs::new(JoinType::Inner),
                            )
                            .map_err(|e| err(format!("picking the bridges: {e}")))?
                            .drop(by_name([EDGE_ROW], true, false));
                        types.remove(EDGE_ROW);
                        edges_joined = true;
                        Some(SolutionMappings::new(mappings, types, bridges.len()))
                    }
                    _ => None,
                };
                if let Some(picked) = picked_rows {
                    picked
                } else {
                let from: Vec<u32> = bridges.iter().map(|(e, _)| edges.from[*e as usize]).collect();
                let to: Vec<u32> = bridges.iter().map(|(e, _)| edges.to[*e as usize]).collect();
                let mut columns = vec![
                    self.node_column(&out_var, &edges.built, &from),
                    self.node_column(&object_var, &edges.built, &to),
                ];
                if let (Some(v), Some(p)) = (&spec.predicate_var, &edges.predicate) {
                    let iris: Vec<String> = bridges
                        .iter()
                        .map(|(e, _)| {
                            edges.built.predicates[p[*e as usize] as usize]
                                .as_str()
                                .to_string()
                        })
                        .collect();
                    columns.push((
                        Column::new(v.into(), iris),
                        RDFNodeState::from_bases(BaseRDFNodeType::IRI, BaseCatState::String),
                    ));
                }
                let cut_off: Vec<i64> = bridges.iter().map(|b| b.1 as i64).collect();
                let mut numbers = vec![(spec.cut_off_var.clone(), cut_off)];
                numbers.extend(component_numbers(&spec, &c.components, &from));
                self.output(&mut columns, &edges.built, numbers, bridges.len())?
                }
            }
        };
        let mut out = out;
        if node_var.is_none() {
            out.mappings = out.mappings.drop(by_name([VALUE_VAR], true, false));
            out.rdf_node_types.remove(VALUE_VAR);
        }

        // Join the other components: first the one that binds the subject, then the rest, but
        // not those that only give the edges or the avoided nodes.
        let mut out_vars: HashSet<String> = out.rdf_node_types.keys().cloned().collect();
        let mut order: Vec<usize> = vec![];
        if let Some(c) = node_var.as_ref().and_then(|v| component_of(v)) {
            order.push(c);
        }
        for c in 0..components.len() {
            if !order.contains(&c) {
                order.push(c);
            }
        }
        let mut components: Vec<Option<(HashSet<String>, SolutionMappings)>> =
            components.into_iter().map(Some).collect();
        for c in order {
            let (vars, sm) = components[c].take().unwrap();
            if consumed.contains(&c) && vars.is_disjoint(&out_vars) {
                continue;
            }
            // the bridges picked from the edge solutions hold them already
            if edges_joined && Some(c) == edge_component {
                continue;
            }
            out_vars.extend(vars);
            out = join(
                out,
                sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(out)
    }

    /// The edges of the edge predicates, from the cached search graph. Each triple is one edge
    /// (from subject to object; for strongly connected components, the edges as followed).
    fn predicate_edges(
        &self,
        spec: &GraphAlgorithmSpec,
        dataset: &QueryGraph,
    ) -> Result<EdgeList, SparqlError> {
        let (forward, inverse) = match spec.any_predicate {
            Some((f, b)) => {
                let mut all = self.resource_predicates(dataset);
                all.retain(|p| !spec.exclude.contains(p));
                (
                    if f { all.clone() } else { spec.forward.clone() },
                    if b { all } else { spec.inverse.clone() },
                )
            }
            None => (spec.forward.clone(), spec.inverse.clone()),
        };
        let built = self.path_search_graph(&forward, &inverse, dataset)?;
        let n = built.graph.n;
        let is_forward: Vec<bool> = built
            .predicates
            .iter()
            .map(|p| forward.contains(p))
            .collect();
        let mut from = vec![];
        let mut to = vec![];
        let mut predicate = vec![];
        for u in 0..n as u32 {
            built
                .graph
                .for_each_neighbour(Direction::Forward, u, |v, label| {
                    let p = label / 2;
                    if spec.strong {
                        from.push(u);
                        to.push(v);
                        predicate.push(p);
                    } else if label % 2 == 0 {
                        // the triple u p v
                        from.push(u);
                        to.push(v);
                        predicate.push(p);
                    } else if !is_forward[p as usize] {
                        // the triple v p u, only followed backwards
                        from.push(v);
                        to.push(u);
                        predicate.push(p);
                    }
                });
        }
        Ok(EdgeList {
            built,
            n,
            from,
            to,
            predicate: Some(predicate),
            rows: None,
        })
    }

    /// The edges given by the solutions that bind `a` and `b`: one edge per distinct solution.
    fn pattern_edges(
        &self,
        sm: SolutionMappings,
        a: &str,
        b: &str,
        _directed: bool,
    ) -> Result<EdgeList, SparqlError> {
        let sm = distinct(sm)?;
        let SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate,
        } = sm;
        // Both ends of every edge in one column, with the edge's row and the end (0 or 1). The
        // union gives the two ends one encoding, so the node numbers are given within that
        // column, not by joining each end with the nodes (whose encodings may differ, e.g. for
        // IRIs from VALUES or BIND that are not in the model).
        // collected once: the row numbers must name the same solutions in the edges and when
        // bridges are picked by row (the order of DISTINCT is not fixed between evaluations)
        let indexed = mappings
            .with_row_index(EDGE_ROW, None)
            .with_column(col(EDGE_ROW).cast(DataType::UInt32))
            .collect_auto()
            .map_err(|e| err(format!("collecting the edges of a graph algorithm: {e}")))?
            .lazy();
        let edges = indexed.clone().select([col(a), col(b), col(EDGE_ROW)]);
        let end = |v: &str, side: u32| {
            SolutionMappings::new(
                edges.clone().select([
                    col(v).alias(VALUE_VAR),
                    col(EDGE_ROW),
                    polars::prelude::lit(side).cast(DataType::UInt32).alias(EDGE_END),
                ]),
                HashMap::from([
                    (VALUE_VAR.to_string(), rdf_node_types.get(v).unwrap().clone()),
                    (EDGE_ROW.to_string(), u32_state()),
                    (EDGE_END.to_string(), u32_state()),
                ]),
                height_estimate,
            )
        };
        let ends = union(vec![end(a, 0), end(b, 1)], false, self.global_cats.clone())?;
        let value_state = ends.rdf_node_types.get(VALUE_VAR).unwrap().clone();
        // an unbound end (e.g. from a UNION branch that binds other variables) is no node; a
        // column of several types is a struct, unbound when all its types are
        let ends = ends
            .mappings
            .filter(expr_is_null_workaround(col(VALUE_VAR), &value_state).not())
            .collect_auto()
            .map_err(|e| err(format!("collecting the edges of a graph algorithm: {e}")))?;
        let lookup = ends
            .clone()
            .lazy()
            .select([col(VALUE_VAR)])
            .unique_stable(None, UniqueKeepStrategy::First)
            .with_row_index(LOOKUP_COLUMN, None)
            .select([
                col(VALUE_VAR).alias(VALUE_COLUMN),
                col(LOOKUP_COLUMN).cast(DataType::UInt32),
            ])
            .collect_auto()
            .map_err(|e| err(format!("collecting the nodes of a graph algorithm: {e}")))?;
        let keyed = ends
            .lazy()
            .join(
                lookup.clone().lazy(),
                [col(VALUE_VAR)],
                [col(VALUE_COLUMN)],
                JoinArgs::new(JoinType::Inner),
            )
            .map_err(|e| err(format!("numbering the nodes of a graph algorithm: {e}")))?
            .select([col(EDGE_ROW), col(EDGE_END), col(LOOKUP_COLUMN)])
            .sort([EDGE_ROW, EDGE_END], Default::default())
            .collect()
            .map_err(|e| err(format!("collecting the edges of a graph algorithm: {e}")))?;
        // pairs of ends of the same edge, in order: an edge with an unbound end has one
        let rows = keyed.column(EDGE_ROW).unwrap().u32().unwrap();
        let sides = keyed.column(EDGE_END).unwrap().u32().unwrap();
        let keys = keyed.column(LOOKUP_COLUMN).unwrap().u32().unwrap();
        let (mut from, mut to, mut edge_rows) = (vec![], vec![], vec![]);
        let mut i = 0;
        while i + 1 < keyed.height() {
            if rows.get(i) == rows.get(i + 1) && sides.get(i) == Some(0) && sides.get(i + 1) == Some(1) {
                from.push(keys.get(i).unwrap());
                to.push(keys.get(i + 1).unwrap());
                edge_rows.push(rows.get(i).unwrap());
                i += 2;
            } else {
                i += 1;
            }
        }
        let n = lookup.height();
        let built = Arc::new(BuiltGraph {
            graph: SearchGraph::default(),
            lookup,
            value_state,
            predicates: vec![],
        });
        let mut row_types = rdf_node_types.clone();
        row_types.insert(EDGE_ROW.to_string(), u32_state());
        Ok(EdgeList {
            n,
            from,
            to,
            built,
            predicate: None,
            rows: Some((
                SolutionMappings::new(indexed, row_types, height_estimate),
                edge_rows,
            )),
        })
    }

    /// The keys of the distinct values of `var` that are nodes of the graph, and the solutions
    /// with the values that are not.
    fn restriction(
        &self,
        sm: SolutionMappings,
        var: &str,
        built: &BuiltGraph,
    ) -> Result<(Vec<u32>, Option<SolutionMappings>), SparqlError> {
        let SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate,
        } = sm;
        let state = rdf_node_types.get(var).unwrap().clone();
        let projected = distinct(SolutionMappings::new(
            mappings
                .select([col(var)])
                .filter(expr_is_null_workaround(col(var), &state).not()),
            HashMap::from([(var.to_string(), state.clone())]),
            height_estimate,
        ))?;
        let k = key_col(var);
        let lookup = SolutionMappings::new(
            built.lookup.clone().lazy().select([
                col(VALUE_COLUMN).alias(var),
                col(LOOKUP_COLUMN).alias(&k),
            ]),
            HashMap::from([
                (var.to_string(), built.value_state.clone()),
                (k.clone(), u32_state()),
            ]),
            built.lookup.height(),
        );
        let joined = join(
            projected,
            lookup,
            JoinType::Left,
            self.global_cats.clone(),
            None,
        )?;
        let var_state = joined.rdf_node_types.get(var).unwrap().clone();
        let df = joined
            .mappings
            .collect_auto()
            .map_err(|e| err(format!("collecting the nodes of a graph algorithm: {e}")))?;
        let key = df.column(&k).unwrap().u32().unwrap();
        let mut keys: Vec<u32> = key.iter().flatten().collect();
        keys.sort_unstable();
        keys.dedup();
        let missing = key.is_null();
        let extras = if missing.any() {
            let extras = df
                .filter(&missing)
                .unwrap()
                .select([var])
                .unwrap();
            let h = extras.height();
            Some(SolutionMappings::new(
                extras.lazy(),
                HashMap::from([(var.to_string(), var_state)]),
                h,
            ))
        } else {
            None
        };
        Ok((keys, extras))
    }

    /// One row per node in a component (or per node of the restriction): the node, the
    /// component number and size. Nodes of the restriction that are not in the graph are
    /// components of one node, numbered after the others.
    fn component_output(
        &self,
        spec: &GraphAlgorithmSpec,
        out_var: &str,
        edges: &EdgeList,
        comps: &Components,
        restriction: Option<(Vec<u32>, Option<SolutionMappings>)>,
    ) -> Result<SolutionMappings, SparqlError> {
        let (nodes, extras) = match restriction {
            Some((keys, extras)) => (
                keys.into_iter()
                    .filter(|k| comps.of[*k as usize] != NONE)
                    .collect::<Vec<u32>>(),
                extras,
            ),
            None => (
                (0..edges.n as u32)
                    .filter(|k| comps.of[*k as usize] != NONE)
                    .collect(),
                None,
            ),
        };
        let mut columns = vec![self.node_column(out_var, &edges.built, &nodes)];
        let numbers = component_numbers(spec, comps, &nodes);
        let main = self.output(&mut columns, &edges.built, numbers, nodes.len())?;
        let Some(extras) = extras else {
            return Ok(main);
        };
        // components of their own, after those of the graph
        let first = comps.sizes.len() as i64 + 1;
        let SolutionMappings {
            mappings,
            mut rdf_node_types,
            height_estimate,
        } = extras;
        // numbered in the order of their values, so the numbers are the same in every run
        let mut mappings = mappings
            .rename([out_var_of(&rdf_node_types)], [out_var], true)
            .sort([out_var], Default::default());
        let state = rdf_node_types.drain().next().unwrap().1;
        let mut types = HashMap::from([(out_var.to_string(), state)]);
        let integer = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
            .into_default_stored_rdf_node_state();
        if let Some(v) = &spec.id_var {
            mappings = mappings.with_column(
                (polars::prelude::int_range(
                    polars::prelude::lit(first),
                    polars::prelude::lit(first) + polars::prelude::len().cast(DataType::Int64),
                    1,
                    DataType::Int64,
                ))
                .alias(v),
            );
            types.insert(v.clone(), integer.clone());
        }
        if let Some(v) = &spec.size_var {
            mappings = mappings.with_column(polars::prelude::lit(1i64).cast(DataType::Int64).alias(v));
            types.insert(v.clone(), integer);
        }
        let extras = SolutionMappings::new(mappings, types, height_estimate);
        Ok(union(
            vec![main, extras],
            false,
            self.global_cats.clone(),
        )?)
    }

    /// The column of the values of the nodes with these keys.
    fn node_column(&self, name: &str, built: &BuiltGraph, keys: &[u32]) -> (Column, RDFNodeState) {
        let values = built.lookup.column(VALUE_COLUMN).unwrap();
        (
            values
                .take(&IdxCa::from_vec("".into(), keys.to_vec()))
                .unwrap()
                .with_name(name.into()),
            built.value_state.clone(),
        )
    }

    /// Solution mappings of the columns and the integer outputs.
    fn output(
        &self,
        columns: &mut Vec<(Column, RDFNodeState)>,
        _built: &BuiltGraph,
        numbers: Vec<(Option<String>, Vec<i64>)>,
        rows: usize,
    ) -> Result<SolutionMappings, SparqlError> {
        let integer = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
            .into_default_stored_rdf_node_state();
        for (name, values) in numbers {
            if let Some(name) = name {
                columns.push((Column::new(name.into(), values), integer.clone()));
            }
        }
        let mut types = HashMap::new();
        let mut cols = vec![];
        for (c, t) in columns.drain(..) {
            types.insert(c.name().to_string(), t);
            cols.push(c);
        }
        let df = DataFrame::new(rows, cols).unwrap();
        let (df, types) = compress_actual_multitypes(df, types);
        let eager = self
            .global_cats
            .read()?
            .encode_solution_mappings(EagerSolutionMappings::new(df, types));
        Ok(eager.as_lazy())
    }
}

fn out_var_of(types: &HashMap<String, RDFNodeState>) -> &str {
    types.keys().next().unwrap().as_str()
}

/// An order of the nodes that is the same in every build of the graph: the dictionary codes of
/// their values when they are encoded as integers (IRIs and blank nodes), else the node numbers.
fn node_order(built: &BuiltGraph) -> Vec<u64> {
    let values = built.lookup.column(VALUE_COLUMN).unwrap();
    match values.cast(&DataType::UInt64) {
        Ok(c) if !values.dtype().is_struct() && c.null_count() == 0 => {
            c.u64().unwrap().into_no_null_iter().collect()
        }
        _ => (0..built.lookup.height() as u64).collect(),
    }
}

/// The components with their sizes counted in weights (the number of counted nodes).
fn weighted(mut comps: Components, weights: Option<&[u64]>) -> Components {
    if let Some(w) = weights {
        let mut sizes = vec![0u64; comps.sizes.len()];
        for (u, &c) in comps.of.iter().enumerate() {
            if c != NONE {
                sizes[c as usize] += w[u];
            }
        }
        comps.sizes = sizes;
    }
    comps
}

/// A mask of the keys.
fn key_mask(n: usize, keys: &[u32]) -> Vec<bool> {
    let mut m = vec![false; n];
    for k in keys {
        m[*k as usize] = true;
    }
    m
}

/// The component numbers (from 1) and sizes of the nodes, for the outputs that ask for them.
fn component_numbers(
    spec: &GraphAlgorithmSpec,
    comps: &Components,
    nodes: &[u32],
) -> Vec<(Option<String>, Vec<i64>)> {
    let mut out = vec![];
    if spec.id_var.is_some() {
        out.push((
            spec.id_var.clone(),
            nodes
                .iter()
                .map(|u| comps.of[*u as usize] as i64 + 1)
                .collect(),
        ));
    }
    if spec.size_var.is_some() {
        out.push((
            spec.size_var.clone(),
            nodes
                .iter()
                .map(|u| comps.sizes[comps.of[*u as usize] as usize] as i64)
                .collect(),
        ));
    }
    out
}
