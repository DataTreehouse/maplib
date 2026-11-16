mod distinct;
mod extend;
mod filter;
mod geo_filter;
mod graph;
mod group;
mod join;
pub(crate) mod lateral;
mod left_join;
pub mod graph_algorithms;
pub mod length_path;
pub mod magic_paths;
mod minus;
mod order_by;
pub mod path;
pub mod path_search;
mod project;
mod pvalues;
pub(crate) mod service;
mod triple;
pub mod triples_ordering;
mod union;
mod values;

use super::{QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::evaluation_hook::HookDecision;
use tracing::{instrument, trace, warn};

use crate::sparql::lazy_graph_patterns::triples_ordering::{
    group_triple_patterns, order_triple_patterns,
};
use polars::prelude::{col, int_range, len, lit, DataType, IntoLazy, JoinType};
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use representation::constants::MAPLIB_PREFIX_IRI;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TriplePattern};
use std::collections::HashMap;
use utils::polars::{pl_interruptable_collect_sorted_slice, InterruptableCollectError};

impl Triplestore {
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub fn lazy_graph_pattern(
        &self,
        graph_pattern: &GraphPattern,
        mut solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!(
            "Start processing graph pattern {:?} at context: {}",
            graph_pattern,
            context.as_str()
        );
        // Inside the right side of a LATERAL, a pattern that no solutions flow into starts from
        // the seed of substituted bindings. Sub-selects and the solution modifiers handle the
        // seed themselves, as they start a new scope or work on whole solution sequences.
        if solution_mappings.is_none()
            && !matches!(
                graph_pattern,
                GraphPattern::Project { .. }
                    | GraphPattern::Slice { .. }
                    | GraphPattern::Distinct { .. }
                    | GraphPattern::Reduced { .. }
                    | GraphPattern::OrderBy { .. }
                    | GraphPattern::Group { .. }
            )
        {
            solution_mappings = lateral::seed_solution_mappings(parameters);
        }
        utils::cancel::check()?;
        if let Some(hook) = &query_settings.evaluation_hook {
            let decision = {
                let mut evaluate = |gp: &GraphPattern, sm: Option<SolutionMappings>, ctx: &Context| {
                    self.lazy_graph_pattern(
                        gp,
                        sm,
                        ctx,
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        dataset,
                    )
                };
                hook.0.before_graph_pattern(
                    self,
                    graph_pattern,
                    solution_mappings,
                    context,
                    &mut evaluate,
                )?
            };
            match decision {
                HookDecision::Evaluate(sm) => solution_mappings = sm,
                HookDecision::Evaluated(sm) => return Ok(sm),
            }
        }
        let sm = match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                // Making sure we use the FTS belonging to the appropriate named graph.
                let graph = if let QueryGraph::NamedGraph(n) = dataset {
                    Some(n)
                } else {
                    // Only warn when the basic graph pattern uses full text search and some graph has an index to miss.
                    if uses_fts(patterns) && !self.fts_index.is_empty() {
                        warn!("FTS is only defined when named graphs are provided or for the default graph");
                    }
                    None
                };
                // Vector searches are answered by the vector index of the graph.
                let vector_patterns;
                let patterns = if vector::uses_vector_search(patterns) {
                    let Some(index) = graph.and_then(|g| self.vector_index.get(g)) else {
                        return Err(vector::VectorError::NoIndex.into());
                    };
                    let (rest, vector_solution_mappings) = index.lookup_from_triple_patterns(
                        patterns,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?;
                    if let Some(vsm) = vector_solution_mappings {
                        solution_mappings = if let Some(sm) = solution_mappings {
                            Some(join(
                                sm,
                                vsm,
                                JoinType::Inner,
                                self.global_cats.clone(),
                                query_settings.max_rows,
                            )?)
                        } else {
                            Some(vsm)
                        };
                    }
                    vector_patterns = rest;
                    &vector_patterns
                } else {
                    patterns
                };
                let patterns = if let Some(graph) = graph {
                    if let Some(fts_index) = self.fts_index.get(graph) {
                        let (patterns, fts_solution_mappings) = fts_index
                            .lookup_from_triple_patterns(
                                patterns,
                                self.global_cats.clone(),
                                query_settings.max_rows,
                            )?;
                        if let Some(fts_solution_mappings) = fts_solution_mappings {
                            solution_mappings = if let Some(solution_mappings) = solution_mappings {
                                Some(join(
                                    solution_mappings,
                                    fts_solution_mappings,
                                    JoinType::Inner,
                                    self.global_cats.clone(),
                                    query_settings.max_rows,
                                )?)
                            } else {
                                Some(fts_solution_mappings)
                            };
                        }
                        patterns
                    } else {
                        patterns.clone()
                    }
                } else {
                    patterns.clone()
                };

                if graph_algorithms::has_graph_algorithm(&patterns) {
                    return self.lazy_graph_algorithm(
                        &patterns,
                        solution_mappings,
                        context,
                        parameters,
                        &pushdowns,
                        query_settings,
                        dataset,
                    );
                }
                if path_search::has_path_search(&patterns) {
                    return self.lazy_path_search(
                        &patterns,
                        solution_mappings,
                        context,
                        parameters,
                        &pushdowns,
                        query_settings,
                        dataset,
                    );
                }
                // maplib:path patterns are evaluated after the rest of the basic graph pattern,
                // so that the values it binds restrict where the paths start.
                let (patterns, length_paths) =
                    length_path::extract_length_path_patterns(&patterns)?;
                self.check_maplib_predicates(&patterns)?;
                pushdowns.add_patterns_pushdowns(&patterns);
                let mut updated_solution_mappings = solution_mappings;
                let bgp_context = context.extension_with(PathEntry::BGP);
                let ordered_patterns =
                    order_triple_patterns(&patterns, &updated_solution_mappings, &pushdowns);
                let groups = group_triple_patterns(ordered_patterns, &updated_solution_mappings);
                for group in &groups {
                    match updated_solution_mappings {
                        // The group shares more bound variables than its first pattern does.
                        Some(sm) if group.len() > 1 && sm.height_estimate > 0 => {
                            updated_solution_mappings = Some(self.lazy_triple_pattern_group(
                                sm,
                                group,
                                &bgp_context,
                                &mut pushdowns,
                                query_settings,
                                dataset,
                            )?);
                        }
                        sm => {
                            updated_solution_mappings = sm;
                            for tp in group {
                                updated_solution_mappings = Some(self.lazy_triple_pattern(
                                    updated_solution_mappings,
                                    tp,
                                    &bgp_context,
                                    &mut pushdowns,
                                    query_settings,
                                    dataset,
                                )?);
                            }
                        }
                    }
                }
                for lp in &length_paths {
                    updated_solution_mappings = Some(self.lazy_length_path(
                        lp,
                        updated_solution_mappings,
                        &pushdowns,
                        query_settings,
                        dataset,
                    )?);
                }

                if let Some(updated_solution_mappings) = updated_solution_mappings {
                    Ok(updated_solution_mappings)
                } else {
                    let map = HashMap::new();
                    Ok(SolutionMappings {
                        mappings: DataFrame::empty_with_height(1).lazy(),
                        rdf_node_types: map,
                        height_estimate: 1,
                    })
                }
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.lazy_path(
                subject,
                path,
                object,
                solution_mappings,
                context,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Join { left, right } => self.lazy_join(
                left,
                right,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.lazy_left_join(
                left,
                right,
                expression,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Filter { expr, inner } => self.lazy_filter(
                inner,
                expr,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Union { left, right } => self.lazy_union(
                left,
                right,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Graph { name, inner } => self.lazy_graph(
                name,
                inner,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.lazy_extend(
                inner,
                variable,
                expression,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Lateral { left, right } => self.lazy_lateral(
                left,
                right,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Minus { left, right } => self.lazy_minus(
                left,
                right,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.lazy_values(
                solution_mappings,
                variables,
                bindings,
                context,
                pushdowns,
                query_settings,
            ),
            GraphPattern::OrderBy { inner, expression } => self.lazy_order_by(
                inner,
                expression,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Project { inner, variables } => self.lazy_project(
                inner,
                variables,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Distinct { inner } => self.lazy_distinct(
                inner,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Reduced { inner } => {
                // REDUCED permits, but does not require, eliminating duplicates.
                self.lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    &context.extension_with(PathEntry::ReducedInner),
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                // The slice applies to the inner pattern only (e.g. a subquery with LIMIT),
                // so it is evaluated without the outer solution mappings and pushdowns.
                let (inner_pushdowns, outer) = if solution_mappings.is_some() {
                    (Pushdowns::new(), solution_mappings)
                } else {
                    (pushdowns, None)
                };
                let mut newsols = self.lazy_graph_pattern(
                    inner,
                    None,
                    &context.extension_with(PathEntry::SliceInner),
                    parameters,
                    inner_pushdowns,
                    query_settings,
                    dataset,
                )?;
                // Inside the right side of a LATERAL, each substituted binding is sliced on its
                // own, e.g. ORDER BY .. LIMIT n gives the top n for every left solution.
                let keys: Vec<_> = lateral::lateral_key_columns(parameters)
                    .into_iter()
                    .filter(|k| newsols.rdf_node_types.contains_key(k))
                    .collect();
                if !keys.is_empty() {
                    let row = int_range(lit(0), len(), 1, DataType::Int64)
                        .over(keys.iter().map(col).collect::<Vec<_>>())
                        .unwrap();
                    let mut keep = row.clone().gt_eq(lit(*start as i64));
                    if let Some(length) = length {
                        keep = keep.and(row.lt(lit((*start + *length) as i64)));
                    }
                    newsols.mappings = newsols.mappings.filter(keep);
                } else if let Some(length) = length {
                    let input_estimate = newsols.height_estimate;
                    newsols.mappings = newsols.mappings.slice(*start as i64, *length as u32);
                    let ordered = is_ordered(inner);
                    if ordered && sorted_slice_in_memory(*start, *length, input_estimate) {
                        // ORDER BY with a large LIMIT (and OFFSET): collected with the
                        // in-memory engine, see pl_interruptable_collect_sorted_slice.
                        let df = match pl_interruptable_collect_sorted_slice(newsols.mappings) {
                            Ok(df) => df,
                            Err(InterruptableCollectError::Cancelled(c)) => {
                                return Err(SparqlError::Cancelled(c))
                            }
                            Err(InterruptableCollectError::Interrupted) => {
                                return Err(SparqlError::InterruptSignal)
                            }
                            Err(e) => {
                                return Err(SparqlError::QueryExecutionError(e.to_string()))
                            }
                        };
                        newsols.height_estimate = df.height();
                        newsols.mappings = df.lazy();
                    } else if !ordered {
                        newsols.height_estimate = newsols.height_estimate.min(*length);
                    }
                    // A small ORDER BY ... LIMIT is left to the engine chosen for the whole
                    // query: the streaming engine's top-k is faster than an in-memory sort
                    // there, so its height estimate is not lowered either.
                } else {
                    newsols.mappings = newsols.mappings.slice(*start as i64, u32::MAX);
                }
                if let Some(outer) = outer {
                    newsols = join(
                        outer,
                        newsols,
                        JoinType::Inner,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?;
                }
                Ok(newsols)
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.lazy_group(
                inner,
                variables,
                aggregates,
                solution_mappings,
                context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            ),
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => self.lazy_service(
                name,
                inner,
                *silent,
                solution_mappings,
                context,
                query_settings,
                dataset,
            ),
            GraphPattern::PValues {
                variables,
                bindings_parameter,
            } => self.lazy_pvalues(
                solution_mappings,
                variables,
                bindings_parameter,
                context,
                parameters,
                pushdowns,
                query_settings,
            ),
        };
        trace!(
            "Finish processing graph pattern {:?} at context: {}",
            graph_pattern,
            context.as_str()
        );
        sm
    }
}

/// Is an ORDER BY with this OFFSET and LIMIT collected with the in-memory engine? The streaming
/// engine's top-k gets slower with the number of rows it keeps (OFFSET plus LIMIT), while the
/// in-memory sort costs about the same for any LIMIT. Measured, streaming wins up to about 1% of
/// 300k rows and 3% of 1M rows, so the in-memory engine is used from 1/32 of the estimated
/// input, and never below 10 000 rows.
fn sorted_slice_in_memory(start: usize, length: usize, input_estimate: usize) -> bool {
    let kept = start.saturating_add(length);
    kept >= SORTED_SLICE_IN_MEMORY_MIN_ROWS.max(input_estimate / 32)
}

const SORTED_SLICE_IN_MEMORY_MIN_ROWS: usize = 10_000;

/// Is the pattern sorted at the top, below projections, DISTINCT and REDUCED?
fn is_ordered(gp: &GraphPattern) -> bool {
    match gp {
        GraphPattern::OrderBy { .. } => true,
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner } => is_ordered(inner),
        _ => false,
    }
}

/// Predicates of the full text search index (maplib vocab fts, ftsQuery, ...).
const FTS_PREDICATE_PREFIX: &str = "https://datatreehouse.github.io/maplib/vocab#fts";

/// The magic predicates that start a magic pattern. maplib has no list of the terms of its
/// vocabulary (maplib, maplib_ee, the portal and applications all add data predicates to it),
/// so only these are checked for misspellings.
const MAGIC_ENTRY_POINTS: [&str; 7] = [
    "path",
    "shortestPath",
    "component",
    "bridge",
    "articulationPoint",
    "fts",
    "vectorSearch",
];

/// The magic predicate that `local` is probably a misspelling of: one that differs only in
/// case, or for longer names by at most two edits.
fn misspelt_magic_predicate(local: &str) -> Option<&'static str> {
    let lower = local.to_lowercase();
    MAGIC_ENTRY_POINTS.iter().copied().find(|m| {
        if *m == local {
            return false;
        }
        let ml = m.to_lowercase();
        lower == ml || (ml.len() >= 6 && edit_distance(&lower, &ml) <= 2)
    })
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for i in 1..=a.len() {
        let mut cur = vec![i; b.len() + 1];
        for j in 1..=b.len() {
            let sub = prev[j - 1] + usize::from(a[i - 1] != b[j - 1]);
            cur[j] = sub.min(prev[j] + 1).min(cur[j - 1] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

impl Triplestore {
    /// The magic predicates this build supports have been taken out of the patterns before
    /// this is called. Of the maplib predicates that remain and that no triples use, a
    /// misspelt magic predicate and maplib:fts without a full text index would silently give
    /// no solutions, so they are errors. Other maplib predicates are ordinary data that a model
    /// may lack (time series, serves relations, config and summary terms, ...).
    fn check_maplib_predicates(&self, patterns: &[TriplePattern]) -> Result<(), SparqlError> {
        for tp in patterns {
            let NamedNodePattern::NamedNode(nn) = &tp.predicate else {
                continue;
            };
            let Some(local) = nn.as_str().strip_prefix(MAPLIB_PREFIX_IRI) else {
                continue;
            };
            let problem = if local == "fts" {
                Some(
                    "maplib:fts needs a full text index on the queried graph \
                     (create_index(IndexingOptions(fts=True)))"
                        .to_string(),
                )
            } else {
                misspelt_magic_predicate(local)
                    .map(|m| format!("did you mean the magic predicate maplib:{m}?"))
            };
            let Some(problem) = problem else {
                continue;
            };
            let used = self.graph_triples_map.values().any(|m| {
                m.get(nn)
                    .is_some_and(|t| t.values().any(|x| x.get_height() > 0))
            });
            if !used {
                return Err(SparqlError::UnknownMaplibPredicate(
                    nn.as_str().to_string(),
                    problem,
                ));
            }
        }
        Ok(())
    }
}

/// Whether any of the triple patterns uses a full text search predicate.
fn uses_fts(patterns: &[TriplePattern]) -> bool {
    patterns.iter().any(|tp| {
        matches!(&tp.predicate, NamedNodePattern::NamedNode(p) if p.as_str().starts_with(FTS_PREDICATE_PREFIX))
    })
}
