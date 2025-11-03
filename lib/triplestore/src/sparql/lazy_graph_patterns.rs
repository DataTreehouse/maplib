mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
mod minus;
mod order_by;
mod path;
mod project;
mod pvalues;
mod triple;
mod triples_ordering;
mod union;
mod values;

use super::{QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use tracing::{info, instrument, trace};

use crate::sparql::lazy_graph_patterns::triples_ordering::order_triple_patterns;
use polars::prelude::{IntoLazy, JoinType};
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    pub fn lazy_graph_pattern(
        &self,
        graph_pattern: &GraphPattern,
        mut solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!(
            "Start processing graph pattern {:?} at context: {}",
            graph_pattern,
            context.as_str()
        );
        let sm = match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let patterns = if let Some(fts_index) =
                    self.fts_index.get(&NamedGraph::DefaultGraph)
                {
                    let (patterns, fts_solution_mappings) = fts_index.lookup_from_triple_patterns(
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
                };

                pushdowns.add_patterns_pushdowns(&patterns);
                let mut updated_solution_mappings = solution_mappings;
                let bgp_context = context.extension_with(PathEntry::BGP);
                let ordered_patterns =
                    order_triple_patterns(&patterns, &updated_solution_mappings, &pushdowns);
                for tp in &ordered_patterns {
                    updated_solution_mappings = Some(self.lazy_triple_pattern(
                        updated_solution_mappings,
                        tp,
                        &bgp_context,
                        &mut pushdowns,
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
            GraphPattern::Graph { name: _, inner: _ } => {
                todo!("Graphs not supported yet")
            }
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
                info!("Reduced has no practical effect in this implementation");
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
                let mut newsols = self.lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    &context.extension_with(PathEntry::ReducedInner),
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                if let Some(length) = length {
                    newsols.mappings = newsols.mappings.slice(*start as i64, *length as u32);
                } else {
                    newsols.mappings = newsols.mappings.slice(*start as i64, u32::MAX);
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
            GraphPattern::Service { .. } => {
                unimplemented!("Services are not implemented")
            }
            GraphPattern::DT { .. } => {
                panic!()
            }
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
