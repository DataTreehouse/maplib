mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
pub(crate) mod load_tt;
mod minus;
mod order_by;
mod path;
mod project;
mod pvalues;
mod triple;
mod union;
mod values;

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::{debug, info};

use oxrdf::vocab::xsd;
use polars::prelude::IntoLazy;
use polars_core::frame::DataFrame;
use polars_core::prelude::{NamedFrom, Series};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeType;
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;

impl Triplestore {
    pub fn lazy_graph_pattern(
        &self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!(
            "Start processing graph pattern {:?} at context: {}",
            graph_pattern,
            context.as_str()
        );
        let sm = match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let mut updated_solution_mappings = solution_mappings;
                let bgp_context = context.extension_with(PathEntry::BGP);
                for tp in patterns {
                    updated_solution_mappings = Some(self.lazy_triple_pattern(
                        updated_solution_mappings,
                        tp,
                        &bgp_context,
                    )?);
                }
                if let Some(updated_solution_mappings) = updated_solution_mappings {
                    Ok(updated_solution_mappings)
                } else {
                    //TODO: FIX THIS PROPERLY
                    let ser = Series::new("DUMMYDUMMY", vec![true]);
                    let mut map = HashMap::new();
                    map.insert(
                        "DUMMYDUMMY".to_string(),
                        RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                    );
                    Ok(SolutionMappings {
                        mappings: DataFrame::new(vec![ser]).unwrap().lazy(),
                        rdf_node_types: map,
                    })
                }
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.lazy_path(subject, path, object, solution_mappings, context),
            GraphPattern::Join { left, right } => {
                self.lazy_join(left, right, solution_mappings, context, parameters)
            }
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
            ),
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(inner, expr, solution_mappings, context, parameters)
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(left, right, solution_mappings, context, parameters)
            }
            GraphPattern::Graph { name: _, inner: _ } => {
                unimplemented!("Graphs not supported")
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
            ),
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(left, right, solution_mappings, context, parameters)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => self.lazy_values(solution_mappings, variables, bindings, context),
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(inner, expression, solution_mappings, context, parameters)
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(inner, variables, solution_mappings, context, parameters)
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(inner, solution_mappings, context, parameters)
            }
            GraphPattern::Reduced { inner } => {
                info!("Reduced has no practical effect in this implementation");
                self.lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    &context.extension_with(PathEntry::ReducedInner),
                    parameters,
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
            ),
        };
        debug!(
            "Finish processing graph pattern {:?} at context: {}",
            graph_pattern,
            context.as_str()
        );
        sm
    }
}
