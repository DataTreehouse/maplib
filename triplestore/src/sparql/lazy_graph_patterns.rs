mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
pub(crate) mod load_tt;
mod minus;
mod order_by;
pub(crate) mod ordering;
mod path;
mod project;
mod triple;
mod union;
mod values;

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::multitype::MultiType;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::{debug, info};
use oxrdf::Literal;
use polars_core::prelude::ObjectChunked;
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub fn lazy_graph_pattern(
        &self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing graph pattern at context: {}", context.as_str());

        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let v = vec![MultiType::Literal(Literal::new_simple_literal("abc"))];
                ObjectChunked::new_from_vec("chunky", v);
                let mut updated_solution_mappings = solution_mappings;
                let bgp_context = context.extension_with(PathEntry::Bgp);
                for tp in patterns {
                    updated_solution_mappings = Some(self.lazy_triple_pattern(
                        updated_solution_mappings,
                        tp,
                        &bgp_context,
                    )?);
                }
                Ok(updated_solution_mappings.unwrap())
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.lazy_path(subject, path, object, solution_mappings, context),
            GraphPattern::Join { left, right } => {
                self.lazy_join(left, right, solution_mappings, context)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.lazy_left_join(left, right, expression, solution_mappings, context),
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(inner, expr, solution_mappings, context)
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(left, right, solution_mappings, context)
            }
            GraphPattern::Graph { name: _, inner: _ } => {
                unimplemented!("Graphs not supported")
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.lazy_extend(inner, variable, expression, solution_mappings, context),
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(left, right, solution_mappings, context)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => self.lazy_values(solution_mappings, variables, bindings, context),
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(inner, expression, solution_mappings, context)
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(inner, variables, solution_mappings, context)
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(inner, solution_mappings, context)
            }
            GraphPattern::Reduced { inner } => {
                info!("Reduced has no practical effect in this implementation");
                self.lazy_graph_pattern(
                    inner,
                    solution_mappings,
                    &context.extension_with(PathEntry::ReducedInner),
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
            } => self.lazy_group(inner, variables, aggregates, solution_mappings, context),
            GraphPattern::Service { .. } => {
                unimplemented!("Services are not implemented")
            }
        }
    }
}
