use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::trace;
use oxrdf::Variable;

use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::{group_by, join, prepare_group_by};
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_group(
        &self,
        inner: &GraphPattern,
        variables: &[Variable],
        aggregates: &[(Variable, AggregateExpression)],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GroupInner);
        pushdowns.limit_to_variables(variables);
        pushdowns.add_graph_pattern_pushdowns(inner);
        let output_solution_mappings = self.lazy_graph_pattern(
            inner,
            None,
            &inner_context,
            parameters,
            pushdowns,
            query_settings,
        )?;
        let (mut output_solution_mappings, by, dummy_varname) =
            prepare_group_by(output_solution_mappings, variables);
        let mut aggregate_expressions = vec![];
        let mut new_rdf_node_types = HashMap::new();
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            //(aggregate_solution_mappings, expr, used_context, datatype)
            let AggregateReturn {
                solution_mappings: aggregate_solution_mappings,
                expr,
                context: _,
                rdf_node_type,
            } = self.sparql_aggregate_expression_as_lazy_column_and_expression(
                v,
                a,
                output_solution_mappings,
                &aggregate_context,
                parameters,
                query_settings,
            )?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.as_str().to_string(), rdf_node_type);
            aggregate_expressions.push(expr);
        }
        let grouped = group_by(
            output_solution_mappings,
            aggregate_expressions,
            by,
            dummy_varname,
            new_rdf_node_types,
        )?;
        let solution_mappings = if let Some(solution_mappings) = solution_mappings {
            join(
                solution_mappings,
                grouped,
                JoinType::Inner,
                self.global_cats.clone(),
            )?
        } else {
            grouped
        };
        Ok(solution_mappings)
    }
}
