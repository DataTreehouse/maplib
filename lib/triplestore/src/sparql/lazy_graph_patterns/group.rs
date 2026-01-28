use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::{group_by, join, prepare_group_by};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use std::collections::HashMap;
use crate::errors::TriplestoreError;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
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
        dataset: &QueryGraph,
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
            dataset,
        )?;
        let (mut output_solution_mappings, by, dummy_varname) =
            prepare_group_by(output_solution_mappings, variables);
        let mut aggregate_expressions = vec![];
        let mut new_rdf_node_types = HashMap::new();
        for v in variables {
            let maybe_t = output_solution_mappings
                .rdf_node_types
                .get(v.as_str());
            if let Some(t) = maybe_t {
            new_rdf_node_types.insert(
                v.as_str().to_string(),
                t.clone(),
            ); }else {
                return Err(SparqlError::GroupByWithUndefinedVariable(v.clone(), inner.clone()));    
            }
            }
        let mut aggregate_contexts = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let AggregateReturn {
                solution_mappings: aggregate_solution_mappings,
                expr,
                context: c,
                rdf_node_type,
            } = self.sparql_aggregate_expression_as_lazy_column_and_expression(
                v,
                a,
                output_solution_mappings,
                &aggregate_context,
                parameters,
                query_settings,
                dataset,
            )?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.as_str().to_string(), rdf_node_type);
            aggregate_expressions.push(expr);
            if let Some(c) = c {
                aggregate_contexts.push(c);
            }
        }
        output_solution_mappings.rdf_node_types = new_rdf_node_types;
        let mut grouped = group_by(
            output_solution_mappings,
            aggregate_expressions,
            by,
            dummy_varname,
        )?;
        for a in aggregate_contexts {
            grouped.rdf_node_types.remove(a.as_str());
        }
        let solution_mappings = if let Some(solution_mappings) = solution_mappings {
            join(
                solution_mappings,
                grouped,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?
        } else {
            grouped
        };
        Ok(solution_mappings)
    }
}
