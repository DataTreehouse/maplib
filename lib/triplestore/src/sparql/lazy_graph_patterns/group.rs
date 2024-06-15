use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;
use oxrdf::Variable;

use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::{group_by, prepare_group_by};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_group(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        solution_mapping: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GroupInner);
        let output_solution_mappings =
            self.lazy_graph_pattern(inner, solution_mapping, &inner_context, parameters)?;
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
            )?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.as_str().to_string(), rdf_node_type);
            aggregate_expressions.push(expr);
        }
        Ok(group_by(
            output_solution_mappings,
            aggregate_expressions,
            by,
            dummy_varname,
            new_rdf_node_types,
        )?)
    }
}
