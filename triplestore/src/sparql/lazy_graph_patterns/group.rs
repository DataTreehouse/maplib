use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_aggregate::AggregateReturn;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::debug;
use oxrdf::Variable;
use polars::prelude::{col, lit, Expr};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use std::collections::HashMap;
use uuid::Uuid;

impl Triplestore {
    pub(crate) fn lazy_group(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        solution_mapping: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GroupInner);

        let mut output_solution_mappings =
            self.lazy_graph_pattern(inner, solution_mapping, &inner_context)?;
        let by: Vec<Expr>;
        let dummy_varname = Uuid::new_v4().to_string();
        if variables.is_empty() {
            by = vec![col(&dummy_varname)];
            output_solution_mappings.mappings = output_solution_mappings
                .mappings
                .with_column(lit(true).alias(&dummy_varname))
        } else {
            by = variables.iter().map(|v| col(v.as_str())).collect();
        };

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
            )?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.clone(), rdf_node_type);
            aggregate_expressions.push(expr);
        }
        let SolutionMappings {
            mut mappings,
            mut columns,
            rdf_node_types: mut datatypes,
        } = output_solution_mappings;
        let grouped_mappings = mappings.group_by(by.as_slice());

        mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
        for (k, v) in new_rdf_node_types {
            datatypes.insert(k.as_str().to_string(), v);
        }
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v, _) in aggregates {
            columns.insert(v.as_str().to_string());
        }
        if variables.is_empty() {
            mappings = mappings.drop_columns([dummy_varname]);
        }
        Ok(SolutionMappings::new(mappings, columns, datatypes))
    }
}
