use super::{QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;

use polars::prelude::{col, Expr, SortMultipleOptions};
use query_processing::aggregates::{
    avg, count_with_expression, count_without_expression, group_concat_sorted,
    list_aggregation_sorted, max, min, sample_sorted, statistical_aggregate, sum, AggregateOrder,
    AggregateReturn,
};
use query_processing::constants::{LIST_AGGREGATION, NEST_AGGREGATION};
use query_processing::graph_patterns::{ordering_exprs, sort_options};
use spargebra::algebra::OrderExpression;
use spargebra::window::OrderedAggregate;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{AggregateExpression, AggregateFunction};
use spargebra::statistics::StatisticalAggregate;
use std::collections::HashMap;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    pub fn sparql_aggregate_expression_as_lazy_column_and_expression(
        &self,
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<AggregateReturn, SparqlError> {
        let output_solution_mappings;
        let mut out_expr;
        let column_context;
        let out_rdf_node_type;
        let mut order: AggregateOrder = None;
        match aggregate_expression {
            AggregateExpression::CountSolutions { distinct } => {
                output_solution_mappings = solution_mappings;
                column_context = None;
                (out_expr, out_rdf_node_type) =
                    count_without_expression(*distinct, &output_solution_mappings.rdf_node_types);
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                let ordered = OrderedAggregate::from_expression(expr);
                let mut sm = self.lazy_expression(
                    ordered.as_ref().map(|o| &o.expr).unwrap_or(expr),
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                    parameters,
                    None,
                    query_settings,
                    dataset,
                )?;
                if let Some(ordered) = &ordered {
                    let (ordered_sm, by) = self.aggregate_order(
                        ordered,
                        sm,
                        column_context.as_ref().unwrap(),
                        parameters,
                        query_settings,
                        dataset,
                    )?;
                    sm = ordered_sm;
                    order = Some(by);
                }
                output_solution_mappings = sm;
                match name {
                    AggregateFunction::Count => {
                        (out_expr, out_rdf_node_type) = count_with_expression(
                            &output_solution_mappings,
                            column_context.as_ref().unwrap(),
                            *distinct,
                        );
                    }
                    AggregateFunction::Sum => {
                        (out_expr, out_rdf_node_type) = sum(
                            &output_solution_mappings,
                            column_context.as_ref().unwrap(),
                            *distinct,
                        );
                    }
                    AggregateFunction::Avg => {
                        (out_expr, out_rdf_node_type) = avg(
                            &output_solution_mappings,
                            column_context.as_ref().unwrap(),
                            *distinct,
                        );
                    }
                    AggregateFunction::Min => {
                        (out_expr, out_rdf_node_type) =
                            min(
                                &output_solution_mappings,
                                column_context.as_ref().unwrap(),
                                self.global_cats.clone(),
                            );
                    }
                    AggregateFunction::Max => {
                        (out_expr, out_rdf_node_type) =
                            max(
                                &output_solution_mappings,
                                column_context.as_ref().unwrap(),
                                self.global_cats.clone(),
                            );
                    }
                    AggregateFunction::GroupConcat { separator } => {
                        (out_expr, out_rdf_node_type) = group_concat_sorted(
                            &output_solution_mappings,
                            column_context.as_ref().unwrap(),
                            separator,
                            *distinct,
                            self.global_cats.clone(),
                            order,
                        );
                    }
                    AggregateFunction::Sample => {
                        (out_expr, out_rdf_node_type) = sample_sorted(
                            &output_solution_mappings,
                            column_context.as_ref().unwrap(),
                            order,
                        );
                    }
                    AggregateFunction::Custom(name) => {
                        if name.as_str() == LIST_AGGREGATION || name.as_str() == NEST_AGGREGATION {
                            (out_expr, out_rdf_node_type) = list_aggregation_sorted(
                                &output_solution_mappings,
                                column_context.as_ref().unwrap(),
                                order,
                            );
                        } else if let Some(aggregate) =
                            StatisticalAggregate::from_iri(name.as_str())
                        {
                            // The order of the values does not change the statistics
                            (out_expr, out_rdf_node_type) = statistical_aggregate(
                                &output_solution_mappings,
                                column_context.as_ref().unwrap(),
                                *distinct,
                                aggregate,
                            );
                        } else {
                            return Err(SparqlError::NotSupportedYet(format!(
                                "Custom aggregate function {name}"
                            )));
                        }
                    }
                }
            }
        }
        out_expr = out_expr.alias(variable.as_str());
        Ok(AggregateReturn {
            solution_mappings: output_solution_mappings,
            expr: out_expr,
            context: column_context,
            rdf_node_type: out_rdf_node_type,
        })
    }

    /// Evaluates the ORDER BY of an ordered aggregate into sort key columns, which are not
    /// solution variables, so they are not added to the types and are dropped by the grouping.
    fn aggregate_order(
        &self,
        ordered: &OrderedAggregate,
        mut solution_mappings: SolutionMappings,
        column_context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<(SolutionMappings, (Vec<Expr>, SortMultipleOptions)), SparqlError> {
        let mut columns = vec![];
        let mut ascending = vec![];
        for (i, o) in ordered.order_by.iter().enumerate() {
            let (e, asc) = match o {
                OrderExpression::Asc(e) => (e, true),
                OrderExpression::Desc(e) => (e, false),
            };
            let c = column_context.extension_with(PathEntry::OrderByExpression(i as u16));
            solution_mappings = self.lazy_expression(
                e,
                solution_mappings,
                &c,
                parameters,
                None,
                query_settings,
                dataset,
            )?;
            columns.push(c.as_str().to_string());
            ascending.push(asc);
        }
        let (keys, key_ascending) = ordering_exprs(
            &solution_mappings,
            &columns,
            ascending,
            self.global_cats.clone(),
        );
        let key_names: Vec<String> = (0..keys.len())
            .map(|i| format!("{}_key{i}", column_context.as_str()))
            .collect();
        solution_mappings.mappings = solution_mappings.mappings.with_columns(
            keys.into_iter()
                .zip(&key_names)
                .map(|(k, n)| k.alias(n))
                .collect::<Vec<_>>(),
        );
        for c in &columns {
            solution_mappings.rdf_node_types.remove(c);
        }
        let by = key_names.iter().map(|n| col(n.as_str())).collect();
        Ok((
            solution_mappings,
            (by, sort_options(&key_ascending).with_maintain_order(true)),
        ))
    }
}
