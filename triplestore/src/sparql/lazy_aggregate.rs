use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::{col, str_concat, DataType, Expr, GetOutput, IntoSeries};
use representation::RDFNodeType;
use spargebra::algebra::AggregateExpression;

pub struct AggregateReturn {
    pub solution_mappings: SolutionMappings,
    pub expr: Expr,
    pub context: Option<Context>,
    pub rdf_node_type: RDFNodeType,
}

impl Triplestore {
    pub fn sparql_aggregate_expression_as_lazy_column_and_expression(
        &self,
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<AggregateReturn, SparqlError> {
        let output_solution_mappings;
        let mut out_expr;
        let column_context;
        let out_rdf_node_type;
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                out_rdf_node_type = RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned());
                if let Some(some_expr) = expr {
                    column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                    output_solution_mappings = self.lazy_expression(
                        some_expr,
                        solution_mappings,
                        column_context.as_ref().unwrap(),
                    )?;
                    if *distinct {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).n_unique();
                    } else {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).count();
                    }
                } else {
                    output_solution_mappings = solution_mappings;
                    column_context = None;
                    let all_proper_column_names: Vec<String> =
                        output_solution_mappings.columns.iter().cloned().collect();
                    let columns_expr = Expr::Columns(all_proper_column_names);
                    if *distinct {
                        out_expr = columns_expr.n_unique();
                    } else {
                        out_expr = columns_expr.unique();
                    }
                }
            }
            AggregateExpression::Sum { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                let expr_rdf_node_type = rdf_node_type_from_context(
                    column_context.as_ref().unwrap(),
                    &output_solution_mappings,
                );
                if expr_rdf_node_type.is_bool() {
                    out_rdf_node_type = RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
                } else {
                    out_rdf_node_type = expr_rdf_node_type.clone()
                }

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .sum();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).sum();
                }
            }
            AggregateExpression::Avg { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                let expr_rdf_node_type = rdf_node_type_from_context(
                    column_context.as_ref().unwrap(),
                    &output_solution_mappings,
                );
                if expr_rdf_node_type.is_float() {
                    out_rdf_node_type = expr_rdf_node_type.clone();
                } else {
                    out_rdf_node_type = RDFNodeType::Literal(xsd::DOUBLE.into_owned());
                }

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .mean();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).mean();
                }
            }
            AggregateExpression::Min { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                out_rdf_node_type = rdf_node_type_from_context(
                    column_context.as_ref().unwrap(),
                    &output_solution_mappings,
                )
                .clone();

                out_expr = col(column_context.as_ref().unwrap().as_str()).min();
            }
            AggregateExpression::Max { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                out_rdf_node_type = rdf_node_type_from_context(
                    column_context.as_ref().unwrap(),
                    &output_solution_mappings,
                )
                .clone();

                out_expr = col(column_context.as_ref().unwrap().as_str()).max();
            }
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                out_rdf_node_type = RDFNodeType::Literal(xsd::STRING.into_owned());

                let use_sep = if let Some(sep) = separator {
                    sep.to_string()
                } else {
                    "".to_string()
                };
                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::Utf8)
                        .list()
                        .0
                        .apply(
                            move |s| {
                                Ok(Some(
                                    str_concat(
                                        s.unique_stable()
                                            .expect("Unique stable error")
                                            .utf8()
                                            .unwrap(),
                                        use_sep.as_str(),
                                    )
                                    .into_series(),
                                ))
                            },
                            GetOutput::from_type(DataType::Utf8),
                        )
                        .first();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::Utf8)
                        .list()
                        .0
                        .apply(
                            move |s| {
                                Ok(Some(
                                    str_concat(s.utf8().unwrap(), use_sep.as_str()).into_series(),
                                ))
                            },
                            GetOutput::from_type(DataType::Utf8),
                        )
                        .first();
                }
            }
            AggregateExpression::Sample { expr, .. } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                output_solution_mappings = self.lazy_expression(
                    expr,
                    solution_mappings,
                    column_context.as_ref().unwrap(),
                )?;
                out_rdf_node_type = rdf_node_type_from_context(
                    column_context.as_ref().unwrap(),
                    &output_solution_mappings,
                )
                .clone();

                out_expr = col(column_context.as_ref().unwrap().as_str()).first();
            }
            AggregateExpression::Custom {
                name,
                expr: _,
                distinct: _,
            } => {
                panic!("Custom aggregation {} not supported", name);
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
}

fn rdf_node_type_from_context<'a>(
    context: &'_ Context,
    solution_mappings: &'a SolutionMappings,
) -> &'a RDFNodeType {
    let datatype = solution_mappings
        .rdf_node_types
        .get(context.as_str())
        .unwrap();
    datatype
}
