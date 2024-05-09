use super::Triplestore;
use std::collections::HashMap;

use crate::sparql::errors::SparqlError;
use oxrdf::vocab::xsd;
use polars::prelude::{col, Expr, LiteralValue, Operator};

use query_processing::exists_helper::rewrite_exists_graph_pattern;
use query_processing::expressions::{
    binary_expression, bound, coalesce_expression, exists, func_expression, if_expression,
    in_expression, literal, named_node, not_expression, unary_minus, unary_plus, variable,
};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeType;
use spargebra::algebra::Expression;

impl Triplestore {
    pub fn lazy_expression(
        &self,
        expr: &Expression,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        let output_solution_mappings = match expr {
            Expression::NamedNode(nn) => named_node(solution_mappings, nn, context)?,
            Expression::Literal(lit) => literal(solution_mappings, lit, context)?,
            Expression::Variable(v) => variable(solution_mappings, v, context)?,
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::OrRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Or,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::AndRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::And,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::EqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Eq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::GreaterRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Gt,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;

                binary_expression(
                    output_solution_mappings,
                    Operator::GtEq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::LessRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Lt,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::LtEq,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::In(left, right) => {
                let left_context = context.extension_with(PathEntry::InLeft);
                let right_contexts: Vec<Context> = (0..right.len())
                    .map(|i| context.extension_with(PathEntry::InRight(i as u16)))
                    .collect();
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    let expr_context = right_contexts.get(i).unwrap();
                    output_solution_mappings = self.lazy_expression(
                        expr,
                        output_solution_mappings,
                        expr_context,
                        parameters,
                    )?;
                }
                in_expression(
                    output_solution_mappings,
                    &left_context,
                    &right_contexts,
                    &context,
                )?
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::AddRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Plus,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::SubtractRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Minus,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                binary_expression(
                    output_solution_mappings,
                    Operator::Multiply,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let right_context = context.extension_with(PathEntry::DivideRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;

                binary_expression(
                    output_solution_mappings,
                    Operator::Divide,
                    &left_context,
                    &right_context,
                    context,
                )?
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);

                let output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &plus_context, parameters)?;
                unary_plus(output_solution_mappings, &plus_context, context)?
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &minus_context, parameters)?;
                unary_minus(output_solution_mappings, &minus_context, context)?
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &not_context, parameters)?;
                not_expression(output_solution_mappings, &not_context, context)?
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                let mut output_solution_mappings = solution_mappings;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(exists_context.as_str()),
                    )
                    .with_column(col(exists_context.as_str()).cum_sum(false));
                output_solution_mappings.rdf_node_types.insert(
                    exists_context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                let new_inner = rewrite_exists_graph_pattern(inner, exists_context.as_str());
                let SolutionMappings {
                    mappings: exists_lf,
                    ..
                } = self.lazy_graph_pattern(
                    &new_inner,
                    Some(output_solution_mappings.clone()),
                    &exists_context,
                    parameters,
                )?;
                exists(
                    output_solution_mappings,
                    exists_lf,
                    &exists_context,
                    &context,
                )?
            }
            Expression::Bound(v) => bound(solution_mappings, v, &context)?,
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context, parameters)?;
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                output_solution_mappings = self.lazy_expression(
                    middle,
                    output_solution_mappings,
                    &middle_context,
                    parameters,
                )?;
                let right_context = context.extension_with(PathEntry::IfRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                )?;
                if_expression(
                    output_solution_mappings,
                    &left_context,
                    &middle_context,
                    &right_context,
                    &context,
                )?
            }
            Expression::Coalesce(inner) => {
                let inner_contexts: Vec<Context> = (0..inner.len())
                    .map(|i| context.extension_with(PathEntry::Coalesce(i as u16)))
                    .collect();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..inner.len() {
                    let inner_context = inner_contexts.get(i).unwrap();
                    output_solution_mappings = self.lazy_expression(
                        inner.get(i).unwrap(),
                        output_solution_mappings,
                        inner_context,
                        parameters,
                    )?;
                }
                coalesce_expression(output_solution_mappings, inner_contexts, &context)?
            }
            Expression::FunctionCall(func, args) => {
                let mut args_contexts: HashMap<usize, Context> = HashMap::new();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..args.len() {
                    let arg_context = context.extension_with(PathEntry::FunctionCall(i as u16));
                    output_solution_mappings = self.lazy_expression(
                        args.get(i).unwrap(),
                        output_solution_mappings,
                        &arg_context,
                        parameters,
                    )?;
                    args_contexts.insert(i, arg_context);
                }
                func_expression(
                    output_solution_mappings,
                    func,
                    args,
                    args_contexts,
                    &context,
                )?
            }
        };
        Ok(output_solution_mappings)
    }
}
