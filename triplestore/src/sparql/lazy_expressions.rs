mod exists_helper;

use super::Triplestore;
use std::collections::HashMap;

use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_expressions::exists_helper::rewrite_exists_graph_pattern;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::lazy::dsl::is_not_null;
use polars::prelude::{
    col, concat_str, is_in, lit, Expr, IntoLazy, LiteralValue, Operator, Series, UniqueKeepStrategy,
};
use representation::RDFNodeType;
use spargebra::algebra::{Expression, Function};

impl Triplestore {
    pub fn lazy_expression(
        &self,
        expr: &Expression,
        mut solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let output_solution_mappings = match expr {
            Expression::NamedNode(nn) => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    Expr::Literal(sparql_named_node_to_polars_literal_value(nn))
                        .alias(context.as_str()),
                );
                solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), RDFNodeType::IRI);
                solution_mappings
            }
            Expression::Literal(lit) => {
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    Expr::Literal(sparql_literal_to_polars_literal_value(lit))
                        .alias(context.as_str()),
                );
                solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(lit.datatype().into_owned()),
                );
                solution_mappings
            }
            Expression::Variable(v) => {
                if !solution_mappings.columns.contains(v.as_str()) {
                    return Err(SparqlError::VariableNotFound(
                        v.as_str().to_string(),
                        context.as_str().to_string(),
                    ));
                }
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(v.as_str()).alias(context.as_str()));
                let existing_type = solution_mappings.rdf_node_types.get(v.as_str()).unwrap();
                solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), existing_type.clone());
                solution_mappings
            }
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::OrRight);

                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Or,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::AndRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::And,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::EqualRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::GreaterRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Gt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::GtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::LessRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Lt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::LtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::In(left, right) => {
                let left_context = context.extension_with(PathEntry::InLeft);
                let right_contexts: Vec<Context> = (0..right.len())
                    .map(|i| context.extension_with(PathEntry::InRight(i as u16)))
                    .collect();
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    let expr_context = right_contexts.get(i).unwrap();
                    output_solution_mappings =
                        self.lazy_expression(expr, output_solution_mappings, expr_context)?;
                }
                let mut expr = Expr::Literal(LiteralValue::Boolean(false));

                for right_context in &right_contexts {
                    expr = Expr::BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        }),
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(expr.alias(context.as_str()))
                    .drop_columns([left_context.as_str()])
                    .drop_columns(
                        right_contexts
                            .iter()
                            .map(|x| x.as_str())
                            .collect::<Vec<&str>>(),
                    );
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::AddRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Plus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                let left_type = output_solution_mappings
                    .rdf_node_types
                    .get(left_context.as_str())
                    .unwrap();
                let right_type = output_solution_mappings
                    .rdf_node_types
                    .get(right_context.as_str())
                    .unwrap();
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    binop_type(left_type, right_type),
                );
                output_solution_mappings
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::SubtractRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Minus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                let left_type = output_solution_mappings
                    .rdf_node_types
                    .get(left_context.as_str())
                    .unwrap();
                let right_type = output_solution_mappings
                    .rdf_node_types
                    .get(right_context.as_str())
                    .unwrap();
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    binop_type(left_type, right_type),
                );
                output_solution_mappings
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Multiply,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                let left_type = output_solution_mappings
                    .rdf_node_types
                    .get(left_context.as_str())
                    .unwrap();
                let right_type = output_solution_mappings
                    .rdf_node_types
                    .get(right_context.as_str())
                    .unwrap();
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    binop_type(left_type, right_type),
                );
                output_solution_mappings
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let right_context = context.extension_with(PathEntry::DivideRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Divide,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                //Todo: probably not true..
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::DOUBLE.into_owned()),
                );
                output_solution_mappings
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);

                let mut output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &plus_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Plus,
                            right: Box::new(col(plus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&plus_context.as_str()]);
                let existing_type = output_solution_mappings
                    .rdf_node_types
                    .get(plus_context.as_str())
                    .unwrap();
                output_solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), existing_type.clone());
                output_solution_mappings
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let mut output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &minus_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Minus,
                            right: Box::new(col(minus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&minus_context.as_str()]);
                let existing_type = output_solution_mappings
                    .rdf_node_types
                    .get(minus_context.as_str())
                    .unwrap();
                output_solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), existing_type.clone());
                output_solution_mappings
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let mut output_solution_mappings =
                    self.lazy_expression(inner, solution_mappings, &not_context)?;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(col(not_context.as_str()).not().alias(context.as_str()))
                    .drop_columns([&not_context.as_str()]);
                output_solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                output_solution_mappings
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                let mut output_solution_mappings = solution_mappings;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(exists_context.as_str()),
                    )
                    .with_column(col(exists_context.as_str()).cumsum(false));

                let new_inner = rewrite_exists_graph_pattern(inner, exists_context.as_str());
                let SolutionMappings {
                    mappings: exists_lf,
                    ..
                } = self.lazy_graph_pattern(
                    &new_inner,
                    Some(output_solution_mappings.clone()),
                    &exists_context,
                )?;
                let SolutionMappings {
                    mappings,
                    columns,
                    mut rdf_node_types,
                } = output_solution_mappings;
                let mut df = mappings.collect().unwrap();
                let exists_df = exists_lf
                    .select([col(exists_context.as_str())])
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .expect("Collect lazy exists error");
                let mut ser = Series::from(
                    is_in(
                        df.column(exists_context.as_str()).unwrap(),
                        exists_df.column(exists_context.as_str()).unwrap(),
                    )
                    .unwrap(),
                );
                ser.rename(context.as_str());
                df.with_column(ser).unwrap();
                df = df.drop(exists_context.as_str()).unwrap();
                rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                SolutionMappings::new(df.lazy(), columns, rdf_node_types)
            }
            Expression::Bound(v) => {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(col(v.as_str()).is_null().alias(context.as_str()));
                solution_mappings.rdf_node_types.insert(
                    context.as_str().to_string(),
                    RDFNodeType::Literal(xsd::BOOLEAN.into_owned()),
                );
                solution_mappings
            }
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let mut output_solution_mappings =
                    self.lazy_expression(left, solution_mappings, &left_context)?;
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                output_solution_mappings =
                    self.lazy_expression(middle, output_solution_mappings, &middle_context)?;
                let right_context = context.extension_with(PathEntry::IfRight);
                output_solution_mappings =
                    self.lazy_expression(right, output_solution_mappings, &right_context)?;

                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(
                        (Expr::Ternary {
                            predicate: Box::new(col(left_context.as_str())),
                            truthy: Box::new(col(middle_context.as_str())),
                            falsy: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([
                        left_context.as_str(),
                        middle_context.as_str(),
                        right_context.as_str(),
                    ]);
                //Todo: generalize..
                let existing_type = output_solution_mappings
                    .rdf_node_types
                    .get(middle_context.as_str())
                    .unwrap();
                output_solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), existing_type.clone());
                output_solution_mappings
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
                    )?;
                }

                let coalesced_context = inner_contexts.get(0).unwrap();
                let mut coalesced = col(coalesced_context.as_str());
                for c in &inner_contexts[1..inner_contexts.len()] {
                    coalesced = Expr::Ternary {
                        predicate: Box::new(is_not_null(coalesced.clone())),
                        truthy: Box::new(coalesced.clone()),
                        falsy: Box::new(col(c.as_str())),
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(coalesced.alias(context.as_str()))
                    .drop_columns(
                        inner_contexts
                            .iter()
                            .map(|c| c.as_str())
                            .collect::<Vec<&str>>(),
                    );
                //TODO: generalize
                let existing_type = output_solution_mappings
                    .rdf_node_types
                    .get(inner_contexts.get(0).unwrap().as_str())
                    .unwrap();
                output_solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), existing_type.clone());
                output_solution_mappings
            }
            Expression::FunctionCall(func, args) => {
                let mut args_contexts: HashMap<usize, Context> = HashMap::new();
                let mut output_solution_mappings = solution_mappings;
                for i in 0..args.len() {
                    let arg = args.get(i).unwrap();
                    if let Expression::Literal(_) = arg {
                        // No operation here..
                    } else {
                        let arg_context = context.extension_with(PathEntry::FunctionCall(i as u16));
                        output_solution_mappings = self.lazy_expression(
                            args.get(i).unwrap(),
                            output_solution_mappings,
                            &arg_context,
                        )?;
                        args_contexts.insert(i, arg_context);
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.collect().unwrap().lazy();
                        //TODO: workaround for stack overflow - post bug?
                    }
                }
                match func {
                    Function::Year => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .year()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Month => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .month()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Day => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .day()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Hours => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .hour()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Minutes => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .minute()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Seconds => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str())
                                    .dt()
                                    .second()
                                    .alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
                        );
                    }
                    Function::Abs => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings = output_solution_mappings
                            .mappings
                            .with_column(col(first_context.as_str()).abs().alias(context.as_str()));
                        let existing_type = output_solution_mappings
                            .rdf_node_types
                            .get(first_context.as_str())
                            .unwrap();
                        output_solution_mappings
                            .rdf_node_types
                            .insert(context.as_str().to_string(), existing_type.clone());
                    }
                    Function::Ceil => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str()).ceil().alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                        );
                    }
                    Function::Floor => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str()).floor().alias(context.as_str()),
                            );
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                        );
                    }
                    Function::Concat => {
                        assert!(args.len() > 1);
                        let SolutionMappings {
                            mappings,
                            columns,
                            rdf_node_types: datatypes,
                        } = output_solution_mappings;
                        let cols: Vec<_> = (0..args.len())
                            .map(|i| col(args_contexts.get(&i).unwrap().as_str()))
                            .collect();
                        let new_mappings =
                            mappings.with_column(concat_str(cols, "").alias(context.as_str()));
                        output_solution_mappings =
                            SolutionMappings::new(new_mappings, columns, datatypes);
                        output_solution_mappings.rdf_node_types.insert(
                            context.as_str().to_string(),
                            RDFNodeType::Literal(xsd::STRING.into_owned()),
                        );
                    }
                    Function::Round => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(&0).unwrap();
                        output_solution_mappings.mappings =
                            output_solution_mappings.mappings.with_column(
                                col(first_context.as_str()).round(0).alias(context.as_str()),
                            );
                        let existing_type = output_solution_mappings
                            .rdf_node_types
                            .get(first_context.as_str())
                            .unwrap();
                        output_solution_mappings
                            .rdf_node_types
                            .insert(context.as_str().to_string(), existing_type.clone());
                    }
                    Function::Regex => {
                        if args.len() != 2 {
                            todo!("Unsupported amount of regex args {:?}", args);
                        } else {
                            let first_context = args_contexts.get(&0).unwrap();
                            if let Expression::Literal(l) = args.get(1).unwrap() {
                                output_solution_mappings.mappings =
                                    output_solution_mappings.mappings.with_column(
                                        col(first_context.as_str())
                                            .str()
                                            .contains(lit(l.value()), false)
                                            .alias(context.as_str()),
                                    );
                                output_solution_mappings.rdf_node_types.insert(
                                    context.as_str().to_string(),
                                    RDFNodeType::Literal(xsd::STRING.into_owned()),
                                );
                            }
                        }
                    }
                    Function::Custom(nn) => {
                        let iri = nn.as_str();
                        if iri == xsd::INTEGER.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(&0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(first_context.as_str())
                                        .cast(DataType::Int64)
                                        .alias(context.as_str()),
                                );
                            output_solution_mappings.rdf_node_types.insert(
                                context.as_str().to_string(),
                                RDFNodeType::Literal(xsd::INTEGER.into_owned()),
                            );
                        } else if iri == xsd::STRING.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(&0).unwrap();
                            output_solution_mappings.mappings =
                                output_solution_mappings.mappings.with_column(
                                    col(first_context.as_str())
                                        .cast(DataType::Utf8)
                                        .alias(context.as_str()),
                                );
                            output_solution_mappings.rdf_node_types.insert(
                                context.as_str().to_string(),
                                RDFNodeType::Literal(xsd::STRING.into_owned()),
                            );
                        } else {
                            todo!("{:?}", nn)
                        }
                    }
                    _ => {
                        todo!()
                    }
                }
                output_solution_mappings.mappings = output_solution_mappings.mappings.drop_columns(
                    args_contexts
                        .values()
                        .map(|x| x.as_str())
                        .collect::<Vec<&str>>(),
                );
                output_solution_mappings
            }
        };
        Ok(output_solution_mappings)
    }
}

fn binop_type(left_type: &RDFNodeType, right_type: &RDFNodeType) -> RDFNodeType {
    if let (RDFNodeType::Literal(left_lit), RDFNodeType::Literal(right_lit)) =
        (left_type, right_type)
    {
        if left_lit.as_ref() == xsd::DOUBLE {
            left_type.clone()
        } else if right_lit.as_ref() == xsd::DOUBLE {
            return right_type.clone();
        } else if left_lit.as_ref() == xsd::FLOAT {
            return left_type.clone();
        } else if right_lit.as_ref() == xsd::FLOAT {
            return right_type.clone();
        } else {
            return RDFNodeType::Literal(xsd::INTEGER.into_owned());
        }
    } else {
        panic!("Incompatible types for operation")
    }
}
