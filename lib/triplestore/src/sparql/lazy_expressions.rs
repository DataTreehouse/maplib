use super::{QuerySettings, Triplestore};
use std::collections::{HashMap, HashSet};

use crate::sparql::errors::SparqlError;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode};
use polars::prelude::{by_name, col, len, lit, Expr, JoinType, LazyFrame, UniqueKeepStrategy};
use query_processing::expressions::functions::func_expression::func_expression;
use query_processing::expressions::functions::xsd_cast_literal::{
    is_xsd_cast_function, xsd_cast_chain_on_codes,
};
use query_processing::expressions::{
    binary_expression, bound, can_use_in_iris, coalesce_contexts, effective_boolean_value,
    if_expression, in_expression, in_integers_expression, in_integers_values, in_iris_expression,
    literal, named_node, not_expression, unary_minus, unary_plus, variable,
};
use query_processing::graph_patterns::{join, union};
use query_processing::pushdowns::{ExpressionInput, Pushdowns};
use representation::cats::LockedCats;
use representation::dataset::QueryGraph;
use representation::null::expr_is_null_workaround;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function, GraphPattern};

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    pub fn lazy_expression(
        &self,
        expr: &Expression,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let cats = self.global_cats.read().unwrap();
        let output_solution_mappings = match expr {
            Expression::NamedNode(nn) => named_node(solution_mappings, nn, context, &cats)?,
            Expression::Literal(lit) => literal(solution_mappings, lit, context, &cats)?,
            Expression::Variable(v) => variable(solution_mappings, v, context)?,
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::OrRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::AndRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::EqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::SameTerm(left, right) => {
                let left_context = context.extension_with(PathEntry::SameTermLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::SameTermRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::GreaterRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;

                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::LessRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::In(left, right) => {
                let left_context = context.extension_with(PathEntry::InLeft);
                let right_contexts: Vec<Context> = (0..right.len())
                    .map(|i| context.extension_with(PathEntry::InRight(i as u16)))
                    .collect();
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let iris: Option<Vec<&NamedNode>> = right
                    .iter()
                    .map(|e| match e {
                        Expression::NamedNode(nn) => Some(nn),
                        _ => None,
                    })
                    .collect();
                if let Some(iris) = iris {
                    if can_use_in_iris(&output_solution_mappings, &left_context) {
                        return Ok(in_iris_expression(
                            output_solution_mappings,
                            &left_context,
                            &iris,
                            context,
                            &cats,
                        ));
                    }
                }
                let literals: Option<Vec<&Literal>> = right
                    .iter()
                    .map(|e| match e {
                        Expression::Literal(l) => Some(l),
                        _ => None,
                    })
                    .collect();
                if let Some(values) = literals.and_then(|literals| {
                    in_integers_values(&output_solution_mappings, &left_context, &literals)
                }) {
                    return Ok(in_integers_expression(
                        output_solution_mappings,
                        &left_context,
                        values,
                        context,
                    ));
                }
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    let expr_context = right_contexts.get(i).unwrap();
                    output_solution_mappings = self.lazy_expression(
                        expr,
                        output_solution_mappings,
                        expr_context,
                        parameters,
                        pushdowns,
                        query_settings,
                        dataset,
                    )?;
                }
                in_expression(
                    output_solution_mappings,
                    &left_context,
                    &right_contexts,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::AddRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::SubtractRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::DivideRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;

                binary_expression(
                    output_solution_mappings,
                    expr,
                    &left_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);

                let output_solution_mappings = self.lazy_expression(
                    inner,
                    solution_mappings,
                    &plus_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                unary_plus(output_solution_mappings, &plus_context, context)?
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let output_solution_mappings = self.lazy_expression(
                    inner,
                    solution_mappings,
                    &minus_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                unary_minus(output_solution_mappings, &minus_context, context)?
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let mut output_solution_mappings = self.lazy_expression(
                    inner,
                    solution_mappings,
                    &not_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                output_solution_mappings = effective_boolean_value(
                    output_solution_mappings,
                    &not_context,
                    self.global_cats.clone(),
                );
                not_expression(output_solution_mappings, &not_context, context)?
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                self.lazy_exists(
                    inner,
                    solution_mappings,
                    pushdowns.and_then(|p| p.expression_input.as_ref()),
                    &exists_context,
                    context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?
            }
            Expression::Bound(v) => bound(solution_mappings, v, context)?,
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let mut output_solution_mappings = self.lazy_expression(
                    left,
                    solution_mappings,
                    &left_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                output_solution_mappings = self.lazy_expression(
                    middle,
                    output_solution_mappings,
                    &middle_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let right_context = context.extension_with(PathEntry::IfRight);
                output_solution_mappings = self.lazy_expression(
                    right,
                    output_solution_mappings,
                    &right_context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                if_expression(
                    output_solution_mappings,
                    &left_context,
                    &middle_context,
                    &right_context,
                    context,
                    self.global_cats.clone(),
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
                        pushdowns,
                        query_settings,
                        dataset,
                    )?;
                }
                coalesce_contexts(
                    output_solution_mappings,
                    inner_contexts,
                    context,
                    self.global_cats.clone(),
                )?
            }
            Expression::FunctionCall(Function::Custom(nn), _)
                if spargebra::window::is_window_function(nn.as_ref()) =>
            {
                let window = spargebra::window::WindowCall::from_expression(expr).ok_or_else(|| {
                    query_processing::errors::QueryProcessingError::UnsupportedWindowFunction(
                        expr.to_string(),
                    )
                })?;
                drop(cats);
                return self.lazy_window(
                    &window,
                    solution_mappings,
                    context,
                    parameters,
                    pushdowns,
                    query_settings,
                    dataset,
                );
            }
            Expression::FunctionCall(Function::Custom(_), _)
                if self.cast_chain_on_codes(expr, &solution_mappings).is_some() =>
            {
                let (cast, state) = self.cast_chain_on_codes(expr, &solution_mappings).unwrap();
                let mut output_solution_mappings = solution_mappings;
                output_solution_mappings.mappings = output_solution_mappings
                    .mappings
                    .with_column(cast.alias(context.as_str()));
                output_solution_mappings
                    .rdf_node_types
                    .insert(context.as_str().to_string(), state);
                output_solution_mappings
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
                        pushdowns,
                        query_settings,
                        dataset,
                    )?;
                    args_contexts.insert(i, arg_context);
                }
                match func {
                    Function::Custom(nn) if geo::is_geo_function(nn) => geo::evaluate_function(
                        self.geo_index.as_ref(),
                        nn,
                        args,
                        output_solution_mappings,
                        &args_contexts,
                        context,
                        self.global_cats.clone(),
                    )?,
                    Function::Custom(nn) if vector::is_vector_function(nn) => {
                        let index = if let QueryGraph::NamedGraph(g) = dataset {
                            self.vector_index.get(g)
                        } else {
                            None
                        };
                        vector::evaluate_function(
                            index,
                            nn,
                            args,
                            output_solution_mappings,
                            &args_contexts,
                            context,
                            self.global_cats.clone(),
                        )?
                    }
                    _ => func_expression(
                        output_solution_mappings,
                        func,
                        args,
                        args_contexts,
                        context,
                        self.global_cats.clone(),
                        Some(&self.udf_registry),
                    )?,
                }
            }
        };
        Ok(output_solution_mappings)
    }
}

impl Triplestore {
    /// EXISTS { P } for each incoming solution μ: whether eval(substitute(P, μ)) has a solution
    /// (SPARQL 1.1 18.6), as a boolean column named `context`.
    ///
    /// The answer depends only on the values μ gives the variables P mentions (the keys). P is
    /// evaluated with the distinct key values as its incoming solutions, and the answers are
    /// joined back to the solutions on the keys. substitute replaces only the variables μ
    /// binds: a key that μ leaves unbound (e.g. by OPTIONAL) stays free in P and matches any
    /// value. So the solutions are split by which keys they leave unbound, among those `input`
    /// says may be (all if it is not known), and each part is evaluated and joined without them.
    ///
    /// Joining on values instead of numbering the solutions lets them be evaluated more than
    /// once: by P's pushdowns, which collect small incoming solutions to look their values up,
    /// and by the join. The numbers did not match between two evaluations when the solutions
    /// came in another order, which needed the solutions collected first. Solutions that may
    /// change between evaluations are cached and given to P without collecting them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_exists(
        &self,
        inner: &GraphPattern,
        solution_mappings: SolutionMappings,
        input: Option<&ExpressionInput>,
        exists_context: &Context,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let may_change = input.is_none_or(|i| i.may_change);
        let mut incoming = solution_mappings;
        let mut inner_pushdowns = pushdowns.cloned().unwrap_or_default();
        inner_pushdowns.expression_input = None;
        if may_change {
            incoming.mappings = incoming.mappings.cache();
            inner_pushdowns.keep_lazy = true;
        }
        let mentioned = mentioned_variable_names(inner);
        let mut keys: Vec<String> = incoming
            .rdf_node_types
            .keys()
            .filter(|v| mentioned.contains(v.as_str()))
            .cloned()
            .collect();
        keys.sort();
        let unbound: Vec<String> = keys
            .iter()
            .filter(|v| input.is_none_or(|i| i.maybe_unbound.contains(v.as_str())))
            .cloned()
            .collect();
        // The semi and anti joins of FILTER (NOT) EXISTS give the distinct keys alone.
        let key_columns = incoming
            .mappings
            .clone()
            .select(keys.iter().map(|k| col(k)).collect::<Vec<_>>());
        let only_keys = incoming.rdf_node_types.len() == keys.len();
        let distinct = SolutionMappings::new(
            if only_keys {
                key_columns
            } else {
                key_columns.unique(None, UniqueKeepStrategy::Any)
            },
            keys.iter()
                .map(|k| (k.clone(), incoming.rdf_node_types.get(k).unwrap().clone()))
                .collect(),
            incoming.height_estimate,
        );
        let mut parts = vec![];
        for (condition, unbound_here) in bound_masks(&incoming, &unbound) {
            // The incoming solutions with this combination of bound and unbound keys keep all
            // their columns; the keys given to P leave the unbound ones out.
            let mut solutions = incoming.clone();
            let mut part_keys = distinct.clone();
            if let Some(condition) = &condition {
                solutions.mappings = solutions.mappings.filter(condition.clone());
                part_keys.mappings = part_keys.mappings.filter(condition.clone());
            }
            part_keys.mappings =
                part_keys
                    .mappings
                    .drop(by_name(unbound_here.clone(), true, false));
            for v in &unbound_here {
                part_keys.rdf_node_types.remove(v);
            }
            let bound_keys: Vec<String> = keys
                .iter()
                .filter(|k| !unbound_here.contains(k))
                .cloned()
                .collect();
            let evaluated = self.lazy_graph_pattern(
                inner,
                if bound_keys.is_empty() {
                    None
                } else {
                    Some(part_keys)
                },
                exists_context,
                parameters,
                inner_pushdowns.clone(),
                query_settings,
                dataset,
            )?;
            let boolean = BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
                .into_default_input_rdf_node_state();
            let answers = if bound_keys.is_empty() {
                // No key is bound here: P has a solution for all of these or for none.
                SolutionMappings::new(
                    evaluated
                        .mappings
                        .select([len().gt(lit(0)).alias(context.as_str())]),
                    HashMap::from([(context.as_str().to_string(), boolean.clone())]),
                    1,
                )
            } else {
                let mut types: HashMap<_, _> = bound_keys
                    .iter()
                    .map(|k| (k.clone(), evaluated.rdf_node_types.get(k).unwrap().clone()))
                    .collect();
                types.insert(context.as_str().to_string(), boolean.clone());
                SolutionMappings::new(
                    evaluated
                        .mappings
                        .select(bound_keys.iter().map(|k| col(k)).collect::<Vec<_>>())
                        .unique(None, UniqueKeepStrategy::Any)
                        .with_column(lit(true).alias(context.as_str())),
                    types,
                    evaluated.height_estimate,
                )
            };
            let mut part = join(
                solutions,
                answers,
                JoinType::Left,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
            part.mappings = part
                .mappings
                .with_column(col(context.as_str()).fill_null(lit(false)));
            part.rdf_node_types
                .insert(context.as_str().to_string(), boolean);
            parts.push(part);
        }
        Ok(union(parts, false, self.global_cats.clone())?)
    }
}

impl Triplestore {
    /// `xsd:T1(xsd:T2(... ?v))`, casts of a variable whose values are dictionary-encoded, as
    /// one expression evaluated once per distinct value (see xsd_cast_chain_on_codes).
    fn cast_chain_on_codes(
        &self,
        expr: &Expression,
        solution_mappings: &SolutionMappings,
    ) -> Option<(polars::prelude::Expr, RDFNodeState)> {
        let mut targets = vec![];
        let mut e = expr;
        let variable = loop {
            match e {
                Expression::FunctionCall(Function::Custom(nn), args)
                    if args.len() == 1 && is_xsd_cast_function(nn.as_ref()) =>
                {
                    targets.push(nn.clone());
                    e = &args[0];
                }
                Expression::Variable(v) if !targets.is_empty() => break v,
                _ => return None,
            }
        };
        targets.reverse();
        let state = solution_mappings.rdf_node_types.get(variable.as_str())?;
        xsd_cast_chain_on_codes(variable.as_str(), state, &targets, self.global_cats.clone())
    }
}

/// The names of the variables written anywhere in the pattern, its expressions and nested
/// patterns included. The pattern's own text is scanned, so no kind of pattern is missed; a
/// name that only looks like a variable (inside a literal) just makes the set larger.
pub(crate) fn mentioned_variable_names(gp: &GraphPattern) -> HashSet<String> {
    let text = gp.to_string();
    let mut names = HashSet::new();
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c != '?' && c != '$' {
            continue;
        }
        let start = i + c.len_utf8();
        let mut end = start;
        while let Some(&(j, d)) = chars.peek() {
            if d.is_alphanumeric() || d == '_' || d == '\u{B7}' {
                end = j + d.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        if end > start {
            names.insert(text[start..end].to_string());
        }
    }
    names
}

impl Triplestore {
    /// The distinct key tuples of `keys` (solutions of bound key variables alone) for which
    /// the pattern has a solution, encoded as in `keys`, for the semi and anti joins of FILTER
    /// (NOT) EXISTS. They are the key columns of the pattern's solutions with `keys` as its
    /// incoming solutions: no answer per tuple is joined back, as lazy_exists does. If the
    /// pattern encodes a key otherwise (e.g. it joined it with values of other dictionaries),
    /// the answers of lazy_exists are used.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_exists_matching_keys(
        &self,
        inner: &GraphPattern,
        keys: SolutionMappings,
        may_change: bool,
        exists_context: &Context,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<LazyFrame, SparqlError> {
        let mut keys = keys;
        let mut names: Vec<String> = keys.rdf_node_types.keys().cloned().collect();
        names.sort();
        let key_columns: Vec<Expr> = names.iter().map(|k| col(k)).collect();
        let mut inner_pushdowns = pushdowns.cloned().unwrap_or_default();
        inner_pushdowns.expression_input = None;
        if may_change {
            keys.mappings = keys.mappings.cache();
            inner_pushdowns.keep_lazy = true;
        }
        let evaluated = self.lazy_graph_pattern(
            inner,
            Some(keys.clone()),
            exists_context,
            parameters,
            inner_pushdowns,
            query_settings,
            dataset,
        )?;
        if names
            .iter()
            .all(|k| same_encoding(evaluated.rdf_node_types.get(k), keys.rdf_node_types.get(k)))
        {
            return Ok(evaluated
                .mappings
                .select(key_columns)
                .unique(None, UniqueKeepStrategy::Any));
        }
        let answers = self.lazy_exists(
            inner,
            keys,
            Some(&ExpressionInput {
                maybe_unbound: HashSet::new(),
                may_change,
            }),
            exists_context,
            context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        Ok(answers
            .mappings
            .filter(col(context.as_str()))
            .select(key_columns))
    }
}

/// Whether two states of a variable have the same types, each encoded the same way, so that
/// their columns can be joined as they are.
fn same_encoding(a: Option<&RDFNodeState>, b: Option<&RDFNodeState>) -> bool {
    let (Some(a), Some(b)) = (a, b) else {
        return false;
    };
    a.map.len() == b.map.len()
        && a.map.iter().all(|(t, s)| match (s, b.map.get(t)) {
            (BaseCatState::String, Some(BaseCatState::String))
            | (BaseCatState::NonString, Some(BaseCatState::NonString))
            | (
                BaseCatState::CategoricalNative(None),
                Some(BaseCatState::CategoricalNative(None)),
            ) => true,
            (
                BaseCatState::CategoricalNative(Some(x)),
                Some(BaseCatState::CategoricalNative(Some(y))),
            ) => LockedCats::ptr_eq(x, y),
            _ => false,
        })
}

/// The combinations of bound and unbound values of `vars` in `sm`: a filter for each (`None`
/// for the only one if `vars` is empty), with the variables it leaves unbound. As
/// query_processing's split_by_bound, without dropping the columns.
fn bound_masks(sm: &SolutionMappings, vars: &[String]) -> Vec<(Option<Expr>, Vec<String>)> {
    if vars.is_empty() {
        return vec![(None, vec![])];
    }
    let is_null: Vec<Expr> = vars
        .iter()
        .map(|v| expr_is_null_workaround(col(v), sm.rdf_node_types.get(v).unwrap()))
        .collect();
    (0usize..(1 << vars.len()))
        .map(|mask| {
            let mut condition: Option<Expr> = None;
            let mut unbound = vec![];
            for (i, v) in vars.iter().enumerate() {
                let c = if mask & (1 << i) != 0 {
                    is_null[i].clone().not()
                } else {
                    unbound.push(v.clone());
                    is_null[i].clone()
                };
                condition = Some(match condition {
                    Some(condition) => condition.and(c),
                    None => c,
                });
            }
            (condition, unbound)
        })
        .collect()
}
