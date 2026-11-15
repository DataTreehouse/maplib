//! Evaluation of function call terms in template instances.
//!
//! As in Frog (Jarholt 2022, section 4.6.2), function calls are evaluated eagerly: when an
//! instance is expanded, before list expansion and before the instance's arguments are passed to
//! the template it instantiates. maplib evaluates a function call once for all rows of the
//! instance, by translating it to a SPARQL expression and evaluating that expression with the
//! triplestore's vectorized expression evaluator over the argument columns.

use super::super::StaticColumn;
use crate::model::constant_terms::constant_to_expr;
use crate::model::errors::MappingError;
use polars::prelude::{Column, DataFrame, IntoLazy};
use representation::cats::maybe_decode_complex_expr;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::RDFNodeState;
use std::collections::{HashMap, HashSet};
use templates::ast::{ConstantTerm, ConstantTermOrList, FunctionCall, StottrTerm};
use templates::functions::{function_call_to_expression, FunctionLibrary};
use templates::MappingColumnType;
use triplestore::sparql::QuerySettings;
use triplestore::Triplestore;

/// What is needed to evaluate function calls during expansion.
pub(crate) struct FunctionContext<'a> {
    pub triplestore: &'a Triplestore,
    pub functions: &'a FunctionLibrary,
    /// The number of rows of the instances, which function results must have.
    pub height: usize,
}

/// The names of the variables that the function call terms of an argument refer to.
pub(crate) fn function_call_variables<'a>(term: &'a StottrTerm, out: &mut Vec<&'a str>) {
    if let StottrTerm::FunctionCall(fc) = term {
        let mut vs = vec![];
        for a in &fc.arguments {
            a.variables(&mut vs);
        }
        for v in vs {
            if !out.contains(&v.as_str()) {
                out.push(v.as_str());
            }
        }
    }
}

/// Evaluates a function call over the columns of the calling template, giving a column with
/// `height` rows named `target_name`, and its type.
pub(crate) fn evaluate_function_call(
    ctx: &FunctionContext,
    function_call: &FunctionCall,
    input_columns: &[Column],
    dynamic_columns: &HashMap<String, MappingColumnType>,
    static_columns: &HashMap<String, StaticColumn>,
    target_name: &str,
) -> Result<(Column, MappingColumnType), MappingError> {
    let height = ctx.height;
    let expression = function_call_to_expression(function_call, ctx.functions)?;

    let term = StottrTerm::FunctionCall(function_call.clone());
    let mut variables = vec![];
    function_call_variables(&term, &mut variables);

    let mut columns = vec![];
    let mut constant_exprs = vec![];
    let mut types = HashMap::new();
    let mut seen = HashSet::new();
    for v in variables {
        if !seen.insert(v) {
            continue;
        }
        if let Some(t) = dynamic_columns.get(v) {
            let MappingColumnType::Flat(state) = t else {
                return Err(MappingError::FunctionListArgument(
                    v.to_string(),
                    function_call.to_string(),
                ));
            };
            if let Some(c) = input_columns.iter().find(|c| c.name().as_str() == v) {
                columns.push(c.clone());
                types.insert(v.to_string(), state.clone());
            }
        } else if let Some(sc) = static_columns.get(v) {
            match &sc.constant_term {
                ConstantTermOrList::ConstantTerm(ConstantTerm::None) => {}
                ConstantTermOrList::ConstantTerm(ConstantTerm::BlankNode(b)) => {
                    return Err(MappingError::FunctionEvaluationError(
                        function_call.to_string(),
                        format!("blank node {b} given for {v} can not be a function argument"),
                    ));
                }
                ConstantTermOrList::ConstantTerm(_) => {
                    let (expr, _, mct) = constant_to_expr(&sc.constant_term, &sc.ptype)?;
                    let MappingColumnType::Flat(state) = mct else {
                        unreachable!("Constant terms that are not lists have flat types")
                    };
                    constant_exprs.push(expr.alias(v));
                    types.insert(v.to_string(), state);
                }
                ConstantTermOrList::ConstantList(_) => {
                    return Err(MappingError::FunctionListArgument(
                        v.to_string(),
                        function_call.to_string(),
                    ));
                }
            }
        }
        // Otherwise the variable is an optional parameter without a value, which is unbound.
    }

    let df = if columns.is_empty() {
        DataFrame::empty_with_height(height)
    } else {
        DataFrame::new(height, columns).map_err(|e| {
            MappingError::FunctionEvaluationError(function_call.to_string(), e.to_string())
        })?
    };
    let mut lf = df.lazy();
    for e in constant_exprs {
        lf = lf.with_column(e);
    }
    let solution_mappings = SolutionMappings::new(lf, types, height);
    let context = Context::new().extension_with(PathEntry::ExtendExpression);
    let query_settings = QuerySettings {
        include_graphs: vec![],
        max_rows: None,
        strict_project: false,
        federation: None,
        evaluation_hook: None,
    };
    let SolutionMappings {
        mappings,
        mut rdf_node_types,
        ..
    } = ctx
        .triplestore
        .lazy_expression(
            &expression,
            solution_mappings,
            &context,
            None,
            None,
            &query_settings,
            &QueryGraph::NamedGraph(NamedGraph::DefaultGraph),
        )
        .map_err(|e| {
            MappingError::FunctionEvaluationError(function_call.to_string(), e.to_string())
        })?;
    let mut state = rdf_node_types.remove(context.as_str()).unwrap();

    // Values the expression evaluator encodes with the global categorical maps are decoded, so
    // that function results have the same representation as other template arguments.
    let mut result_expr = polars::prelude::col(context.as_str());
    if !state.is_multi() {
        let (base_type, base_state) = state.map.iter().next().unwrap();
        if matches!(base_state, BaseCatState::CategoricalNative(_)) {
            result_expr = maybe_decode_complex_expr(
                result_expr,
                base_type,
                base_state,
                ctx.triplestore.global_cats.clone(),
            );
            state = base_type.clone().into_default_input_rdf_node_state();
        }
    }
    let mut df = mappings
        .select([result_expr.alias(target_name)])
        .collect()
        .map_err(|e| {
            MappingError::FunctionEvaluationError(function_call.to_string(), e.to_string())
        })?;
    let column = df.drop_in_place(target_name).unwrap();
    Ok((column, MappingColumnType::Flat(state)))
}

/// Whether a value of a function call result is missing (ottr:none), for the given type.
pub(crate) fn function_result_is_null(name: &str, state: &RDFNodeState) -> polars::prelude::Expr {
    representation::null::expr_is_null_workaround(polars::prelude::col(name), state)
}
