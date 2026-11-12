use crate::expressions::operations::numeric_promotion_level;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{
    any_horizontal, as_struct, coalesce, col, lit, when, Expr, LiteralValue, SortMultipleOptions,
};
use representation::multitype::all_multi_cols;
use representation::null::expr_is_null_workaround;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use std::collections::{BTreeMap, HashMap};

pub struct AggregateReturn {
    pub solution_mappings: SolutionMappings,
    pub expr: Expr,
    pub context: Option<Context>,
    pub rdf_node_type: RDFNodeState,
}

pub fn count_with_expression(column_context: &Context, distinct: bool) -> (Expr, RDFNodeState) {
    let out_expr = if distinct {
        col(column_context.as_str()).n_unique()
    } else {
        col(column_context.as_str()).count()
    };
    count_output(out_expr)
}

pub fn count_without_expression(
    distinct: bool,
    rdf_node_types: &HashMap<String, RDFNodeState>,
) -> (Expr, RDFNodeState) {
    let out_expr = if distinct {
        todo!("Try count without distinct")
    } else {
        let mut is_not_null = vec![];
        for (c, s) in rdf_node_types {
            is_not_null.push(expr_is_null_workaround(col(c), s).not())
        }
        any_horizontal(is_not_null).unwrap().sum()
    };
    count_output(out_expr)
}

/// COUNT returns xsd:integer (SPARQL 1.1 section 18.5.1.2), which is stored as Int64.
fn count_output(expr: Expr) -> (Expr, RDFNodeState) {
    (
        expr.cast(DataType::Int64),
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned()).into_default_input_rdf_node_state(),
    )
}

pub fn sum(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
) -> (Expr, RDFNodeState) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    if expr_rdf_node_type.is_multi() {
        return multi_numeric_aggregate(
            column_context,
            expr_rdf_node_type,
            distinct,
            NumericAggregate::Sum,
        );
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
            .into_default_input_rdf_node_state()
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = if distinct {
        col(column_context.as_str()).unique().sum()
    } else {
        col(column_context.as_str()).sum()
    };
    (out_expr, out_rdf_node_type)
}

pub fn avg(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
) -> (Expr, RDFNodeState) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    if expr_rdf_node_type.is_multi() {
        return multi_numeric_aggregate(
            column_context,
            expr_rdf_node_type,
            distinct,
            NumericAggregate::Avg,
        );
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
            .into_default_input_rdf_node_state()
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = if distinct {
        col(column_context.as_str()).unique().mean()
    } else {
        col(column_context.as_str()).mean()
    };
    (out_expr, out_rdf_node_type)
}

pub fn min(solution_mappings: &SolutionMappings, column_context: &Context) -> (Expr, RDFNodeState) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    if expr_rdf_node_type.is_multi() {
        return multi_min_max(column_context, expr_rdf_node_type, false);
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
            .into_default_input_rdf_node_state()
    } else {
        expr_rdf_node_type.clone()
    };
    let out_expr = col(column_context.as_str()).min();

    (out_expr, out_rdf_node_type)
}

pub fn max(solution_mappings: &SolutionMappings, column_context: &Context) -> (Expr, RDFNodeState) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    if expr_rdf_node_type.is_multi() {
        return multi_min_max(column_context, expr_rdf_node_type, true);
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
            .into_default_input_rdf_node_state()
    } else {
        expr_rdf_node_type.clone()
    };

    let out_expr = col(column_context.as_str()).max();

    (out_expr, out_rdf_node_type)
}

pub fn group_concat(
    column_context: &Context,
    separator: &Option<String>,
    distinct: bool,
) -> (Expr, RDFNodeState) {
    let out_rdf_node_type =
        BaseRDFNodeType::Literal(xsd::STRING.into_owned()).into_default_input_rdf_node_state();

    let use_sep = if let Some(sep) = separator {
        sep.to_string()
    } else {
        "".to_string()
    };
    let out_expr = if distinct {
        col(column_context.as_str())
            .cast(DataType::String)
            .unique_stable()
            .str()
            .join(use_sep.as_str(), true)
    } else {
        col(column_context.as_str())
            .cast(DataType::String)
            .str()
            .join(use_sep.as_str(), true)
    };
    (out_expr, out_rdf_node_type)
}

pub fn sample(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
) -> (Expr, RDFNodeState) {
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();

    let out_expr = col(column_context.as_str()).first();
    (out_expr, out_rdf_node_type)
}

fn rdf_node_type_from_context<'a>(
    context: &'_ Context,
    solution_mappings: &'a SolutionMappings,
) -> &'a RDFNodeState {
    let datatype = solution_mappings
        .rdf_node_types
        .get(context.as_str())
        .unwrap();
    datatype
}

pub fn list_aggregation(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
) -> (Expr, RDFNodeState) {
    //Todo: add list types..
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();
    let out_expr = col(column_context.as_str());
    (out_expr, out_rdf_node_type)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NumericAggregate {
    Sum,
    Avg,
}

/// The value of one type in a multitype (struct) column.
fn multi_field(multi: &Expr, t: &BaseRDFNodeType) -> Expr {
    multi.clone().struct_().field_by_name(&t.field_col_name())
}

fn null_expr(dtype: DataType) -> Expr {
    lit(LiteralValue::untyped_null()).cast(dtype)
}

fn numeric_level(t: &BaseRDFNodeType) -> Option<u8> {
    if let BaseRDFNodeType::Literal(l) = t {
        numeric_promotion_level(l.as_ref())
    } else {
        None
    }
}

/// The result type of SUM or AVG over values whose highest promotion level is `level`.
/// AVG over integers divides integers, which gives xsd:decimal.
fn numeric_aggregate_type(level: u8, agg: NumericAggregate) -> BaseRDFNodeType {
    let nn = match level {
        3 => xsd::DOUBLE,
        2 => xsd::FLOAT,
        1 => xsd::DECIMAL,
        _ if agg == NumericAggregate::Avg => xsd::DECIMAL,
        _ => xsd::INTEGER,
    };
    BaseRDFNodeType::Literal(nn.into_owned())
}

/// SUM and AVG over a column with multiple types. The values in each group are promoted to the
/// highest numeric type present in that group, so the output may again have multiple types.
/// A group containing a non-numeric value gives an error (unbound), as in SPARQL op:numeric-add.
fn multi_numeric_aggregate(
    column_context: &Context,
    state: &RDFNodeState,
    distinct: bool,
    agg: NumericAggregate,
) -> (Expr, RDFNodeState) {
    let multi = if distinct {
        col(column_context.as_str()).unique()
    } else {
        col(column_context.as_str())
    };
    let mut numeric = vec![];
    let mut non_numeric_present = vec![];
    for t in state.get_sorted_types() {
        if t.is_none() {
            continue;
        }
        let f = multi_field(&multi, t);
        if let Some(level) = numeric_level(t) {
            numeric.push((f, level));
        } else {
            non_numeric_present.push(f.is_not_null());
        }
    }
    if numeric.is_empty() {
        let t = BaseRDFNodeType::None;
        return (
            null_expr(t.default_input_polars_data_type()),
            t.into_default_input_rdf_node_state(),
        );
    }

    let row_level = coalesce(
        &numeric
            .iter()
            .map(|(f, level)| {
                when(f.clone().is_not_null())
                    .then(lit(*level as i32))
                    .otherwise(null_expr(DataType::Int32))
            })
            .collect::<Vec<_>>(),
    );
    let group_level = row_level.max();
    let has_error = if non_numeric_present.is_empty() {
        lit(false)
    } else {
        any_horizontal(non_numeric_present).unwrap().any(true)
    };
    let count = coalesce(
        &numeric
            .iter()
            .map(|(f, _)| f.clone().cast(DataType::Float64))
            .collect::<Vec<_>>(),
    )
    .count()
    .cast(DataType::Float64);

    // Output types, with the promotion levels that produce them
    let mut out_types: BTreeMap<BaseRDFNodeType, Vec<u8>> = BTreeMap::new();
    for (_, level) in &numeric {
        let levels = out_types
            .entry(numeric_aggregate_type(*level, agg))
            .or_default();
        if !levels.contains(level) {
            levels.push(*level);
        }
    }

    let mut outputs = vec![];
    for (out_type, levels) in &out_types {
        let dtype = out_type.default_input_polars_data_type();
        let max_level = *levels.iter().max().unwrap();
        // Only values of lower or equal level are present when this output type is chosen
        let sum = coalesce(
            &numeric
                .iter()
                .filter(|(_, level)| *level <= max_level)
                .map(|(f, _)| f.clone().cast(dtype.clone()))
                .collect::<Vec<_>>(),
        )
        .sum();
        let value = match agg {
            NumericAggregate::Sum => sum,
            NumericAggregate::Avg => {
                (sum.cast(DataType::Float64) / count.clone()).cast(dtype.clone())
            }
        };
        let mut chosen: Option<Expr> = None;
        for level in levels {
            let is_level = group_level.clone().eq(lit(*level as i32));
            chosen = Some(match chosen {
                None => is_level,
                Some(c) => c.or(is_level),
            });
        }
        let chosen = chosen.unwrap().and(has_error.clone().not());
        let e = when(chosen).then(value).otherwise(null_expr(dtype));
        outputs.push((e, out_type.clone()));
    }

    if outputs.len() == 1 {
        let (e, t) = outputs.pop().unwrap();
        (e, t.into_default_input_rdf_node_state())
    } else {
        let types: Vec<_> = outputs.iter().map(|(_, t)| t.clone()).collect();
        let fields: Vec<_> = outputs
            .into_iter()
            .map(|(e, t)| e.alias(t.field_col_name()))
            .collect();
        (as_struct(fields), RDFNodeState::default_from_types(types))
    }
}

/// MIN and MAX over a column with multiple types, using the SPARQL ORDER BY ordering
/// (section 15.1): blank nodes < IRIs < literals, numeric literals compared by value.
/// Other literal types are ordered after numerics, by type and then by value.
/// The selected value keeps its own type.
fn multi_min_max(
    column_context: &Context,
    state: &RDFNodeState,
    max: bool,
) -> (Expr, RDFNodeState) {
    let multi = col(column_context.as_str());
    let types: Vec<_> = state.get_sorted_types();
    let mut ranks = vec![];
    let mut numeric_keys = vec![];
    let mut other_keys = vec![];
    let mut next_other_rank = 4;
    for t in &types {
        if t.is_none() {
            continue;
        }
        let f = multi_field(&multi, t);
        let rank = match t {
            BaseRDFNodeType::BlankNode => 1,
            BaseRDFNodeType::IRI => 2,
            _ if numeric_level(t).is_some() => 3,
            _ => {
                next_other_rank += 1;
                next_other_rank
            }
        };
        ranks.push(
            when(f.clone().is_not_null())
                .then(lit(rank))
                .otherwise(null_expr(DataType::Int32)),
        );
        if numeric_level(t).is_some() {
            numeric_keys.push(f.cast(DataType::Float64));
        } else {
            other_keys.push(f);
        }
    }
    // Unbound values have no rank and are sorted last, so they are only chosen if all are unbound
    let mut by = vec![coalesce(&ranks)];
    if !numeric_keys.is_empty() {
        by.push(coalesce(&numeric_keys));
    }
    by.extend(other_keys);
    let options = SortMultipleOptions::default()
        .with_order_descending(max)
        .with_nulls_last(true)
        .with_maintain_order(true);

    // All fields are sorted by the same keys, so the first row is the same row for every field
    let fields: Vec<_> = all_multi_cols(types)
        .into_iter()
        .map(|name| {
            multi
                .clone()
                .struct_()
                .field_by_name(&name)
                .sort_by(by.clone(), options.clone())
                .first()
                .alias(name)
        })
        .collect();
    (as_struct(fields), state.clone())
}
