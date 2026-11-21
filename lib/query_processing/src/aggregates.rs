use crate::expressions::functions::str_function::str_function;
use crate::expressions::operations::numeric_promotion_level;
use crate::extreme_value::{extreme_value, ExtremeField, ExtremeKey};
use crate::graph_patterns::make_ordering_expr;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{
    any_horizontal, as_struct, coalesce, col, len, lit, when, Expr, LiteralValue,
    SortMultipleOptions,
};
use representation::cats::{optional_maybe_decode_expr, LockedCats};
use representation::multitype::all_multi_cols;
use representation::null::expr_is_null_workaround;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::statistics::StatisticalAggregate;
use std::collections::{BTreeMap, HashMap};

pub struct AggregateReturn {
    pub solution_mappings: SolutionMappings,
    pub expr: Expr,
    pub context: Option<Context>,
    pub rdf_node_type: RDFNodeState,
}

pub fn count_with_expression(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
) -> (Expr, RDFNodeState) {
    let c = col(column_context.as_str());
    // Unbound values are not counted (SPARQL 1.1 section 18.5.1.2), also not as a distinct value.
    // Multi-type and language-tagged columns are structs, so their unbound values are not null structs.
    let is_bound = match solution_mappings.rdf_node_types.get(column_context.as_str()) {
        Some(state) => expr_is_null_workaround(c.clone(), state).not(),
        None => c.clone().is_not_null(),
    };
    let out_expr = if distinct {
        c.filter(is_bound).n_unique()
    } else {
        is_bound.sum()
    };
    count_output(out_expr)
}

pub fn count_without_expression(
    distinct: bool,
    rdf_node_types: &HashMap<String, RDFNodeState>,
) -> (Expr, RDFNodeState) {
    if rdf_node_types.is_empty() {
        // Solutions without variables, e.g. COUNT(*) over { }
        let out_expr = if distinct {
            len().gt(lit(0)).cast(DataType::Int64)
        } else {
            len()
        };
        return count_output(out_expr);
    }
    // Sorted for a deterministic struct field order
    let mut cols: Vec<_> = rdf_node_types.iter().collect();
    cols.sort_by_key(|(c, _)| c.as_str());
    let mut is_not_null = vec![];
    for (c, s) in &cols {
        is_not_null.push(expr_is_null_workaround(col(c.as_str()), s).not())
    }
    let any_not_null = any_horizontal(is_not_null).unwrap();
    let out_expr = if distinct {
        as_struct(cols.iter().map(|(c, _)| col(c.as_str())).collect())
            .filter(any_not_null)
            .n_unique()
    } else {
        any_not_null.sum()
    };
    count_output(out_expr)
}

/// COUNT returns xsd:integer (SPARQL 1.1 section 18.5.1.2), which is stored as Int64.
fn count_output(expr: Expr) -> (Expr, RDFNodeState) {
    (
        expr.cast(DataType::Int64),
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
            .into_default_input_rdf_node_state(),
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
    if let Some(unbound) = non_numeric_aggregate(expr_rdf_node_type) {
        return unbound;
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned(), None)
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
    if let Some(unbound) = non_numeric_aggregate(expr_rdf_node_type) {
        return unbound;
    }
    let mean = if distinct {
        col(column_context.as_str()).unique().mean()
    } else {
        col(column_context.as_str()).mean()
    };
    // The average of no values is 0 (SPARQL 1.1, 18.5.1.4).
    let mean = mean.fill_null(lit(0));
    if expr_rdf_node_type.is_bool() {
        return (
            mean,
            BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned(), None)
                .into_default_input_rdf_node_state(),
        );
    }
    // The type of op:numeric-divide of the sum by the count: AVG over integers is xsd:decimal.
    let level = expr_rdf_node_type
        .get_base_type()
        .and_then(numeric_level)
        .unwrap();
    let out_type = numeric_aggregate_type(level, NumericAggregate::Avg);
    let out_expr = mean.cast(out_type.default_input_polars_data_type());
    (out_expr, out_type.into_default_input_rdf_node_state())
}

pub fn min(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    min_max(solution_mappings, column_context, global_cats, false, false)
}

pub fn max(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    min_max(solution_mappings, column_context, global_cats, true, false)
}

/// MIN over the frames of a window function. Frames overlap, so the values of a frame are
/// ordered where they are instead of being gathered for a scan.
pub fn min_over_frames(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    min_max(solution_mappings, column_context, global_cats, false, true)
}

/// MAX over the frames of a window function, see `min_over_frames`.
pub fn max_over_frames(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    min_max(solution_mappings, column_context, global_cats, true, true)
}

fn min_max(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    global_cats: LockedCats,
    max: bool,
    frames: bool,
) -> (Expr, RDFNodeState) {
    let expr_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings);
    if expr_rdf_node_type.is_multi() {
        return multi_min_max(column_context, expr_rdf_node_type, max, frames, global_cats);
    }
    let out_rdf_node_type = if expr_rdf_node_type.is_bool() {
        BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned(), None)
            .into_default_input_rdf_node_state()
    } else {
        expr_rdf_node_type.clone()
    };
    let c = col(column_context.as_str());
    // Dictionary encoded values (strings, IRIs) are ordered by their decoded value, not by
    // their codes, which are in insertion order. The code of the smallest value is kept.
    let base = expr_rdf_node_type
        .get_base_type()
        .zip(expr_rdf_node_type.get_base_state());
    if let (false, Some((bt, BaseCatState::CategoricalNative(local_cats)))) = (frames, base) {
        if !bt.is_lang_string() {
            // The smallest decoded value is found in a scan, decoding a batch at a time
            let field = ExtremeField {
                name: None,
                kind: 0,
                key: ExtremeKey::Decoded(bt.clone(), local_cats.clone()),
            };
            let out_expr = extreme_value(c, vec![field], max, global_cats);
            return (out_expr, out_rdf_node_type);
        }
    }
    let keys = base.and_then(|(bt, bs)| sort_keys(c.clone(), bt, bs, global_cats));
    let out_expr = if let Some(keys) = keys {
        let options = SortMultipleOptions::default()
            .with_order_descending(max)
            .with_nulls_last(true)
            .with_maintain_order(true);
        c.sort_by(keys, options).first()
    } else if max {
        c.max()
    } else {
        c.min()
    };

    (out_expr, out_rdf_node_type)
}

/// The decoded values of a dictionary encoded (non language-tagged) column, to order it by.
/// The keys that order the values of a column as ORDER BY does: language-tagged strings by the
/// rank of their value, then of their language (a struct of codes has no order of its own).
fn sort_keys(
    e: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    global_cats: LockedCats,
) -> Option<Vec<Expr>> {
    if t.is_lang_string() {
        if !matches!(s, BaseCatState::CategoricalNative(_)) {
            return None;
        }
        let ranks = make_ordering_expr(e, global_cats, t.clone(), s.clone());
        return Some(
            [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD]
                .into_iter()
                .map(|f| ranks.clone().struct_().field_by_name(f))
                .collect(),
        );
    }
    optional_maybe_decode_expr(e, t, s, global_cats).map(|k| vec![k])
}

/// GROUP_CONCAT: joins the string values of the group (as with STR()) with the separator,
/// which is a single space by default. The result is an xsd:string.
pub fn group_concat(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    separator: &Option<String>,
    distinct: bool,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    group_concat_sorted(
        solution_mappings,
        column_context,
        separator,
        distinct,
        global_cats,
        None,
    )
}

/// Sort keys for the values of a group, as in `GROUP_CONCAT(?x ORDER BY ?y)`.
pub type AggregateOrder = Option<(Vec<Expr>, SortMultipleOptions)>;

fn sorted(e: Expr, order: AggregateOrder) -> Expr {
    match order {
        Some((by, options)) => e.sort_by(by, options),
        None => e,
    }
}

/// GROUP_CONCAT with the values in the given order. With DISTINCT, the first occurrence of each
/// value is kept.
pub fn group_concat_sorted(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    separator: &Option<String>,
    distinct: bool,
    global_cats: LockedCats,
    order: AggregateOrder,
) -> (Expr, RDFNodeState) {
    let out_rdf_node_type = BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None)
        .into_default_input_rdf_node_state();
    let use_sep = separator.as_deref().unwrap_or(" ");
    let t = rdf_node_type_from_context(column_context, solution_mappings);
    let mut strings = sorted(str_function(column_context.as_str(), t, global_cats), order);
    if distinct {
        strings = strings.unique_stable();
    }
    let out_expr = strings.str().join(use_sep, true);
    (out_expr, out_rdf_node_type)
}

pub fn sample(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
) -> (Expr, RDFNodeState) {
    sample_sorted(solution_mappings, column_context, None)
}

/// SAMPLE, choosing the first value in the given order (any value without an order). Unbound
/// values (errors) are not values of the aggregate, so they are skipped.
pub fn sample_sorted(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    order: AggregateOrder,
) -> (Expr, RDFNodeState) {
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();

    let bound = expr_is_null_workaround(col(column_context.as_str()), &out_rdf_node_type).not();
    let out_expr = sorted(col(column_context.as_str()), order.clone())
        .filter(sorted(bound, order))
        .first();
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
    list_aggregation_sorted(solution_mappings, column_context, None)
}

pub fn list_aggregation_sorted(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    order: AggregateOrder,
) -> (Expr, RDFNodeState) {
    //Todo: add list types..
    let out_rdf_node_type = rdf_node_type_from_context(column_context, solution_mappings).clone();
    let out_expr = sorted(col(column_context.as_str()), order);
    (out_expr, out_rdf_node_type)
}

/// The statistical aggregates of Apache Jena (`agg:stdev`, `agg:var_pop`, ...), see
/// `spargebra::statistics`. The values are converted to xsd:double and the result is an
/// xsd:double, as in Jena. A group with a non-numeric value gives an error (unbound), and so does
/// a group without values, or a sample of a single value. The variance of a population of a
/// single value is 0. Unbound values are skipped, as in SUM and AVG.
pub fn statistical_aggregate(
    solution_mappings: &SolutionMappings,
    column_context: &Context,
    distinct: bool,
    aggregate: StatisticalAggregate,
) -> (Expr, RDFNodeState) {
    let state = rdf_node_type_from_context(column_context, solution_mappings);
    let out_type = BaseRDFNodeType::Literal(xsd::DOUBLE.into_owned(), None);
    let column = if distinct {
        col(column_context.as_str()).unique()
    } else {
        col(column_context.as_str())
    };
    let (values, has_error) = if state.is_multi() {
        let mut numeric = vec![];
        let mut non_numeric_present = vec![];
        for t in state.get_sorted_types() {
            if t.is_none() {
                continue;
            }
            let f = multi_field(&column, t);
            if numeric_level(t).is_some() {
                numeric.push(f.cast(DataType::Float64));
            } else {
                non_numeric_present.push(f.is_not_null());
            }
        }
        if numeric.is_empty() {
            return (
                null_expr(DataType::Float64),
                out_type.into_default_input_rdf_node_state(),
            );
        }
        let has_error = if non_numeric_present.is_empty() {
            None
        } else {
            Some(any_horizontal(non_numeric_present).unwrap().any(true))
        };
        (coalesce(&numeric), has_error)
    } else {
        let numeric = state.get_base_type().and_then(numeric_level).is_some();
        if !numeric {
            // A null-only column has the type None, and gives an unbound result as well.
            return (
                null_expr(DataType::Float64),
                out_type.into_default_input_rdf_node_state(),
            );
        }
        (column.cast(DataType::Float64), None)
    };
    let ddof = aggregate.ddof();
    let value = if aggregate.is_stdev() {
        values.clone().std(ddof)
    } else {
        values.clone().var(ddof)
    };
    let enough_values = values.count().cast(DataType::Int64).gt(lit(ddof as i64));
    let ok = match has_error {
        None => enough_values,
        Some(e) => enough_values.and(e.not()),
    };
    let out_expr = when(ok)
        .then(value)
        .otherwise(null_expr(DataType::Float64));
    (out_expr, out_type.into_default_input_rdf_node_state())
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

/// SUM and AVG of a non-numeric column are errors in SPARQL, so the result is unbound.
fn non_numeric_aggregate(state: &RDFNodeState) -> Option<(Expr, RDFNodeState)> {
    let numeric = state.is_bool()
        || state
            .get_base_type()
            .and_then(numeric_level)
            .is_some();
    if numeric {
        return None;
    }
    // A null xsd:integer, so that only an empty group gives 0.
    let t = BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None);
    Some((
        null_expr(t.default_input_polars_data_type()),
        t.into_default_input_rdf_node_state(),
    ))
}

fn null_expr(dtype: DataType) -> Expr {

    lit(LiteralValue::untyped_null()).cast(dtype)
}

fn numeric_level(t: &BaseRDFNodeType) -> Option<u8> {
    if let BaseRDFNodeType::Literal(l, _) = t {
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
    BaseRDFNodeType::Literal(nn.into_owned(), None)
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
    frames: bool,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    let multi = col(column_context.as_str());
    let types: Vec<_> = state.get_sorted_types();
    if !frames {
        let mut fields = vec![];
        let mut next_other_kind = 4;
        for t in &types {
            if t.is_none() {
                continue;
            }
            let numeric = numeric_level(t).is_some();
            let kind = match t {
                BaseRDFNodeType::BlankNode => 1,
                BaseRDFNodeType::IRI => 2,
                _ if numeric => 3,
                _ => {
                    next_other_kind += 1;
                    next_other_kind
                }
            };
            let key = match state.map.get(*t).unwrap() {
                _ if numeric => ExtremeKey::Numeric,
                BaseCatState::CategoricalNative(local_cats) if !t.is_lang_string() => {
                    ExtremeKey::Decoded((*t).clone(), local_cats.clone())
                }
                _ => ExtremeKey::Stored,
            };
            fields.push(ExtremeField {
                name: Some(t.field_col_name()),
                kind,
                key,
            });
        }
        return (extreme_value(multi, fields, max, global_cats), state.clone());
    }
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
        } else if let Some(keys) =
            sort_keys(f.clone(), t, state.map.get(t).unwrap(), global_cats.clone())
        {
            other_keys.extend(keys);
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
