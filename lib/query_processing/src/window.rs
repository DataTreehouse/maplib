//! Window functions (`ROW_NUMBER() OVER (PARTITION BY ?a ORDER BY ?b)`, see spargebra's
//! window.rs for the syntax) on Polars window expressions.
//!
//! - Aggregates without ORDER BY and frame are the aggregate of the whole partition, as in
//!   GROUP BY (with the same functions and types), with `over` the partition columns.
//! - RANK and DENSE_RANK with one ordering key use Polars' rank in each partition, without
//!   sorting.
//! - ROW_NUMBER is the position in the partition (`int_range` over the partition columns) after
//!   one sort by the ORDER BY keys.
//! - Otherwise the partitions get an integer id (the position of their first row), and the
//!   solutions are sorted once by it and the ORDER BY keys of the window (as SPARQL ORDER BY sorts
//!   them), so that every partition is contiguous and in window order. ROW_NUMBER is the position
//!   in the partition, a row starts a group of peers when it differs from the previous row in a
//!   key, DENSE_RANK counts the groups of peers and RANK is the ROW_NUMBER of the first peer, all
//!   from neighbouring rows.
//! - With ORDER BY, the default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, as in
//!   SQL: the running aggregate up to and including the last peer of the row. COUNT, and SUM,
//!   AVG, MIN and MAX of a numeric column of a single type, use cumulative functions over the
//!   partition id. Other aggregates, and frames `ROWS BETWEEN ...`, use a rolling window over an
//!   index that numbers the rows of each partition, with the aggregate functions of GROUP BY.
//!
//! [`rank_one`] keeps the rows with RANK or DENSE_RANK 1, the rows that are peers of the first
//! row of their partition, without ranking them.

use crate::aggregates::{
    avg, count_with_expression, group_concat, max_over_frames, min_over_frames, sample,
    statistical_aggregate, sum,
};
use crate::errors::QueryProcessingError;
use crate::graph_patterns::{ordering_exprs, sort_options};
use oxrdf::vocab::xsd;
use polars::prelude::{
    by_name, col, int_range, len, lit, ClosedWindow, DataType, Duration, Expr, FillNullStrategy,
    RankMethod, RankOptions, IDX_DTYPE,
};
use representation::cats::LockedCats;
use representation::null::expr_is_null_workaround;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::AggregateFunction;
use spargebra::statistics::StatisticalAggregate;
use spargebra::window::{WindowFrame, WindowFunction};

/// Frames are clamped to this many rows before and after the current row, and the rows of
/// partition i are numbered from i * 2^32 in the rolling index, so that frames do not reach into
/// other partitions.
const MAX_FRAME_OFFSET: i64 = 1 << 31;
const PARTITION_STRIDE: i64 = 1 << 32;

pub struct WindowColumns<'a> {
    /// The aggregated expression, already evaluated.
    pub arg: Option<&'a Context>,
    pub partition_by: &'a [Context],
    /// The ORDER BY expressions, already evaluated, and whether they are ascending.
    pub order_by: &'a [Context],
    pub ascending: Vec<bool>,
}

/// Evaluates the window function into the column `output`. The rows of the solution mappings
/// are reordered.
pub fn window_function(
    mut solution_mappings: SolutionMappings,
    function: &WindowFunction,
    columns: WindowColumns,
    frame: Option<WindowFrame>,
    output: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let tmp = |name: &str| format!("{}_{name}", output.as_str());
    let mut tmp_cols: Vec<String> = vec![];
    let partition: Vec<Expr> = columns
        .partition_by
        .iter()
        .map(|c| col(c.as_str()))
        .collect();
    let over = |e: Expr, extra: &[&str]| -> Expr {
        let mut by = partition.clone();
        by.extend(extra.iter().map(|c| col(*c)));
        if by.is_empty() {
            e
        } else {
            e.over(by).unwrap()
        }
    };

    // Sort by the window ORDER BY, with the keys of SPARQL ORDER BY.
    let order_cols: Vec<String> = columns
        .order_by
        .iter()
        .map(|c| c.as_str().to_string())
        .collect();
    let (key_exprs, key_asc) = ordering_exprs(
        &solution_mappings,
        &order_cols,
        columns.ascending.clone(),
        global_cats.clone(),
    );
    let key_cols: Vec<String> = (0..key_exprs.len())
        .map(|i| tmp(&format!("key{i}")))
        .collect();
    if key_exprs.len() == 1 && matches!(function, WindowFunction::Rank | WindowFunction::DenseRank)
    {
        // One key: Polars' rank in each partition, without sorting the solutions.
        let key = key_exprs.into_iter().next().unwrap();
        let out = single_key_rank(
            key,
            key_asc[0],
            matches!(function, WindowFunction::Rank),
            over,
        );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(out.alias(output.as_str()));
        solution_mappings.rdf_node_types.insert(
            output.as_str().to_string(),
            BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                .into_default_input_rdf_node_state(),
        );
        return Ok(solution_mappings);
    }
    let has_order = !key_exprs.is_empty();
    let integer = || {
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
            .into_default_input_rdf_node_state()
    };
    let whole_partition = !function.is_ranking() && !has_order && frame.is_none();
    if whole_partition {
        // The aggregate of the whole partition, no order needed.
        let (agg, agg_type) = aggregate(function, columns.arg, &solution_mappings, &global_cats)?;
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(over(agg, &[]).alias(output.as_str()));
        solution_mappings
            .rdf_node_types
            .insert(output.as_str().to_string(), agg_type);
        return Ok(solution_mappings);
    }

    if matches!(function, WindowFunction::RowNumber) {
        // The position in the partition after one sort by the keys: no partition id is needed.
        if has_order {
            solution_mappings.mappings = solution_mappings
                .mappings
                .with_columns(
                    key_exprs
                        .into_iter()
                        .zip(&key_cols)
                        .map(|(e, c)| e.alias(c))
                        .collect::<Vec<_>>(),
                )
                .sort(key_cols.clone(), sort_options(&key_asc));
        }
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(
                (over(int_range(lit(0i64), len(), 1, DataType::Int64), &[]) + lit(1i64))
                    .alias(output.as_str()),
            )
            .drop(by_name(key_cols, true, false));
        solution_mappings
            .rdf_node_types
            .insert(output.as_str().to_string(), integer());
        return Ok(solution_mappings);
    }

    // The rows of each partition are made contiguous and put in window order by one sort, by an
    // integer partition id (the position of the first row of the partition) and the keys. The
    // rest is computed from neighbouring rows, without grouping by the partition columns again.
    let partitioned = !partition.is_empty();
    let pid = tmp("pid");
    let pos = tmp("pos");
    let position = || int_range(lit(0i64), len(), 1, DataType::Int64);
    let mut sort_by = vec![];
    let mut sort_asc = vec![];
    if partitioned {
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(position().alias(&pid))
            .with_column(col(&pid).first().over(partition.clone()).unwrap().alias(&pid));
        tmp_cols.push(pid.clone());
        sort_by.push(pid.clone());
        sort_asc.push(true);
    }
    if has_order {
        solution_mappings.mappings = solution_mappings.mappings.with_columns(
            key_exprs
                .into_iter()
                .zip(&key_cols)
                .map(|(e, c)| e.alias(c))
                .collect::<Vec<_>>(),
        );
        tmp_cols.extend(key_cols.iter().cloned());
        sort_by.extend(key_cols.iter().cloned());
        sort_asc.extend(key_asc.iter().cloned());
    }
    if !sort_by.is_empty() {
        solution_mappings.mappings = solution_mappings
            .mappings
            .sort(sort_by, sort_options(&sort_asc));
    }
    let over_pid = |e: Expr| -> Expr {
        if partitioned {
            e.over([col(&pid)]).unwrap()
        } else {
            e
        }
    };
    // The value of `e` at the first row of the run that `starts` begins, for every row.
    let at_start = |starts: Expr, e: Expr| -> Expr {
        polars::prelude::when(starts)
            .then(e)
            .otherwise(lit(polars::prelude::NULL))
            .fill_null_with_strategy(FillNullStrategy::Forward(None))
    };
    let partition_start = if partitioned {
        col(&pid).neq_missing(col(&pid).shift(lit(1)))
    } else {
        col(&pos).eq(lit(0i64))
    };

    let row_number = tmp("rn");
    let peer_group = tmp("peers");
    let peer_start = tmp("peerstart");
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(position().alias(&pos))
        .with_column(
            (col(&pos) - at_start(partition_start.clone(), col(&pos)) + lit(1i64))
                .alias(&row_number),
        );
    tmp_cols.push(pos.clone());
    tmp_cols.push(row_number.clone());
    let needs_peers = has_order
        && (matches!(function, WindowFunction::Rank | WindowFunction::DenseRank)
            || (!function.is_ranking() && frame.is_none()));
    if needs_peers {
        // A row starts a new group of peers if it is the first in its partition or differs
        // from the previous row in a key. Unbound keys are equal to each other.
        let mut starts = col(&row_number).eq(lit(1i64));
        for k in &key_cols {
            starts = starts.or(col(k).neq_missing(col(k).shift(lit(1))));
        }
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(starts.alias(&peer_start))
            .with_column(
                col(&peer_start)
                    .cast(DataType::Int64)
                    .cum_sum(false)
                    .alias(&peer_group),
            );
        tmp_cols.push(peer_start.clone());
        tmp_cols.push(peer_group.clone());
    }

    let (out_expr, out_type) = match function {
        WindowFunction::RowNumber => (col(&row_number), integer()),
        WindowFunction::DenseRank => (
            if has_order {
                col(&peer_group) - at_start(partition_start.clone(), col(&peer_group))
                    + lit(1i64)
            } else {
                lit(1i64)
            },
            integer(),
        ),
        WindowFunction::Rank => (
            if has_order {
                at_start(col(&peer_start), col(&row_number))
            } else {
                lit(1i64)
            },
            integer(),
        ),
        WindowFunction::CountSolutions | WindowFunction::Aggregate { .. } => {
            let (agg, agg_type) = aggregate(function, columns.arg, &solution_mappings, &global_cats)?;
            let framed = tmp("framed");
            let running = if frame.is_none() {
                cumulative(function, columns.arg, &solution_mappings, &row_number)
            } else {
                None
            };
            if let Some(running) = running {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(over_pid(running).alias(&framed));
            } else {
                let frame = frame.unwrap_or(WindowFrame {
                    start: None,
                    end: Some(0),
                });
                solution_mappings =
                    rolling(solution_mappings, agg, partitioned.then_some(pid.as_str()), &row_number, frame, &tmp("index"), &framed);
                tmp_cols.push(tmp("index"));
            }
            tmp_cols.push(framed.clone());
            if frame.is_none() {
                // The default frame ends with the last peer of the row: the value at the last
                // row of the peer group.
                let last = tmp("last");
                let peer_end = col(&peer_group)
                    .neq_missing(col(&peer_group).shift(lit(-1)));
                solution_mappings.mappings = solution_mappings.mappings.with_column(
                    polars::prelude::when(peer_end)
                        .then(col(&pos))
                        .otherwise(lit(polars::prelude::NULL))
                        .fill_null_with_strategy(FillNullStrategy::Backward(None))
                        .cast(IDX_DTYPE)
                        .alias(&last),
                );
                tmp_cols.push(last.clone());
                (col(&framed).gather(col(&last), false), agg_type)
            } else {
                (col(&framed), agg_type)
            }
        }
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(out_expr.alias(output.as_str()))
        .drop(by_name(tmp_cols, true, false));
    solution_mappings
        .rdf_node_types
        .insert(output.as_str().to_string(), out_type);
    Ok(solution_mappings)
}

/// Keeps the rows whose RANK (and DENSE_RANK) is 1, the peers of the first row of their
/// partition, and binds `output` to 1 for them, for a window with one ordering key of one type:
/// the rows with the smallest (largest for DESC) key, or with an unbound key when the partition
/// has one in ascending order. Gives back the solution mappings unchanged otherwise. The flag
/// says whether the rows were filtered.
pub fn rank_one(
    mut solution_mappings: SolutionMappings,
    partition_by: &[Context],
    order_by: &Context,
    ascending: bool,
    output: &Context,
    global_cats: LockedCats,
) -> (SolutionMappings, bool) {
    let (keys, asc) = ordering_exprs(
        &solution_mappings,
        &[order_by.as_str().to_string()],
        vec![ascending],
        global_cats,
    );
    if keys.len() != 1 {
        return (solution_mappings, false);
    }
    let key = keys.into_iter().next().unwrap();
    let partition: Vec<Expr> = partition_by.iter().map(|c| col(c.as_str())).collect();
    let over = |e: Expr| -> Expr {
        if partition.is_empty() {
            e
        } else {
            e.over(partition.clone()).unwrap()
        }
    };
    let keep = if asc[0] {
        polars::prelude::when(over(key.clone().null_count().cast(DataType::Int64)).gt(lit(0i64)))
            .then(key.clone().is_null())
            .otherwise(key.clone().eq(over(key.min())))
    } else {
        polars::prelude::when(over(key.clone().count().cast(DataType::Int64)).eq(lit(0i64)))
            .then(lit(true))
            .otherwise(key.clone().eq(over(key.max())))
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .filter(keep)
        // A plain integer literal is materialized as Int32; RANK is Int64 on every other path.
        .with_column(lit(1i64).cast(DataType::Int64).alias(output.as_str()));
    solution_mappings.rdf_node_types.insert(
        output.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
            .into_default_input_rdf_node_state(),
    );
    (solution_mappings, true)
}

/// RANK (`min`) or DENSE_RANK of one ordering key in each partition. Polars gives unbound keys no
/// rank, they come first in ascending and last in descending order, as in ORDER BY.
fn single_key_rank(key: Expr, ascending: bool, min: bool, over: impl Fn(Expr, &[&str]) -> Expr) -> Expr {
    let ranked = key
        .clone()
        .rank(
            RankOptions {
                method: if min { RankMethod::Min } else { RankMethod::Dense },
                descending: !ascending,
            },
            None,
        )
        .cast(DataType::Int64);
    let (bound, unbound) = if ascending {
        // Unbound keys are rank 1, and the others come after them.
        let has_unbound = key.clone().null_count().cast(DataType::Int64);
        let shift = if min {
            has_unbound
        } else {
            has_unbound.gt(lit(0i64)).cast(DataType::Int64)
        };
        (over(ranked + shift, &[]), lit(1i64))
    } else if min {
        // After all bound keys.
        (
            over(ranked, &[]),
            over(key.clone().count().cast(DataType::Int64), &[]) + lit(1i64),
        )
    } else {
        (
            over(ranked.clone(), &[]),
            over(ranked.max(), &[]).fill_null(lit(0i64)) + lit(1i64),
        )
    };
    polars::prelude::when(key.is_null())
        .then(unbound)
        .otherwise(bound)
}

/// The aggregate over all rows of a group, as in GROUP BY.
fn aggregate(
    function: &WindowFunction,
    arg: Option<&Context>,
    solution_mappings: &SolutionMappings,
    global_cats: &LockedCats,
) -> Result<(Expr, RDFNodeState), QueryProcessingError> {
    let WindowFunction::Aggregate { name, distinct } = function else {
        // COUNT(*)
        return Ok((
            len().cast(DataType::Int64),
            BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                .into_default_input_rdf_node_state(),
        ));
    };
    let arg = arg.unwrap();
    Ok(match name {
        AggregateFunction::Count => count_with_expression(solution_mappings, arg, *distinct),
        AggregateFunction::Sum => sum(solution_mappings, arg, *distinct),
        AggregateFunction::Avg => avg(solution_mappings, arg, *distinct),
        AggregateFunction::Min => min_over_frames(solution_mappings, arg, global_cats.clone()),
        AggregateFunction::Max => max_over_frames(solution_mappings, arg, global_cats.clone()),
        AggregateFunction::Sample => sample(solution_mappings, arg),
        AggregateFunction::GroupConcat { separator } => group_concat(
            solution_mappings,
            arg,
            separator,
            *distinct,
            global_cats.clone(),
        ),
        AggregateFunction::Custom(nn) => match StatisticalAggregate::from_iri(nn.as_str()) {
            Some(aggregate) => statistical_aggregate(solution_mappings, arg, *distinct, aggregate),
            None => {
                return Err(QueryProcessingError::UnsupportedWindowFunction(format!(
                    "{nn} OVER (...)"
                )))
            }
        },
    })
}

/// The running aggregate from the start of the partition to the current row with cumulative
/// functions, when there is one: COUNT, and SUM, AVG, MIN and MAX of a single numeric type that
/// Polars orders as SPARQL does.
fn cumulative(
    function: &WindowFunction,
    arg: Option<&Context>,
    solution_mappings: &SolutionMappings,
    row_number: &str,
) -> Option<Expr> {
    let (name, distinct) = match function {
        WindowFunction::CountSolutions => return Some(col(row_number)),
        WindowFunction::Aggregate { name, distinct } => (name, *distinct),
        _ => return None,
    };
    if distinct {
        return None;
    }
    let arg = arg.unwrap();
    let state = solution_mappings.rdf_node_types.get(arg.as_str()).unwrap();
    let c = col(arg.as_str());
    if matches!(name, AggregateFunction::Count) {
        return Some(
            expr_is_null_workaround(c, state)
                .not()
                .cast(DataType::Int64)
                .cum_sum(false),
        );
    }
    if state.is_multi() || !state.is_numeric() || state.is_bool() {
        return None;
    }
    let t = state.get_base_type().unwrap();
    let dtype = t.polars_data_type(state.get_base_state().unwrap(), true);
    if !dtype.is_primitive_numeric() {
        return None;
    }
    let count = c.clone().is_not_null().cast(DataType::Int64).cum_sum(false);
    let filled_sum = c.clone().fill_null(lit(0).cast(dtype.clone())).cum_sum(false);
    Some(match name {
        AggregateFunction::Sum => filled_sum,
        // As AVG in GROUP BY, which is Polars' mean. AVG over integers and decimals is an
        // xsd:decimal, which the rolling window computes.
        AggregateFunction::Avg if !dtype.is_float() => return None,
        AggregateFunction::Avg => {
            let mean = filled_sum.cast(DataType::Float64) / count.clone().cast(DataType::Float64);
            let dtype_of_mean = if dtype == DataType::Float32 {
                DataType::Float32
            } else {
                DataType::Float64
            };
            let mean = mean.cast(dtype_of_mean.clone());
            // The average of no values is 0, as in GROUP BY.
            polars::prelude::when(count.gt(lit(0i64)))
                .then(mean)
                .otherwise(lit(0.0).cast(dtype_of_mean))
        }
        AggregateFunction::Min => c
            .cum_min(false)
            .fill_null_with_strategy(FillNullStrategy::Forward(None)),
        AggregateFunction::Max => c
            .cum_max(false)
            .fill_null_with_strategy(FillNullStrategy::Forward(None)),
        _ => return None,
    })
}

/// Evaluates the aggregate over the frame of each row with a rolling window. The rows are
/// sorted by partition id (`partition_id`, the position of the first row of the partition, so
/// less than 2^31) and then by position in the partition.
fn rolling(
    mut solution_mappings: SolutionMappings,
    agg: Expr,
    partition_id: Option<&str>,
    row_number: &str,
    frame: WindowFrame,
    index: &str,
    output: &str,
) -> SolutionMappings {
    // The rows of partition i are numbered from i * PARTITION_STRIDE in window order, so the
    // index is ascending (rolling checks it) and frames do not reach into other partitions.
    let index_expr = match partition_id {
        Some(p) => col(p) * lit(PARTITION_STRIDE) + col(row_number),
        None => col(row_number),
    };
    let clamp = |b: Option<i64>, default: i64| {
        b.unwrap_or(default)
            .clamp(-MAX_FRAME_OFFSET, MAX_FRAME_OFFSET)
    };
    let start = clamp(frame.start, -MAX_FRAME_OFFSET);
    let end = clamp(frame.end, MAX_FRAME_OFFSET);
    // The window of index t is (t + offset, t + offset + period].
    let offset = Duration::new(start - 1);
    let period = Duration::new(end - start + 1);
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(index_expr.alias(index))
        .with_column(
            agg.rolling(col(index), period, offset, ClosedWindow::Right)
                .alias(output),
        );
    solution_mappings
}
