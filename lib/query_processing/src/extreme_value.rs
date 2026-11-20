//! MIN and MAX as one pass over each group.
//!
//! The values of the group are ordered as SPARQL ORDER BY orders them: first by kind (blank
//! nodes, IRIs, numbers, then every other literal type), and within a kind by a key (the number
//! as a double, the decoded string of a dictionary code, or the stored value). Only the values
//! of the first kind present in the group (the last for MAX) can be chosen, so only their keys
//! are computed, in batches of bounded size, and compared in a scan. Ties go to the first row.

use polars::datatypes::{DataType, Field};
use polars::prelude::{
    Column, Expr, IdxCa, IdxSize, IntoColumn, NewChunkedArray, PolarsResult, Series, SortOptions,
    StaticArray,
};
use rayon::prelude::*;
use representation::cats::LockedCats;
use representation::BaseRDFNodeType;

/// Candidate rows whose keys are computed and compared together.
const BATCH_ROWS: usize = 1 << 20;
const GROUPS_PER_TASK: usize = 1 << 12;

/// How the values of one type are compared with each other.
#[derive(Clone)]
pub(crate) enum ExtremeKey {
    /// By value as a double, together with the other numeric types of the column.
    Numeric,
    /// Dictionary codes, by their decoded strings.
    Decoded(BaseRDFNodeType, Option<LockedCats>),
    /// By the stored value.
    Stored,
}

/// One type of the column: where its values are and how they are ordered.
#[derive(Clone)]
pub(crate) struct ExtremeField {
    /// The field of the struct column holding the values, or None if the column is not a struct.
    pub name: Option<String>,
    /// Position of the kind of this type in the ordering. Numeric types share one.
    pub kind: u16,
    pub key: ExtremeKey,
}

/// The first (or with `max` the last) value of each group in the ordering given by `fields`.
/// An unbound value is only chosen if all the values of the group are unbound.
pub(crate) fn extreme_value(
    e: Expr,
    fields: Vec<ExtremeField>,
    max: bool,
    global_cats: LockedCats,
) -> Expr {
    e.implode(true).map(
        move |c| extreme_of_lists(&c, &fields, max, &global_cats),
        |_, f| {
            let dtype = match f.dtype() {
                DataType::List(inner) => inner.as_ref().clone(),
                other => other.clone(),
            };
            Ok(Field::new(f.name().clone(), dtype))
        },
    )
}

struct Kind<'a> {
    key: &'a ExtremeKey,
    values: Vec<Series>,
}

fn extreme_of_lists(
    c: &Column,
    fields: &[ExtremeField],
    max: bool,
    global_cats: &LockedCats,
) -> PolarsResult<Column> {
    let lists = c.as_materialized_series().rechunk();
    let lists = lists.list()?;
    let inner = lists.get_inner().rechunk();
    let n_rows = inner.len();
    let Some(arr) = lists.downcast_iter().next() else {
        return Ok(inner.clear().with_name(c.name().clone()).into_column());
    };
    let offsets: Vec<usize> = arr.offsets().iter().map(|o| *o as usize).collect();
    let n_groups = offsets.len() - 1;

    // The code of a kind is larger the later the kind is in the ordering. With MIN the smallest
    // code of a group is chosen and unbound has the largest code, with MAX it is the reverse.
    let none: u16 = if max { 0 } else { u16::MAX };
    let code_of = |kind: u16| if max { kind + 1 } else { kind };

    let mut kinds: Vec<(u16, Kind)> = vec![];
    let mut codes = vec![none; n_rows];
    for f in fields {
        let values = match &f.name {
            Some(name) => inner.struct_()?.field_by_name(name)?.rechunk(),
            None => inner.clone(),
        };
        // As in a coalesce over the types, the first type with a value decides the kind of a row
        let code = code_of(f.kind);
        if values.null_count() == 0 {
            codes
                .iter_mut()
                .filter(|c| **c == none)
                .for_each(|c| *c = code);
        } else if values.null_count() < n_rows {
            let valid = values.is_not_null();
            for a in valid.downcast_iter() {
                for i in a.values().true_idx_iter() {
                    if codes[i] == none {
                        codes[i] = code;
                    }
                }
            }
        }
        match kinds.iter_mut().find(|(k, _)| *k == f.kind) {
            Some((_, kind)) => kind.values.push(values),
            None => kinds.push((
                f.kind,
                Kind {
                    key: &f.key,
                    values: vec![values],
                },
            )),
        }
    }

    let best: Vec<u16> = (0..n_groups)
        .into_par_iter()
        .with_min_len(GROUPS_PER_TASK)
        .map(|g| {
            let group = &codes[offsets[g]..offsets[g + 1]];
            if max {
                group.iter().copied().max().unwrap_or(none)
            } else {
                group.iter().copied().min().unwrap_or(none)
            }
        })
        .collect();

    // A group without bound values gives its first row, an empty group gives null
    let mut chosen: Vec<Option<IdxSize>> = (0..n_groups)
        .map(|g| {
            let valid = arr.validity().is_none_or(|v| v.get_bit(g));
            (valid && offsets[g] < offsets[g + 1]).then_some(offsets[g] as IdxSize)
        })
        .collect();

    for (kind, k) in &kinds {
        let code = code_of(*kind);
        let mut rows: Vec<IdxSize> = vec![];
        // The candidates of a group: the group, and a range of `rows`
        let mut segments: Vec<(usize, usize, usize)> = vec![];
        for g in 0..n_groups {
            if best[g] != code {
                continue;
            }
            let mut from = rows.len();
            for i in offsets[g]..offsets[g + 1] {
                if codes[i] != code {
                    continue;
                }
                rows.push(i as IdxSize);
                if rows.len() >= BATCH_ROWS {
                    segments.push((g, from, rows.len()));
                    choose(k, &rows, &segments, max, global_cats, &mut chosen)?;
                    rows.clear();
                    segments.clear();
                    // The best so far is compared with the rest of the group, and is before it
                    rows.push(chosen[g].unwrap());
                    from = 0;
                }
            }
            if rows.len() - from == 1 {
                chosen[g] = rows.pop();
            } else if rows.len() > from {
                segments.push((g, from, rows.len()));
            }
        }
        if !segments.is_empty() {
            choose(k, &rows, &segments, max, global_cats, &mut chosen)?;
        }
    }

    let chosen = IdxCa::from_iter_options("".into(), chosen.into_iter());
    let mut out = inner.take(&chosen)?;
    out.rename(c.name().clone());
    Ok(out.into_column())
}

/// Sets the chosen row of every segment to its first row with the smallest (largest) key.
fn choose(
    kind: &Kind,
    rows: &[IdxSize],
    segments: &[(usize, usize, usize)],
    max: bool,
    global_cats: &LockedCats,
    chosen: &mut [Option<IdxSize>],
) -> PolarsResult<()> {
    let idx = IdxCa::from_slice("".into(), rows);
    let keys = match kind.key {
        ExtremeKey::Numeric => {
            let mut keys: Option<Series> = None;
            for values in &kind.values {
                let k = values.take(&idx)?.cast(&DataType::Float64)?;
                keys = Some(match keys {
                    None => k,
                    Some(first) => first.zip_with(&first.is_not_null(), &k)?,
                });
            }
            keys.unwrap()
        }
        ExtremeKey::Decoded(t, local_cats) => {
            let codes = kind.values[0].take(&idx)?;
            global_cats
                .read()
                .unwrap()
                .decode_of_type(&codes, t, local_cats.clone())?
        }
        ExtremeKey::Stored => kind.values[0].take(&idx)?,
    };
    let keys = keys.to_physical_repr().rechunk();
    let positions = match keys.dtype() {
        DataType::String => {
            let ca = keys.str()?;
            let a = ca.downcast_iter().next().unwrap();
            first_extreme(segments, max, |i| a.get(i), |x, y| x < y)
        }
        DataType::Float32 | DataType::Float64 => {
            let keys = keys.cast(&DataType::Float64)?;
            let a = keys.f64()?.downcast_iter().next().unwrap();
            // As in a sort: NaN is the largest value, and -0.0 equals 0.0
            first_extreme(
                segments,
                max,
                |i| a.get(i),
                |x, y| !x.is_nan() && (y.is_nan() || x < y),
            )
        }
        DataType::UInt64 => {
            let a = keys.u64()?.downcast_iter().next().unwrap();
            first_extreme(segments, max, |i| a.get(i), |x, y| x < y)
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            let keys = keys.cast(&DataType::Int64)?;
            let a = keys.i64()?.downcast_iter().next().unwrap();
            first_extreme(segments, max, |i| a.get(i), |x, y| x < y)
        }
        _ => {
            let options = SortOptions::default()
                .with_order_descending(max)
                .with_nulls_last(true)
                .with_maintain_order(true)
                .with_multithreaded(false);
            segments
                .iter()
                .map(|(_, from, to)| {
                    let order = keys.slice(*from as i64, to - from).arg_sort(options);
                    from + order.get(0).unwrap() as usize
                })
                .collect()
        }
    };
    for ((g, _, _), p) in segments.iter().zip(positions) {
        chosen[*g] = Some(rows[p]);
    }
    Ok(())
}

/// The position of the first smallest (with `max` the first largest) key of each segment, where
/// a null key is only chosen if all the keys of the segment are null.
fn first_extreme<T: Copy>(
    segments: &[(usize, usize, usize)],
    max: bool,
    get: impl Fn(usize) -> Option<T> + Sync,
    less: impl Fn(T, T) -> bool + Sync,
) -> Vec<usize> {
    segments
        .par_iter()
        .with_min_len(GROUPS_PER_TASK)
        .map(|(_, from, to)| {
            let mut at = *from;
            let mut best = get(at);
            for i in from + 1..*to {
                let Some(v) = get(i) else { continue };
                let better = match best {
                    None => true,
                    Some(b) if max => less(b, v),
                    Some(b) => less(v, b),
                };
                if better {
                    at = i;
                    best = Some(v);
                }
            }
            at
        })
        .collect()
}
