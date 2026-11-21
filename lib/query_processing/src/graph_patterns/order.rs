use crate::errors::QueryProcessingError;
use polars::datatypes::{DataType, Field};
use polars::error::PolarsResult;
use polars::prelude::{
    by_name, coalesce, col, lit, when, Column, DataFrame, Expr, IntoColumn, IntoLazy, NULL, PlSmallStr, Schema, SortMultipleOptions, StructChunked,
};
use polars::prelude::{IntoSeries, UInt32Chunked};
use polars::series::Series;
use std::sync::Arc;
use representation::cats::{distinct_codes, extend_codes, maybe_decode_expr, LockedCats, RankMap};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::rdf_to_polars::default_decimal_type;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::xsd;
use std::cmp::Ordering;

pub fn order_by(
    mut solution_mappings: SolutionMappings,
    columns: &[String],
    asc_ordering: Vec<bool>,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if columns.is_empty() {
        return Ok(solution_mappings);
    }
    let (order_exprs, asc_bools) =
        ordering_exprs(&solution_mappings, columns, asc_ordering, global_cats);
    solution_mappings.mappings = solution_mappings.mappings.sort_by_exprs(
        order_exprs,
        sort_options(&asc_bools).with_maintain_order(false),
    );
    Ok(solution_mappings)
}

/// Sort options for keys from `ordering_exprs`: unbound values are lowest, so first in
/// ascending order and last in descending order.
pub fn sort_options(asc_bools: &[bool]) -> SortMultipleOptions {
    SortMultipleOptions::default()
        .with_order_descending_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
        .with_nulls_last_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
}

/// Where a page of an ordered result starts (keyset pagination): the solutions are those after
/// the marker row (the last row of the previous page), in the order of the sort columns.
#[derive(Debug, Clone)]
pub struct KeysetBound {
    pub marker: KeysetMarker,
    /// Also keep the rows equal to the marker row in all sort columns, which come first. For
    /// results with duplicate rows, where the caller skips those already returned.
    pub include_equal: bool,
}

#[derive(Debug, Clone)]
pub enum KeysetMarker {
    /// The marker row is among the solutions, where this boolean column is true only for it. It
    /// is ranked together with them, so this works with any dictionaries.
    Column(String),
    /// The marker row, one row with the sort columns, typed as the solutions. Its sort keys are
    /// computed once and compared with those of the solutions as constants, so the filter is
    /// element-wise. Only for sort keys ranked with the cached ranks of in-memory dictionaries,
    /// which are the same in both frames, see [`ranks_are_global`].
    Row(DataFrame),
}

const KEYSET_KEY_PREFIX: &str = "__maplib_keyset_key_";

/// Sorts as [`order_by`], keeping only the rows after the marker row of `bound` in that order.
/// The marker row is compared with the other rows with the same sort keys as the sort uses, so
/// the rows kept are exactly those that the sort puts after it. The marker row is removed.
pub fn keyset_order_by(
    mut solution_mappings: SolutionMappings,
    columns: &[String],
    asc_ordering: Vec<bool>,
    global_cats: LockedCats,
    bound: &KeysetBound,
) -> Result<SolutionMappings, QueryProcessingError> {
    let (exprs, ascs, is_lang) =
        ordering_exprs_with_lang(&solution_mappings, columns, asc_ordering, global_cats);
    // Language-tagged strings are ranked as structs of the ranks of the value and the language,
    // sorted by the value, then the language. Structs can not be compared with < and >, so
    // they are split into their fields.
    let mut key_exprs = vec![];
    let mut asc_bools = vec![];
    for ((e, asc), lang) in exprs.into_iter().zip(ascs).zip(is_lang) {
        if lang {
            for f in [LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD] {
                key_exprs.push(e.clone().struct_().field_by_name(f));
                asc_bools.push(asc);
            }
        } else {
            key_exprs.push(e);
            asc_bools.push(asc);
        }
    }
    let key_names: Vec<String> = (0..key_exprs.len())
        .map(|i| format!("{KEYSET_KEY_PREFIX}{i}"))
        .collect();
    let aliased: Vec<Expr> = key_exprs
        .into_iter()
        .zip(&key_names)
        .map(|(e, n)| e.alias(n))
        .collect();
    let mut lf = solution_mappings.mappings.with_columns(aliased.clone());

    // The keys of the marker row.
    let marker_keys: Vec<Expr> = match &bound.marker {
        KeysetMarker::Column(marker) => key_names
            .iter()
            .map(|k| col(k).filter(col(marker)).first())
            .collect(),
        KeysetMarker::Row(row) => {
            let row = row
                .clone()
                .lazy()
                .select(aliased)
                .collect()
                .map_err(|e| QueryProcessingError::BadArgument(e.to_string()))?;
            let mut keys = vec![];
            for k in &key_names {
                let c = row
                    .column(k)
                    .map_err(|e| QueryProcessingError::BadArgument(e.to_string()))?
                    .as_materialized_series()
                    .clone();
                keys.push(lit(c).first());
            }
            keys
        }
    };
    // Is the row after the marker in the sort order? Ascending sorts put nulls first and
    // descending sorts put them last, so null is the smallest value either way.
    let mut after: Option<Expr> = None;
    let mut all_equal: Option<Expr> = None;
    for ((k, m), asc) in key_names
        .iter()
        .zip(&marker_keys)
        .zip(&asc_bools)
        .rev()
    {
        let v = col(k);
        let gt = if *asc {
            (v.clone().is_not_null().and(m.clone().is_null()))
                .or(v.clone().gt(m.clone()).fill_null(lit(false)))
        } else {
            (v.clone().is_null().and(m.clone().is_not_null()))
                .or(v.clone().lt(m.clone()).fill_null(lit(false)))
        };
        let eq = v.eq_missing(m.clone());
        after = Some(match after {
            None => gt,
            Some(after) => gt.or(eq.clone().and(after)),
        });
        all_equal = Some(match all_equal {
            None => eq,
            Some(all_equal) => eq.and(all_equal),
        });
    }
    let mut keep = after.unwrap_or(lit(false));
    if bound.include_equal {
        keep = keep.or(all_equal.unwrap_or(lit(true)));
    }
    if let KeysetMarker::Column(marker) = &bound.marker {
        keep = col(marker).not().and(keep);
    }
    lf = lf.filter(keep);
    lf = lf.sort(key_names.clone(), sort_options(&asc_bools).with_maintain_order(false));
    let mut to_drop = key_names;
    if let KeysetMarker::Column(marker) = &bound.marker {
        to_drop.push(marker.clone());
        solution_mappings.rdf_node_types.remove(marker);
    }
    solution_mappings.mappings = lf.drop(by_name(to_drop, true, false));
    Ok(solution_mappings)
}

/// The sort keys that order the columns as SPARQL ORDER BY does, with whether each key is
/// ascending. A column with several types gives several keys.
pub fn ordering_exprs(
    solution_mappings: &SolutionMappings,
    columns: &[String],
    asc_ordering: Vec<bool>,
    global_cats: LockedCats,
) -> (Vec<Expr>, Vec<bool>) {
    let (exprs, ascs, _) =
        ordering_exprs_with_lang(solution_mappings, columns, asc_ordering, global_cats);
    (exprs, ascs)
}

/// As [`ordering_exprs`], also telling for each key whether it is a struct of the ranks of the
/// value and the language of language-tagged strings.
pub fn ordering_exprs_with_lang(
    solution_mappings: &SolutionMappings,
    columns: &[String],
    asc_ordering: Vec<bool>,
    global_cats: LockedCats,
) -> (Vec<Expr>, Vec<bool>, Vec<bool>) {
    let mut is_lang = vec![];
    let mut order_exprs = vec![];
    let mut asc_bools = vec![];
    for (c, a) in columns.iter().zip(asc_ordering) {
        let t = solution_mappings.rdf_node_types.get(c).unwrap();
        if t.is_multi() {
            let ts: Vec<_> = t.map.iter().collect();
            let mut ts = ts.clone();
            ts.sort_by(|(x, _), (y, _)| {
                if x.is_none() {
                    if y.is_none() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if x.is_blank_node() {
                    // SPARQL 1.1, 15.1: unbound, then blank nodes, then IRIs, then literals.
                    if y.is_none() {
                        Ordering::Greater
                    } else if y.is_blank_node() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if x.is_iri() {
                    if y.is_none() || y.is_blank_node() {
                        Ordering::Greater
                    } else if y.is_iri() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if let BaseRDFNodeType::Literal(x_l, _) = x {
                    if let BaseRDFNodeType::Literal(y_l, _) = y {
                        // Numbers of all datatypes are ordered together, where xsd:decimal is.
                        // Types of the same datatype differ only in the time zone, which gives a
                        // deterministic order to the fields of the datatype.
                        let group = |t: &BaseRDFNodeType, l| {
                            if t.is_numeric() {
                                xsd::DECIMAL.into_owned()
                            } else {
                                l
                            }
                        };
                        group(x, x_l.clone())
                            .cmp(&group(y, y_l.clone()))
                            .then_with(|| x_l.cmp(y_l))
                            .then_with(|| x.cmp(y))
                    } else {
                        Ordering::Greater
                    }
                } else {
                    unreachable!()
                }
            });
            // A bit complicated:
            // Nulls are first in ascending sort
            // By placing literals first, we are using the subsequent types to break ties where the literal is null.
            ts.reverse();
            let mut i = 0;
            while i < ts.len() {
                let (t, s) = ts[i];
                // dateTimes with different time zones are different types, stored as UTC
                // instants. They are ordered together by instant, then by time zone.
                let run = ts[i..]
                    .iter()
                    .take_while(|(u, _)| is_date_time(u) && same_datatype(t, u))
                    .count();
                // Numbers of different datatypes are ordered by value (SPARQL 1.1, 15.1: the `<`
                // operator), so equal numbers of different datatypes are equal and ordered by
                // the next sort key, as `0` and `0.0e0`.
                let numeric_run = ts[i..].iter().take_while(|(u, _)| u.is_numeric()).count();
                if numeric_run > 1 || (numeric_run == 1 && is_lexical_number(t, s)) {
                    let numeric_types = &ts[i..i + numeric_run];
                    order_exprs.push(numeric_value_expr(c, numeric_types, &global_cats, true));
                    asc_bools.push(a);
                    is_lang.push(false);
                    for e in exact_remainder_exprs(c, numeric_types, &global_cats, true) {
                        order_exprs.push(e);
                        asc_bools.push(a);
                        is_lang.push(false);
                    }
                    i += numeric_run;
                    continue;
                }
                if run > 1 {
                    let mut run_types: Vec<_> = ts[i..i + run].iter().map(|(u, _)| *u).collect();
                    run_types.sort();
                    order_exprs.push(date_time_instant_expr(c, &run_types));
                    order_exprs.push(time_zone_rank_expr(c, &run_types));
                    is_lang.push(false);
                    is_lang.push(false);
                    asc_bools.push(a);
                    asc_bools.push(a);
                    i += run;
                    continue;
                }
                i += 1;
                asc_bools.push(a);
                is_lang.push(t.is_lang_string());
                if t.is_lang_string() {
                    order_exprs.push(make_ordering_expr(
                        col(c),
                        global_cats.clone(),
                        t.clone(),
                        s.clone(),
                    ));
                } else {
                    order_exprs.push(make_ordering_expr(
                        col(c).struct_().field_by_name(&t.field_col_name()),
                        global_cats.clone(),
                        t.clone(),
                        s.clone(),
                    ));
                }
            }
        } else if is_lexical_number(t.get_base_type().unwrap(), t.get_base_state().unwrap()) {
            let types = [(t.get_base_type().unwrap(), t.get_base_state().unwrap())];
            order_exprs.push(numeric_value_expr(c, &types, &global_cats, false));
            asc_bools.push(a);
            is_lang.push(false);
            for e in exact_remainder_exprs(c, &types, &global_cats, false) {
                order_exprs.push(e);
                asc_bools.push(a);
                is_lang.push(false);
            }
        } else {
            asc_bools.push(a);
            is_lang.push(t.get_base_type().unwrap().is_lang_string());
            order_exprs.push(make_ordering_expr(
                col(c),
                global_cats.clone(),
                t.get_base_type().unwrap().clone(),
                t.get_base_state().unwrap().clone(),
            ));
        }
    }
    (order_exprs, asc_bools, is_lang)
}

/// Is this a number stored as its lexical form (the derived integer types, as
/// xsd:nonNegativeInteger), which is ordered by value, not as a string.
fn is_lexical_number(t: &BaseRDFNodeType, s: &BaseCatState) -> bool {
    t.is_numeric() && !matches!(s, BaseCatState::NonString)
}

fn is_date_time(t: &BaseRDFNodeType) -> bool {
    t.is_lit_type(xsd::DATE_TIME) || t.is_lit_type(xsd::DATE_TIME_STAMP)
}

fn same_datatype(x: &BaseRDFNodeType, y: &BaseRDFNodeType) -> bool {
    matches!((x, y), (BaseRDFNodeType::Literal(x, _), BaseRDFNodeType::Literal(y, _)) if x == y)
}

/// The number in the field of `t` of the column: as stored for the datatypes stored as numbers,
/// else (the derived integer types, as xsd:nonNegativeInteger) as its lexical form, which is
/// cast to the number.
fn numeric_field_expr(
    c: &str,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    global_cats: &LockedCats,
    multi: bool,
) -> Expr {
    let f = if multi {
        col(c).struct_().field_by_name(&t.field_col_name())
    } else {
        col(c)
    };
    match s {
        BaseCatState::NonString => f,
        _ => maybe_decode_expr(f, t, s, global_cats.clone()),
    }
}

/// The value as a double of the number in any of the fields of `types`, null for other rows.
fn numeric_value_expr(
    c: &str,
    types: &[(&BaseRDFNodeType, &BaseCatState)],
    global_cats: &LockedCats,
    multi: bool,
) -> Expr {
    coalesce(
        &types
            .iter()
            .map(|(t, s)| numeric_field_expr(c, t, s, global_cats, multi).cast(DataType::Float64))
            .collect::<Vec<_>>(),
    )
}

const TWO_63: f64 = 9_223_372_036_854_775_808.0;
const TWO_64: f64 = 18_446_744_073_709_551_616.0;

/// Breaks ties of [`numeric_value_expr`] between integers and decimals that a double can not
/// tell apart. Integers and decimals are compared exactly, and numbers are compared with floats
/// and doubles as doubles (XPath numeric type promotion), so a number equal to a double as a
/// double is equal to it.
///
/// The key is the deviation of the exact value from its double, zero for floats and doubles,
/// which orders the values with the same double. Without decimals, it is computed without
/// decimal arithmetic: the double of a 64 bit integer is a whole number, and the deviation from
/// it is a whole number of at most 2^10, exact as a double. Integers of at most 32 bits are
/// exact as doubles, with no deviation. With decimals (or the derived integer types, stored as
/// their lexical forms), the deviation of every integer and decimal is a decimal, from the
/// double rounded to the scale. No key if there are only floats and doubles among `types`.
fn exact_remainder_exprs(
    c: &str,
    types: &[(&BaseRDFNodeType, &BaseCatState)],
    global_cats: &LockedCats,
    multi: bool,
) -> Vec<Expr> {
    let exact_types: Vec<_> = types
        .iter()
        .filter(|(t, s)| !is_exact_as_double(t, s))
        .collect();
    if exact_types.is_empty() {
        return vec![];
    }
    let with_decimals = exact_types.iter().any(|(t, s)| {
        !matches!(s, BaseCatState::NonString)
            || !(t.is_lit_type(xsd::INTEGER)
                || t.is_lit_type(xsd::LONG)
                || t.is_lit_type(xsd::UNSIGNED_LONG))
    });
    let deviations: Vec<_> = exact_types
        .iter()
        .map(|(t, s)| {
            let f = numeric_field_expr(c, t, s, global_cats, multi);
            if with_decimals {
                decimal_deviation(f, matches!(s, BaseCatState::NonString))
            } else if t.is_lit_type(xsd::UNSIGNED_LONG) {
                uint64_deviation(f)
            } else {
                int64_deviation(f)
            }
        })
        .collect();
    let zero = if with_decimals {
        lit(0).cast(default_decimal_type())
    } else {
        lit(0.0)
    };
    vec![coalesce(&deviations).fill_null(zero)]
}

/// Floats, doubles and integers of at most 32 bits stored as numbers, whose doubles are exact.
fn is_exact_as_double(t: &BaseRDFNodeType, s: &BaseCatState) -> bool {
    t.is_lit_type(xsd::DOUBLE)
        || t.is_lit_type(xsd::FLOAT)
        || (matches!(s, BaseCatState::NonString)
            && [
                xsd::INT,
                xsd::SHORT,
                xsd::BYTE,
                xsd::UNSIGNED_INT,
                xsd::UNSIGNED_SHORT,
                xsd::UNSIGNED_BYTE,
            ]
            .iter()
            .any(|dt| t.is_lit_type(*dt)))
}

/// The deviation of a number from its double rounded to the scale, as a decimal. Numbers
/// stored as their lexical forms may be beyond the range of decimals, whose doubles are not
/// cast, as the cast may overflow; the number is then null too.
fn decimal_deviation(f: Expr, native: bool) -> Expr {
    let exact = default_decimal_type();
    let mut v = f.clone().cast(DataType::Float64);
    if !native {
        v = when(v.clone().abs().lt(lit(1e22)))
            .then(v)
            .otherwise(lit(0.0));
    }
    f.cast(exact.clone()) - v.cast(exact)
}

/// The deviation of a 64 bit integer from its double, as a double (exact, at most 2^10). The
/// double of integers close to 2^63 is 2^63, which is not a 64 bit integer.
fn int64_deviation(x: Expr) -> Expr {
    let v = x.clone().cast(DataType::Float64);
    when(v.clone().gt_eq(lit(TWO_63)))
        .then((x.clone() - lit(i64::MAX)).cast(DataType::Float64) - lit(1.0))
        .otherwise((x - v.cast(DataType::Int64)).cast(DataType::Float64))
}

/// As [`int64_deviation`] for unsigned 64 bit integers, whose double may be 2^64. The
/// subtraction that is not chosen may wrap around, which is harmless.
fn uint64_deviation(x: Expr) -> Expr {
    let v = x.clone().cast(DataType::Float64);
    let base = v.clone().cast(DataType::UInt64);
    when(v.gt_eq(lit(TWO_64)))
        .then(lit(-1.0) - (lit(u64::MAX) - x.clone()).cast(DataType::Float64))
        .when(x.clone().gt_eq(base.clone()))
        .then((x.clone() - base.clone()).cast(DataType::Float64))
        .otherwise(lit(0.0) - (base - x).cast(DataType::Float64))
}

/// The UTC instant of the dateTime in any of the fields of `types`, null for other rows.
///
/// A dateTime without a time zone is stored as if it was in UTC, so it is ordered as if its
/// implicit time zone was Z. XPath leaves the implicit time zone to the implementation, and
/// SPARQL 1.1 only requires that ORDER BY gives the same order every time.
fn date_time_instant_expr(c: &str, types: &[&BaseRDFNodeType]) -> Expr {
    coalesce(
        &types
            .iter()
            .map(|t| {
                col(c)
                    .struct_()
                    .field_by_name(&t.field_col_name())
                    .cast(DataType::Int64)
            })
            .collect::<Vec<_>>(),
    )
}

/// Breaks ties between dateTimes at the same instant by time zone: no time zone first, then by
/// increasing offset.
fn time_zone_rank_expr(c: &str, types: &[&BaseRDFNodeType]) -> Expr {
    coalesce(
        &types
            .iter()
            .enumerate()
            .map(|(rank, t)| {
                when(
                    col(c)
                        .struct_()
                        .field_by_name(&t.field_col_name())
                        .is_not_null(),
                )
                .then(lit(rank as u32))
                .otherwise(lit(NULL).cast(DataType::UInt32))
            })
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn make_ordering_expr(
    e: Expr,
    cats: LockedCats,
    t: BaseRDFNodeType,
    s: BaseCatState,
) -> Expr {
    if let BaseCatState::CategoricalNative(local) = s {
        let output_dtype = if t.is_lang_string() {
            DataType::Struct(vec![
                Field::new(LANG_STRING_VALUE_FIELD.into(), DataType::UInt32),
                Field::new(LANG_STRING_LANG_FIELD.into(), DataType::UInt32),
            ])
        } else {
            DataType::UInt32
        };
        let output_type = move |_: &Schema, f: &Field| Ok(Field::new(f.name().clone(), output_dtype.clone()));

        // With in-memory dictionaries, the ranks of all codes are cached, so they are taken once
        // here and the column is ranked batch by batch (the elementwise `map`). That lets the
        // streaming engine sort without holding the whole input in memory, and spill.
        let all_global = cats.read().unwrap().all_global_ranks(&t);
        let all_local = match &local {
            Some(local) => local.read().unwrap().all_global_ranks(&t).map(Some),
            None => Some(None),
        };
        if let (Some(global), Some(local)) = (all_global, all_local) {
            let ranks = Arc::new(Ranks { local, global });
            return e.map(move |x| rank_column(&x, &t, &ranks), output_type);
        }

        // Otherwise the ranks are looked up for the codes in the column, which gives ranks
        // comparable only within that call, so it must see the whole column at once: `apply`.
        e.apply(
            move |x| {
                let mut u32_ser_vec = vec![];
                if t.is_lang_string() {
                    u32_ser_vec.push(x.struct_()?.field_by_name(LANG_STRING_VALUE_FIELD)?);
                    u32_ser_vec.push(x.struct_()?.field_by_name(LANG_STRING_LANG_FIELD)?);
                } else {
                    u32_ser_vec.push(x.as_materialized_series_maintain_scalar());
                }
                let mut codes = Vec::new();
                for u32_ser in u32_ser_vec {
                    extend_codes(&mut codes, &u32_ser.into_column());
                }
                let codes = distinct_codes(codes);

                // Local codes take precedence over global ones, as they are re-encodings.
                let local_ranks = local
                    .as_ref()
                    .map(|local| local.read().unwrap().global_ranks(&codes, &t));
                let global_ranks = cats.read().unwrap().global_ranks(&codes, &t);
                let ranks = Ranks {
                    local: local_ranks,
                    global: global_ranks,
                };
                rank_column(&x, &t, &ranks)
            },
            output_type,
        )
    } else {
        e
    }
}

/// Replaces the codes in `x` by their ranks, field by field for language-tagged strings.
fn rank_column(x: &Column, t: &BaseRDFNodeType, ranks: &Ranks) -> PolarsResult<Column> {
    let original_name = x.name().clone();
    let mut ser = if t.is_lang_string() {
        let st = x.struct_()?;
        let mut value_rank = map_to_rank(&st.field_by_name(LANG_STRING_VALUE_FIELD)?, ranks)?;
        value_rank.rename(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD));
        let mut lang_rank = map_to_rank(&st.field_by_name(LANG_STRING_LANG_FIELD)?, ranks)?;
        lang_rank.rename(PlSmallStr::from_str(LANG_STRING_LANG_FIELD));
        StructChunked::from_series(
            original_name.clone(),
            x.len(),
            [value_rank, lang_rank].iter(),
        )?
        .into_series()
    } else {
        map_to_rank(x.as_materialized_series(), ranks)?
    };
    ser.rename(original_name);
    Ok(ser.into_column())
}

struct Ranks {
    local: Option<RankMap>,
    global: RankMap,
}

impl Ranks {
    #[inline]
    fn get(&self, u: u32) -> Option<u32> {
        if let Some(local) = &self.local {
            if let Some(r) = local.get(u) {
                return Some(r);
            }
        }
        self.global.get(u)
    }
}

fn map_to_rank(series: &Series, ranks: &Ranks) -> PolarsResult<Series> {
    let ch = series.u32()?;
    let out: UInt32Chunked = if ch.null_count() == 0 {
        ch.into_no_null_iter().map(|u| ranks.get(u)).collect()
    } else {
        ch.iter().map(|u| u.and_then(|u| ranks.get(u))).collect()
    };
    Ok(out.into_series())
}

/// Are the sort keys of these columns ranked with ranks that are the same in any frame with
/// the same types, so that the keys of a marker row can be computed on their own: the cached
/// ranks of in-memory dictionaries, with no local dictionaries. Columns that are not dictionary
/// encoded (numbers, dates, ...) are compared by value.
pub fn ranks_are_global(
    solution_mappings: &SolutionMappings,
    columns: &[String],
    global_cats: &LockedCats,
) -> bool {
    let cats = global_cats.read().unwrap();
    for c in columns {
        let Some(t) = solution_mappings.rdf_node_types.get(c) else {
            return false;
        };
        for (bt, bs) in &t.map {
            match bs {
                BaseCatState::CategoricalNative(None) => {
                    if cats.all_global_ranks(bt).is_none() {
                        return false;
                    }
                }
                BaseCatState::CategoricalNative(Some(_)) | BaseCatState::String => return false,
                BaseCatState::NonString => {}
            }
        }
    }
    true
}
