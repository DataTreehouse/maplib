use crate::errors::QueryProcessingError;
use polars::datatypes::{DataType, Field};
use polars::error::PolarsResult;
use polars::prelude::{
    coalesce, col, lit, when, Column, Expr, IntoColumn, NULL, PlSmallStr, Schema, SortMultipleOptions, StructChunked,
};
use polars::prelude::{IntoSeries, UInt32Chunked};
use polars::series::Series;
use std::sync::Arc;
use representation::cats::{distinct_codes, extend_codes, LockedCats, RankMap};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
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
                } else if x.is_iri() {
                    if y.is_none() {
                        Ordering::Greater
                    } else if y.is_iri() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if x.is_blank_node() {
                    if y.is_none() || y.is_iri() {
                        Ordering::Greater
                    } else if y.is_blank_node() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if let BaseRDFNodeType::Literal(x_l, _) = x {
                    if let BaseRDFNodeType::Literal(y_l, _) = y {
                        // Types of the same datatype differ only in the time zone, which gives a
                        // deterministic order to the fields of the datatype.
                        x_l.cmp(y_l).then_with(|| x.cmp(y))
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
                if run > 1 {
                    let mut run_types: Vec<_> = ts[i..i + run].iter().map(|(u, _)| *u).collect();
                    run_types.sort();
                    order_exprs.push(date_time_instant_expr(c, &run_types));
                    order_exprs.push(time_zone_rank_expr(c, &run_types));
                    asc_bools.push(a);
                    asc_bools.push(a);
                    i += run;
                    continue;
                }
                i += 1;
                asc_bools.push(a);
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
        } else {
            asc_bools.push(a);
            order_exprs.push(make_ordering_expr(
                col(c),
                global_cats.clone(),
                t.get_base_type().unwrap().clone(),
                t.get_base_state().unwrap().clone(),
            ));
        }
    }

    solution_mappings.mappings = solution_mappings.mappings.sort_by_exprs(
        order_exprs,
        SortMultipleOptions::default()
            .with_order_descending_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
            .with_nulls_last_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
            .with_maintain_order(false),
    );

    Ok(solution_mappings)
}

fn is_date_time(t: &BaseRDFNodeType) -> bool {
    t.is_lit_type(xsd::DATE_TIME) || t.is_lit_type(xsd::DATE_TIME_STAMP)
}

fn same_datatype(x: &BaseRDFNodeType, y: &BaseRDFNodeType) -> bool {
    matches!((x, y), (BaseRDFNodeType::Literal(x, _), BaseRDFNodeType::Literal(y, _)) if x == y)
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

fn make_ordering_expr(e: Expr, cats: LockedCats, t: BaseRDFNodeType, s: BaseCatState) -> Expr {
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
