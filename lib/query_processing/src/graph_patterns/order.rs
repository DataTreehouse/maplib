use crate::errors::QueryProcessingError;
use polars::datatypes::{DataType, Field};
use polars::error::PolarsResult;
use polars::frame::DataFrame;
use polars::prelude::{
    as_struct, col, Expr, IntoColumn, IntoLazy, PlSmallStr, SortMultipleOptions,
};
use polars::prelude::{IntoSeries, UInt32Chunked};
use polars::series::Series;
use representation::cats::{distinct_codes, extend_codes, LockedCats, RankMap};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
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
        solution_mappings = solution_mappings.as_eager().as_lazy();

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
                        x_l.cmp(y_l)
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
            for (t, s) in ts {
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
        // Ranks are local to the values seen in one call, so this must see the whole column at
        // once: `apply`, not the elementwise `map`, which the streaming engine runs per batch.
        e.apply(
            move |x| {
                let original_name = x.name().clone();
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

                let t = t.clone();
                // Local codes take precedence over global ones, as they are re-encodings.
                let local_ranks = local
                    .as_ref()
                    .map(|local| local.read().unwrap().global_ranks(&codes, &t));
                let global_ranks = cats.read().unwrap().global_ranks(&codes, &t);
                let ranks = Ranks {
                    local: local_ranks,
                    global: global_ranks,
                };
                let mut ser = if t.is_lang_string() {
                    let value_ser = x.struct_()?.field_by_name(LANG_STRING_VALUE_FIELD)?;
                    let lang_ser = x.struct_()?.field_by_name(LANG_STRING_LANG_FIELD)?;

                    let mut value_ser_rank = map_to_rank(&value_ser, &ranks)?;
                    value_ser_rank.rename(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD));
                    let mut lang_ser_rank = map_to_rank(&lang_ser, &ranks)?;
                    lang_ser_rank.rename(PlSmallStr::from_str(LANG_STRING_LANG_FIELD));
                    let mut df = DataFrame::new(
                        x.len(),
                        vec![value_ser_rank.into_column(), lang_ser_rank.into_column()],
                    )?;
                    df = df
                        .lazy()
                        .with_column(
                            as_struct(vec![
                                col(LANG_STRING_VALUE_FIELD),
                                col(LANG_STRING_LANG_FIELD),
                            ])
                            .alias(original_name.as_str()),
                        )
                        .select([col(original_name.as_str())])
                        .collect()?;
                    let column = df.drop_in_place(original_name.as_str())?;
                    column.as_materialized_series().clone()
                } else {
                    map_to_rank(x.as_materialized_series(), &ranks)?
                };
                ser.rename(original_name);
                Ok(ser.into_column())
            },
            move |_, f| Ok(Field::new(f.name().clone(), output_dtype.clone())),
        )
    } else {
        e
    }
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
