use crate::errors::QueryProcessingError;
use polars::datatypes::{DataType, Field};
use polars::frame::DataFrame;
use polars::prelude::{col, Column, Expr, IntoColumn, SortMultipleOptions};
use polars::series::Series;
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::BaseRDFNodeType;
use std::cmp::Ordering;
use std::collections::HashSet;

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
        solution_mappings = solution_mappings.as_eager(false).as_lazy();

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
                } else if let BaseRDFNodeType::Literal(x_l) = x {
                    if let BaseRDFNodeType::Literal(y_l) = y {
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
                order_exprs.push(make_ordering_expr(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    global_cats.clone(),
                    t.clone(),
                    s.clone(),
                ));
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
    if let BaseCatState::CategoricalNative(_, local) = s {
        e.map(
            move |x| {
                let x_len = x.len();
                let original_name = x.name().clone();

                let u32s: HashSet<_> = x
                    .u32()
                    .unwrap()
                    .iter()
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .collect();
                let t = t.clone();
                let local = local.clone();
                let local_rank_map = if let Some(local) = local {
                    let rank = local.read().unwrap().rank_map(u32s.clone(), &t);
                    Some(rank)
                } else {
                    None
                };
                let mut global_rank_map = cats.read().unwrap().rank_map(u32s, &t);
                if let Some(local_rank_map) = local_rank_map {
                    global_rank_map.extend(local_rank_map);
                }
                let mut ser = Series::from_iter(x.u32().unwrap().iter().map(|x| {
                    if let Some(x) = x {
                        global_rank_map.get(&x).cloned()
                    } else {
                        None
                    }
                }));
                ser.rename(original_name);
                Ok(ser.into_column())
            },
            |_, f| Ok(Field::new(f.name().clone(), DataType::UInt32)),
        )
    } else {
        e
    }
}
