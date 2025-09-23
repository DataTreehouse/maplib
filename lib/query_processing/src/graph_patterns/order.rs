use crate::errors::QueryProcessingError;
use polars::prelude::{col, Expr, SortMultipleOptions};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::BaseRDFNodeType;
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
        solution_mappings = make_sortable(c, solution_mappings, global_cats.clone());
        solution_mappings = solution_mappings.as_eager(false).as_lazy();

        let t = solution_mappings.rdf_node_types.get(c).unwrap();
        if t.is_multi() {
            let ts: Vec<_> = t.map.keys().collect();
            let mut ts = ts.clone();
            ts.sort_by(|x, y| {
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
            for t in ts {
                asc_bools.push(a);
                order_exprs.push(col(c).struct_().field_by_name(&t.field_col_name()))
            }
        } else {
            asc_bools.push(a);
            order_exprs.push(col(c))
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

pub fn make_sortable(
    c: &str,
    mut solution_mappings: SolutionMappings,
    global_cats: LockedCats,
) -> SolutionMappings {
    if let Some(state) = solution_mappings.rdf_node_types.get_mut(c) {
        let expr = col(c);
        let mut exprs = vec![];
        if !state.is_multi() {
            let b = state.get_base_type().unwrap();
            let s = state.get_base_state().unwrap();
            let (e, new_state) = make_base_cat_sortable(expr, b, s, global_cats);
            state.map.insert(b.clone(), new_state);
            solution_mappings.mappings = solution_mappings.mappings.with_column(e);
        } else {
            for (t, s) in state.map.iter_mut() {
                let (e, new_s) = make_base_cat_sortable(
                    expr.clone().struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    global_cats.clone(),
                );
                *s = new_s;
                exprs.push(e);
            }
            if !exprs.is_empty() {
                solution_mappings.mappings = solution_mappings
                    .mappings
                    .with_column(expr.struct_().with_fields(exprs));
            }
        }
    }
    solution_mappings
}

pub fn make_base_cat_sortable(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> (Expr, BaseCatState) {
    match base_state {
        BaseCatState::CategoricalNative(_, _) => (
            maybe_decode_expr(expr, base_type, base_state, global_cats),
            BaseCatState::String,
        ),
        BaseCatState::String => (expr, BaseCatState::String),
        BaseCatState::NonString => (expr, BaseCatState::NonString),
    }
}
