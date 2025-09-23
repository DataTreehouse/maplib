use crate::cats::create_compatible_cats;
use crate::expressions::operations::compatible_operation;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, LiteralValue};
use representation::cats::LockedCats;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Expression;

pub fn typed_equals_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    global_cats: LockedCats,
) -> Expr {
    let mut exploded: Vec<_> = create_compatible_cats(
        vec![Some(col(left_col)), Some(col(right_col))],
        vec![Some(left_type.clone()), Some(right_type.clone())],
        global_cats,
    )
    .into_iter()
    .map(|x| x.unwrap())
    .collect();
    let left_map = exploded.remove(0);
    let mut right_map = exploded.remove(0);
    let mut eq: Option<Expr> = None;
    for (lt, (les, _ls)) in left_map {
        if let Some((res, _rs)) = right_map.remove(&lt) {
            for le in les {
                for re in &res {
                    let new_eq = le
                        .clone()
                        .is_not_null()
                        .and(re.clone().is_not_null())
                        .and(le.clone().eq(re.clone()));
                    eq = if let Some(eq) = eq {
                        Some(eq.or(new_eq))
                    } else {
                        Some(new_eq)
                    };
                }
            }
        }
    }

    if let Some(eq) = eq {
        eq
    } else {
        lit(false)
    }
}

pub fn typed_comparison_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    expression: &Expression,
) -> Expr {
    let mut comps = vec![];
    if left_type.is_multi() {
        if right_type.is_multi() {
            for lt in left_type.map.keys() {
                for rt in right_type.map.keys() {
                    if let BaseRDFNodeType::Literal(lt_nn) = lt {
                        if let BaseRDFNodeType::Literal(rt_nn) = rt {
                            comps.push(comp(
                                col(left_col).struct_().field_by_name(&lt.field_col_name()),
                                col(right_col).struct_().field_by_name(&rt.field_col_name()),
                                lt_nn.as_ref(),
                                rt_nn.as_ref(),
                                expression,
                            ));
                        }
                    }
                }
            }
        } else if !right_type.is_multi() && right_type.is_literal() {
            let rt = right_type.get_base_type().unwrap();
            let rt_nn = if let BaseRDFNodeType::Literal(rt_nn) = rt {
                rt_nn
            } else {
                unreachable!("Should never happen")
            };
            //Only left multi
            for lt in left_type.map.keys() {
                if let BaseRDFNodeType::Literal(lt_nn) = lt {
                    comps.push(comp(
                        col(left_col).struct_().field_by_name(&lt.field_col_name()),
                        col(right_col),
                        lt_nn.as_ref(),
                        rt_nn.as_ref(),
                        expression,
                    ));
                }
            }
        }
    } else if right_type.is_multi() {
        if !left_type.is_multi() && left_type.is_literal() {
            let lt = right_type.get_base_type().unwrap();
            let lt_nn = if let BaseRDFNodeType::Literal(lt_nn) = lt {
                lt_nn
            } else {
                unreachable!("Should never happen")
            };
            for rt in right_type.map.keys() {
                if let BaseRDFNodeType::Literal(rt_nn) = rt {
                    comps.push(comp(
                        col(left_col),
                        col(right_col).struct_().field_by_name(&rt.field_col_name()),
                        lt_nn.as_ref(),
                        rt_nn.as_ref(),
                        expression,
                    ));
                }
            }
        }
    } else if left_type.is_literal() && right_type.is_literal() {
        let lt = left_type.get_base_type().unwrap();
        let rt = right_type.get_base_type().unwrap();
        let lt_nn = if let BaseRDFNodeType::Literal(lt_nn) = lt {
            lt_nn
        } else {
            unreachable!("Should never happen")
        };
        let rt_nn = if let BaseRDFNodeType::Literal(rt_nn) = rt {
            rt_nn
        } else {
            unreachable!("Should never happen")
        };
        comps.push(comp(
            col(left_col),
            col(right_col),
            lt_nn.as_ref(),
            rt_nn.as_ref(),
            expression,
        ));
    }

    if comps.is_empty() {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    } else {
        coalesce(&comps)
    }
}

fn comp(
    e_left: Expr,
    e_right: Expr,
    dt_left: NamedNodeRef,
    dt_right: NamedNodeRef,
    expression: &Expression,
) -> Expr {
    let e = match expression {
        Expression::Greater(_, _) => e_left.clone().gt(e_right.clone()),
        Expression::GreaterOrEqual(_, _) => e_left.clone().gt_eq(e_right.clone()),
        Expression::Less(_, _) => e_left.clone().lt(e_right.clone()),
        Expression::LessOrEqual(_, _) => e_left.clone().lt_eq(e_right.clone()),
        _ => panic!("Should never happen"),
    };
    if compatible_operation(expression, dt_left, dt_right) {
        e
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    }
}
