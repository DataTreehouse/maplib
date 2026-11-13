use crate::cats::create_compatible_cats;
use crate::expressions::operations::compatible_operation;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, LiteralValue};
use representation::cats::LockedCats;
use representation::{literal_is_datetime, BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Expression;

/// RDF term equality (sameTerm): only values of the same type can be equal.
pub fn typed_equals_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    global_cats: LockedCats,
) -> Expr {
    typed_equals_expr_impl(left_col, right_col, left_type, right_type, global_cats, false)
}

/// Value equality (=): as sameTerm, but numbers of different numeric datatypes are equal when
/// their values are (op:numeric-equal with type promotion, e.g. 1 = 1.0), and dateTimes (and
/// dateTimeStamps) with time zone offsets are equal when they are the same instant. Values are
/// stored as UTC instants, so they can be compared.
pub fn typed_value_equals_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    global_cats: LockedCats,
) -> Expr {
    typed_equals_expr_impl(left_col, right_col, left_type, right_type, global_cats, true)
}

fn typed_equals_expr_impl(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    global_cats: LockedCats,
    value_equality: bool,
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
    let right_map = exploded.remove(0);
    let mut eq: Option<Expr> = None;
    for (lt, (les, _ls)) in left_map {
        let matching: Vec<_> = right_map
            .iter()
            .filter(|(rt, _)| {
                *rt == &lt
                    || (value_equality
                        && (matches!((&lt, *rt),
                            (BaseRDFNodeType::Literal(l, Some(_)), BaseRDFNodeType::Literal(r, Some(_)))
                            if literal_is_datetime(l.as_ref()) && literal_is_datetime(r.as_ref()))
                            // Numbers are compared with numeric type promotion.
                            || (lt.is_numeric() && rt.is_numeric())))
            })
            .map(|(_, (res, _))| res)
            .collect();
        for res in matching {
            for le in &les {
                for re in res {
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
                    if let BaseRDFNodeType::Literal(lt_nn, _) = lt {
                        if let BaseRDFNodeType::Literal(rt_nn, _) = rt {
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
            let rt_nn = if let BaseRDFNodeType::Literal(rt_nn, _) = rt {
                rt_nn
            } else {
                unreachable!("Should never happen")
            };
            //Only left multi
            for lt in left_type.map.keys() {
                if let BaseRDFNodeType::Literal(lt_nn, _) = lt {
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
            let lt = left_type.get_base_type().unwrap();
            let lt_nn = if let BaseRDFNodeType::Literal(lt_nn, _) = lt {
                lt_nn
            } else {
                unreachable!("Should never happen")
            };
            for rt in right_type.map.keys() {
                if let BaseRDFNodeType::Literal(rt_nn, _) = rt {
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
        let lt_nn = if let BaseRDFNodeType::Literal(lt_nn, _) = lt {
            lt_nn
        } else {
            unreachable!("Should never happen")
        };
        let rt_nn = if let BaseRDFNodeType::Literal(rt_nn, _) = rt {
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
