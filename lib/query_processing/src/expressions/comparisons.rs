use crate::cats::create_compatible_cats;
use crate::expressions::durations::{duration_equal_expr, is_duration_type, temporal_operation};
use crate::expressions::operations::{compatible_operation, literal_parts};
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::{literal_is_datetime, literal_is_string, BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Expression;

/// RDF term equality (sameTerm): only values of the same type can be equal.
pub fn typed_equals_expr(
    left_col: &str,
    right_col: &str,
    left_type: &RDFNodeState,
    right_type: &RDFNodeState,
    global_cats: LockedCats,
) -> Expr {
    typed_equals_expr_impl(
        left_col,
        right_col,
        left_type,
        right_type,
        global_cats,
        false,
    )
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
    typed_equals_expr_impl(
        left_col,
        right_col,
        left_type,
        right_type,
        global_cats,
        true,
    )
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
        global_cats.clone(),
    )
    .into_iter()
    .map(|x| x.unwrap())
    .collect();
    let left_map = exploded.remove(0);
    let right_map = exploded.remove(0);
    let mut eq: Option<Expr> = None;
    if value_equality {
        // Durations are equal when they have the same months and seconds, whatever their
        // lexical forms and duration datatypes, e.g. "P1D"^^xsd:dayTimeDuration = "PT24H".
        let right_parts = literal_parts(right_col, right_type);
        for (le, lt, ls) in literal_parts(left_col, left_type) {
            for (re, rt, rs) in &right_parts {
                if let Some(new_eq) = duration_equal_expr(
                    le.clone(),
                    re.clone(),
                    &lt,
                    &ls,
                    rt,
                    rs,
                    global_cats.clone(),
                ) {
                    eq = if let Some(eq) = eq {
                        Some(eq.or(new_eq))
                    } else {
                        Some(new_eq)
                    };
                }
            }
        }
    }
    for (lt, (les, _ls)) in left_map {
        let matching: Vec<_> = right_map
            .iter()
            .filter(|(rt, _)| {
                if value_equality && is_duration_type(&lt) && is_duration_type(rt) {
                    // Compared above
                    return false;
                }
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
    global_cats: LockedCats,
) -> Expr {
    let mut comps = vec![];
    let right_parts = literal_parts(right_col, right_type);
    for (le, lt, ls) in literal_parts(left_col, left_type) {
        for (re, rt, rs) in &right_parts {
            let (BaseRDFNodeType::Literal(lt_nn, _), BaseRDFNodeType::Literal(rt_nn, _)) =
                (&lt, rt)
            else {
                unreachable!("Should never happen")
            };
            // yearMonthDurations, dayTimeDurations and times are compared by value
            if let Some((e, _)) = temporal_operation(
                le.clone(),
                re.clone(),
                &lt,
                &ls,
                rt,
                rs,
                expression,
                global_cats.clone(),
            ) {
                comps.push(e);
            } else {
                // Dictionary encoded strings are compared by value, not by their codes, which
                // are in insertion order (and local and global codes are not comparable).
                let (le, re) = if literal_is_string(lt_nn.as_ref())
                    && literal_is_string(rt_nn.as_ref())
                {
                    (
                        maybe_decode_expr(le.clone(), &lt, &ls, global_cats.clone()),
                        maybe_decode_expr(re.clone(), rt, rs, global_cats.clone()),
                    )
                } else {
                    (le.clone(), re.clone())
                };
                comps.push(comp(
                    le,
                    re,
                    lt_nn.as_ref(),
                    rt_nn.as_ref(),
                    expression,
                ));
            }
        }
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
