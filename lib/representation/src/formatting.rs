use crate::cats::{maybe_decode_expr, LockedCats};
use crate::polars_to_rdf::{
    datetime_column_to_strings, XSD_DATETIME_WITH_TZ_FORMAT, XSD_DATE_WITHOUT_TZ_FORMAT,
};
use crate::solution_mapping::BaseCatState;
use crate::{BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::{DataType, Field};
use polars::prelude::{as_struct, coalesce, col, lit, Expr, IntoColumn, LazyFrame, LiteralValue};
use std::collections::HashMap;

pub fn format_columns(
    mut lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
    global_cats: LockedCats,
) -> LazyFrame {
    for (c, t) in rdf_node_types {
        lf = lf.with_column(expression_to_formatted(
            col(c),
            c,
            t.clone(),
            global_cats.clone(),
            false,
        ));
    }
    lf
}

pub fn base_expression_to_formatted(
    expr: Expr,
    name: &str,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
    literal_string_rep: bool,
) -> Expr {
    let expr = match base_type {
        BaseRDFNodeType::IRI => {
            lit("<") + maybe_decode_expr(expr, base_type, base_state, global_cats) + lit(">")
        }
        BaseRDFNodeType::BlankNode => {
            lit("_:") + maybe_decode_expr(expr, base_type, base_state, global_cats)
        }
        BaseRDFNodeType::Literal(l) => {
            if literal_string_rep && l.as_ref() == xsd::DATE_TIME {
                lit("\"")
                    + expr.map(
                        |x| {
                            let dt = x.dtype();
                            let tz = if let DataType::Datetime(_, tz) = dt {
                                tz
                            } else {
                                panic!()
                            };
                            Ok(datetime_column_to_strings(&x, tz).into_column())
                        },
                        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
                    )
                    + lit(format!("\"^^{l}"))
            } else if literal_string_rep && l.as_ref() == xsd::DATE_TIME_STAMP {
                lit("\"")
                    + expr.dt().strftime(XSD_DATETIME_WITH_TZ_FORMAT)
                    + lit(format!("\"^^{l}"))
            } else if literal_string_rep && l.as_ref() == xsd::DATE {
                lit("\"") + expr.dt().strftime(XSD_DATE_WITHOUT_TZ_FORMAT) + lit(format!("\"^^{l}"))
            } else if l.as_ref() == rdf::LANG_STRING {
                lit("\"")
                    + expr
                        .clone()
                        .struct_()
                        .field_by_name(LANG_STRING_VALUE_FIELD)
                    + lit("\"@")
                    + expr.struct_().field_by_name(LANG_STRING_LANG_FIELD)
            } else if literal_string_rep && l.as_ref() == xsd::STRING {
                lit("\"") + maybe_decode_expr(expr, base_type, base_state, global_cats) + lit("\"")
            } else if literal_string_rep {
                lit("\"")
                    + maybe_decode_expr(expr, base_type, base_state, global_cats)
                        .cast(DataType::String)
                    + lit(format!("\"^^{l}"))
            } else {
                maybe_decode_expr(expr, base_type, base_state, global_cats)
            }
        }
        BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
    };
    expr.alias(name)
}
pub fn expression_to_formatted(
    expr: Expr,
    name: &str,
    rdf_node_state: RDFNodeState,
    global_cats: LockedCats,
    cast_string: bool,
) -> Expr {
    if rdf_node_state.is_multi() {
        let mut exprs = vec![];
        for (t, s) in &rdf_node_state.map {
            if t.is_lang_string() {
                exprs.push(base_expression_to_formatted(
                    expr.clone(),
                    name,
                    t,
                    s,
                    global_cats.clone(),
                    true,
                ));
            } else {
                exprs.push(base_expression_to_formatted(
                    expr.clone().struct_().field_by_name(&t.field_col_name()),
                    name,
                    t,
                    s,
                    global_cats.clone(),
                    true,
                ));
            }
        }
        coalesce(&exprs)
    } else {
        base_expression_to_formatted(
            expr,
            name,
            rdf_node_state.get_base_type().unwrap(),
            rdf_node_state.get_base_state().unwrap(),
            global_cats,
            cast_string,
        )
    }
}

pub fn format_native_columns(
    mut lf: LazyFrame,
    rdf_node_types: &mut HashMap<String, RDFNodeState>,
    global_cats: LockedCats,
) -> LazyFrame {
    for (c, t) in rdf_node_types.iter() {
        lf = lf.with_column(expression_to_native(
            col(c),
            c,
            t.clone(),
            global_cats.clone(),
        ));
    }
    for s in rdf_node_types.values_mut() {
        for v in s.map.values_mut() {
            if matches!(v, BaseCatState::CategoricalNative(..)) {
                *v = BaseCatState::String;
            }
        }
    }
    lf
}

pub fn expression_to_native(
    expr: Expr,
    name: &str,
    rdf_node_state: RDFNodeState,
    global_cats: LockedCats,
) -> Expr {
    if rdf_node_state.is_multi() {
        let mut exprs = vec![];
        let sorted = rdf_node_state.get_sorted_types();
        for t in &sorted {
            let s = rdf_node_state.map.get(*t).unwrap();
            if t.is_lang_string() {
                exprs.extend(base_expression_to_native(
                    expr.clone(),
                    *t,
                    s,
                    global_cats.clone(),
                ));
            } else {
                exprs.extend(base_expression_to_native(
                    expr.clone().struct_().field_by_name(&t.field_col_name()),
                    *t,
                    s,
                    global_cats.clone(),
                ));
            }
        }
        as_struct(exprs).alias(name)
    } else {
        let mut exprs = base_expression_to_native(
            expr,
            rdf_node_state.get_base_type().unwrap(),
            rdf_node_state.get_base_state().unwrap(),
            global_cats,
        );
        if exprs.len() > 1 {
            as_struct(exprs).alias(name)
        } else {
            exprs.pop().unwrap().alias(name)
        }
    }
}

pub fn base_expression_to_native(
    expr: Expr,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: LockedCats,
) -> Vec<Expr> {
    let mut exprs = vec![];
    match base_type {
        BaseRDFNodeType::IRI => {
            exprs.push(maybe_decode_expr(expr, base_type, base_state, global_cats))
        }
        BaseRDFNodeType::BlankNode => {
            exprs.push(maybe_decode_expr(expr, base_type, base_state, global_cats))
        }
        BaseRDFNodeType::Literal(_) => {
            if base_type.is_lang_string() {
                exprs.push(
                    maybe_decode_expr(
                        expr.clone()
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD),
                        base_type,
                        base_state,
                        global_cats.clone(),
                    )
                    .alias(LANG_STRING_VALUE_FIELD),
                );
                exprs.push(
                    maybe_decode_expr(
                        expr.struct_().field_by_name(LANG_STRING_LANG_FIELD),
                        base_type,
                        base_state,
                        global_cats,
                    )
                    .alias(LANG_STRING_LANG_FIELD),
                );
            } else {
                exprs.push(maybe_decode_expr(expr, base_type, base_state, global_cats));
            }
        }
        BaseRDFNodeType::None => exprs.push(
            lit(LiteralValue::untyped_null())
                .cast(BaseRDFNodeType::None.default_input_polars_data_type()),
        ),
    };
    exprs
}
