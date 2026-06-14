use crate::expressions::cast_lang_string_to_string;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT};
use representation::{BaseRDFNodeType, RDFNodeState};

pub fn str_function(c: &str, t: &RDFNodeState, global_cats: LockedCats) -> Expr {
    if t.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &t.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_IRI_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::BlankNode => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_BLANK_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::Literal(_) => {
                    if t.is_lang_string() {
                        cast_lang_string_to_string(c, t, s, global_cats.clone())
                    } else {
                        maybe_decode_expr(
                            col(c).struct_().field_by_name(&t.field_col_name()),
                            t,
                            s,
                            global_cats.clone(),
                        )
                        .cast(DataType::String)
                    }
                }
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
            })
        }
        coalesce(to_coalesce.as_slice()).alias(c)
    } else {
        let b = t.get_base_type().unwrap();
        let s = t.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                maybe_decode_expr(col(c), b, s, global_cats)
            }
            BaseRDFNodeType::Literal(_) => {
                if t.is_lang_string() {
                    cast_lang_string_to_string(c, b, s, global_cats)
                } else {
                    maybe_decode_expr(col(c), b, s, global_cats).cast(DataType::String)
                }
            }
            BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
        }
    }
}
