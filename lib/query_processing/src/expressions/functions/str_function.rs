use crate::expressions::cast_lang_string_to_string;
use oxrdf::vocab::xsd;
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
                        literal_to_string(
                            maybe_decode_expr(
                                col(c).struct_().field_by_name(&t.field_col_name()),
                                t,
                                s,
                                global_cats.clone(),
                            ),
                            t,
                        )
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
                    literal_to_string(maybe_decode_expr(col(c), b, s, global_cats), b)
                }
            }
            BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
        }
    }
}

/// Lexical form of a non-language-tagged literal. Decimals are stored with a fixed scale,
/// so the plain string cast pads them with zeros (1.5 -> "1.5000000000000000"). Trim them
/// to the XSD canonical form, as the N-Triples writer does ("1.5", "798").
fn literal_to_string(e: Expr, t: &BaseRDFNodeType) -> Expr {
    let s = e.cast(DataType::String);
    if t.is_lit_type(xsd::DECIMAL) {
        s.str()
            .replace_all(lit(r"(\.\d*?)0+$"), lit("${1}"), false)
            .str()
            .replace_all(lit(r"\.$"), lit(""), false)
    } else {
        s
    }
}
