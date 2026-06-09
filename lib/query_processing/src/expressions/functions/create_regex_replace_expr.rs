use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::BaseCatState;
use representation::BaseRDFNodeType;

pub fn create_regex_replace_expr(
    expr: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    pattern: &str,
    replacement: &Expr,
    global_cats: LockedCats,
) -> Expr {
    let do_regex_replace = match t {
        BaseRDFNodeType::BlankNode | BaseRDFNodeType::None | BaseRDFNodeType::IRI => false,
        BaseRDFNodeType::Literal(l) => {
            matches!(l.as_ref(), xsd::STRING | rdf::LANG_STRING)
        }
    };
    if do_regex_replace {
        maybe_decode_expr(expr, t, s, global_cats)
            .str()
            .replace_all(lit(pattern), replacement.clone(), false)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::String)
    }
}
