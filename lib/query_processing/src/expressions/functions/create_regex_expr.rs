use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::BaseCatState;
use representation::BaseRDFNodeType;

pub fn create_regex_expr(
    expr: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    pattern: &str,
    global_cats: LockedCats,
) -> Expr {
    let do_regex = match t {
        BaseRDFNodeType::BlankNode | BaseRDFNodeType::None | BaseRDFNodeType::IRI => false,
        BaseRDFNodeType::Literal(l) => {
            matches!(l.as_ref(), xsd::STRING | rdf::LANG_STRING)
        }
    };
    if do_regex {
        maybe_decode_expr(expr, t, s, global_cats)
            .str()
            .contains(lit(pattern), true)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    }
}
