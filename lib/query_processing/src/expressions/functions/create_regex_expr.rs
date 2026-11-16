use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::BaseCatState;
use representation::BaseRDFNodeType;

/// Whether the text `expr` matches `pattern`, which is a constant or, for patterns that differ
/// per solution, a column. With `strict`, an invalid pattern is an error; otherwise it gives
/// null (unbound) for the solutions it is used in.
pub fn create_regex_expr(
    expr: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    pattern: Expr,
    strict: bool,
    global_cats: LockedCats,
) -> Expr {
    let do_regex = match t {
        BaseRDFNodeType::BlankNode | BaseRDFNodeType::None | BaseRDFNodeType::IRI => false,
        BaseRDFNodeType::Literal(l, _) => {
            matches!(l.as_ref(), xsd::STRING | rdf::LANG_STRING)
        }
    };
    if do_regex {
        maybe_decode_expr(expr, t, s, global_cats)
            .str()
            .contains(pattern, strict)
    } else {
        lit(LiteralValue::untyped_null()).cast(DataType::Boolean)
    }
}
