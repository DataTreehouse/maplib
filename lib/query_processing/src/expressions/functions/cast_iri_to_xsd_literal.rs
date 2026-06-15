use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{lit, Expr, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::BaseCatState;
use representation::BaseRDFNodeType;

pub fn cast_iri_to_xsd_literal(
    e: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    trg_nn: NamedNodeRef,
    trg_type: DataType,
    global_cats: LockedCats,
) -> Result<Expr, QueryProcessingError> {
    if trg_nn == xsd::STRING {
        Ok(maybe_decode_expr(e, t, s, global_cats))
    } else {
        Ok(lit(LiteralValue::untyped_null()).cast(trg_type.clone()))
    }
}
