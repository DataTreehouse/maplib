use crate::errors::QueryProcessingError;
use crate::expressions::functions::{cast_iri_to_xsd_literal, cast_literal};
use polars::prelude::{coalesce, col, lit, Expr, LiteralValue};
use representation::cats::LockedCats;
use representation::{BaseRDFNodeType, RDFNodeState};

pub fn xsd_cast_literal(
    c: &str,
    src: &RDFNodeState,
    trg: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Result<Expr, QueryProcessingError> {
    let trg_type = trg.default_input_polars_data_type();
    let trg_nn = if let BaseRDFNodeType::Literal(nn) = trg {
        nn.as_ref()
    } else {
        panic!("Invalid state")
    };
    if src.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &src.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => cast_iri_to_xsd_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    trg_nn,
                    trg_type.clone(),
                    global_cats.clone(),
                )?,
                BaseRDFNodeType::BlankNode => {
                    return Err(QueryProcessingError::BadCastDatatype(
                        c.to_string(),
                        trg.clone(),
                        t.clone(),
                    ))
                }
                BaseRDFNodeType::Literal(src_nn) => cast_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    global_cats.clone(),
                    src_nn.as_ref(),
                    trg_nn,
                    trg_type.clone(),
                ),
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(trg_type.clone()),
            });
        }
        Ok(coalesce(to_coalesce.as_slice()).alias(c))
    } else {
        let t = src.get_base_type().unwrap();
        let s = src.get_base_state().unwrap();
        match &t {
            BaseRDFNodeType::IRI => {
                cast_iri_to_xsd_literal(col(c), t, s, trg_nn, trg_type.clone(), global_cats.clone())
            }
            BaseRDFNodeType::BlankNode => Err(QueryProcessingError::BadCastDatatype(
                c.to_string(),
                trg.clone(),
                t.clone(),
            )),
            BaseRDFNodeType::Literal(src_nn) => Ok(cast_literal(
                col(c),
                t,
                s,
                global_cats.clone(),
                src_nn.as_ref(),
                trg_nn,
                trg_type.clone(),
            )),
            BaseRDFNodeType::None => Ok(lit(LiteralValue::untyped_null()).cast(trg_type)),
        }
    }
}
