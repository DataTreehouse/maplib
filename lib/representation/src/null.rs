use crate::multitype::all_multi_main_cols;
use crate::{BaseRDFNodeType, RDFNodeState, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::rdf;
use polars::prelude::Expr;

pub fn expr_is_null_workaround(expr: Expr, rdf_node_type: &RDFNodeState) -> Expr {
    if !rdf_node_type.is_multi() {
        let b = rdf_node_type.get_base_type().unwrap();
        non_multi_col_is_null_workaround(expr, b)
    } else {
        create_all_types_null_expression(expr, rdf_node_type.get_sorted_types())
    }
}

pub fn non_multi_col_is_null_workaround(expr: Expr, base_rdf_node_type: &BaseRDFNodeType) -> Expr {
    match base_rdf_node_type {
        BaseRDFNodeType::Literal(l) => {
            if l.as_ref() == rdf::LANG_STRING {
                expr.struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .is_null()
            } else {
                expr.is_null()
            }
        }
        _ => expr.is_null(),
    }
}

pub fn create_all_types_null_expression(expr: Expr, types: Vec<&BaseRDFNodeType>) -> Expr {
    let mut all_types_null: Option<Expr> = None;
    for x in all_multi_main_cols(types) {
        let e = expr.clone().struct_().field_by_name(&x).is_null();
        all_types_null = if let Some(all_types_null) = all_types_null {
            Some(all_types_null.and(e))
        } else {
            Some(e)
        };
    }
    all_types_null.unwrap()
}
