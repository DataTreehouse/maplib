use oxrdf::NamedNode;
use representation::subtypes::is_literal_subtype;
use crate::ast::ptype_is_possibly_literal;

pub fn is_literal_subtype_ext(s: &NamedNode, t: &NamedNode) -> bool {
    if !ptype_is_possibly_literal(s) || !ptype_is_possibly_literal(t) {
        false
    } else {
        is_literal_subtype(s, t)
    }
}