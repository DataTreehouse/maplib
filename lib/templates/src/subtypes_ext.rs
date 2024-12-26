use crate::ast::ptype_is_possibly_literal;
use oxrdf::NamedNodeRef;
use representation::subtypes::is_literal_subtype;

pub fn is_literal_subtype_ext(s: NamedNodeRef, t: NamedNodeRef) -> bool {
    if !ptype_is_possibly_literal(s) || !ptype_is_possibly_literal(t) {
        false
    } else {
        is_literal_subtype(s, t)
    }
}
