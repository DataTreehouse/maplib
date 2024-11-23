use crate::ast::ptype_is_possibly_literal;
use oxrdf::vocab::{rdfs, xsd};
use oxrdf::NamedNode;

const OWL_REAL: &str = "http://www.w3.org/2002/07/owl#real";
const OWL_RATIONAL: &str = "http://www.w3.org/2002/07/owl#rational";

pub fn is_literal_subtype(s: &NamedNode, t: &NamedNode) -> bool {
    if !ptype_is_possibly_literal(s) || !ptype_is_possibly_literal(t) {
        false
    } else if s == t {
        true
    } else if t.as_ref() == rdfs::LITERAL {
        true
    } else if t.as_str() == OWL_REAL {
        owl_real_subtype(s)
    } else if t.as_str() == OWL_RATIONAL {
        owl_rational_subtype(s)
    } else {
        match t.as_ref() {
            xsd::DECIMAL => xsd_decimal_subtype(s),
            xsd::INTEGER => xsd_integer_subtype(s),
            xsd::LONG => xsd_long_subtype(s),
            xsd::INT => xsd_int_subtype(s),
            xsd::SHORT => xsd_short_subtype(s),
            xsd::NON_NEGATIVE_INTEGER => xsd_non_negative_integer_subtype(s),
            xsd::POSITIVE_INTEGER => xsd_positive_integer_subtype(s),
            xsd::UNSIGNED_LONG => xsd_unsigned_long_subtype(s),
            xsd::UNSIGNED_INT => xsd_unsigned_int_subtype(s),
            xsd::UNSIGNED_SHORT => xsd_unsigned_short_subtype(s),
            xsd::NON_POSITIVE_INTEGER => xsd_non_positive_integer_subtype(s),
            xsd::DURATION => {
                matches!(
                    s.as_ref(),
                    xsd::YEAR_MONTH_DURATION | xsd::DAY_TIME_DURATION
                )
            }
            xsd::DATE_TIME => {
                matches!(s.as_ref(), xsd::DATE_TIME_STAMP)
            }
            _ => false,
        }
    }
}

fn owl_real_subtype(s: &NamedNode) -> bool {
    matches!(s.as_str(), OWL_RATIONAL) || owl_rational_subtype(s)
}

fn owl_rational_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::DECIMAL) || xsd_decimal_subtype(s)
}

fn xsd_decimal_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::INTEGER) || xsd_integer_subtype(s)
}

fn xsd_integer_subtype(s: &NamedNode) -> bool {
    matches!(
        s.as_ref(),
        xsd::LONG | xsd::NON_NEGATIVE_INTEGER | xsd::NON_POSITIVE_INTEGER
    ) || xsd_long_subtype(s)
        || xsd_non_negative_integer_subtype(s)
        || xsd_non_positive_integer_subtype(s)
}

fn xsd_non_positive_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::NEGATIVE_INTEGER)
}

fn xsd_non_negative_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::POSITIVE_INTEGER) || xsd_positive_integer_subtype(s)
}

fn xsd_positive_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_LONG) || xsd_unsigned_long_subtype(s)
}

fn xsd_unsigned_long_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_INT) || xsd_unsigned_int_subtype(s)
}

fn xsd_unsigned_int_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_SHORT) || xsd_unsigned_short_subtype(s)
}

fn xsd_unsigned_short_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_BYTE)
}

fn xsd_long_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::INT) || xsd_int_subtype(s)
}

fn xsd_int_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::SHORT) || xsd_short_subtype(s)
}

fn xsd_short_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::BYTE)
}
