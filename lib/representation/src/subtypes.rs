use oxrdf::vocab::{rdfs, xsd};
use oxrdf::NamedNodeRef;

pub const OWL_REAL: &str = "http://www.w3.org/2002/07/owl#real";
pub const OWL_RATIONAL: &str = "http://www.w3.org/2002/07/owl#rational";

// s literal subtype of t
pub fn is_literal_subtype(s: NamedNodeRef, t: NamedNodeRef) -> bool {
    if s == t || t == rdfs::LITERAL {
        true
    } else if t.as_str() == OWL_REAL {
        owl_real_subtype(s)
    } else if t.as_str() == OWL_RATIONAL {
        owl_rational_subtype(s)
    } else {
        match t {
            xsd::DECIMAL => xsd_decimal_subtype(s),
            xsd::DOUBLE => xsd_double_subtype(s),
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
                matches!(s, xsd::YEAR_MONTH_DURATION | xsd::DAY_TIME_DURATION)
            }
            xsd::DATE_TIME => {
                matches!(s, xsd::DATE_TIME_STAMP)
            }
            _ => false,
        }
    }
}

fn owl_real_subtype(s: NamedNodeRef) -> bool {
    matches!(s.as_str(), OWL_RATIONAL) || owl_rational_subtype(s)
}

fn owl_rational_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::DECIMAL) || xsd_decimal_subtype(s)
}

fn xsd_decimal_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::INTEGER) || xsd_integer_subtype(s)
}

fn xsd_double_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::FLOAT)
}

fn xsd_integer_subtype(s: NamedNodeRef) -> bool {
    matches!(
        s,
        xsd::LONG | xsd::NON_NEGATIVE_INTEGER | xsd::NON_POSITIVE_INTEGER
    ) || xsd_long_subtype(s)
        || xsd_non_negative_integer_subtype(s)
        || xsd_non_positive_integer_subtype(s)
}

fn xsd_non_positive_integer_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::NEGATIVE_INTEGER)
}

fn xsd_non_negative_integer_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::POSITIVE_INTEGER) || xsd_positive_integer_subtype(s)
}

fn xsd_positive_integer_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::UNSIGNED_LONG) || xsd_unsigned_long_subtype(s)
}

fn xsd_unsigned_long_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::UNSIGNED_INT) || xsd_unsigned_int_subtype(s)
}

fn xsd_unsigned_int_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::UNSIGNED_SHORT) || xsd_unsigned_short_subtype(s)
}

fn xsd_unsigned_short_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::UNSIGNED_BYTE)
}

fn xsd_long_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::INT) || xsd_int_subtype(s)
}

fn xsd_int_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::SHORT) || xsd_short_subtype(s)
}

fn xsd_short_subtype(s: NamedNodeRef) -> bool {
    matches!(s, xsd::BYTE)
}
