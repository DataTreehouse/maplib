//! The RDF 1.2 features that maplib does not support yet.
//!
//! The parsers and the SPARQL parser are built with RDF 1.2, so triple terms (`<<( s p o )>>`)
//! and literals with a base direction (`"text"@ar--rtl`) can reach maplib. Until they are
//! supported throughout, they are rejected with these messages where data and queries enter
//! maplib, instead of being dropped or stored wrongly: a base direction in particular would
//! otherwise be lost, as language strings have no place for it.

use oxrdf::{Literal, Term};

pub const TRIPLE_TERMS_NOT_SUPPORTED: &str = "RDF 1.2 triple terms are not supported yet";
pub const BASE_DIRECTION_NOT_SUPPORTED: &str =
    "RDF 1.2 literals with a base direction are not supported yet";

/// Whether a literal has an RDF 1.2 base direction ("text"@en--rtl).
pub fn has_base_direction(l: &Literal) -> bool {
    l.direction().is_some()
}

/// The message for an RDF 1.2 feature of a term in data that maplib does not support yet, if
/// any: a base direction, also inside a triple term. Triple terms are supported in data.
pub fn unsupported_in_term(t: &Term) -> Option<&'static str> {
    match t {
        Term::Triple(tt) => unsupported_in_term(&tt.object),
        Term::Literal(l) if has_base_direction(l) => Some(BASE_DIRECTION_NOT_SUPPORTED),
        _ => None,
    }
}

/// Marks code that cannot handle triple terms yet. Triple terms are rejected where data and
/// queries enter maplib, so a triple term reaching this is a bug. Each call is a place that
/// needs triple term support, found by searching for this function.
#[track_caller]
pub fn unreachable_triple_term() -> ! {
    panic!("{TRIPLE_TERMS_NOT_SUPPORTED}: a triple term reached code that cannot handle it")
}
