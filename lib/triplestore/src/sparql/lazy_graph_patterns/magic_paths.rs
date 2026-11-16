//! What the magic path predicates `maplib:path` ([super::length_path]) and `maplib:shortestPath`
//! ([super::path_search]) have in common: their IRIs, how a group recognises them, and the parsing
//! of their option values.
use crate::sparql::errors::SparqlError;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode};
use representation::constants::MAPLIB_PREFIX_IRI;
use representation::subtypes::is_literal_subtype;
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

/// The local name of `maplib:path`.
pub const PATH: &str = "path";
/// The local name of `maplib:shortestPath`.
pub const SHORTEST_PATH: &str = "shortestPath";

/// The local name of a `maplib:` IRI.
pub fn maplib_local_name(nn: &NamedNode) -> Option<&str> {
    nn.as_str().strip_prefix(MAPLIB_PREFIX_IRI)
}

/// Whether the predicate of the triple pattern is `maplib:{local}`.
pub fn has_maplib_predicate(tp: &TriplePattern, local: &str) -> bool {
    matches!(&tp.predicate, NamedNodePattern::NamedNode(nn) if maplib_local_name(nn) == Some(local))
}

/// Whether a graph pattern is a basic graph pattern with `maplib:path` or `maplib:shortestPath`.
/// Their start nodes (and the targets of `maplib:shortestPath`) come from the other patterns of
/// the group, so they are joined after them.
pub fn is_magic_path_bgp(gp: &GraphPattern) -> bool {
    matches!(gp, GraphPattern::Bgp { patterns } if patterns
        .iter()
        .any(|tp| has_maplib_predicate(tp, PATH) || has_maplib_predicate(tp, SHORTEST_PATH)))
}

/// An error in the use of a magic path predicate or its options.
pub fn path_error(msg: impl Into<String>) -> SparqlError {
    SparqlError::PathError(msg.into())
}

/// The value of the option `maplib:{name}`, which must be an IRI.
pub fn iri_option<'a>(name: &str, t: &'a TermPattern) -> Result<&'a NamedNode, SparqlError> {
    match t {
        TermPattern::NamedNode(nn) => Ok(nn),
        _ => Err(path_error(format!("maplib:{name} must be an IRI, got {t}"))),
    }
}

/// The value of the option `maplib:{name}`, which must be a literal.
pub fn literal_option<'a>(name: &str, t: &'a TermPattern) -> Result<&'a Literal, SparqlError> {
    match t {
        TermPattern::Literal(l) => Ok(l),
        _ => Err(path_error(format!("maplib:{name} must be a literal, got {t}"))),
    }
}

/// The value of the option `maplib:{name}`, which must be a non-negative integer.
pub fn non_negative_integer_option(name: &str, t: &TermPattern) -> Result<u64, SparqlError> {
    let l = literal_option(name, t)?;
    if is_literal_subtype(l.datatype(), xsd::INTEGER) {
        if let Ok(n) = l.value().parse::<u64>() {
            return Ok(n);
        }
    }
    Err(path_error(format!(
        "maplib:{name} must be a non-negative integer, got {l}"
    )))
}

/// The value of the option `maplib:{name}`, which must be a number.
pub fn number_option(name: &str, t: &TermPattern) -> Result<f64, SparqlError> {
    let l = literal_option(name, t)?;
    l.value()
        .parse::<f64>()
        .map_err(|_| path_error(format!("maplib:{name} must be a number, got {l}")))
}

/// The value of the option `maplib:{name}`, which must be true or false (or 1 or 0).
pub fn boolean_option(name: &str, t: &TermPattern) -> Result<bool, SparqlError> {
    let l = literal_option(name, t)?;
    match l.value() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(path_error(format!(
            "maplib:{name} must be true or false, got {l}"
        ))),
    }
}
