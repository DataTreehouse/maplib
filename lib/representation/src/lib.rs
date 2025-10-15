#![feature(pattern)]

pub mod multitype;
pub mod polars_to_rdf;
pub mod query_context;
pub mod rdf_to_polars;
pub mod solution_mapping;

mod base_rdf_type;
pub mod cats;
pub mod dataset;
pub mod errors;
pub mod formatting;
pub mod literals;
pub mod python;
mod rdf_state;
mod rdf_type;
pub mod subtypes;

pub use base_rdf_type::*;
pub use rdf_state::*;
pub use rdf_type::*;

use crate::subtypes::{is_literal_subtype, OWL_REAL};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, NamedNode, NamedNodeRef, NamedOrBlankNode, Term};

pub const PREDICATE_COL_NAME: &str = "predicate";
pub const OBJECT_COL_NAME: &str = "object";
pub const SUBJECT_COL_NAME: &str = "subject";
pub const LANG_STRING_VALUE_FIELD: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>";
pub const LANG_STRING_LANG_FIELD: &str = "l";

pub const RDF_NODE_TYPE_IRI: &str = "IRI";
pub const RDF_NODE_TYPE_BLANK_NODE: &str = "Blank";
pub const RDF_NODE_TYPE_NONE: &str = "None";

pub fn literal_iri_to_namednode(s: &str) -> NamedNode {
    NamedNode::new_unchecked(s)
}

pub fn literal_blanknode_to_blanknode(b: &str) -> BlankNode {
    BlankNode::new_unchecked(b)
}

pub fn owned_term_to_named_or_blank_node(t: Term) -> Option<NamedOrBlankNode> {
    match t {
        Term::NamedNode(nn) => Some(NamedOrBlankNode::NamedNode(nn)),
        Term::BlankNode(bl) => Some(NamedOrBlankNode::BlankNode(bl)),
        _ => None,
    }
}

pub fn owned_term_to_named_node(t: Term) -> Option<NamedNode> {
    match t {
        Term::NamedNode(nn) => Some(nn),
        _ => None,
    }
}

pub fn literal_is_numeric(l: NamedNodeRef) -> bool {
    matches!(l, xsd::FLOAT | xsd::DOUBLE)
        || is_literal_subtype(l, NamedNode::new_unchecked(OWL_REAL).as_ref())
}

pub fn literal_is_boolean(l: NamedNodeRef) -> bool {
    matches!(l, xsd::BOOLEAN)
}

pub fn literal_is_datetime(l: NamedNodeRef) -> bool {
    matches!(l, xsd::DATE_TIME) || matches!(l, xsd::DATE_TIME_STAMP)
}

pub fn literal_is_date(l: NamedNodeRef) -> bool {
    matches!(l, xsd::DATE)
}

pub fn literal_is_string(l: NamedNodeRef) -> bool {
    matches!(l, xsd::STRING)
}
