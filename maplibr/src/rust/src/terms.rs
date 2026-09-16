use oxrdf::{BlankNode, Literal, NamedNode, Variable};
use savvy::savvy;
use spargebra::term::GroundTerm;

/// Validate an IRI's syntax (mirrors oxrdf::NamedNode::new, the same check
/// py_maplib's PyIRI::new uses, lib/representation/src/python.rs:279-282).
/// Raises an R error on invalid syntax; returns nothing on success.
///
/// @param iri IRI string to validate.
/// @export
#[savvy]
fn validate_iri(iri: &str) -> savvy::Result<()> {
    NamedNode::new(iri).map_err(crate::errors::argument_error)?;
    Ok(())
}

/// Validate a SPARQL variable name's syntax (mirrors oxrdf::Variable::new,
/// used by py_maplib's PyVariable::new, lib/representation/src/python.rs:340-343).
/// Raises an R error on invalid syntax; returns nothing on success.
///
/// @param name Variable name, without the leading `?`/`$`.
/// @export
#[savvy]
fn validate_variable_name(name: &str) -> savvy::Result<()> {
    Variable::new(name).map_err(crate::errors::argument_error)?;
    Ok(())
}

/// Validate a blank node identifier's syntax (mirrors oxrdf::BlankNode::new,
/// used by py_maplib's PyBlankNode::new, lib/representation/src/python.rs:531-535).
/// Raises an R error on invalid syntax; returns nothing on success.
///
/// @param id Blank node identifier, without the leading `_:`.
/// @export
#[savvy]
fn validate_blank_node_id(id: &str) -> savvy::Result<()> {
    BlankNode::new(id).map_err(crate::errors::argument_error)?;
    Ok(())
}

/// A ground SPARQL binding value: an IRI or a Literal (no blank nodes,
/// unlike `RConstantTerm` -- blank nodes can't be bound to a query variable,
/// so there's no variant for one to construct in the first place, unlike
/// py_maplib's equivalent, which has to reject a blank node at runtime since
/// it accepts a plain Python value that could be anything, see
/// `maybe_parse_bindings`, py_maplib/src/lib.rs:479-528). Built from an R
/// `IRI`/`Literal` (terms.R) by `.to_ground_term()`, and consumed by
/// `RModel::query`'s `bindings` argument.
///
/// @export
#[savvy]
pub struct RGroundTerm {
    pub inner: GroundTerm,
}

#[savvy]
impl RGroundTerm {
    fn iri(iri: &str) -> savvy::Result<Self> {
        let nn = NamedNode::new(iri).map_err(crate::errors::argument_error)?;
        Ok(RGroundTerm {
            inner: GroundTerm::NamedNode(nn),
        })
    }

    /// Mirrors `RConstantTerm::literal` (templates.rs) exactly: a language
    /// tag takes precedence over -- rather than being combined with --
    /// `datatype_iri`, matching py_maplib's `PyLiteral::new`.
    fn literal(value: &str, datatype_iri: &str, language: Option<&str>) -> savvy::Result<Self> {
        let literal = if let Some(language) = language {
            Literal::new_language_tagged_literal_unchecked(value, language)
        } else {
            let dt = NamedNode::new(datatype_iri).map_err(crate::errors::argument_error)?;
            Literal::new_typed_literal(value, dt)
        };
        Ok(RGroundTerm {
            inner: GroundTerm::Literal(literal),
        })
    }
}
