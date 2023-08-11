pub mod literals;

use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef};

#[derive(PartialEq, Clone)]
pub enum TripleType {
    ObjectProperty,
    StringProperty,
    NonStringProperty,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode),
    None,
}

impl RDFNodeType {
    pub fn is_lit_type(&self, nnref: NamedNodeRef) -> bool {
        if let RDFNodeType::Literal(l) = self {
            if l.as_ref() == nnref {
                return true;
            }
        }
        false
    }

    pub fn is_bool(&self) -> bool {
        self.is_lit_type(xsd::BOOLEAN)
    }

    pub fn is_float(&self) -> bool {
        self.is_lit_type(xsd::FLOAT)
    }

    pub fn find_triple_type(&self) -> TripleType {
        let triple_type = if let RDFNodeType::IRI = self {
            TripleType::ObjectProperty
        } else if let RDFNodeType::Literal(lit) = self {
            if lit.as_ref() == xsd::STRING {
                TripleType::StringProperty
            } else {
                TripleType::NonStringProperty
            }
        } else {
            todo!("Triple type {:?} not supported", self)
        };
        triple_type
    }
}
