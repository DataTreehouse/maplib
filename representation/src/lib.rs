pub mod literals;

use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, NamedNode, NamedNodeRef};
use polars_core::prelude::{DataType, TimeUnit};
use spargebra::term::TermPattern;

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
    MultiType,
}

impl RDFNodeType {
    pub fn infer_from_term_pattern(tp: &TermPattern) -> Option<Self> {
        match tp {
            TermPattern::NamedNode(_) => Some(RDFNodeType::IRI),
            TermPattern::BlankNode(_) => None,
            TermPattern::Literal(l) => Some(RDFNodeType::Literal(l.datatype().into_owned())),
            _ => {
                unimplemented!()
            }
            TermPattern::Variable(_v) => None,
        }
    }

    pub fn union(&self, other: &RDFNodeType) -> RDFNodeType {
        if self == other {
            self.clone()
        } else {
            RDFNodeType::MultiType
        }
    }

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

    pub fn is_numeric(&self) -> bool {
        match self {
            RDFNodeType::Literal(l) => {
                matches!(
                    l.as_ref(),
                    xsd::INT
                        | xsd::BYTE
                        | xsd::SHORT
                        | xsd::INTEGER
                        | xsd::UNSIGNED_BYTE
                        | xsd::UNSIGNED_INT
                        | xsd::DECIMAL
                        | xsd::FLOAT
                        | xsd::DOUBLE
                )
            }
            _ => false,
        }
    }

    pub fn find_triple_type(&self) -> TripleType {
        if let RDFNodeType::IRI = self {
            TripleType::ObjectProperty
        } else if let RDFNodeType::Literal(lit) = self {
            if lit.as_ref() == xsd::STRING {
                TripleType::StringProperty
            } else {
                TripleType::NonStringProperty
            }
        } else {
            todo!("Triple type {:?} not supported", self)
        }
    }

    pub fn polars_data_type(&self) -> DataType {
        match self {
            RDFNodeType::IRI => DataType::Utf8,
            RDFNodeType::BlankNode => DataType::Utf8,
            RDFNodeType::Literal(l) => match l.as_ref() {
                xsd::STRING => DataType::Utf8,
                xsd::UNSIGNED_INT => DataType::UInt32,
                xsd::UNSIGNED_LONG => DataType::UInt64,
                xsd::INTEGER | xsd::LONG => DataType::Int64,
                xsd::INT => DataType::Int32,
                xsd::DOUBLE => DataType::Float64,
                xsd::FLOAT => DataType::Float32,
                xsd::BOOLEAN => DataType::Boolean,
                xsd::DATE_TIME => DataType::Datetime(TimeUnit::Nanoseconds, None),
                n => {
                    todo!("Datatype {} not supported yet", n)
                }
            },
            RDFNodeType::None => DataType::Null,
            RDFNodeType::MultiType => todo!(),
        }
    }
}

pub fn literal_iri_to_namednode(s: &str) -> NamedNode {
    NamedNode::new_unchecked(&s[1..(s.len() - 1)])
}

pub fn literal_blanknode_to_blanknode(b: &str) -> BlankNode {
    BlankNode::new_unchecked(&b[2..b.len()])
}
