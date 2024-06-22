use oxrdf::{IriParseError, NamedNode};
use pyo3::pyclass;

#[pyclass]
#[derive(Clone)]
pub enum RDFType {
    IRI {},
    Blank {},
    Literal { iri: String },
    Unknown {},
}

impl RDFType {
    pub fn to_rust(&self) -> Result<crate::BaseRDFNodeType, IriParseError> {
        Ok(match self {
            RDFType::IRI { .. } => crate::BaseRDFNodeType::IRI,
            RDFType::Blank { .. } => crate::BaseRDFNodeType::BlankNode,
            RDFType::Literal { iri } => crate::BaseRDFNodeType::Literal(NamedNode::new(iri)?),
            RDFType::Unknown { .. } => crate::BaseRDFNodeType::None,
        })
    }
}
