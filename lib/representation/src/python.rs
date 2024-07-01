use oxrdf::{IriParseError, NamedNode};
use pyo3::pyclass;

#[derive(Clone, Debug)]
#[pyclass(name = "RDFType")]
pub enum PyRDFType {
    IRI {},
    Blank {},
    Literal { iri: String },
    Unknown {},
}

impl PyRDFType {
    pub fn to_rust(&self) -> Result<crate::BaseRDFNodeType, IriParseError> {
        Ok(match self {
            PyRDFType::IRI { .. } => crate::BaseRDFNodeType::IRI,
            PyRDFType::Blank { .. } => crate::BaseRDFNodeType::BlankNode,
            PyRDFType::Literal { iri } => crate::BaseRDFNodeType::Literal(NamedNode::new(iri)?),
            PyRDFType::Unknown { .. } => crate::BaseRDFNodeType::None,
        })
    }
}
