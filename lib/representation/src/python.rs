use oxrdf::{IriParseError, NamedNode};
use pyo3::{Py, pyclass};
use crate::RDFNodeType;

#[derive(Clone, Debug)]
#[pyclass(name = "RDFType")]
pub enum PyRDFType {
    IRI {},
    Blank {},
    Literal { iri: String },
    Nested {rdf_type: Py<PyRDFType>},
    Unknown {},
}

impl PyRDFType {
    pub fn as_rdf_node_type(&self) -> Result<RDFNodeType, IriParseError> {
        Ok(match self {
            PyRDFType::IRI { .. } => RDFNodeType::IRI,
            PyRDFType::Blank { .. } => RDFNodeType::BlankNode,
            PyRDFType::Literal { iri } => RDFNodeType::Literal(NamedNode::new(iri)?),
            PyRDFType::Unknown { .. } => RDFNodeType::None,
            PyRDFType::Nested { .. } => todo!("Support for natively represented lists")
        })
    }
}