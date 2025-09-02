use crate::base_rdf_type::BaseRDFNodeTypeRef;
use oxrdf::NamedNodeRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeTypeRef<'a> {
    IRI,
    BlankNode,
    Literal(NamedNodeRef<'a>),
    None,
    MultiType(Vec<BaseRDFNodeTypeRef<'a>>),
}
