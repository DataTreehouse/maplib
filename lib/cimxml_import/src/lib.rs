use oxrdf::{NamedNode, Quad};
use representation::BaseRDFNodeTypeRef;

pub struct Remapper {}

impl Remapper {
    pub fn new() -> Self {
        Remapper {}
    }

    pub fn remap_predicate_datatype<'a>(
        &'a self,
        _predicate: &NamedNode,
        _data_type: &BaseRDFNodeTypeRef<'a>,
    ) -> BaseRDFNodeTypeRef<'a> {
        unimplemented!("Contact Data Treehouse to try!")
    }
}

pub fn fix_cim_quad(_quad: Quad, _base_iri: Option<&String>) -> Quad {
    unimplemented!("Contact Data Treehouse to try!")
}
