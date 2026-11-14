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

/// A cim:PositionPoint of a cim:Location, with coordinates as their lexical forms.
#[derive(Debug, Clone)]
pub struct CimPositionPoint {
    pub location: NamedNode,
    pub crs_urn: Option<String>,
    pub sequence_number: Option<i64>,
    pub x: String,
    pub y: String,
    pub z: Option<String>,
}

/// Map the position points of CIM locations to GeoSPARQL features and geometries.
pub fn cim_geometries_to_geosparql(
    _points: Vec<CimPositionPoint>,
) -> Result<Vec<oxrdf::Triple>, String> {
    unimplemented!("Contact Data Treehouse to try!")
}
