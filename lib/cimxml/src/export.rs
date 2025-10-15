use oxrdf::{Literal, NamedNode};
use std::collections::HashMap;
use std::io::Write;
use thiserror::Error;
use representation::dataset::NamedGraph;
use triplestore::Triplestore;

#[derive(Error, Debug)]
pub enum CIMXMLError {}

pub fn cim_xml_write<W: Write>(
    _buf: &mut W,
    _triplestore: &mut Triplestore,
    _graph: &NamedGraph,
    _profile_graph: &NamedGraph,
    _prefixes: HashMap<String, NamedNode>,
    _fullmodel_details: FullModelDetails,
) -> Result<(), CIMXMLError> {
    unimplemented!("Contact Data Treehouse to try")
}

#[allow(dead_code)]
pub struct FullModelDetails {
    id: NamedNode,
    description: Option<Literal>,
    version: Option<Literal>,
    created: Literal,
    scenario_time: Literal,
    modeling_authority_set: Option<Literal>,
}

impl FullModelDetails {
    pub fn new(
        _id: NamedNode,
        _description: Option<Literal>,
        _version: Option<Literal>,
        _created: Literal,
        _scenario_time: Literal,
        _modeling_authority_set: Option<Literal>,
    ) -> Result<Self, CIMXMLError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}
