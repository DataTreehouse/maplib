use oxrdf::NamedNode;
use std::collections::HashMap;
use std::io::Write;
use thiserror::*;
use triplestore::Triplestore;

#[derive(Error, Debug)]
pub enum CIMXMLError {}

pub fn cim_xml_write<W: Write>(
    _buf: &mut W,
    _triplestore: &mut Triplestore,
    _profile_triplestore: &mut Triplestore,
    _cim_prefix: &NamedNode,
    _fullmodel_details: HashMap<String, String>,
) -> Result<(), CIMXMLError> {
    unimplemented!("Contact Data Treehouse to try")
}
