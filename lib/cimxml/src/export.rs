use oxrdf::{Literal, NamedNode};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CIMXMLError {
}

pub struct FullModelDetails {
    id: NamedNode,
    description: Option<Literal>,
    version: Option<Literal>,
    created: Literal,
    scenario_time: Literal,
    modeling_authority_set: Option<Literal>,
    profiles: Vec<NamedNode>,
}

impl FullModelDetails {
    pub fn new(
        _id: NamedNode,
        _description: Option<Literal>,
        _version: Option<Literal>,
        _created: Literal,
        _scenario_time: Literal,
        _modeling_authority_set: Option<Literal>,
        _profiles: Vec<NamedNode>,
    ) -> Result<Self, CIMXMLError> {
        unimplemented!("Contact Data Treehouse to try")
}