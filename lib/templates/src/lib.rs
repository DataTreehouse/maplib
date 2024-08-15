#[cfg(test)]
#[macro_use]
extern crate unic_char_range;
use crate::ast::{ptype_nn_to_rdf_node_type, PType};
use representation::{BaseRDFNodeType, RDFNodeType};

pub mod ast;
pub mod constants;
pub mod dataset;
pub mod document;
mod parsing;
pub mod python;
mod resolver;

#[derive(Clone, Debug, PartialEq)]
pub enum MappingColumnType {
    Flat(RDFNodeType),
    Nested(Box<MappingColumnType>),
}

impl MappingColumnType {
    pub fn as_ptype(&self) -> PType {
        match self {
            MappingColumnType::Flat(f) => PType::from(f),
            MappingColumnType::Nested(n) => PType::List(Box::new(n.as_ptype())),
        }
    }
}
