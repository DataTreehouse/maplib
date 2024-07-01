#[cfg(test)]
#[macro_use]
extern crate unic_char_range;
use crate::ast::PType;
use representation::{BaseRDFNodeType, RDFNodeType};

pub mod ast;
pub mod constants;
pub mod dataset;
pub mod document;
mod parsing;
pub mod python;
mod resolver;

#[derive(Clone, Debug)]
pub enum MappingColumnType {
    Flat(RDFNodeType),
    Nested(Box<MappingColumnType>),
}

impl MappingColumnType {
    pub fn as_ptype(&self) -> PType {
        match self {
            MappingColumnType::Flat(f) => {
                PType::Basic(BaseRDFNodeType::from_rdf_node_type(f), None)
            }
            MappingColumnType::Nested(n) => PType::List(Box::new(n.as_ptype())),
        }
    }
}
