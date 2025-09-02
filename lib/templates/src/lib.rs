#[cfg(test)]
#[macro_use]
extern crate unic_char_range;
use crate::ast::PType;
use representation::RDFNodeState;

pub mod ast;
mod compatible;
pub mod constants;
pub mod dataset;
pub mod document;
mod parsing;
pub mod python;
mod resolver;
pub mod subtypes_ext;

#[derive(Clone, Debug)]
pub enum MappingColumnType {
    Flat(RDFNodeState),
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

impl PartialEq for MappingColumnType {
    fn eq(&self, other: &Self) -> bool {
        match self {
            MappingColumnType::Flat(state_self) => {
                if let MappingColumnType::Flat(state_other) = other {
                    state_self.types_equal(state_other)
                } else {
                    false
                }
            }
            MappingColumnType::Nested(t) => {
                if let MappingColumnType::Nested(t_other) = other {
                    t.eq(t_other)
                } else {
                    false
                }
            }
        }
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}
