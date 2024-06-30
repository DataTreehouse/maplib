#[cfg(test)]
#[macro_use]
extern crate unic_char_range;

pub mod ast;
pub mod constants;
pub mod dataset;
pub mod document;
mod parsing;
mod python;
mod resolver;
