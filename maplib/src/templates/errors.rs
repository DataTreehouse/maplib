use crate::ast::StottrVariable;
use crate::resolver::ResolutionError;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TemplateError {
    InconsistentNumberOfArguments(String, String, usize, usize),
    IncompatibleTypes(String, StottrVariable, String, String),
    ReadTemplateFileError(io::Error),
    ResolveDirectoryEntryError(io::Error),
    ReadTemplateDirectoryError(io::Error),
    ParsingError(crate::parsing::errors::ParsingError),
    ResolutionError(ResolutionError),
}

impl Display for TemplateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            TemplateError::InconsistentNumberOfArguments(calling, template, given, expected) => {
                write!(
                    f,
                    "Template {} called {} with {} arguments, but expected {}",
                    calling, template, given, expected
                )
            }
            TemplateError::IncompatibleTypes(nn, var, given, expected) => {
                write!(
                    f,
                    "Template {} variable {} was given argument of type {:?} but expected {:?}",
                    nn, var.name, given, expected
                )
            }
            TemplateError::ReadTemplateFileError(e) => {
                write!(f, "Error reading template file {}", e)
            }
            TemplateError::ResolveDirectoryEntryError(e) => {
                write!(f, "Resolve template directory entry error {}", e)
            }
            TemplateError::ReadTemplateDirectoryError(e) => {
                write!(f, "Read template directory error: {}", e)
            }
            TemplateError::ParsingError(p) => {
                write!(f, "Template parsing error: {}", p)
            }
            TemplateError::ResolutionError(r) => {
                write!(f, "Template resolution error {}", r)
            }
        }
    }
}
