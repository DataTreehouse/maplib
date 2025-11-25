use oxrdf::Variable;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;
use crate::parsing::peg_parsing::ParseErrorKind;

#[derive(Error, Debug)]
pub enum TemplateError {
    InconsistentNumberOfArguments(String, String, usize, usize),
    IncompatibleTypes(String, Variable, String, String),
    ReadTemplateFileError(io::Error),
    ResolveDirectoryEntryError(walkdir::Error),
    ParsingError(ParseErrorKind),
    TemplateNotFound(String, String),
}

impl Display for TemplateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            TemplateError::InconsistentNumberOfArguments(calling, template, given, expected) => {
                write!(
                    f,
                    "Template {calling} called {template} with {given} arguments, but expected {expected}"
                )
            }
            TemplateError::IncompatibleTypes(nn, var, given, expected) => {
                write!(
                    f,
                    "Template {nn} variable {var} was given argument of type {given:?} but expected {expected:?}"
                )
            }
            TemplateError::ReadTemplateFileError(e) => {
                write!(f, "Error reading template file {e}")
            }
            TemplateError::ResolveDirectoryEntryError(e) => {
                write!(f, "Resolve template directory entry error {e}")
            }
            TemplateError::ParsingError(p) => {
                write!(f, "Template parsing error: {p}")
            }
            TemplateError::TemplateNotFound(container, inner) => {
                write!(
                    f,
                    "Could not find template {inner} referenced from template {container}"
                )
            }
        }
    }
}
