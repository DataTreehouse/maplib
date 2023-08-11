use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ParsingErrorKind {
    CouldNotParseEverything(String),
    NomParserError(String),
}

#[derive(Debug)]
pub struct ParsingError {
    pub(crate) kind: ParsingErrorKind,
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self.kind {
            ParsingErrorKind::CouldNotParseEverything(s) => {
                write!(
                    f,
                    "Could not parse entire string as sttotr document, rest: {}",
                    s
                )
            }
            ParsingErrorKind::NomParserError(s) => {
                write!(f, "Nom parser error with code {}", s)
            }
        }
    }
}

impl Error for ParsingError {}
