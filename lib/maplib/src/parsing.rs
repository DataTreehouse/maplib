use crate::parsing::errors::{ParsingError, ParsingErrorKind};
use crate::parsing::nom_parsing::stottr_doc;
use crate::parsing::parsing_ast::UnresolvedStottrDocument;
use nom::Finish;

pub mod errors;
mod nom_parsing;
mod parser_test;
pub mod parsing_ast;

pub fn whole_stottr_doc(s: &str) -> Result<UnresolvedStottrDocument, ParsingError> {
    let result = stottr_doc(s).finish();
    match result {
        Ok((rest, doc)) => {
            if !rest.is_empty() {
                Err(ParsingError {
                    kind: ParsingErrorKind::CouldNotParseEverything(rest.to_string()),
                })
            } else {
                Ok(doc)
            }
        }
        Err(e) => Err(ParsingError {
            kind: ParsingErrorKind::NomParserError(format!("{:?}", e.code)),
        }),
    }
}
