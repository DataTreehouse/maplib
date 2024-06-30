use crate::ast::StottrDocument;
use crate::dataset::errors::TemplateError;
use crate::parsing::whole_stottr_doc;
use crate::resolver::resolve_document;
use std::fs::read_to_string;
use std::path::Path;

pub fn document_from_str(s: &str) -> Result<StottrDocument, TemplateError> {
    let unresolved = whole_stottr_doc(s).map_err(TemplateError::ParsingError)?;
    resolve_document(unresolved).map_err(TemplateError::ResolutionError)
}

pub fn document_from_file<P: AsRef<Path>>(p: P) -> Result<StottrDocument, TemplateError> {
    let s = read_to_string(p).map_err(TemplateError::ReadTemplateFileError)?;
    document_from_str(&s)
}
