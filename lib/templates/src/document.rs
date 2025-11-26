use crate::ast::StottrDocument;
use crate::dataset::errors::TemplateError;
use crate::parsing::parse_stottr;
use std::fs::read_to_string;
use std::path::Path;

pub fn document_from_str(s: &str) -> Result<StottrDocument, TemplateError> {
    let doc = parse_stottr(s)?;
    Ok(doc)
}

pub fn document_from_file<P: AsRef<Path>>(p: P) -> Result<StottrDocument, TemplateError> {
    let s = read_to_string(p).map_err(TemplateError::ReadTemplateFileError)?;
    document_from_str(&s)
}
