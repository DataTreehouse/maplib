use crate::ast::StottrDocument;
use crate::dataset::errors::TemplateError;
use crate::parsing::{parse_stottr, parse_stottr_from_source};
use oxrdf::NamedNode;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::path::Path;

pub fn document_from_str(
    s: &str,
    prefixes: Option<&HashMap<String, NamedNode>>,
) -> Result<StottrDocument, TemplateError> {
    let doc = parse_stottr(s, prefixes)?;
    Ok(doc)
}

pub fn document_from_file<P: AsRef<Path>>(
    p: P,
    prefixes: Option<&HashMap<String, NamedNode>>,
) -> Result<StottrDocument, TemplateError> {
    let path = p.as_ref();
    let s = read_to_string(path).map_err(TemplateError::ReadTemplateFileError)?;
    parse_stottr_from_source(&s, prefixes, Some(path.display().to_string()))
}
