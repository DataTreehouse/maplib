use std::collections::HashMap;
use templates::ast::Template;
use crate::python::VirtualizedPythonDatabase;

pub mod python;

#[derive(Debug, Clone)]
pub struct Virtualization {
    pub resources: HashMap<String, Template>,
}

pub enum VirtualizedDatabase {
    VirtualizedPythonDatabase(VirtualizedPythonDatabase),
}