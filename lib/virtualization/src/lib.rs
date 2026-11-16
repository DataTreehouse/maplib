use crate::osipi::OSIPIDatabase;
use crate::python::VirtualizedPythonDatabase;
use std::collections::HashMap;
use templates::ast::Template;

pub mod osipi;
pub mod insights_hub;
pub mod python;

#[derive(Debug, Clone)]
pub struct Virtualization {
    pub resources: HashMap<String, Template>,
}

pub enum VirtualizedDatabase {
    VirtualizedPythonDatabase(VirtualizedPythonDatabase),
    OSIPIDatabase(Box<OSIPIDatabase>),
}
