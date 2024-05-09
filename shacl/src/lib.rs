pub mod errors;
use errors::ShaclError;
use polars::prelude::DataFrame;
use representation::RDFNodeType;
use std::collections::HashMap;
use triplestore::Triplestore;

pub struct ValidationReport {
    pub conforms: bool,
    pub df: Option<DataFrame>,
    pub rdf_node_types: Option<HashMap<String, RDFNodeType>>,
}

pub fn validate(_triplestore: &mut Triplestore) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}

pub fn validate_shacl(_triplestore: &mut Triplestore) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
