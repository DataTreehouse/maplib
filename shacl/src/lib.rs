pub mod errors;
use std::collections::HashMap;
use errors::ShaclError;
use polars_core::prelude::DataFrame;
use representation::RDFNodeType;
use triplestore::Triplestore;

pub struct ValidationReport {
    pub conforms: bool,
    pub df: Option<DataFrame>,
    pub rdf_node_types: Option<HashMap<String, RDFNodeType>>,
}

pub fn validate(_triplestore: &mut Triplestore) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Enterprise edition only")
}
