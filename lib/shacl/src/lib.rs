pub mod errors;
use errors::ShaclError;
use polars::prelude::DataFrame;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use std::collections::HashMap;
use triplestore::Triplestore;
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub conforms: bool,
    pub df: Option<DataFrame>,
    pub rdf_node_types: Option<HashMap<String, RDFNodeType>>,
    pub details: Option<EagerSolutionMappings>,
}

pub fn validate(
    _data_triplestore: &mut Triplestore,
    _shape_triplestore: &mut Triplestore,
    _include_details: bool,
    _include_conforms: bool,
) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
