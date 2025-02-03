pub mod errors;
pub mod storage;

use crate::storage::StoredResults;
use errors::ShaclError;
use representation::solution_mapping::SolutionMappings;
use std::path::PathBuf;
use triplestore::Triplestore;

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub conforms: bool,
    pub results: Option<StoredResults>,
}

impl ValidationReport {
    pub fn concatenated_results(&self) -> Result<Option<SolutionMappings>, ShaclError> {
        unimplemented!("Contact Data Treehouse to try")
    }

    pub fn concatenated_details(&self) -> Result<Option<SolutionMappings>, ShaclError> {
        unimplemented!("Contact Data Treehouse to try")
    }
}

pub fn validate(
    _data_triplestore: &mut Triplestore,
    _shape_triplestore: &mut Triplestore,
    _include_details: bool,
    _include_conforms: bool,
    _streaming: bool,
    _max_shape_results: Option<usize>,
    _folder_path: Option<&PathBuf>,
) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
