pub mod errors;

use polars_core::prelude::DataFrame;
//Placeholder
use triplestore::Triplestore;
use crate::errors::ShaclError;

pub struct ValidationReport {
    pub conforms: bool,
    pub df: Option<DataFrame>,
}

pub fn validate(_triplestore: &mut Triplestore) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Enterprise edition only")
}