use crate::errors::GeoError;
use polars::prelude::Series;

pub mod errors;
pub struct GeoBuilder {}

impl GeoBuilder {
    pub fn new() -> Self {
        unimplemented!("Contact Data Treehouse to try")
    }
    pub fn append(&mut self, s: &str) -> Result<(), GeoError> {
        unimplemented!("Contact Data Treehouse to try")
    }

    pub fn finish(self, name: &str) -> Series {
        unimplemented!("Contact Data Treehouse to try")
    }
}
