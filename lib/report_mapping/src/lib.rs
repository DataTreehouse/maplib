use maplib::mapping::Mapping;
use shacl::errors::ShaclError;
use shacl::ValidationReport;
use triplestore::{IndexingOptions, Triplestore};

pub fn report_to_mapping(
    _report: &ValidationReport,
    _shape_graph: &Option<Triplestore>,
    _indexing: Option<IndexingOptions>,
) -> Result<Mapping, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
