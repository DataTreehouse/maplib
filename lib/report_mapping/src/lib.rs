use maplib::mapping::Mapping;
use shacl::ValidationReport;
use triplestore::{IndexingOptions, Triplestore};

pub fn report_to_mapping(
    _report: &ValidationReport,
    _shape_graph: &Option<Triplestore>,
    _indexing: Option<IndexingOptions>,
) -> Mapping {
    unimplemented!("Contact Data Treehouse to try")
}
