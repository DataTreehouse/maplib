use maplib::mapping::Mapping;
use shacl::ValidationReport;
use triplestore::Triplestore;

pub fn report_to_mapping(
    _report: &ValidationReport,
    _shape_graph: &Option<Triplestore>,
) -> Mapping {
    unimplemented!("Contact Data Treehouse to try")
}
