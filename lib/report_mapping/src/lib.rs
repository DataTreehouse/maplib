use maplib::mapping::Model;
use shacl::errors::ShaclError;
use shacl::ValidationReport;
use triplestore::Triplestore;

pub fn report_to_model(
    _report: &ValidationReport,
    _shape_graph: &Option<Triplestore>,
) -> Result<Model, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
