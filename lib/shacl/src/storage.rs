use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct StoredResults {
    folder_path: Option<PathBuf>,
    stored_results: Vec<StoredSolutionMappings>,
    stored_details: Vec<StoredSolutionMappings>,
}

#[derive(Debug, Clone)]
pub enum StoredSolutionMappings {
    EagerSolutionMappings(EagerSolutionMappings),
    SolutionMappingsOnDisk(SolutionMappingsOnDisk),
}

#[derive(Debug, Clone)]
pub struct SolutionMappingsOnDisk {
    height: usize,
    rdf_node_types: HashMap<String, RDFNodeType>,
    file: PathBuf,
}
