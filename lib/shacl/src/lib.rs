pub mod errors;
pub mod storage;

use std::collections::HashMap;
use crate::storage::StoredResults;
use errors::ShaclError;
use oxrdf::{NamedNode, NamedOrBlankNode};
use polars::prelude::DataFrame;
use representation::solution_mapping::SolutionMappings;
use std::time::Duration;
use triplestore::Triplestore;

use representation::cats::LockedCats;
use representation::dataset::NamedGraph;

#[derive(Debug, Clone)]
pub struct ShapeTargets {
    pub shape_node: NamedOrBlankNode,
    pub context: String,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct Performance {
    pub shape_node: NamedOrBlankNode,
    pub context: String,
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub conforms: Option<bool>,
    pub results: Option<StoredResults>,
    pub validation_performance: Vec<Performance>,
    pub targets_performance: Vec<Performance>,
    pub shape_targets: Vec<ShapeTargets>,
    pub cats: Option<LockedCats>,
}

impl ValidationReport {
    pub fn concatenated_results(&self) -> Result<Option<SolutionMappings>, ShaclError> {
        unimplemented!("Contact Data Treehouse to try")
    }

    pub fn concatenated_details(&self) -> Result<Option<SolutionMappings>, ShaclError> {
        unimplemented!("Contact Data Treehouse to try")
    }
    pub fn performance_df(&self) -> DataFrame {
        unimplemented!("Contact Data Treehouse to try")
    }

    pub fn shape_targets_df(&self) -> DataFrame {
        unimplemented!("Contact Data Treehouse to try")
    }
}

pub fn validate(
    _data_triplestore: &mut Triplestore,
    _data_graph: &NamedGraph,
    _shapes_graph: &NamedGraph,
    _include_details: bool,
    _include_conforms: bool,
    _streaming: bool,
    _max_shape_constraint_results: Option<usize>,
    _include_transient: bool,
    _max_rows: Option<usize>,
    _only_shapes: Option<Vec<NamedNode>>,
    _deactivate_shapes: Vec<NamedNode>,
    _dry_run: bool,
    _prefixes: Option<&HashMap<String, NamedNode>>,
) -> Result<ValidationReport, ShaclError> {
    unimplemented!("Contact Data Treehouse to try")
}
