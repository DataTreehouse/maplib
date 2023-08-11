use oxrdf::vocab::xsd;
use polars::prelude::LazyFrame;
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub columns: HashSet<String>,
    pub rdf_node_types: HashMap<String, RDFNodeType>,
}

impl SolutionMappings {
    pub fn new(
        mappings: LazyFrame,
        columns: HashSet<String>,
        datatypes: HashMap<String, RDFNodeType>,
    ) -> SolutionMappings {
        SolutionMappings {
            mappings,
            columns,
            rdf_node_types: datatypes,
        }
    }
}

pub fn is_string_col(rdf_node_type: &RDFNodeType) -> bool {
    match rdf_node_type {
        RDFNodeType::IRI => {
            return true;
        }
        RDFNodeType::Literal(lit) => {
            if lit.as_ref() == xsd::STRING {
                return true;
            }
        }
        _ => {
            panic!("No support for datatype {:?}", rdf_node_type)
        }
    }
    return false;
}
