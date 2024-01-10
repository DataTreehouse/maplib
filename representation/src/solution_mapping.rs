use oxrdf::vocab::xsd;
use polars::prelude::LazyFrame;
use crate::RDFNodeType;
use std::collections::{HashMap};

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub rdf_node_types: HashMap<String, RDFNodeType>,
}

impl SolutionMappings {
    pub fn new(
        mappings: LazyFrame,
        datatypes: HashMap<String, RDFNodeType>,
    ) -> SolutionMappings {
        SolutionMappings {
            mappings,
            rdf_node_types: datatypes,
        }
    }
}

pub fn is_string_col(rdf_node_type: &RDFNodeType) -> bool {
    match rdf_node_type {
        RDFNodeType::IRI => true,
        RDFNodeType::BlankNode => true,
        RDFNodeType::Literal(lit) => lit.as_ref() == xsd::STRING,
        RDFNodeType::MultiType => false,
        RDFNodeType::None => false,
    }
}
