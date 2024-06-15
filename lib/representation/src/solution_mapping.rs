use crate::RDFNodeType;
use oxrdf::vocab::xsd;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::collections::HashMap;

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub rdf_node_types: HashMap<String, RDFNodeType>,
}

#[derive(Clone, Debug)]
pub struct EagerSolutionMappings {
    pub mappings: DataFrame,
    pub rdf_node_types: HashMap<String, RDFNodeType>,
}

impl EagerSolutionMappings {
    pub fn new(
        mappings: DataFrame,
        rdf_node_types: HashMap<String, RDFNodeType>,
    ) -> EagerSolutionMappings {
        EagerSolutionMappings {
            mappings,
            rdf_node_types,
        }
    }
    pub fn as_lazy(self) -> SolutionMappings {
        let EagerSolutionMappings {
            mappings,
            rdf_node_types,
        } = self;
        SolutionMappings::new(mappings.lazy(), rdf_node_types)
    }
}

impl SolutionMappings {
    pub fn new(
        mappings: LazyFrame,
        rdf_node_types: HashMap<String, RDFNodeType>,
    ) -> SolutionMappings {
        SolutionMappings {
            mappings,
            rdf_node_types,
        }
    }

    pub fn as_eager(self) -> EagerSolutionMappings {
        EagerSolutionMappings {
            mappings: self.mappings.collect().unwrap(),
            rdf_node_types: self.rdf_node_types,
        }
    }
}

pub fn is_string_col(rdf_node_type: &RDFNodeType) -> bool {
    match rdf_node_type {
        RDFNodeType::IRI => true,
        RDFNodeType::BlankNode => true,
        RDFNodeType::Literal(lit) => lit.as_ref() == xsd::STRING,
        RDFNodeType::MultiType(..) => false,
        RDFNodeType::None => false,
    }
}
