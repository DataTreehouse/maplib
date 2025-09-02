use crate::RDFNodeState;
use oxrdf::vocab::xsd;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::collections::HashMap;
use std::sync::Arc;
use utils::polars::InterruptableCollectError;

#[cfg(feature = "pyo3")]
use utils::polars::pl_interruptable_collect;

use crate::cats::Cats;
#[cfg(feature = "pyo3")]
use pyo3::Python;

#[derive(Clone, Debug)]
pub enum BaseCatState {
    CategoricalNative(bool, Option<Arc<Cats>>),
    String,
    NonString,
}

impl BaseCatState {
    pub fn get_local_cats(&self) -> Option<Arc<Cats>> {
        match self {
            BaseCatState::CategoricalNative(_, local_cats) => {
                if let Some(local_cats) = local_cats {
                    Some(local_cats.clone())
                } else {
                    None
                }
            }
            BaseCatState::String | BaseCatState::NonString => None,
        }
    }
}

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub rdf_node_types: HashMap<String, RDFNodeState>,
    pub height_estimate: usize,
}

#[derive(Clone, Debug)]
pub struct EagerSolutionMappings {
    pub mappings: DataFrame,
    pub rdf_node_types: HashMap<String, RDFNodeState>,
}

impl EagerSolutionMappings {
    pub fn new(
        mappings: DataFrame,
        rdf_node_types: HashMap<String, RDFNodeState>,
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
        let height = mappings.height();
        SolutionMappings::new(mappings.lazy(), rdf_node_types, height)
    }
}

impl SolutionMappings {
    pub fn new(
        mappings: LazyFrame,
        rdf_node_types: HashMap<String, RDFNodeState>,
        height_upper_bound: usize,
    ) -> SolutionMappings {
        SolutionMappings {
            mappings,
            rdf_node_types,
            height_estimate: height_upper_bound,
        }
    }

    pub fn as_eager(self, streaming: bool) -> EagerSolutionMappings {
        EagerSolutionMappings {
            mappings: self
                .mappings
                .with_new_streaming(streaming)
                .collect()
                .unwrap(),
            rdf_node_types: self.rdf_node_types,
        }
    }

    pub fn as_eager_interruptable(
        self,
        streaming: bool,
        #[cfg(feature = "pyo3")] py: Python,
    ) -> Result<EagerSolutionMappings, InterruptableCollectError> {
        #[cfg(feature = "pyo3")]
        {
            let df = pl_interruptable_collect(self.mappings.with_new_streaming(streaming), py)?;
            Ok(EagerSolutionMappings {
                mappings: df,
                rdf_node_types: self.rdf_node_types,
            })
        }

        #[cfg(not(feature = "pyo3"))]
        Ok(self.as_eager(streaming))
    }
}

pub fn is_literal_string_col(rdf_node_type: &RDFNodeState) -> bool {
    rdf_node_type.is_lit_type(xsd::STRING)
}
