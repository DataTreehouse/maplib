//! Vector similarity search. This is the open source placeholder: vector search is part of
//! maplib EE. The API here is the one the triplestore uses, so that the EE implementation can
//! replace it.

use oxrdf::NamedNode;
use polars::frame::DataFrame;
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::Expression;
use spargebra::term::{NamedNodePattern, TriplePattern};
use std::collections::HashMap;
use thiserror::Error;

pub use representation::vectors::{is_vector_literal_type, VECTOR_DATATYPE};

pub const VECTOR_NS: &str = "https://datatreehouse.github.io/maplib/vocab#";
pub const VECTOR_SEARCH_PREDICATE: &str = "https://datatreehouse.github.io/maplib/vocab#vectorSearch";
pub const COSINE_SIMILARITY: &str = "https://datatreehouse.github.io/maplib/vocab#cosineSimilarity";
pub const DOT_PRODUCT: &str = "https://datatreehouse.github.io/maplib/vocab#dotProduct";
pub const EUCLIDEAN_DISTANCE: &str =
    "https://datatreehouse.github.io/maplib/vocab#euclideanDistance";

const NOT_ENABLED: &str = "Vector search is part of maplib EE. Contact Data Treehouse to enable it";

#[derive(Debug, Error)]
pub enum VectorError {
    #[error("{}", NOT_ENABLED)]
    NotEnabled,
    #[error("Vector search requires a vector index, create it with IndexingOptions(vectors=True)")]
    NoIndex,
}

/// Options for the vector index.
#[derive(Clone, Debug, Default)]
pub struct VectorIndexOptions {
    /// Also build an approximate nearest neighbour index, used for cosine similarity searches.
    pub approximate: bool,
}

/// Whether the function is a vector function, which is evaluated by the vector module.
pub fn is_vector_function(iri: &NamedNode) -> bool {
    matches!(
        iri.as_str(),
        COSINE_SIMILARITY | DOT_PRODUCT | EUCLIDEAN_DISTANCE
    )
}

/// Whether a basic graph pattern uses vector search.
pub fn uses_vector_search(patterns: &[TriplePattern]) -> bool {
    patterns.iter().any(|p| {
        matches!(&p.predicate, NamedNodePattern::NamedNode(nn) if nn.as_str() == VECTOR_SEARCH_PREDICATE)
    })
}

#[derive(Clone, Debug)]
pub struct VectorIndex {}

impl VectorIndex {
    pub fn new(_options: &VectorIndexOptions) -> Result<VectorIndex, VectorError> {
        Err(VectorError::NotEnabled)
    }

    /// Adds the vectors in the object column of stored triples.
    pub fn add_vectors(
        &self,
        _df: &DataFrame,
        _predicate: &NamedNode,
        _subject_type: &BaseRDFNodeType,
        _object_type: &BaseRDFNodeType,
        _global_cats: LockedCats,
    ) -> Result<(), VectorError> {
        Err(VectorError::NotEnabled)
    }

    /// Removes all vectors, e.g. before a rebuild when the cats are renumbered.
    pub fn clear(&self) -> Result<(), VectorError> {
        Err(VectorError::NotEnabled)
    }

    /// Called when all vectors are added, e.g. to build the approximate index.
    pub fn finish(&self) -> Result<(), VectorError> {
        Err(VectorError::NotEnabled)
    }

    /// Answers the vector search patterns of a basic graph pattern, returning the other
    /// patterns and the solution mappings of the searches.
    pub fn lookup_from_triple_patterns(
        &self,
        _patterns: &[TriplePattern],
        _global_cats: LockedCats,
        _max_rows: Option<usize>,
    ) -> Result<(Vec<TriplePattern>, Option<SolutionMappings>), VectorError> {
        Err(VectorError::NotEnabled)
    }
}

/// Evaluates a vector function. The arguments are already evaluated into the columns named by
/// the argument contexts, the result goes into the column named by the outer context.
pub fn evaluate_function(
    _index: Option<&VectorIndex>,
    _iri: &NamedNode,
    _args: &[Expression],
    _solution_mappings: SolutionMappings,
    _args_contexts: &HashMap<usize, Context>,
    _outer_context: &Context,
    _global_cats: LockedCats,
) -> Result<SolutionMappings, VectorError> {
    Err(VectorError::NotEnabled)
}
