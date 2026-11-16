use oxrdf::NamedNode;
use polars::frame::DataFrame;
use representation::cats::LockedCats;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::term::TriplePattern;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FtsError {}

/// What the full text search index covers.
#[derive(Debug, Clone, Default)]
pub struct FtsOptions {
    /// Only index string literals of these predicates. All predicates when `None`.
    pub predicates: Option<Vec<NamedNode>>,
    /// Also index the local names of subject IRIs (after the last `#`, `/` or `:`).
    pub local_names: bool,
    /// Also index IRIs in the object position, by the whole IRI.
    pub iris: bool,
}

#[derive(Debug, Clone)]
pub struct FtsIndex {}

impl FtsIndex {
    pub fn new(_path: Option<&Path>, _options: FtsOptions) -> Result<FtsIndex, FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_literal_string(
        &mut self,
        _df: &DataFrame,
        _predicate: &NamedNode,
        _subject_type: &BaseRDFNodeType,
        _subject_state: &BaseCatState,
        _object_type: &BaseRDFNodeType,
        _object_state: &BaseCatState,
        _global_cats: LockedCats,
    ) -> Result<(), FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    pub fn clear(&mut self, _global_cats: LockedCats) -> Result<(), FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    pub fn commit(&mut self, _drop_writer: bool) -> Result<(), FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    pub fn lookup_from_triple_patterns(
        &self,
        _patterns: &Vec<TriplePattern>,
        _global_cats: LockedCats,
        _max_rows: Option<usize>,
    ) -> Result<(Vec<TriplePattern>, Option<SolutionMappings>), FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }
}
