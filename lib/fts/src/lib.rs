use oxrdf::NamedNode;
use polars::frame::DataFrame;
use representation::cats::serialization::CatMapsOnDisk;
use representation::cats::{CatType, Cats, LockedCats};
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

/// What a serialized index keeps outside of the databases of the cat maps, where its tables are
/// (see [`FtsIndex::serialize_into`]).
#[derive(Debug, Clone)]
pub struct FtsSerializedState {
    /// Identifies the tables of the index.
    pub id: String,
    /// The predicates of the index, in the order of the ids that the tables refer to them by.
    pub predicates: Vec<NamedNode>,
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

    pub fn serialized_state(&self) -> FtsSerializedState {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    /// Writes the tables of the index that go with the cat map of `cat_type` into `db`, the raw
    /// database that this cat map of `cats` is being serialized to.
    pub fn serialize_into(
        &self,
        _cat_type: &CatType,
        _cats: &Cats,
        _db: &mut CatMapsOnDisk,
    ) -> Result<(), FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    /// An index that was serialized, without its tables, which are loaded from the databases
    /// they were serialized into with [`FtsIndex::load_serialized`].
    pub fn from_serialized(
        _options: FtsOptions,
        _state: FtsSerializedState,
    ) -> Result<FtsIndex, FtsError> {
        unimplemented!("Contact Data Treehouse to enable full text search")
    }

    /// Loads the tables of the index that were serialized into `db`, the raw database of the cat
    /// map of `cat_type`. With `in_place`, `db` is the database of the cat map of the deserialized
    /// model and the tables are used where they are, otherwise they are read into memory.
    pub fn load_serialized(
        &self,
        _cat_type: &CatType,
        _db: &CatMapsOnDisk,
        _in_place: bool,
    ) -> Result<(), FtsError> {
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
