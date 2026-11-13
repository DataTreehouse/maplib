//! GeoSPARQL support. This is the open source placeholder: GeoSPARQL is part of maplib EE.
//! The API here is the one the triplestore uses, so that the EE implementation can replace it.

use oxrdf::{NamedNode, Variable};
use polars::frame::DataFrame;
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::Expression;
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

pub const GEO_NS: &str = "http://www.opengis.net/ont/geosparql#";
pub const GEOF_NS: &str = "http://www.opengis.net/def/function/geosparql/";
pub const WKT_LITERAL: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";
pub const WKB_LITERAL: &str = "http://www.opengis.net/ont/geosparql#wkbLiteral";
pub const GEOJSON_LITERAL: &str = "http://www.opengis.net/ont/geosparql#geoJSONLiteral";

const NOT_ENABLED: &str = "GeoSPARQL is part of maplib EE. Contact Data Treehouse to enable it";

#[derive(Debug, Error)]
pub enum GeoError {
    #[error("{}", NOT_ENABLED)]
    NotEnabled,
}

/// Whether the function is a GeoSPARQL function, which is evaluated by the geo module.
pub fn is_geo_function(iri: &NamedNode) -> bool {
    iri.as_str().starts_with(GEOF_NS)
}

/// Whether the type is a geometry literal type that the geo index stores.
pub fn is_geo_literal_type(t: &BaseRDFNodeType) -> bool {
    if let BaseRDFNodeType::Literal(l, _) = t {
        matches!(l.as_str(), WKT_LITERAL | WKB_LITERAL | GEOJSON_LITERAL)
    } else {
        false
    }
}

#[derive(Clone, Debug)]
pub struct GeoIndex {}

impl GeoIndex {
    /// Creates an empty geometry index, in memory or in a database file in the folder.
    pub fn new(_folder: Option<&Path>) -> Result<GeoIndex, GeoError> {
        Err(GeoError::NotEnabled)
    }

    /// Adds the geometries in the object column of stored triples.
    pub fn add_geometries(
        &self,
        _df: &DataFrame,
        _object_type: &BaseRDFNodeType,
        _object_state: &BaseCatState,
        _global_cats: LockedCats,
    ) -> Result<(), GeoError> {
        Err(GeoError::NotEnabled)
    }

    /// Removes all geometries, e.g. before a rebuild when the cats are renumbered.
    pub fn clear(&self) -> Result<(), GeoError> {
        Err(GeoError::NotEnabled)
    }
}

/// Evaluates a GeoSPARQL function. The arguments are already evaluated into the columns named
/// by the argument contexts, the result goes into the column named by the outer context.
pub fn evaluate_function(
    _index: Option<&GeoIndex>,
    _iri: &NamedNode,
    _args: &[Expression],
    _solution_mappings: SolutionMappings,
    _args_contexts: &HashMap<usize, Context>,
    _outer_context: &Context,
    _global_cats: LockedCats,
) -> Result<SolutionMappings, GeoError> {
    Err(GeoError::NotEnabled)
}

/// Bindings of a variable that satisfy one conjunct of a filter, found with the spatial index.
pub struct FilterCandidates {
    /// The position of the conjunct among the conjuncts passed in.
    pub conjunct: usize,
    pub variable: Variable,
    pub mappings: EagerSolutionMappings,
}

/// Finds the conjuncts of a filter that compare a variable against a constant geometry, and the
/// stored geometries that satisfy them.
pub fn filter_candidates(
    _index: &GeoIndex,
    _conjuncts: &[&Expression],
    _global_cats: LockedCats,
) -> Result<Vec<FilterCandidates>, GeoError> {
    Ok(vec![])
}

/// The two variables of a conjunct that relates two geometry variables, if it can be evaluated
/// as a spatial join.
pub fn spatial_join_variables(_conjunct: &Expression) -> Option<(Variable, Variable)> {
    None
}

/// Joins two solution mappings on a spatial relation between a geometry variable on each side.
/// If the relation cannot be answered with a spatial join, the solution mappings are given back
/// unchanged.
pub fn spatial_join(
    _index: &GeoIndex,
    _conjunct: &Expression,
    left: SolutionMappings,
    right: SolutionMappings,
    _global_cats: LockedCats,
) -> Result<Result<SolutionMappings, (SolutionMappings, SolutionMappings)>, GeoError> {
    Ok(Err((left, right)))
}
