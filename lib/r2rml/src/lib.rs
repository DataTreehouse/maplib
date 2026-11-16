//! R2RML: RDB to RDF Mapping Language, <https://www.w3.org/TR/r2rml/>.
//!
//! A mapping is parsed into triples maps (`mapping`). The caller runs the effective SQL query of
//! each logical table (`R2RMLMapping::queries`) in its database and passes the results as polars
//! DataFrames to `execute`, which evaluates the term maps column-wise and returns batches of
//! triples, one per predicate, object map and graph, for maplib's ottr:Triple expansion.

pub mod errors;
pub mod execute;
pub mod mapping;

pub use errors::R2RMLError;
pub use execute::{execute, MappedTriples, R2RMLOptions};
pub use mapping::{parse_mapping, R2RMLMapping};
