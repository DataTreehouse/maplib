use oxrdf::{BlankNode, Literal as OxLiteral, NamedNode, Term, Variable};
use polars::prelude::*;
use polars_arrow::ffi::{export_iterator, ArrowArrayStream, ArrowArrayStreamReader};
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use savvy::{savvy, ExternalPointerSexp, Sexp};
use serde::Serialize;
use sparesults::QuerySolution;
use std::collections::HashMap;

/// Build a small hardcoded i32 Series in Rust and stream it out to R
/// through the Arrow C Stream Interface.
///
/// @param stream_ptr An external pointer from `nanoarrow::nanoarrow_allocate_array_stream()`.
/// @export
#[savvy]
fn export_test_series(stream_ptr: Sexp) -> savvy::Result<()> {
    let series = Series::new("x".into(), &[1i32, 2, 3, 4, 5]);

    let field = series.field().to_arrow(CompatLevel::newest());
    let chunks = series.chunks().clone();
    let iter = Box::new(chunks.into_iter().map(Ok));
    let stream = export_iterator(iter, field);

    let stream_ptr = unsafe {
        ExternalPointerSexp::try_from(stream_ptr)?.cast_mut_unchecked::<ArrowArrayStream>()
    };
    unsafe { std::ptr::replace(stream_ptr, stream) };

    Ok(())
}

/// Read an Arrow C stream from R back into a Rust Series, then hand back
/// its length and first value as a sanity check (i32 only, for this prototype).
///
/// @param stream_ptr An external pointer holding a filled ArrowArrayStream.
/// @returns An integer vector: `c(length, first_value)`.
/// @export
#[savvy]
fn import_test_series(stream_ptr: Sexp) -> savvy::Result<Sexp> {
    let mut stream = unsafe {
        let boxed = Box::new(std::ptr::replace(
            ExternalPointerSexp::try_from(stream_ptr)?.cast_mut_unchecked::<ArrowArrayStream>(),
            ArrowArrayStream::empty(),
        ));
        ArrowArrayStreamReader::try_new(boxed).map_err(|e| savvy::Error::new(&e.to_string()))?
    };

    let mut arrays = Vec::new();
    while let Some(arr) = unsafe { stream.next() } {
        arrays.push(arr.map_err(|e| savvy::Error::new(&e.to_string()))?);
    }

    let series = Series::from_arrow_chunks("x".into(), arrays)
        .map_err(|e| savvy::Error::new(&e.to_string()))?;

    let mut out = savvy::OwnedIntegerSexp::new(2)?;
    out[0] = series.len() as i32;
    out[1] = series
        .i32()
        .map_err(|e| savvy::Error::new(&e.to_string()))?
        .get(0)
        .unwrap_or(0);
    Ok(out.into())
}

// --- DataFrame<->R bridge (maplib-fat) ---
//
// Extends the Series-level mechanism above (export_test_series/
// import_test_series, maplib-qi9) to a full `EagerSolutionMappings
// { mappings: DataFrame, rdf_node_types: HashMap<String, RDFNodeState> }`
// (lib/representation/src/solution_mapping.rs:43), the shape RModel's future
// query/insert/update methods will need to move across the R/Rust boundary.
//
// Two independent problems, per docs/r-wrapper-plan.md Phase 2:
//
// 1. DataFrame -> single Series -> one Arrow C stream. Mirrors r-polars'
//    `as_polars_series(df)` trick: `DataFrame::into_struct(name)` collapses
//    every column into one Struct-typed Series, then the already-proven
//    Series-level export_iterator/ArrowArrayStreamReader mechanism carries it
//    across unchanged (`unnest()` reverses the collapse on the way back). No
//    new FFI mechanism needed -- same stream, one more level of Struct
//    nesting. This also has to survive genuinely nested Struct-in-Struct data,
//    since a "multi" RDFNodeState column (RDFNodeState::polars_data_type,
//    lib/representation/src/rdf_state.rs:46-67) is *already* Struct-typed
//    before the whole-DataFrame collapse adds another layer -- exercised
//    below via the "o" test column.
//
// 2. rdf_node_types has no r-polars equivalent (needed to reconstruct
//    IRI/literal/blank-node distinctions from what the DataFrame otherwise
//    carries as plain string/list/struct columns). Design chosen here: a
//    single JSON string, one flat side-channel value returned alongside the
//    Arrow stream, rather than a second stream or an extra struct field
//    smuggled into the DataFrame itself -- keeps the Arrow payload exactly
//    what Model methods already produce, no need for a schema-aware "does
//    this column carry an extra sidecar" convention on the Arrow side. Only
//    the semantic type per column travels (`RColumnType`, one entry per
//    BaseRDFNodeType a column can hold) -- BaseCatState (RDFNodeState's other
//    HashMap value, an internal categorical-encoding cache) does NOT travel:
//    it's a Rust-side storage optimization, not something R needs, and the
//    physical Arrow encoding (dictionary-typed array vs plain) already
//    reflects it structurally.

/// One RDF node type a solution-mappings column can hold. Mirrors
/// `BaseRDFNodeType` (lib/representation/src/base_rdf_type.rs) but is its own
/// type rather than reusing that enum directly, so the JSON side-channel's
/// shape doesn't change every time `representation`'s internals do.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RColumnType {
    Iri,
    BlankNode,
    Literal { datatype: String },
    None,
}

impl From<&BaseRDFNodeType> for RColumnType {
    fn from(t: &BaseRDFNodeType) -> Self {
        match t {
            BaseRDFNodeType::IRI => RColumnType::Iri,
            BaseRDFNodeType::BlankNode => RColumnType::BlankNode,
            BaseRDFNodeType::Literal(dt) => RColumnType::Literal {
                datatype: dt.as_str().to_string(),
            },
            BaseRDFNodeType::None => RColumnType::None,
        }
    }
}

/// Serialize `rdf_node_types` (per-column possible RDF node types) to the
/// JSON side-channel string. One entry per column name; each column maps to
/// a list of 1 (single-typed) or more (a "multi" column, RDFNodeState::is_multi)
/// `RColumnType`s.
fn rdf_node_types_to_json(rdf_node_types: &HashMap<String, RDFNodeState>) -> savvy::Result<String> {
    let summary: HashMap<&str, Vec<RColumnType>> = rdf_node_types
        .iter()
        .map(|(col, state)| {
            let types = state.get_sorted_types().into_iter().map(RColumnType::from).collect();
            (col.as_str(), types)
        })
        .collect();
    serde_json::to_string(&summary).map_err(|e| savvy::Error::new(&e.to_string()))
}

/// A small, genuinely mixed-type `EagerSolutionMappings` fixture, built the
/// same way real query results are (`EagerSolutionMappings::from_query_solutions`,
/// lib/representation/src/solution_mapping.rs:56-62) rather than hand-rolled,
/// so the DataFrame's column layout matches what a real `Model::query()` will
/// eventually produce. "s" is single-typed (IRI only); "o" is deliberately
/// multi-typed (IRI, then a plain literal, then a blank node across its three
/// rows) to force RDFNodeState::is_multi -- i.e. a Struct-typed column even
/// before the whole-DataFrame collapse.
fn build_test_solution_mappings() -> EagerSolutionMappings {
    let vars: std::sync::Arc<[Variable]> =
        vec![Variable::new("s").unwrap(), Variable::new("o").unwrap()].into();

    let sols = vec![
        QuerySolution::from((
            vars.clone(),
            vec![
                Some(Term::NamedNode(NamedNode::new("http://example.org/a").unwrap())),
                Some(Term::NamedNode(NamedNode::new("http://example.org/b").unwrap())),
            ],
        )),
        QuerySolution::from((
            vars.clone(),
            vec![
                Some(Term::NamedNode(NamedNode::new("http://example.org/c").unwrap())),
                Some(Term::Literal(OxLiteral::new_simple_literal("hello"))),
            ],
        )),
        QuerySolution::from((
            vars,
            vec![
                Some(Term::NamedNode(NamedNode::new("http://example.org/d").unwrap())),
                Some(Term::BlankNode(BlankNode::new("x1").unwrap())),
            ],
        )),
    ];

    EagerSolutionMappings::from_query_solutions(&sols).unwrap()
}

/// Recursively cast any Categorical/Enum-typed Series to plain String
/// (recursing into Struct fields too, since a "multi" RDFNodeState column is
/// itself Struct-typed). Real query results commonly store repeated IRI/
/// literal values as `BaseCatState::CategoricalNative` (a Rust-side storage
/// optimization, lib/representation/src/solution_mapping.rs:13-30) -- Arrow's
/// dictionary encoding for a categorical column round-trips fine at the
/// top level, but confirmed live that nanoarrow's struct-to-data.frame
/// conversion does NOT resolve a dictionary-encoded field nested inside a
/// struct column back to its string values, instead surfacing the raw
/// integer codes. Decategorizing before the whole-DataFrame struct-collapse
/// sidesteps that -- the memory-efficiency benefit of categorical encoding
/// only matters Rust-side anyway, not for the R-bound wire format.
fn decategorize(s: Series) -> savvy::Result<Series> {
    match s.dtype() {
        DataType::Categorical(_, _) | DataType::Enum(_, _) => s
            .cast(&DataType::String)
            .map_err(|e| crate::errors::runtime_error(e.to_string())),
        DataType::Struct(_) => {
            let name = s.name().clone();
            let len = s.len();
            let ca = s
                .struct_()
                .map_err(|e| crate::errors::runtime_error(e.to_string()))?;
            let fields: Vec<Series> = ca
                .fields_as_series()
                .into_iter()
                .map(decategorize)
                .collect::<savvy::Result<_>>()?;
            StructChunked::from_series(name, len, fields.iter())
                .map(|sc| sc.into_series())
                .map_err(|e| crate::errors::runtime_error(e.to_string()))
        }
        _ => Ok(s),
    }
}

/// Collapse an `EagerSolutionMappings`' DataFrame to a Struct-typed Series
/// (`DataFrame::into_struct`) and stream it out to R through the Arrow C
/// Stream Interface, exactly like `export_test_series` does for a bare
/// Series -- the mechanism `RModel::query()` uses for a real query result,
/// proven first here against a hand-built fixture
/// (`export_test_solution_mappings`).
///
/// @param stream_ptr An external pointer from `nanoarrow::nanoarrow_allocate_array_stream()`.
/// @returns The `rdf_node_types` side-channel, as a JSON string.
pub(crate) fn export_solution_mappings(
    sm: EagerSolutionMappings,
    stream_ptr: Sexp,
) -> savvy::Result<Sexp> {
    let EagerSolutionMappings {
        mappings,
        rdf_node_types,
    } = sm;

    let height = mappings.height();
    let columns: Vec<Column> = mappings
        .columns()
        .iter()
        .map(|c| decategorize(c.as_materialized_series().clone()).map(Column::from))
        .collect::<savvy::Result<_>>()?;
    let mappings = DataFrame::new(height, columns)
        .map_err(|e| crate::errors::runtime_error(e.to_string()))?;

    let struct_series = mappings.into_struct("solution_mappings".into()).into_series();
    let field = struct_series.field().to_arrow(CompatLevel::newest());
    let chunks = struct_series.chunks().clone();
    let iter = Box::new(chunks.into_iter().map(Ok));
    let stream = export_iterator(iter, field);

    let stream_ptr = unsafe {
        ExternalPointerSexp::try_from(stream_ptr)?.cast_mut_unchecked::<ArrowArrayStream>()
    };
    unsafe { std::ptr::replace(stream_ptr, stream) };

    rdf_node_types_to_json(&rdf_node_types)?.try_into()
}

/// Build the `build_test_solution_mappings()` fixture and stream it out via
/// `export_solution_mappings` -- proving the DataFrame case reuses the
/// Series-level mechanism unchanged, against a fixture with a genuinely
/// multi-typed column (see `build_test_solution_mappings`'s doc comment).
///
/// @param stream_ptr An external pointer from `nanoarrow::nanoarrow_allocate_array_stream()`.
/// @returns The `rdf_node_types` side-channel, as a JSON string.
/// @export
#[savvy]
fn export_test_solution_mappings(stream_ptr: Sexp) -> savvy::Result<Sexp> {
    export_solution_mappings(build_test_solution_mappings(), stream_ptr)
}

/// Read an Arrow C stream from R into a plain `DataFrame` -- the mapping-side
/// counterpart of `export_solution_mappings`. Used by `RModel::map`/
/// `RModel::add_template` (maplib-cgd) to bring a plain R data.frame (via
/// `nanoarrow::as_nanoarrow_array_stream(df)` on the R side) across as the
/// `data` argument to `Model::expand`. Unlike the export path, there is no
/// `rdf_node_types` side-channel here -- an input mapping data.frame is
/// plain tabular data, its RDF typing comes from the target Template's
/// signature, not from the data itself. Columns are decategorized the same
/// way `export_solution_mappings` decategorizes on the way out: an R factor
/// column arrives as a dictionary-encoded Arrow array, and `expand()`'s
/// column validation (model/expansion/validation.rs) expects plain String
/// columns, not Categorical/Enum.
pub(crate) fn import_dataframe(stream_ptr: Sexp) -> savvy::Result<DataFrame> {
    let mut stream = unsafe {
        let boxed = Box::new(std::ptr::replace(
            ExternalPointerSexp::try_from(stream_ptr)?.cast_mut_unchecked::<ArrowArrayStream>(),
            ArrowArrayStream::empty(),
        ));
        ArrowArrayStreamReader::try_new(boxed).map_err(|e| savvy::Error::new(&e.to_string()))?
    };

    let mut arrays = Vec::new();
    while let Some(arr) = unsafe { stream.next() } {
        arrays.push(arr.map_err(|e| savvy::Error::new(&e.to_string()))?);
    }

    let struct_series = Series::from_arrow_chunks("data".into(), arrays)
        .map_err(|e| savvy::Error::new(&e.to_string()))?;
    let struct_series = decategorize(struct_series)?;
    struct_series
        .struct_()
        .map_err(|e| crate::errors::runtime_error(e.to_string()))
        .map(|ca| ca.clone().unnest())
}

/// Read an Arrow C stream from R back into a Struct-typed Series, unnest it
/// back into a DataFrame (`StructChunked::unnest`, the reverse of
/// `into_struct`), and return a small human-readable summary combining the
/// recovered DataFrame's shape with the `rdf_node_types` JSON side-channel
/// handed back in -- a sanity check that both halves survive the round trip
/// together, matching `import_test_series`'s role for the Series-only case.
///
/// @param stream_ptr An external pointer holding a filled ArrowArrayStream.
/// @param rdf_node_types_json The JSON string returned by
///   `export_test_solution_mappings`.
/// @returns A character vector: one line per column, `"<name>: <dtype> [<rdf types>]"`.
/// @export
#[savvy]
fn import_test_solution_mappings(
    stream_ptr: Sexp,
    rdf_node_types_json: &str,
) -> savvy::Result<Sexp> {
    let mut stream = unsafe {
        let boxed = Box::new(std::ptr::replace(
            ExternalPointerSexp::try_from(stream_ptr)?.cast_mut_unchecked::<ArrowArrayStream>(),
            ArrowArrayStream::empty(),
        ));
        ArrowArrayStreamReader::try_new(boxed).map_err(|e| savvy::Error::new(&e.to_string()))?
    };

    let mut arrays = Vec::new();
    while let Some(arr) = unsafe { stream.next() } {
        arrays.push(arr.map_err(|e| savvy::Error::new(&e.to_string()))?);
    }

    let struct_series = Series::from_arrow_chunks("solution_mappings".into(), arrays)
        .map_err(|e| savvy::Error::new(&e.to_string()))?;
    let df = struct_series
        .struct_()
        .map_err(|e| savvy::Error::new(&e.to_string()))?
        .clone()
        .unnest();

    let rdf_node_types: HashMap<String, serde_json::Value> =
        serde_json::from_str(rdf_node_types_json).map_err(|e| savvy::Error::new(&e.to_string()))?;

    let mut lines: Vec<String> = df
        .get_column_names()
        .into_iter()
        .zip(df.dtypes())
        .map(|(name, dtype)| {
            let types = rdf_node_types
                .get(name.as_str())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".to_string());
            format!("{}: {} {}", name, dtype, types)
        })
        .collect();
    lines.push(format!("{} rows", df.height()));

    let mut out = savvy::OwnedStringSexp::new(lines.len())?;
    for (i, line) in lines.iter().enumerate() {
        out.set_elt(i, line)?;
    }
    Ok(out.into())
}
