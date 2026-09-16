use crate::errors::argument_error;
use crate::errors::maplib_error;
use crate::templates::RTemplate;
use crate::terms::RGroundTerm;
use maplib::model::MapOptions;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::{
    coalesce, col, concat, Column, DataFrame, DataType, Expr, IntoLazy, Series, UnionArgs,
};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, PREDICATE_COL_NAME,
    SUBJECT_COL_NAME,
};
use savvy::{savvy, ListSexp, Sexp};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Mutex;
use triplestore::triples_read::ExtendedRdfFormat;
use triplestore::IndexingOptions;

const DEFAULT_TRIPLES_BATCH_SIZE: usize = 10_000_000;

/// Mirrors py_maplib's `resolve_normal_format` (py_maplib/src/lib.rs:303-316).
fn resolve_normal_format(format: &str) -> savvy::Result<RdfFormat> {
    match format.to_lowercase().as_str() {
        "ntriples" => Ok(RdfFormat::NTriples),
        "turtle" => Ok(RdfFormat::Turtle),
        "rdf/xml" | "xml" | "rdfxml" => Ok(RdfFormat::RdfXml),
        _ => Err(argument_error(format!("Unknown format: {}", format))),
    }
}

/// Mirrors py_maplib's `resolve_format` (py_maplib/src/lib.rs:318-327).
/// CIM XML and HDT deliberately unsupported here (out of scope for v1, see plan).
fn resolve_format(format: &str) -> savvy::Result<ExtendedRdfFormat> {
    resolve_normal_format(format).map(ExtendedRdfFormat::Normal)
}

/// Guesses a file's RDF format from its extension, for `read()`'s
/// `format = NULL` case -- deliberately its own, narrower copy of
/// `Triplestore::read_triples_from_path`'s own extension-guessing
/// (lib/triplestore/src/triples_read.rs:62-82), NOT a passthrough of `None`
/// into that function. That function's guesser falls into a bare `todo!()`
/// for any extension it doesn't recognize (also true of JSON-LD/HDT, which
/// it *does* recognize but this package deliberately doesn't support yet,
/// see `resolve_format`) -- a `todo!()` panics, and this crate's release
/// profile sets `panic = "abort"` (src/rust/Cargo.toml), so reaching that
/// code path with an unsupported or missing extension would abort the whole
/// R session, not raise a catchable error. Guessing here instead means an
/// unrecognized extension hits this function's own `Err` arm, which
/// `read()` turns into a normal `maplibr_argument_error` well before ever
/// calling into `read_triples`.
fn guess_format_from_extension(path: &str) -> savvy::Result<ExtendedRdfFormat> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());
    match ext.as_deref() {
        Some("ttl") => Ok(ExtendedRdfFormat::Normal(RdfFormat::Turtle)),
        Some("nt") => Ok(ExtendedRdfFormat::Normal(RdfFormat::NTriples)),
        Some("xml") | Some("rdf") => Ok(ExtendedRdfFormat::Normal(RdfFormat::RdfXml)),
        _ => Err(argument_error(format!(
            "Could not guess an RDF format from the extension of '{}' -- pass `format` explicitly \
             (one of \"ntriples\", \"turtle\", \"xml\")",
            path
        ))),
    }
}

fn parse_optional_named_graph(graph: Option<&str>) -> savvy::Result<NamedGraph> {
    let nn = graph
        .map(|g| NamedNode::new(g).map_err(argument_error))
        .transpose()?;
    Ok(NamedGraph::from_maybe_named_node(nn.as_ref()))
}

/// Parses `query()`'s `graph` argument. Unlike `reads`/`writes`, where a
/// missing graph means "the default graph" (`parse_optional_named_graph`),
/// a missing graph here means "no restriction to one named graph" -- mirrors
/// py_maplib's own `query`, which passes `graph: Option<&NamedGraph>`
/// through to `Model::query` unchanged rather than defaulting it (py_model.rs:359-360).
fn parse_query_graph(graph: Option<&str>) -> savvy::Result<Option<NamedGraph>> {
    graph
        .map(|g| {
            let nn = NamedNode::new(g).map_err(argument_error)?;
            Ok(NamedGraph::from_maybe_named_node(Some(&nn)))
        })
        .transpose()
}

/// Flattens a CONSTRUCT query's per-pattern results (each pattern's own
/// solution mappings, plus a constant predicate when the pattern used one
/// literally rather than binding it, e.g. `CONSTRUCT { ?s a ?type }`) into a
/// single subject/predicate/object `EagerSolutionMappings`, matching
/// `RModel::query()`'s single-DataFrame-per-call design. Mirrors py_maplib's
/// own per-pattern handling (`query_to_result`, py_maplib/src/lib.rs:235-263)
/// for adding the constant predicate column, but concatenates every
/// pattern's rows into one combined DataFrame instead of returning a
/// separate DataFrame per pattern -- py_maplib can return a heterogeneous
/// Python list; a single R data.frame per query() call cannot.
///
/// Deliberately simplified relative to `query()`'s SELECT path: every
/// pattern's subject/predicate/object columns are cast to plain strings
/// before concatenating, rather than preserved as natively-typed (e.g.
/// integer/date) or per-row multi-typed columns. Different CONSTRUCT
/// patterns can produce genuinely different column dtypes (one pattern's
/// object might be an integer literal, another's a string), which a single
/// concatenated DataFrame can't represent without collapsing to one common
/// type anyway -- so the `rdf_node_types` side-channel reports predicate as
/// IRI (always true per the CONSTRUCT grammar) and subject/object as
/// untyped, rather than claiming a precision this can't actually preserve.
fn construct_result_to_solution_mappings(
    groups: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
    global_cats: LockedCats,
) -> savvy::Result<EagerSolutionMappings> {
    let mut lfs = Vec::with_capacity(groups.len());
    for (sm, predicate) in groups {
        let EagerSolutionMappings {
            mappings,
            mut rdf_node_types,
        } = sm;
        // A pattern's own subject/object binds against a WHERE clause
        // variable, which is very commonly multi-typed (its RDF node type
        // can vary by row, e.g. one row's ?o is a string, another's an
        // integer) -- format_native_columns represents that as a Struct
        // column (one field per possible type, at most one non-null per
        // row), the same shape query()'s SELECT path sends to R for
        // R-side collapsing (.collapse_multitype_columns). That collapsing
        // has to happen here instead, before casting to String and
        // concatenating: a Struct column can't be cast to String directly
        // (confirmed live -- it silently casts every row to NA rather than
        // erroring), and each pattern's own R-side round trip is skipped
        // for CONSTRUCT (only the final combined frame goes to R).
        let subject_state = rdf_node_types.get(SUBJECT_COL_NAME).cloned();
        let object_state = rdf_node_types.get(OBJECT_COL_NAME).cloned();
        // A variable-bound predicate (the `None` case below) is, in
        // practice, always singly IRI-typed -- RDF's predicate position is
        // always an IRI -- but the same variable name could in principle
        // also appear in a non-predicate position elsewhere in the WHERE
        // clause (e.g. across a UNION), making its RDFNodeState multi just
        // like subject/object can be. Flattened the same way, rather than
        // assumed safe to cast directly.
        let predicate_state = rdf_node_types.get(PREDICATE_COL_NAME).cloned();
        let mut lf = representation::formatting::format_native_columns(
            mappings.lazy(),
            &mut rdf_node_types,
            global_cats.clone(),
        );
        if let Some(state) = subject_state {
            lf = lf.with_column(flatten_to_string(SUBJECT_COL_NAME, &state));
        }
        if let Some(state) = object_state {
            lf = lf.with_column(flatten_to_string(OBJECT_COL_NAME, &state));
        }
        if let Some(predicate) = &predicate {
            lf = lf.with_column(
                polars::prelude::lit(rdf_named_node_to_polars_literal_value(predicate))
                    .alias(PREDICATE_COL_NAME),
            );
        } else if let Some(state) = predicate_state {
            lf = lf.with_column(flatten_to_string(PREDICATE_COL_NAME, &state));
        }
        lf = lf.select([
            col(SUBJECT_COL_NAME),
            col(PREDICATE_COL_NAME),
            col(OBJECT_COL_NAME),
        ]);
        lfs.push(lf);
    }
    let mappings = if lfs.is_empty() {
        // A CONSTRUCT template with literally zero triple patterns (valid
        // but pointless SPARQL) -- build an empty, correctly-shaped
        // DataFrame rather than concat()ing an empty list of LazyFrames
        // (which would produce a schema-less frame, not the three empty
        // String columns export_solution_mappings expects).
        let empty_col =
            |name: &str| Column::from(Series::new_empty(name.into(), &DataType::String));
        DataFrame::new(
            0,
            vec![
                empty_col(SUBJECT_COL_NAME),
                empty_col(PREDICATE_COL_NAME),
                empty_col(OBJECT_COL_NAME),
            ],
        )
        .map_err(|e| crate::errors::runtime_error(e.to_string()))?
    } else {
        concat(
            lfs,
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )
        .map_err(|e| crate::errors::runtime_error(e.to_string()))?
        .collect()
        .map_err(|e| crate::errors::runtime_error(e.to_string()))?
    };

    let rdf_node_types = HashMap::from([
        (
            SUBJECT_COL_NAME.to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        ),
        (
            PREDICATE_COL_NAME.to_string(),
            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
        ),
        (
            OBJECT_COL_NAME.to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        ),
    ]);
    Ok(EagerSolutionMappings {
        mappings,
        rdf_node_types,
    })
}

/// Flattens `col(name)` -- already run through `format_native_columns`, so
/// either a single decoded column or (if `state.is_multi()`) a Struct with
/// one field per possible type -- down to one plain String column. For a
/// multi-typed column, coalesces across the Struct's fields (named per
/// `BaseRDFNodeType::field_col_name()`, matching exactly what
/// `expression_to_native`'s multi branch built them as; the lang-string
/// type is the one exception, itself a 2-field value/lang Struct rather
/// than a single field, hence the `LANG_STRING_VALUE_FIELD` special case --
/// its language tag is dropped here, same simplification as
/// `.collapse_multitype_columns` on the R side), each field individually
/// cast to String first so mismatched underlying dtypes (e.g. an integer
/// field alongside a string field) don't need a common supertype.
fn flatten_to_string(name: &str, state: &RDFNodeState) -> Expr {
    if !state.is_multi() {
        // A single-typed rdf:langString column is *also* Struct-shaped
        // (value + language tag fields, not one flat column) --
        // expression_to_native's non-multi branch itself falls back to
        // as_struct(exprs) whenever the base type needs more than one
        // expression, which is exactly the lang-string case.
        return if state.is_literal() && state.get_base_type().unwrap().is_lang_string() {
            col(name)
                .struct_()
                .field_by_name(LANG_STRING_VALUE_FIELD)
                .cast(DataType::String)
                .alias(name)
        } else {
            col(name).cast(DataType::String).alias(name)
        };
    }
    let candidates: Vec<Expr> = state
        .get_sorted_types()
        .into_iter()
        .map(|t| {
            let field = if t.is_lang_string() {
                LANG_STRING_VALUE_FIELD.to_string()
            } else {
                t.field_col_name()
            };
            col(name)
                .struct_()
                .field_by_name(&field)
                .cast(DataType::String)
        })
        .collect();
    coalesce(&candidates).alias(name)
}

/// A maplib knowledge graph model.
///
/// @export
#[savvy]
pub struct RModel {
    inner: Mutex<maplib::model::Model>,
}

impl RModel {
    /// Lock the inner Model, recovering from poison rather than panicking.
    ///
    /// std's Mutex poisons itself if a panic happens while it's held (e.g.
    /// serialize()/deserialize() hitting the lib/disk stub's unimplemented!())
    /// -- left as the default `.lock().unwrap()`, every later call on the
    /// SAME RModel would then panic forever with PoisonError, even for
    /// operations unrelated to whatever originally panicked. Python's GIL
    /// mutex has no poisoning concept, so `into_inner()` (ignore the poison,
    /// keep using the data) is the closest match to py_maplib's behavior --
    /// the data itself is still perfectly valid, since maplib's own methods
    /// don't panic mid-mutation in a way that would leave it inconsistent.
    fn lock(&self) -> std::sync::MutexGuard<'_, maplib::model::Model> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[savvy]
impl RModel {
    /// Create a new, empty Model.
    ///
    /// @export
    fn new() -> savvy::Result<Self> {
        // Required: see CLAUDE.md's pyo3-leak note. lib/utils/src/polars.rs's
        // pl_interruptable_collect (used by query/insert/update/map/
        // infer_rdfs -- anything that materializes a LazyFrame) calls
        // pyo3::Python::attach unconditionally when built with the pyo3
        // feature, which panics unless some Python interpreter has been
        // initialized in this process. Python::initialize() is pyo3's own
        // documented mechanism for embedding in a non-Python host; safe to
        // call on every RModel::new() (verified idempotent -- a second call
        // in the same session doesn't panic or error).
        pyo3::Python::initialize();

        let model = maplib::model::Model::new(None, None, None, None)
            .map_err(maplib_error)?;
        Ok(Self {
            inner: Mutex::new(model),
        })
    }

    /// Number of triples in a graph (mirrors PyModel::size,
    /// py_maplib/src/py_model.rs:144-151 / size_mutex, py_maplib/src/
    /// mutexes.rs:66-73).
    ///
    /// @param graph Optional named graph IRI to count (default graph if NULL).
    /// @export
    fn size(&self, graph: Option<&str>) -> savvy::Result<savvy::Sexp> {
        let named_graph = parse_optional_named_graph(graph)?;
        let inner = self.lock();
        (inner.graph_size(&named_graph) as i32).try_into()
    }

    /// Run a SPARQL SELECT or CONSTRUCT query, streaming the result out
    /// through the Arrow C Stream Interface (mirrors PyModel::query,
    /// py_maplib/src/py_model.rs:344-387; CONSTRUCT results are flattened
    /// into one combined subject/predicate/object DataFrame, see
    /// construct_result_to_solution_mappings).
    ///
    /// @param sparql The SPARQL query string.
    /// @param stream_ptr An external pointer from `nanoarrow::nanoarrow_allocate_array_stream()`.
    /// @param include_transient Whether to include transient (e.g. inferred)
    ///   triples in the query. Defaults to FALSE.
    /// @param graph Optional named graph IRI to restrict the query to (searches
    ///   across the whole store if NULL, matching py_maplib's default).
    /// @param bindings Optional named list of `RGroundTerm` (see terms.R's
    ///   `.to_ground_term()`) -- pre-binds each named SPARQL variable to a
    ///   fixed IRI/Literal value before the query runs, mirroring py_maplib's
    ///   `query(bindings=)`. A bound variable can't also be a `SELECT`-ed
    ///   output column (`Triplestore::query`'s underlying `maybe_replace_bindings`
    ///   rejects that combination with a normal error, not a panic --
    ///   `SELECT *` is fine, since it names no variables itself).
    /// @returns The `rdf_node_types` side-channel, as a JSON string.
    /// @export
    fn query(
        &self,
        sparql: &str,
        stream_ptr: savvy::Sexp,
        include_transient: bool,
        graph: Option<&str>,
        bindings: Option<ListSexp>,
    ) -> savvy::Result<savvy::Sexp> {
        let named_graph = parse_query_graph(graph)?;
        let bindings = bindings
            .map(|b| -> savvy::Result<HashMap<String, spargebra::term::GroundTerm>> {
                let mut map = HashMap::new();
                for (name, value) in b.iter() {
                    let term = <&RGroundTerm>::try_from(value)?;
                    map.insert(name.to_string(), term.inner.clone());
                }
                Ok(map)
            })
            .transpose()?;
        let mut inner = self.lock();
        let global_cats = inner.triplestore.global_cats.clone();
        let result = inner
            .query(
                sparql,
                None,
                named_graph.as_ref(),
                false,
                include_transient,
                None,
                false,
                bindings.as_ref(),
            )
            .map_err(maplib_error)?;
        match result.kind {
            representation::result::QueryResultKind::Select(sm) => {
                // Query results store repeated IRI/literal values as category
                // codes into a Model-wide dictionary (Triplestore::global_cats,
                // LockedCats) rather than plain polars Categorical-typed
                // columns -- format_native_columns (mirrors py_maplib's
                // native_dataframe=True path, py_model.rs query_to_result via
                // fix_cats_and_multicolumns) is what actually resolves those
                // codes back to real values (via maybe_decode_expr) and
                // collapses "multi" RDF-typed columns into one Struct field
                // per possible type, matching the shape export_solution_mappings
                // expects. A plain Series-level Categorical->String cast
                // (arrow_bridge::decategorize) does NOT do this -- confirmed
                // live, a query result round-tripped to R without this step
                // came back as raw integer category codes, not strings.
                let representation::solution_mapping::EagerSolutionMappings {
                    mappings,
                    mut rdf_node_types,
                } = sm;
                let lf = representation::formatting::format_native_columns(
                    mappings.lazy(),
                    &mut rdf_node_types,
                    global_cats,
                );
                let mappings = lf
                    .collect()
                    .map_err(|e| crate::errors::runtime_error(e.to_string()))?;
                let sm = representation::solution_mapping::EagerSolutionMappings {
                    mappings,
                    rdf_node_types,
                };
                crate::arrow_bridge::export_solution_mappings(sm, stream_ptr)
            }
            representation::result::QueryResultKind::Construct(groups) => {
                construct_result_to_solution_mappings(groups, global_cats)
                    .and_then(|sm| crate::arrow_bridge::export_solution_mappings(sm, stream_ptr))
            }
        }
    }

    /// Run a SPARQL UPDATE (INSERT DATA/DELETE DATA/DELETE-INSERT-WHERE)
    /// against this Model in place (mirrors PyModel::update,
    /// py_maplib/src/py_model.rs:415-469 / update_mutex, py_maplib/src/
    /// mutexes.rs:375-397).
    ///
    /// @param update The SPARQL UPDATE string. Must be a DELETE/INSERT/WHERE
    ///   form (`INSERT { ... } WHERE { ... }`, optionally with a DELETE
    ///   clause too, and an empty `WHERE {}` when there's nothing to match
    ///   against) -- the bare `INSERT DATA { ... }`/`DELETE DATA { ... }`
    ///   forms are unimplemented in the core engine (`Triplestore::
    ///   update_parsed`, lib/triplestore/src/sparql.rs:302-307, a bare
    ///   `todo!()`) and panic if used. Not fixable in this package -- see
    ///   maplib-cai upstream issue.
    /// @param include_transient Whether the WHERE clause may match transient
    ///   (e.g. inferred) triples. Defaults to FALSE.
    /// @param graph Optional named graph IRI to restrict the update to
    ///   (no restriction, i.e. the whole store, if NULL -- same "missing
    ///   means unrestricted" semantics as query()'s `graph` argument, not
    ///   reads()/writes()'s "missing means the default graph").
    /// @export
    fn update(&self, update: &str, include_transient: bool, graph: Option<&str>) -> savvy::Result<()> {
        let named_graph = parse_query_graph(graph)?;
        let mut inner = self.lock();
        inner
            .update(update, None, named_graph.as_ref(), false, include_transient, None, false)
            .map_err(maplib_error)?;
        Ok(())
    }

    /// Run a SPARQL SELECT/CONSTRUCT query and insert its results as new
    /// triples into a (possibly different) graph (mirrors PyModel::insert,
    /// py_maplib/src/py_model.rs:540-600 / insert_mutex, py_maplib/src/
    /// mutexes.rs:498-519). New triples' solution mappings (py_maplib's
    /// optional return dict) aren't surfaced here -- callers who need the
    /// new triples back can query() the target graph again.
    ///
    /// @param query The SPARQL CONSTRUCT query string to source new triples
    ///   from (a SELECT query errors -- INSERT needs a triple-shaped result,
    ///   matching `Triplestore::insert_parsed`'s own constraint,
    ///   lib/triplestore/src/sparql.rs:240-264).
    /// @param include_transient Whether the query may match transient (e.g.
    ///   inferred) triples in the source graph. Defaults to FALSE.
    /// @param transient Whether the newly-inserted triples themselves should
    ///   be marked transient rather than permanent. Defaults to FALSE.
    /// @param source_graph Optional named graph IRI to query from (default
    ///   graph if NULL).
    /// @param target_graph Optional named graph IRI to insert into (default
    ///   graph if NULL).
    /// @export
    fn insert(
        &self,
        query: &str,
        include_transient: bool,
        transient: bool,
        source_graph: Option<&str>,
        target_graph: Option<&str>,
    ) -> savvy::Result<()> {
        let source_graph = parse_optional_named_graph(source_graph)?;
        let target_graph = parse_optional_named_graph(target_graph)?;
        let mut inner = self.lock();
        inner
            .insert(
                query,
                None,
                &source_graph,
                include_transient,
                &target_graph,
                transient,
                false,
                None,
                false,
            )
            .map_err(maplib_error)?;
        Ok(())
    }

    /// Parse RDF triples from a string into this Model.
    ///
    /// @param s RDF data as a string.
    /// @param format One of "ntriples", "turtle", "xml" (rdf/xml).
    /// @param graph Optional named graph IRI to read into (default graph if NULL).
    /// @export
    fn reads(&self, s: &str, format: &str, graph: Option<&str>) -> savvy::Result<()> {
        let format = resolve_format(format)?;
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .reads(
                s,
                format,
                None,
                false,
                None,
                true,
                &named_graph,
                false,
                DEFAULT_TRIPLES_BATCH_SIZE,
                HashMap::new(),
            )
            .map_err(maplib_error)
    }

    /// Parse RDF triples from a file into this Model (mirrors PyModel::read,
    /// py_maplib/src/py_model.rs:616-647 / read_mutex, py_maplib/src/
    /// mutexes.rs:527-563 -- CIM XML and HDT deliberately unsupported here,
    /// same as reads()/resolve_format). Never passes `format = NULL` through
    /// to the core engine's own extension-guessing (see
    /// guess_format_from_extension's doc comment for why: its guesser's
    /// unrecognized-extension fallback is a process-aborting panic, not a
    /// catchable error).
    ///
    /// @param path Path to the RDF file to read.
    /// @param format One of "ntriples", "turtle", "xml" (rdf/xml). Guessed
    ///   from the file extension if NULL (.ttl -> turtle, .nt -> ntriples,
    ///   .xml/.rdf -> xml) -- any other or missing extension is a normal
    ///   error, pass `format` explicitly instead.
    /// @param graph Optional named graph IRI to read into (default graph if NULL).
    /// @export
    fn read(&self, path: &str, format: Option<&str>, graph: Option<&str>) -> savvy::Result<()> {
        let format = match format {
            Some(f) => resolve_format(f)?,
            None => guess_format_from_extension(path)?,
        };
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .read_triples(
                Path::new(path),
                Some(format),
                None,
                false,
                None,
                true,
                &named_graph,
                false,
                DEFAULT_TRIPLES_BATCH_SIZE,
                HashMap::new(),
            )
            .map_err(maplib_error)
    }

    /// Serialize this Model's triples to a string.
    ///
    /// @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
    /// @param graph Optional named graph IRI to write (default graph if NULL).
    /// @export
    fn writes(&self, format: Option<&str>, graph: Option<&str>) -> savvy::Result<savvy::Sexp> {
        let format = format
            .map(resolve_normal_format)
            .transpose()?
            .unwrap_or(RdfFormat::NTriples);
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        let mut out = Vec::new();
        inner
            .write_triples(&mut out, &named_graph, format, None)
            .map_err(maplib_error)?;
        String::from_utf8(out)
            .map_err(crate::errors::runtime_error)?
            .try_into()
    }

    /// Serialize this Model's triples to a file (mirrors PyModel::write,
    /// py_maplib/src/py_model.rs:737-752 / write_triples_mutex, py_maplib/src/
    /// mutexes.rs:613-650 -- CIM XML and HDT deliberately unsupported here,
    /// same as writes()/resolve_normal_format; there's no dedicated
    /// path-taking write method on `Model` itself, py_maplib's own mutex
    /// opens the `File` at this same layer).
    ///
    /// @param path Path to write the RDF file to (overwritten if it exists).
    /// @param format One of "ntriples", "turtle", "xml" (rdf/xml). Defaults to "ntriples".
    /// @param graph Optional named graph IRI to write (default graph if NULL).
    /// @export
    fn write(&self, path: &str, format: Option<&str>, graph: Option<&str>) -> savvy::Result<()> {
        let format = format
            .map(resolve_normal_format)
            .transpose()?
            .unwrap_or(RdfFormat::NTriples);
        let named_graph = parse_optional_named_graph(graph)?;
        let mut file = File::create(path).map_err(crate::errors::runtime_error)?;
        let mut inner = self.lock();
        inner
            .write_triples(&mut file, &named_graph, format, None)
            .map_err(maplib_error)
    }

    /// Build the default (non-FTS) indexes, matching py_maplib's create_index()
    /// with no options (create_index_mutex, py_maplib/src/mutexes.rs:402-413).
    ///
    /// @export
    fn create_index(&self) -> savvy::Result<()> {
        let mut inner = self.lock();
        inner
            .create_index(IndexingOptions::default())
            .map_err(maplib_error)
    }

    /// Remove all triples from a graph (mirrors truncate_graph_mutex,
    /// py_maplib/src/mutexes.rs:98-104 -- delegates straight to the
    /// triplestore, Model has no dedicated wrapper method).
    ///
    /// @param graph Optional named graph IRI to truncate (default graph if NULL).
    /// @export
    fn truncate_graph(&self, graph: Option<&str>) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner.triplestore.truncate(&named_graph);
        Ok(())
    }

    /// Add prefix -> IRI mappings, used when serializing (mirrors
    /// add_prefixes_mutex, py_maplib/src/mutexes.rs:90-96 -- direct field
    /// extend, no dedicated Model method).
    ///
    /// @param prefixes A named R list of single strings: names are prefixes,
    ///   values are IRIs, e.g. `list(ex = "http://example.org/")`.
    /// @export
    fn add_prefixes(&self, prefixes: ListSexp) -> savvy::Result<()> {
        let mut parsed = HashMap::new();
        for (name, value) in prefixes.iter() {
            let iri = <&str>::try_from(value)?;
            let nn = NamedNode::new(iri).map_err(argument_error)?;
            parsed.insert(name.to_string(), nn);
        }
        let mut inner = self.lock();
        inner.prefixes.extend(parsed);
        Ok(())
    }

    /// Copy a graph from another Model's triplestore into this one (mirrors
    /// PyModel::add_graph, py_maplib/src/py_model.rs:997-1015).
    ///
    /// @param other Another Model to copy a graph from.
    /// @param source_graph Optional named graph IRI in `other` (default graph if NULL).
    /// @param target_graph Optional named graph IRI in this Model to copy into (default graph if NULL).
    /// @export
    fn add_graph(
        &self,
        other: &RModel,
        source_graph: Option<&str>,
        target_graph: Option<&str>,
    ) -> savvy::Result<()> {
        let source_graph = parse_optional_named_graph(source_graph)?;
        let target_graph = parse_optional_named_graph(target_graph)?;
        let other_inner = other.lock();
        let mut inner = self.lock();
        inner
            .add_graph(&other_inner.triplestore, source_graph, target_graph)
            .map_err(maplib_error)
    }

    /// Split a graph out of this Model into a brand new, standalone Model
    /// (mirrors detach_graph_mutex, py_maplib/src/mutexes.rs:106-118).
    ///
    /// @param preserve_name Keep the graph's own name in the new Model rather
    ///   than moving it to the default graph there.
    /// @param graph Optional named graph IRI to detach (default graph if NULL).
    /// @returns A new Model containing only the detached graph.
    /// @export
    fn detach_graph(&self, preserve_name: bool, graph: Option<&str>) -> savvy::Result<RModel> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        let sprout = inner
            .detach_graph(&named_graph, preserve_name)
            .map_err(maplib_error)?;
        Ok(RModel {
            inner: Mutex::new(sprout),
        })
    }

    /// Run RDFS inference over a graph in place, returning the number of new
    /// triples inferred -- a triple count, not a rule count
    /// (Triplestore::interesting_rdfs_rules, lib/triplestore/src/
    /// rdfs_inferencing.rs, sums per-rule inserted-triple counts) -- mirrors
    /// PyModel::infer_rdfs (py_maplib/src/py_model.rs:903-916).
    ///
    /// @param graph Optional named graph IRI to infer over (default graph if NULL).
    /// @export
    fn infer_rdfs(&self, graph: Option<&str>) -> savvy::Result<savvy::Sexp> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        let n = inner
            .infer_rdfs(&named_graph)
            .map_err(maplib_error)?;
        (n as i32).try_into()
    }

    /// Compact on-disk storage (mirrors PyModel::compact, py_maplib/src/py_model.rs:919-924).
    ///
    /// @export
    fn compact(&self) -> savvy::Result<()> {
        let mut inner = self.lock();
        inner.compact().map_err(maplib_error)
    }

    /// Serialize this Model's triples to a directory in maplib's own compact
    /// on-disk format (not an RDF interchange format -- use write()/writes()
    /// for that). Mirrors PyModel::serialize, py_maplib/src/py_model.rs:926-934.
    ///
    /// @param path Directory to serialize into. Defaults to
    ///   "./serialized_triples", matching py_maplib.
    /// @export
    fn serialize(&self, path: Option<&str>) -> savvy::Result<()> {
        let mut inner = self.lock();
        inner
            .serialize_triples(Path::new(path.unwrap_or("./serialized_triples")))
            .map_err(maplib_error)
    }

    /// Register an already-built Template (mirrors `Model::add_template`,
    /// lib/maplib/src/model.rs:165 -- py_maplib's own `add_template` just
    /// forwards a parsed `Template` here too, py_model.rs:74-80).
    ///
    /// @param template An RTemplate (the raw pointer behind maplibr's S7
    ///   `Template` class, `template@raw`).
    /// @export
    fn add_template(&self, template: &RTemplate) -> savvy::Result<()> {
        let mut inner = self.lock();
        inner
            .add_template(template.inner.clone())
            .map_err(maplib_error)
    }

    /// Parse an stOTTR document string and register every template it
    /// defines (mirrors `Model::add_templates_from_string`,
    /// lib/maplib/src/model.rs:177-194). Errors if the document defines no
    /// templates at all, matching py_maplib's own message for that case
    /// (py_maplib/src/mutexes.rs:132-138).
    ///
    /// @param doc An stOTTR document, as a string.
    /// @returns The IRI of the first template the document defines.
    /// @export
    fn add_template_string(&self, doc: &str) -> savvy::Result<savvy::Sexp> {
        let mut inner = self.lock();
        let iri = inner
            .add_templates_from_string(doc)
            .map_err(maplib_error)?
            .ok_or_else(|| {
                argument_error("Template stOTTR document contained no templates")
            })?;
        iri.as_str().to_string().try_into()
    }

    /// Expand a template against a data.frame, adding the resulting triples
    /// to this Model (mirrors `map_mutex`'s data-driven branch,
    /// py_maplib/src/mutexes.rs:120-169 -- `Model::expand`,
    /// lib/maplib/src/model/expansion.rs:58).
    ///
    /// @param template_iri IRI of an already-registered template (see
    ///   `add_template`/`add_template_string`).
    /// @param stream_ptr A *filled* Arrow C Stream Interface pointer (e.g.
    ///   from `nanoarrow::as_nanoarrow_array_stream(df)`), one row per
    ///   template instantiation.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param validate_iris Whether to validate that IRI-typed columns
    ///   contain valid IRIs. Defaults to TRUE, matching py_maplib.
    /// @export
    fn map(
        &self,
        template_iri: &str,
        stream_ptr: Sexp,
        graph: Option<&str>,
        validate_iris: Option<bool>,
    ) -> savvy::Result<()> {
        let df = crate::arrow_bridge::import_dataframe(stream_ptr)?;
        if df.height() == 0 {
            // Matches py_maplib's own map_mutex (py_maplib/src/mutexes.rs:
            // 158-162): an empty-but-present DataFrame is a silent no-op,
            // not the same as no DataFrame at all -- expand(None) assumes a
            // signature with no variables, which isn't true here.
            return Ok(());
        }
        let named_graph = parse_optional_named_graph(graph)?;
        let options = MapOptions::from_args(named_graph, validate_iris);
        let mut inner = self.lock();
        inner
            .expand(template_iri, Some(df), None, options)
            .map_err(maplib_error)
    }

    /// Expand a template with no input data.frame -- for templates whose
    /// instances are entirely made of constant terms (mirrors `map_mutex`'s
    /// `df.is_none()` branch, py_maplib/src/mutexes.rs:163-168).
    ///
    /// @param template_iri IRI of an already-registered template.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param validate_iris Whether to validate that IRI-typed columns
    ///   contain valid IRIs. Defaults to TRUE.
    /// @export
    fn map_no_data(
        &self,
        template_iri: &str,
        graph: Option<&str>,
        validate_iris: Option<bool>,
    ) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let options = MapOptions::from_args(named_graph, validate_iris);
        let mut inner = self.lock();
        inner
            .expand(template_iri, None, None, options)
            .map_err(maplib_error)
    }

    /// Map a JSON file straight to triples via a fixed convention (Facade-X;
    /// no template involved), mirroring PyModel::map_json's path branch
    /// (py_maplib/src/py_model.rs:153-167 / map_json_mutex, py_maplib/src/
    /// mutexes.rs:181-228 -- split into a path- and a string-taking method
    /// here rather than one auto-detecting argument, matching this
    /// package's own read()/reads() split).
    ///
    /// @param path Path to the JSON file to map.
    /// @param transient Whether the resulting triples should be transient
    ///   rather than permanent. Defaults to FALSE.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param uuid_namespace Optional namespace string for the UUIDv5 blank
    ///   node IRIs minted for JSON objects/arrays. Defaults to the path.
    /// @export
    fn map_json(
        &self,
        path: &str,
        transient: bool,
        graph: Option<&str>,
        uuid_namespace: Option<&str>,
    ) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .map_json_path(
                Path::new(path),
                &named_graph,
                transient,
                uuid_namespace.map(String::from),
            )
            .map_err(maplib_error)
    }

    /// Map a JSON string straight to triples via a fixed convention
    /// (mirrors PyModel::map_json's string branch). See map_json() for the
    /// file-based equivalent.
    ///
    /// @param json The JSON document, as a string.
    /// @param transient Whether the resulting triples should be transient
    ///   rather than permanent. Defaults to FALSE.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param uuid_namespace Optional namespace string for the UUIDv5 blank
    ///   node IRIs minted for JSON objects/arrays. Defaults to a random UUID
    ///   (v4) if not given, matching `Model::map_json_string`.
    /// @export
    fn map_json_string(
        &self,
        json: &str,
        transient: bool,
        graph: Option<&str>,
        uuid_namespace: Option<&str>,
    ) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .map_json_string(
                json.to_string(),
                &named_graph,
                transient,
                uuid_namespace.map(String::from),
            )
            .map_err(maplib_error)
    }

    /// Map an XML file straight to triples via a fixed convention (mirrors
    /// PyModel::map_xml's path branch, py_maplib/src/py_model.rs:169-183 /
    /// map_xml_mutex, py_maplib/src/mutexes.rs:230-278 -- split into a path-
    /// and a string-taking method here, same rationale as map_json()).
    ///
    /// @param path Path to the XML file to map.
    /// @param transient Whether the resulting triples should be transient
    ///   rather than permanent. Defaults to FALSE.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param uuid_namespace Optional namespace string for the UUIDv5 blank
    ///   node IRIs minted for XML elements. Defaults to the path.
    /// @export
    fn map_xml(
        &self,
        path: &str,
        transient: bool,
        graph: Option<&str>,
        uuid_namespace: Option<&str>,
    ) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .map_xml_path(
                Path::new(path),
                &named_graph,
                transient,
                uuid_namespace.map(String::from),
            )
            .map_err(maplib_error)
    }

    /// Map an XML string straight to triples via a fixed convention (mirrors
    /// PyModel::map_xml's string branch). See map_xml() for the file-based
    /// equivalent.
    ///
    /// @param xml The XML document, as a string.
    /// @param transient Whether the resulting triples should be transient
    ///   rather than permanent. Defaults to FALSE.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param uuid_namespace Optional namespace string for the UUIDv5 blank
    ///   node IRIs minted for XML elements. Defaults to a random UUID (v4)
    ///   if not given, matching `Model::map_xml_string`.
    /// @export
    fn map_xml_string(
        &self,
        xml: &str,
        transient: bool,
        graph: Option<&str>,
        uuid_namespace: Option<&str>,
    ) -> savvy::Result<()> {
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .map_xml_string(
                xml.to_string(),
                &named_graph,
                transient,
                uuid_namespace.map(String::from),
            )
            .map_err(maplib_error)
    }

    /// Map a data.frame's columns directly to subject/predicate/object
    /// triples -- one column per predicate, a fresh IRI subject per row, no
    /// OTTR template involved (mirrors PyModel::map_df, py_maplib/src/
    /// py_model.rs:216-243 / map_df_mutex, py_maplib/src/mutexes.rs:324-355).
    /// Unlike map()'s own zero-row handling, a zero-row data.frame here is
    /// NOT a no-op: `Triplestore::map_df` (lib/triplestore/src/map_df.rs:
    /// 115-132) unconditionally adds one `<root> rdf:type fx:root` triple
    /// regardless of row count, and py_maplib's own map_df has no
    /// zero-row early return either -- so neither does this one, to match.
    ///
    /// @param stream_ptr A *filled* Arrow C Stream Interface pointer (e.g.
    ///   from `nanoarrow::as_nanoarrow_array_stream(df)`).
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param uuid_namespace Optional namespace string for the UUIDv5
    ///   subject IRIs minted per row. Defaults to a random UUID (v4) if not
    ///   given, matching `Triplestore::map_df`.
    /// @export
    fn map_df(
        &self,
        stream_ptr: Sexp,
        graph: Option<&str>,
        uuid_namespace: Option<&str>,
    ) -> savvy::Result<()> {
        let df = crate::arrow_bridge::import_dataframe(stream_ptr)?;
        let named_graph = parse_optional_named_graph(graph)?;
        let mut inner = self.lock();
        inner
            .map_df(&df, &named_graph, uuid_namespace.map(String::from))
            .map_err(maplib_error)
    }

    /// Map a data.frame's columns directly to subject/predicate/object
    /// triples using the built-in `ottr:Triple` template, optionally fixing
    /// every row's predicate to a single constant IRI instead of reading it
    /// from a `predicate` column (mirrors PyModel::map_triples, py_maplib/src/
    /// py_model.rs:184-214 / map_triples_mutex, py_maplib/src/mutexes.rs:
    /// 280-301 -- both call `Model::expand_triples`, which is really an
    /// `expand()` against the built-in triple template rather than a
    /// separate `Triplestore` method). Unlike map()'s zero-row handling,
    /// there is deliberately no zero-row early return here: `expand_triples`
    /// runs the same subject/predicate/object column-existence validation
    /// (lib/maplib/src/model/expansion/validation.rs) regardless of row
    /// count, and py_maplib's own map_triples has no early return either --
    /// skipping it would let a misnamed/missing column go undetected on an
    /// (incidentally) zero-row call instead of raising a normal error.
    ///
    /// @param stream_ptr A *filled* Arrow C Stream Interface pointer, with
    ///   subject/predicate/object columns (or just subject/object if
    ///   `predicate` is given).
    /// @param predicate Optional constant predicate IRI to use for every
    ///   row, instead of a `predicate` column in the data.
    /// @param graph Optional named graph IRI to add the resulting triples to
    ///   (default graph if NULL).
    /// @param validate_iris Whether to validate that IRI-typed columns
    ///   contain valid IRIs. Defaults to TRUE.
    /// @export
    fn map_triples(
        &self,
        stream_ptr: Sexp,
        predicate: Option<&str>,
        graph: Option<&str>,
        validate_iris: Option<bool>,
    ) -> savvy::Result<()> {
        let df = crate::arrow_bridge::import_dataframe(stream_ptr)?;
        let predicate = predicate
            .map(NamedNode::new)
            .transpose()
            .map_err(argument_error)?;
        let named_graph = parse_optional_named_graph(graph)?;
        let options = MapOptions::from_args(named_graph, validate_iris);
        let mut inner = self.lock();
        inner
            .expand_triples(df, None, predicate, options)
            .map_err(maplib_error)
    }

    /// Load a Model previously written by serialize() (mirrors
    /// PyModel::deserialize, py_maplib/src/py_model.rs:936-954). This is an
    /// associated function, not a method -- call as `RModel$deserialize(path)`.
    ///
    /// @param path Directory previously written by serialize(). Defaults to
    ///   "./serialized_triples", matching py_maplib.
    /// @param storage_folder Optional folder for on-disk (rather than
    ///   in-memory) triplestore storage.
    /// @export
    fn deserialize(path: Option<&str>, storage_folder: Option<&str>) -> savvy::Result<RModel> {
        let model = maplib::model::Model::deserialize_triples(
            Path::new(path.unwrap_or("./serialized_triples")),
            storage_folder.map(String::from),
        )
        .map_err(maplib_error)?;
        Ok(RModel {
            inner: Mutex::new(model),
        })
    }
}
