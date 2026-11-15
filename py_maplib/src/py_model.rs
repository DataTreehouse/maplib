use file_io::remote::is_remote;
use crate::error::PyMaplibError;
use crate::mutexes::{
    add_graph_mutex, add_prefixes_mutex, add_template_mutex, add_udf_mutex,
    add_virtualization_mutex, compact_mutex, create_index_mutex, detach_graph_mutex,
    generate_template_mutex, get_predicate_iris_mutex, get_predicate_mutex, infer_mutex,
    infer_rdfs_mutex, insert_mutex, reason_mutex, list_udfs_mutex, map_default_batch_mutex, map_default_first_batch_mutex, map_default_mutex,
    map_df_mutex, map_mutex, map_resolved_mutex,
    map_triples_mutex, query_external_mutex, query_mutex, read_mutex, read_template_mutex,
    reads_mutex, resolve_template_mutex, serialize_triples_mutex, size_mutex, truncate_graph_mutex, drop_graphs_mutex,
    update_mutex, validate_mutex, write_bytes_mutex, write_cim_xml_mutex, write_triples_mutex,
    writes_mutex, GraphArg,
};
use crate::py_server::{server_error, PyServer};
use crate::py_io::{ReadSource, WriteTarget};
use crate::py_stream::{parse_graphs, DocumentKind, PyModelStream, StreamSource};
use crate::shacl::PyValidationReport;
use crate::{
    classify_document_data, classify_mapping_data, create_prefix_map, data_to_mappings_types,
    document_uuid_namespace, for_each_document, for_each_mapping_batch, map_parameters,
    changes_to_dict, maybe_parse_bindings, new_triples_to_dict, parse_include_graphs,
    parse_named_node,
    parse_optional_named_node, print_debug_if_exists, query_to_result, resolve_rdf_node_type,
    DocumentData, MappingData, ParametersType, PyIndexingOptions,
};
use server::{serve as serve_model, ArrowCompression, ServerOptions, SnapshotFormat, SnapshotOptions};

use datalog::python::{PyInferenceResult, PyReasoningResult};
use external_sparql::federation::{AllowedEndpoints, FederationSettings, ServiceMethod};
use crate::error::{parse_timeout, timeout_error, QueryTimeoutError};
use external_sparql::maplib_endpoint::{
    query_maplib_endpoint, MaplibEndpointError, MaplibEndpointQuery,
};
use utils::cancel::{with_token, CancelToken};
use external_sparql::SparqlMethod;
use maplib::model::Model as InnerModel;
use cdf::map_cdf;
use fabric::map_fabric;
use opc_ua::mappings::map_opc_ua;
use opc_ua::typing_shapes::{add_typing_shapes as add_opcua_typing_shapes, TypeNamespaces};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use pyo3::{pyclass, pymethods, Bound, Py, PyAny, PyRef, PyResult, Python};
use oxrdf::NamedNode;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::df_to_python::df_to_py_df;
use representation::python::PyIRI;
use representation::result::{QueryResult, QueryResultKind};
use triplestore::query_cache::{CachePlan, KeyBuilder, Lookup, QueryCache};
use crate::py_query_cache::{options_from, stats_dict, PyQueryCacheOptions};
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use templates::python::PyTemplate;
use tracing::instrument;
use triplestore::sparql::InsertResult;
use triplestore::{StorageMode, StorageOptions};
use virtualization::osipi::{OSIPIAuth, OSIPISettings, PIPointSelection, OSIPI_NAMESPACE};
use virtualization::insights_hub::{
    read_insights_hub, InsightsHubAuth, InsightsHubSettings, INSIGHTS_HUB_NAMESPACE,
};
use virtualization::python::VirtualizedPythonDatabase;

#[pyclass(name = "Model", frozen)]
pub struct PyModel {
    // Shared with the endpoints started by serve().
    pub inner: Arc<RwLock<InnerModel>>,
    // The query result cache of the inner model, used without locking it.
    query_cache: Arc<QueryCache>,
}

impl PyModel {
    pub fn from_inner_mapping(inner: InnerModel) -> PyModel {
        PyModel {
            query_cache: inner.query_cache(),
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Locks the model for reading. A panic in an earlier call poisons the lock; the model is
    /// still used afterwards, rather than failing every later call.
    pub(crate) fn read_inner(&self) -> RwLockReadGuard<'_, InnerModel> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Locks the model for writing, also after a panic in an earlier call (see [Self::read_inner]).
    pub(crate) fn write_inner(&self) -> RwLockWriteGuard<'_, InnerModel> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// A query result kept in the query cache for Model.query().
struct CachedQueryResult {
    kind: QueryResultKind,
    cats: LockedCats,
    prefixes: HashMap<String, NamedNode>,
}

fn result_bytes(kind: &QueryResultKind) -> usize {
    match kind {
        QueryResultKind::Select(sm) => sm.mappings.estimated_size(false),
        QueryResultKind::Construct(v) => v.iter().map(|(sm, _, _)| sm.mappings.estimated_size(false)).sum(),
        QueryResultKind::Ask(_) => 1,
    }
}

#[pymethods]
impl PyModel {
    #[new]
    #[pyo3(signature = (indexing_options=None, storage_folder=None, storage=None))]
    #[instrument(skip_all)]
    fn new(
        indexing_options: Option<PyIndexingOptions>,
        storage_folder: Option<PathBuf>,
        storage: Option<String>,
    ) -> PyResult<PyModel> {
        let storage = storage_options_from(storage, storage_folder)?;
        let indexing = if let Some(indexing_options) = indexing_options {
            Some(indexing_options.inner)
        } else {
            None
        };
        Ok(PyModel::from_inner_mapping(
            InnerModel::new(None, storage, indexing, None).map_err(PyMaplibError::from)?,
        ))
    }

    #[instrument(skip_all)]
    fn add_template(&self, py: Python<'_>, template: Bound<'_, PyAny>) -> PyResult<()> {
        let template = template.try_into()?;
        py.detach(move || {
            let mut inner = self.write_inner();
            add_template_mutex(&mut inner, template)
        })
    }

    #[instrument(skip_all)]
    fn add_prefixes(&self, py: Python<'_>, prefixes: Bound<'_, PyAny>) -> PyResult<()> {
        let use_prefixes = create_prefix_map(Some(prefixes))?.unwrap();
        py.detach(move || {
            let mut inner = self.write_inner();
            add_prefixes_mutex(&mut inner, use_prefixes)
        })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn truncate_graph(&self, py: Python<'_>, graph: Option<String>) -> PyResult<()> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(move || {
            let mut inner = self.write_inner();
            truncate_graph_mutex(&mut inner, &graph)
        })
    }

    /// Removes the graphs and their triples (None is the default graph). Graphs that do not exist
    /// are ignored.
    #[instrument(skip_all)]
    fn drop_graphs(&self, py: Python<'_>, graphs: Vec<Option<String>>) -> PyResult<()> {
        let graphs = parse_graphs(graphs)?;
        py.detach(move || {
            let mut inner = self.write_inner();
            drop_graphs_mutex(&mut inner, &graphs)
        })
    }

    #[pyo3(signature=(graph=None, preserve_name=None))]
    #[instrument(skip_all)]
    fn detach_graph(
        &self,
        py: Python<'_>,
        graph: Option<String>,
        preserve_name: Option<bool>,
    ) -> PyResult<PyModel> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(move || {
            let mut inner = self.write_inner();
            detach_graph_mutex(&mut inner, &graph, preserve_name.unwrap_or(false))
        })
    }

    #[pyo3(signature = (template, data=None, graph=None, validate_iris=None, stream=None, in_place=false, compact_every=None))]
    #[instrument(skip_all)]
    fn map(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        template: Bound<'_, PyAny>,
        data: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let this = slf.get();
        let template = template.try_into()?;
        if stream.unwrap_or(false) {
            let data = stream_data(data)?;
            parse_optional_named_node(graph.clone())?;
            let template_iri = py.detach(|| {
                let mut inner = this.write_inner();
                resolve_template_mutex(&mut inner, template)
            })?;
            let stream = PyModelStream::new(
                slf.clone().unbind(),
                data,
                StreamSource::Map {
                    template_iri,
                    graph,
                    validate_iris,
                },
                in_place,
                compact_every,
            )?;
            return Ok(Some(Py::new(py, stream)?.into_any()));
        }
        let data = data.map(|x| classify_mapping_data(x, py)).transpose()?;
        match data {
            None => py.detach(move || {
                let mut inner = this.write_inner();
                map_mutex(&mut inner, template, None, graph, None, validate_iris)
            }),
            Some(MappingData::Eager(mappings, types)) => py.detach(move || {
                let mut inner = this.write_inner();
                map_mutex(
                    &mut inner,
                    template,
                    Some(mappings),
                    graph,
                    types,
                    validate_iris,
                )
            }),
            Some(MappingData::Batched(data)) => {
                let template_iri = py.detach(|| {
                    let mut inner = this.write_inner();
                    resolve_template_mutex(&mut inner, template)
                })?;
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.write_inner();
                        map_resolved_mutex(
                            &mut inner,
                            &template_iri,
                            Some(mappings),
                            graph.clone(),
                            types,
                            validate_iris,
                        )
                    })?;
                    Ok(())
                })?;
                Ok(None)
            }
        }
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn size(&self, py: Python<'_>, graph: Option<String>) -> PyResult<usize> {
        py.detach(move || {
            let inner = self.read_inner();
            size_mutex(&inner, graph)
        })
    }

    #[pyo3(signature = (path_or_string, graph=None, uuid_namespace=None, stream=None, documents_per_batch=None, in_place=false, compact_every=None))]
    #[instrument(skip_all)]
    fn map_json(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        path_or_string: &Bound<'_, PyAny>,
        graph: Option<String>,
        uuid_namespace: Option<String>,
        stream: Option<bool>,
        documents_per_batch: Option<usize>,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Option<Py<PyAny>>> {
        map_documents(
            slf,
            py,
            DocumentKind::Json,
            path_or_string,
            graph,
            uuid_namespace,
            stream,
            documents_per_batch,
            in_place,
            compact_every,
        )
    }

    #[pyo3(signature = (path_or_string, graph=None, uuid_namespace=None, stream=None, documents_per_batch=None, in_place=false, compact_every=None))]
    #[instrument(skip_all)]
    fn map_xml(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        path_or_string: &Bound<'_, PyAny>,
        graph: Option<String>,
        uuid_namespace: Option<String>,
        stream: Option<bool>,
        documents_per_batch: Option<usize>,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Option<Py<PyAny>>> {
        map_documents(
            slf,
            py,
            DocumentKind::Xml,
            path_or_string,
            graph,
            uuid_namespace,
            stream,
            documents_per_batch,
            in_place,
            compact_every,
        )
    }

    #[pyo3(signature = (data, predicate=None, graph=None, validate_iris=None, stream=None, in_place=false, compact_every=None))]
    #[instrument(skip_all)]
    fn map_triples(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        predicate: Option<Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let this = slf.get();
        let predicate = if let Some(predicate) = predicate {
            Some(if let Ok(predicate) = predicate.extract::<PyIRI>() {
                predicate.iri.as_str().to_string()
            } else if let Ok(predicate) = predicate.extract::<String>() {
                predicate
            } else {
                return Err(PyMaplibError::FunctionArgumentError(
                    "predicate argument should be IRI or str, but was neither".to_string(),
                )
                .into());
            })
        } else {
            None
        };
        if stream.unwrap_or(false) {
            parse_optional_named_node(graph.clone())?;
            let stream = PyModelStream::new(
                slf.clone().unbind(),
                data.clone().unbind(),
                StreamSource::MapTriples {
                    predicate,
                    graph,
                    validate_iris,
                },
                in_place,
                compact_every,
            )?;
            return Ok(Some(Py::new(py, stream)?.into_any()));
        }
        match classify_mapping_data(data, py)? {
            MappingData::Eager(mappings, types) => py.detach(move || {
                let mut inner = this.write_inner();
                map_triples_mutex(&mut inner, mappings, predicate, graph, types, validate_iris)
            }),
            MappingData::Batched(data) => {
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.write_inner();
                        map_triples_mutex(
                            &mut inner,
                            mappings,
                            predicate.clone(),
                            graph.clone(),
                            types,
                            validate_iris,
                        )
                    })?;
                    Ok(())
                })?;
                Ok(None)
            }
        }
    }

    /// Generates a default template from the data with the same code as generate_template, adds it
    /// to the model and maps the data with it. Returns the template, or a ModelStream when stream
    /// is set.
    #[pyo3(signature = (data, primary_key_column, graph=None, validate_iris=None, stream=None, in_place=false, compact_every=None))]
    #[instrument(skip_all)]
    fn map_default(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        primary_key_column: String,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.get();
        parse_optional_named_node(graph.clone())?;
        if stream.unwrap_or(false) {
            let stream = PyModelStream::new(
                slf.clone().unbind(),
                data.clone().unbind(),
                StreamSource::MapDefault {
                    primary_key_column,
                    graph,
                    validate_iris,
                },
                in_place,
                compact_every,
            )?;
            return Ok(Py::new(py, stream)?.into_any());
        }
        let template = match classify_mapping_data(data, py)? {
            MappingData::Eager(mappings, types) => py.detach(move || {
                let mut inner = this.write_inner();
                map_default_mutex(
                    &mut inner,
                    mappings,
                    &primary_key_column,
                    graph,
                    types,
                    validate_iris,
                )
            })?,
            MappingData::Batched(data) => {
                let mut template = None;
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    py.detach(|| -> PyResult<()> {
                        let mut inner = this.write_inner();
                        if let Some(template) = &template {
                            map_default_batch_mutex(
                                &mut inner,
                                template,
                                mappings,
                                &primary_key_column,
                                graph.clone(),
                                types,
                                validate_iris,
                            )
                        } else {
                            template = Some(map_default_first_batch_mutex(
                                &mut inner,
                                mappings,
                                &primary_key_column,
                                graph.clone(),
                                types,
                                validate_iris,
                            )?);
                            Ok(())
                        }
                    })
                })?;
                template.ok_or_else(|| {
                    PyMaplibError::FunctionArgumentError(
                        "map_default received no batches, so no template could be created"
                            .to_string(),
                    )
                })?
            }
        };
        Ok(PyString::new(py, &format!("{template}")).into_any().unbind())
    }

    /// Generates a default template from the data. This is always a dry run: nothing is mapped
    /// and the template is not added to the model.
    #[pyo3(signature = (data, primary_key_column))]
    #[instrument(skip_all)]
    fn generate_template(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        primary_key_column: String,
    ) -> PyResult<String> {
        let MappingData::Eager(df, types) = classify_mapping_data(data, py)? else {
            return Err(PyMaplibError::FunctionArgumentError(
                "generate_template takes a DataFrame or SolutionMappings, collect LazyFrames or pass a single batch".to_string(),
            )
            .into());
        };
        py.detach(move || generate_template_mutex(df, &primary_key_column, types))
    }

    #[pyo3(signature = (source, metadata=None, graph=None, minimal=None, iri_mappings=None))]
    #[instrument(skip_all)]
    fn map_csvw(
        &self,
        py: Python<'_>,
        source: PathBuf,
        metadata: Option<PathBuf>,
        graph: Option<String>,
        minimal: Option<bool>,
        iri_mappings: Option<HashMap<String, PathBuf>>,
    ) -> PyResult<String> {
        let source = source.to_string_lossy().to_string();
        let metadata = metadata.map(|m| m.to_string_lossy().to_string());
        let mut mappings: Vec<(String, PathBuf)> = iri_mappings.unwrap_or_default().into_iter().collect();
        mappings.sort();
        let res = py.detach(move || -> PyResult<csvw::CsvwResult> {
            let mut inner = self.write_inner();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            inner
                .map_csvw(
                    &source,
                    metadata.as_deref(),
                    &named_graph,
                    minimal.unwrap_or(false),
                    &mappings,
                )
                .map_err(|x| PyMaplibError::from(x).into())
        })?;
        let category = py.get_type::<pyo3::exceptions::PyUserWarning>();
        for w in &res.warnings {
            let msg = std::ffi::CString::new(format!("CSVW: {w}").replace('\0', ""))
                .unwrap_or_default();
            PyErr::warn(py, category.as_any(), &msg, 1)?;
        }
        Ok(res.update)
    }

    #[pyo3(signature = (mapping, database=None, tables=None, base_iri=None, graph=None,
        ignore_data_errors=None))]
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn map_r2rml(
        &self,
        py: Python<'_>,
        mapping: &Bound<'_, PyAny>,
        database: Option<&Bound<'_, PyAny>>,
        tables: Option<HashMap<String, Bound<'_, PyAny>>>,
        base_iri: Option<String>,
        graph: Option<String>,
        ignore_data_errors: Option<bool>,
    ) -> PyResult<()> {
        let base_iri = base_iri.unwrap_or_else(|| "urn:maplib:r2rml:".to_string());
        // The mapping is a path to a Turtle file, or the Turtle itself.
        let text: String = if let Ok(p) = mapping.extract::<PathBuf>() {
            let looks_like_turtle = mapping
                .extract::<String>()
                .map(|s| s.contains('\n') || s.contains("rr:") || s.contains("r2rml#"))
                .unwrap_or(false);
            if !looks_like_turtle && p.exists() {
                std::fs::read_to_string(&p).map_err(|e| {
                    PyMaplibError::FunctionArgumentError(format!(
                        "could not read the R2RML mapping {}: {e}",
                        p.display()
                    ))
                })?
            } else {
                mapping.extract::<String>()?
            }
        } else {
            mapping.extract::<String>()?
        };
        let parsed = r2rml::parse_mapping(&text, Some(&base_iri))
            .map_err(|e| PyMaplibError::from(maplib::errors::MaplibError::from(e)))?;
        // The result of each logical table query: given in tables (by query or table name), or
        // from the database.
        let mut results: HashMap<String, polars::prelude::DataFrame> = HashMap::new();
        let fetch_start = std::time::Instant::now();
        for tm in &parsed.triples_maps {
            let sql = tm.logical_table.sql();
            if results.contains_key(&sql) {
                continue;
            }
            let mut given = tables.as_ref().and_then(|t| t.get(&sql));
            if given.is_none() {
                if let r2rml::mapping::LogicalTable::Table(name) = &tm.logical_table {
                    let unquoted = name.trim_matches('"').to_string();
                    given = tables
                        .as_ref()
                        .and_then(|t| t.get(name).or_else(|| t.get(&unquoted)));
                }
            }
            let frame = match (given, database) {
                (Some(df), _) => df.clone(),
                (None, Some(db)) => db.call_method1("query", (sql.as_str(),))?,
                (None, None) => {
                    return Err(PyMaplibError::FunctionArgumentError(format!(
                        "no database was given, and tables has no result for the logical table {sql}"
                    ))
                    .into())
                }
            };
            let df = if frame.hasattr("to_arrow")? && frame.hasattr("height")? {
                // A polars DataFrame
                representation::python_df_to_rust::polars_df_to_rust_df(&frame)?
            } else if frame.hasattr("to_arrow_table")? {
                // duckdb relations give Arrow record batches directly, without a round trip
                // through polars in Python.
                let table = frame.call_method0("to_arrow_table")?;
                let batches: Vec<Bound<'_, PyAny>> =
                    table.call_method0("to_batches")?.extract()?;
                if batches.is_empty() {
                    polars::prelude::DataFrame::empty()
                } else {
                    representation::python_df_to_rust::array_to_rust_df(&batches)?
                }
            } else if frame.hasattr("pl")? {
                representation::python_df_to_rust::polars_df_to_rust_df(
                    &frame.call_method0("pl")?,
                )?
            } else {
                representation::python_df_to_rust::polars_df_to_rust_df(&frame)?
            };
            results.insert(sql, df);
        }
        tracing::debug!(target: "r2rml_timing", "r2rml_timing: fetch={}", fetch_start.elapsed().as_secs_f64());
        let graph = parse_optional_named_node(graph)?;
        let options = r2rml::R2RMLOptions {
            base_iri,
            graph: NamedGraph::from_maybe_named_node(graph.as_ref()),
            ignore_data_errors: ignore_data_errors.unwrap_or(false),
            blank_node_prefix: format!("r2rml{}_", uuid::Uuid::new_v4().simple()),
        };
        let warnings = py.detach(move || -> PyResult<Vec<String>> {
            let mut inner = self.write_inner();
            inner
                .map_r2rml(&parsed, &results, &options)
                .map_err(|x| PyMaplibError::from(x).into())
        })?;
        let category = py.get_type::<pyo3::exceptions::PyUserWarning>();
        for w in &warnings {
            let msg = std::ffi::CString::new(format!("R2RML: {w}").replace('\0', ""))
                .unwrap_or_default();
            PyErr::warn(py, category.as_any(), &msg, 1)?;
        }
        Ok(())
    }

    #[pyo3(signature = (df, graph=None, uuid_namespace=None))]
    #[instrument(skip_all)]
    fn map_df(
        &self,
        py: Python<'_>,
        df: &Bound<'_, PyAny>,
        graph: Option<String>,
        uuid_namespace: Option<String>,
    ) -> PyResult<()> {
        let (df, _) = data_to_mappings_types(df, py)?;
        py.detach(move || -> PyResult<()> {
            let mut inner = self.write_inner();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            map_df_mutex(&mut inner, df, named_graph, uuid_namespace)
        })
    }

    /// Starts a graph explorer session.
    ///
    /// To run from Jupyter Notebook use:
    /// ```
    /// from maplib import Model
    /// m = Model()
    /// ...
    /// server = m.explore()
    /// ```
    /// You can later stop the server with
    /// `server.stop()`
    /// :param host: The hostname that we will point the browser to
    /// :param port: The port where the graph explorer webserver listens on
    /// :param bind: Bind to the following host / ip
    /// :param popup: Pop up the browser window (deprecated, open server.url yourself instead)
    /// :param fts: Enable full text search indexing
    /// :param fts_path: Path to the fts index
    /// :param graph: The named graph to explore, None is the default graph
    /// :param page: We use this feature flag to test new frontends (try "new" or "yasgui")
    #[pyo3(signature = (*args, **kwargs), text_signature = "(/)")]
    fn explore(
        self_: PyRef<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, pyo3::types::PyTuple>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let module = py.import("maplib")?;
        let func = module.getattr("_explore")?;

        let mut old_args: Vec<_> = args.iter().collect();
        let mut new_args = Vec::new();
        new_args.push(self_.into_pyobject(py)?.into_any());
        new_args.append(&mut old_args);

        let new_args = pyo3::types::PyTuple::new(py, new_args)?;

        let res = func.call(new_args, kwargs)?;
        Ok(res.into())
    }

    #[pyo3(signature = (folder, graph=None))]
    #[instrument(skip_all)]
    fn map_opc_ua(&self, py: Python<'_>, folder: PathBuf, graph: Option<String>) -> PyResult<()> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(|| {
            let mut inner = self.write_inner();
            map_opc_ua(&mut inner, &folder, &graph);
        });

        Ok(())
    }

    /// See maplib.opcua.OPCUA.add_typing_shapes.
    #[pyo3(signature = (shapes_graph=None, type_namespaces=None, instance_namespaces=None))]
    #[instrument(skip_all)]
    fn _add_opcua_typing_shapes(
        &self,
        py: Python<'_>,
        shapes_graph: Option<String>,
        type_namespaces: Option<Vec<String>>,
        instance_namespaces: Option<Vec<String>>,
    ) -> PyResult<()> {
        let shapes_graph = parse_optional_named_node(shapes_graph)?;
        let shapes_graph = NamedGraph::from_maybe_named_node(shapes_graph.as_ref());
        let parse = |nss: Vec<String>| -> PyResult<Vec<NamedNode>> {
            nss.into_iter()
                .map(|ns| {
                    NamedNode::new(&ns).map_err(|e| {
                        PyValueError::new_err(format!("Invalid namespace IRI {ns}: {e}"))
                    })
                })
                .collect()
        };
        let type_namespaces = type_namespaces.map(parse).transpose()?;
        let instance_namespaces = instance_namespaces.map(parse).transpose()?;
        let namespaces = match (&type_namespaces, &instance_namespaces) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "Only one of type_namespaces and instance_namespaces may be given",
                ))
            }
            (Some(t), None) => TypeNamespaces::Types(t),
            (None, Some(i)) => TypeNamespaces::Instances(i),
            (None, None) => TypeNamespaces::Derived,
        };
        py.detach(|| {
            let mut inner = self.write_inner();
            add_opcua_typing_shapes(&mut inner, &shapes_graph, namespaces)
                .map_err(PyMaplibError::from)
        })?;
        Ok(())
    }

    #[pyo3(signature = (client, data_models=None, spaces=None, graph=None, base_iri=None,
        include_schema=None, include_edges=None, virtualize_time_series=None))]
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn map_cdf(
        slf: &Bound<'_, Self>,
        client: &Bound<'_, PyAny>,
        data_models: Option<Bound<'_, PyAny>>,
        spaces: Option<Bound<'_, PyAny>>,
        graph: Option<String>,
        base_iri: Option<String>,
        include_schema: Option<bool>,
        include_edges: Option<bool>,
        virtualize_time_series: Option<bool>,
    ) -> PyResult<()> {
        parse_optional_named_node(graph.clone())?;
        let options = PyDict::new(slf.py());
        options.set_item("data_models", data_models)?;
        options.set_item("spaces", spaces)?;
        options.set_item("graph", graph)?;
        options.set_item("base_iri", base_iri)?;
        options.set_item("include_schema", include_schema)?;
        options.set_item("include_edges", include_edges)?;
        options.set_item("virtualize_time_series", virtualize_time_series)?;
        // The hosts of the client (its base_url and token_url) with the outbound policy
        slf.py()
            .import("maplib.outbound")?
            .call_method1("check_cognite_client", (client,))?;
        // The mapping adds triples through this Model, so the inner model must not be locked here.
        map_cdf(slf.as_any(), client, &options)
    }

    #[pyo3(signature = (ontology, lakehouse=None, eventhouse=None, graph=None, base_iri=None,
        include_schema=None, virtualize_time_series=None, table_reference=None))]
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn map_fabric<'py>(
        slf: &Bound<'py, Self>,
        ontology: &Bound<'py, PyAny>,
        lakehouse: Option<Bound<'py, PyAny>>,
        eventhouse: Option<Bound<'py, PyAny>>,
        graph: Option<String>,
        base_iri: Option<String>,
        include_schema: Option<bool>,
        virtualize_time_series: Option<bool>,
        table_reference: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        parse_optional_named_node(graph.clone())?;
        let options = PyDict::new(slf.py());
        options.set_item("lakehouse", lakehouse)?;
        options.set_item("eventhouse", eventhouse)?;
        options.set_item("graph", graph)?;
        options.set_item("base_iri", base_iri)?;
        options.set_item("include_schema", include_schema)?;
        options.set_item("virtualize_time_series", virtualize_time_series)?;
        options.set_item("table_reference", table_reference)?;
        // The mapping adds triples through this Model, so the inner model must not be locked here.
        map_fabric(slf.as_any(), ontology, &options)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        url,
        asset_database=None,
        asset_server=None,
        root_element=None,
        username=None,
        password=None,
        token=None,
        verify_ssl=true,
        graph=None,
        base_iri=None,
        read_static_values=true,
        default_start_time=None,
        default_end_time=None,
        page_size=10_000,
        max_concurrency=8,
        timeout=None,
    ))]
    #[instrument(skip_all)]
    fn map_osipi(
        &self,
        py: Python<'_>,
        url: String,
        asset_database: Option<String>,
        asset_server: Option<String>,
        root_element: Option<String>,
        username: Option<String>,
        password: Option<String>,
        token: Option<String>,
        verify_ssl: bool,
        graph: Option<String>,
        base_iri: Option<String>,
        read_static_values: bool,
        default_start_time: Option<String>,
        default_end_time: Option<String>,
        page_size: usize,
        max_concurrency: usize,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let auth = osipi_auth(username, password, token)?;
        check_osipi_limits(page_size, max_concurrency)?;
        check_outbound_host(py, &url, "https", 443)?;
        let settings = OSIPISettings {
            url,
            auth,
            verify_ssl,
            asset_server,
            asset_database,
            root_element,
            base_iri: base_iri.unwrap_or_else(|| OSIPI_NAMESPACE.to_string()),
            read_static_values,
            default_start_time,
            default_end_time,
            page_size,
            max_concurrency,
            timeout_seconds: timeout,
        };
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(|| {
            let mut inner = self.write_inner();
            inner
                .map_osipi(settings, &graph)
                .map_err(PyMaplibError::from)?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        host,
        tenant,
        client_id=None,
        client_secret=None,
        token=None,
        token_url=None,
        app_name=None,
        app_version=None,
        user_tenant=None,
        root_asset=None,
        graph=None,
        base_iri=None,
        default_start_time=None,
        default_end_time=None,
        aggregate_pushdown=false,
        page_size=2000,
        max_concurrency=8,
        timeout=None,
        verify_ssl=true,
    ))]
    #[instrument(skip_all)]
    fn map_insights_hub(
        &self,
        py: Python<'_>,
        host: String,
        tenant: String,
        client_id: Option<String>,
        client_secret: Option<String>,
        token: Option<String>,
        token_url: Option<String>,
        app_name: Option<String>,
        app_version: Option<String>,
        user_tenant: Option<String>,
        root_asset: Option<String>,
        graph: Option<String>,
        base_iri: Option<String>,
        default_start_time: Option<String>,
        default_end_time: Option<String>,
        aggregate_pushdown: bool,
        page_size: usize,
        max_concurrency: usize,
        timeout: Option<f64>,
        verify_ssl: bool,
    ) -> PyResult<()> {
        let argument_error =
            |s: &str| -> PyErr { PyMaplibError::FunctionArgumentError(s.to_string()).into() };
        // The API host and the token endpoint with the outbound policy
        {
            let parameters = PyDict::new(py);
            parameters.set_item("host", &host)?;
            parameters.set_item("tenant", &tenant)?;
            parameters.set_item("token_url", &token_url)?;
            parameters.set_item("token", token.as_ref().map(|_| "1"))?;
            parameters.set_item("app_name", &app_name)?;
            py.import("maplib.outbound")?
                .call_method1("check_connection", ("insights_hub", parameters))?;
        }
        let auth = match (client_id, client_secret, token) {
            (Some(client_id), Some(client_secret), None) => match (app_name, app_version) {
                (Some(app_name), Some(app_version)) => InsightsHubAuth::AppCredentials {
                    client_id,
                    client_secret,
                    app_name,
                    app_version,
                    user_tenant,
                },
                (None, None) => {
                    if user_tenant.is_some() {
                        return Err(argument_error(
                            "user_tenant is only used with app credentials (app_name and app_version)",
                        ));
                    }
                    InsightsHubAuth::ServiceCredentials {
                        client_id,
                        client_secret,
                        token_url,
                    }
                }
                _ => {
                    return Err(argument_error(
                        "Give both app_name and app_version for app credentials",
                    ))
                }
            },
            (None, None, Some(token)) => InsightsHubAuth::Token { token },
            _ => {
                return Err(argument_error(
                    "Give either both client_id and client_secret, or a token, not both",
                ))
            }
        };
        if page_size == 0 || max_concurrency == 0 {
            return Err(argument_error(
                "page_size and max_concurrency must be positive",
            ));
        }
        let settings = InsightsHubSettings {
            host,
            tenant,
            auth,
            root_asset,
            base_iri: base_iri.unwrap_or_else(|| INSIGHTS_HUB_NAMESPACE.to_string()),
            default_start_time,
            default_end_time,
            aggregate_pushdown,
            page_size,
            max_concurrency,
            timeout_seconds: timeout,
            verify_ssl,
        };
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        // Read while holding the GIL, the reading is done in Python
        let asset_model = read_insights_hub(&settings)
            .map_err(|e| PyMaplibError::from(maplib::errors::MaplibError::from(e)))?;
        py.detach(|| {
            let mut inner = self.write_inner();
            inner
                .add_insights_hub(asset_model, &graph)
                .map_err(PyMaplibError::from)?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        url,
        data_server=None,
        name_filter="*".to_string(),
        source_filter=None,
        point_attributes=None,
        username=None,
        password=None,
        token=None,
        verify_ssl=true,
        graph=None,
        base_iri=None,
        default_start_time=None,
        default_end_time=None,
        page_size=10_000,
        max_concurrency=8,
        timeout=None,
    ))]
    #[instrument(skip_all)]
    fn map_pi_points(
        &self,
        py: Python<'_>,
        url: String,
        data_server: Option<String>,
        name_filter: String,
        source_filter: Option<String>,
        point_attributes: Option<Vec<String>>,
        username: Option<String>,
        password: Option<String>,
        token: Option<String>,
        verify_ssl: bool,
        graph: Option<String>,
        base_iri: Option<String>,
        default_start_time: Option<String>,
        default_end_time: Option<String>,
        page_size: usize,
        max_concurrency: usize,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let auth = osipi_auth(username, password, token)?;
        check_osipi_limits(page_size, max_concurrency)?;
        check_outbound_host(py, &url, "https", 443)?;
        let settings = OSIPISettings {
            url,
            auth,
            verify_ssl,
            asset_server: None,
            asset_database: None,
            root_element: None,
            base_iri: base_iri.unwrap_or_else(|| OSIPI_NAMESPACE.to_string()),
            read_static_values: false,
            default_start_time,
            default_end_time,
            page_size,
            max_concurrency,
            timeout_seconds: timeout,
        };
        let selection = PIPointSelection {
            data_server,
            name_filter,
            source_filter,
            point_attributes: point_attributes.unwrap_or_else(|| vec!["pointsource".to_string()]),
        };
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(|| {
            let mut inner = self.write_inner();
            inner
                .map_pi_points(settings, selection, &graph)
                .map_err(PyMaplibError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (virtualized_database, resources))]
    #[instrument(skip_all)]
    fn add_virtualization(
        &self,
        py: Python<'_>,
        virtualized_database: VirtualizedPythonDatabase,
        resources: HashMap<String, PyTemplate>,
    ) -> PyResult<()> {
        py.detach(|| {
            let mut inner = self.write_inner();
            add_virtualization_mutex(&mut inner, virtualized_database, resources)
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        query,
        parameters=None,
        solution_mappings=None,
        graph=None,
        return_json=None,
        return_format=None,
        include_graphs=None,
        max_rows=None,
        debug=None,
        bindings=None,
        endpoint=None,
        endpoint_token=None,
        cache=true,
        timeout=None,
    ))]
    #[instrument(skip_all)]
    fn query(
        &self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        solution_mappings: Option<bool>,
        graph: Option<String>,
        return_json: Option<bool>,
        return_format: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
        bindings: Option<HashMap<String, Py<PyAny>>>,
        endpoint: Option<String>,
        endpoint_token: Option<String>,
        cache: bool,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let timeout_duration = parse_timeout(timeout)?;
        if let Some(endpoint) = endpoint {
            if parameters.is_some() {
                return Err(PyMaplibError::FunctionArgumentError(
                    "parameters can not be used with endpoint".to_string(),
                )
                .into());
            }
            let parsed_bindings = maybe_parse_bindings(bindings, py)?;
            let q = MaplibEndpointQuery {
                query,
                graph,
                include_graphs,
                max_rows,
                bindings: parsed_bindings
                    .map(|b| b.into_iter().map(|(k, v)| (k, v.to_string())).collect()),
                auth_token: endpoint_token,
                timeout,
            };
            let (kind, cats, prefixes) = py.detach(|| -> PyResult<_> {
                let kind = query_maplib_endpoint(&endpoint, &q).map_err(|e| match &e {
                    MaplibEndpointError::Status(503 | 504, msg) => {
                        QueryTimeoutError::new_err(msg.clone())
                    }
                    _ => PyMaplibError::RuntimeError(e.to_string()).into(),
                })?;
                // The results have no dictionary encoded columns, the model's dictionaries
                // are only used for formatting, and the model's prefixes for Turtle.
                let inner = self.read_inner();
                Ok((
                    kind,
                    inner.triplestore.global_cats.clone(),
                    inner.prefixes.clone(),
                ))
            })?;
            let res = QueryResult {
                kind,
                debug: None,
                pushdown_paths: vec![],
            };
            return query_to_result(
                res,
                solution_mappings.unwrap_or(false),
                solution_mappings.unwrap_or(false),
                return_json.unwrap_or(false),
                return_format,
                cats,
                &prefixes,
                py,
            );
        }
        // The timeout counts from here, and covers formatting the result.
        let token = timeout_duration.map(|t| CancelToken::new(Some(t)));
        let use_cache = cache
            && parameters.is_none()
            && !debug.unwrap_or(false)
            && self.query_cache.enabled();
        if !use_cache && cache && self.query_cache.enabled() {
            self.query_cache.note_bypass();
        }
        let mapped_parameters = map_parameters(parameters, py)?;
        let graph = parse_optional_named_node(graph)?;
        let graph = graph.map(|graph| NamedGraph::from_maybe_named_node(Some(&graph)));
        let parsed_bindings = maybe_parse_bindings(bindings, py)?;
        let key = use_cache.then(|| {
            let include = parse_include_graphs(include_graphs.clone(), true);
            let mut b: Vec<(&String, String)> = parsed_bindings
                .iter()
                .flatten()
                .map(|(k, v)| (k, v.to_string()))
                .collect();
            b.sort();
            KeyBuilder::new("model.query")
                .part("query", query.trim())
                .part("graph", format!("{graph:?}"))
                .part("include", format!("{:?}", include.ok()))
                .part("max_rows", format!("{max_rows:?}"))
                .part("bindings", format!("{b:?}"))
                .finish()
        });
        let mut key = key;
        let hit = match key.as_ref().map(|k| self.query_cache.get(k)) {
            Some(Lookup::Hit(v)) => v.downcast::<CachedQueryResult>().ok(),
            Some(Lookup::Bypass) => {
                key = None;
                None
            }
            _ => None,
        };
        let (res, cats, prefixes) = if let Some(hit) = hit {
            let res = QueryResult {
                kind: hit.kind.clone(),
                debug: None,
                pushdown_paths: vec![],
            };
            (res, hit.cats.clone(), hit.prefixes.clone())
        } else {
            let query_cache = self.query_cache.clone();
            let token = token.clone();
            py.detach(|| -> PyResult<(_, LockedCats, _)> {
                let inner = self.read_inner();
                // Read under the read lock: no write runs until it is released.
                let stamp = query_cache.versions.stamp();
                let plan = if key.is_some() {
                    let include = parse_include_graphs(include_graphs.clone(), true)?;
                    Some(inner.cache_plan(&query, graph.as_ref(), &include, parsed_bindings.as_ref()))
                } else {
                    None
                };
                let cats = inner.triplestore.global_cats.clone();
                let started = std::time::Instant::now();
                let res = with_token(token.clone(), || {
                    query_mutex(
                        &inner,
                        query,
                        mapped_parameters,
                        graph,
                        include_graphs,
                        max_rows,
                        debug,
                        parsed_bindings,
                    )
                })
                .map_err(|e| timeout_error(e, token.as_ref()))?;
                let run_time = started.elapsed();
                let prefixes = inner.prefixes.clone();
                drop(inner);
                match (key, plan) {
                    (Some(key), Some(CachePlan::Cache { virtualized: true, .. }))
                        if !query_cache.options().cache_virtualized =>
                    {
                        query_cache.note_uncacheable(key)
                    }
                    (Some(key), Some(CachePlan::Cache { reads, .. })) => {
                        let bytes = result_bytes(&res.kind);
                        if query_cache.worth_keeping(run_time, bytes) {
                            let value = CachedQueryResult {
                                kind: res.kind.clone(),
                                cats: cats.clone(),
                                prefixes: prefixes.clone(),
                            };
                            query_cache.insert(key, stamp, reads, Arc::new(value), bytes);
                        }
                    }
                    (Some(key), Some(CachePlan::Bypass(_))) => query_cache.note_uncacheable(key),
                    _ => {}
                }
                Ok((res, cats, prefixes))
            })?
        };
        print_debug_if_exists(res.debug.as_ref());

        let out = with_token(token.clone(), || {
            query_to_result(
                res,
                solution_mappings.unwrap_or(false),
                solution_mappings.unwrap_or(false),
                return_json.unwrap_or(false),
                return_format,
                cats,
                &prefixes,
                py,
            )
        });
        match token.as_ref().and_then(|t| t.stopped()) {
            Some(c @ utils::cancel::Cancelled::Timeout(_)) => {
                Err(QueryTimeoutError::new_err(c.to_string()))
            }
            _ => out,
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        query,
        endpoint,
        method=None,
        solution_mappings=None,
        return_json=None,
        return_format=None,
        bindings=None,
        headers=None,
        ))]
    #[instrument(skip_all)]
    fn query_external(
        &self,
        py: Python<'_>,
        query: String,
        endpoint: String,
        method: Option<String>,
        solution_mappings: Option<bool>,
        return_json: Option<bool>,
        return_format: Option<String>,
        bindings: Option<HashMap<String, Py<PyAny>>>,
        headers: Option<Vec<(String, String)>>,
    ) -> PyResult<Py<PyAny>> {
        let parsed_bindings = maybe_parse_bindings(bindings, py)?;
        let method = if let Some(method) = method {
            match method.to_lowercase().trim() {
                "get" => SparqlMethod::GET,
                "post" => SparqlMethod::POST,
                _ => {
                    return Err(PyMaplibError::FunctionArgumentError(format!(
                        "Unsupported method {}",
                        method
                    ))
                    .into())
                }
            }
        } else {
            SparqlMethod::GET
        };

        let (res, cats, prefixes) = py.detach(|| -> PyResult<(_, LockedCats, _)> {
            let inner = self.read_inner();
            let cats = inner.triplestore.global_cats.clone();
            let res =
                query_external_mutex(&inner, query, endpoint, method, parsed_bindings, headers)?;
            Ok((res, cats, inner.prefixes.clone()))
        })?;
        print_debug_if_exists(res.debug.as_ref());
        query_to_result(
            res,
            solution_mappings.unwrap_or(false),
            solution_mappings.unwrap_or(false),
            return_json.unwrap_or(false),
            return_format,
            cats,
            &prefixes,
            py,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
            update,
            parameters=None,
            graph=None,
            include_graphs=None,
            max_rows=None,
            debug=None,
            timeout=None,
            return_triples=None,
            solution_mappings=None,
    ))]
    #[instrument(skip_all)]
    fn update(
        &self,
        py: Python<'_>,
        update: String,
        parameters: Option<ParametersType>,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
        timeout: Option<f64>,
        return_triples: Option<bool>,
        solution_mappings: Option<bool>,
    ) -> PyResult<HashMap<String, Py<PyAny>>> {
        let mapped_parameters = map_parameters(parameters, py)?;
        let timeout = parse_timeout(timeout)?;
        let (res, cats) = py.detach(|| -> PyResult<_> {
            let mut inner = self.write_inner();
            let cats = inner.triplestore.global_cats.clone();
            // The timeout counts from when the model is locked for the update.
            let token = timeout.map(|t| CancelToken::new(Some(t)));
            let res = with_token(token.clone(), || {
                update_mutex(
                    &mut inner,
                    update,
                    mapped_parameters,
                    graph,
                    include_graphs,
                    max_rows,
                    debug,
                    return_triples.unwrap_or(false),
                )
            })
            .map_err(|e| timeout_error(e, token.as_ref()))?;
            Ok((res, cats))
        })?;
        print_debug_if_exists(res.debug.as_ref());
        changes_to_dict(res.changes, solution_mappings.unwrap_or(false), cats, py)
    }

    #[pyo3(signature = (options=None))]
    #[instrument(skip_all)]
    fn create_index(&self, py: Python<'_>, options: Option<PyIndexingOptions>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.write_inner();
            create_index_mutex(&mut inner, options)
        })
    }

    #[pyo3(signature = (
        shape_graph=None,
        data_graph=None,
        report_graph="".to_string(),
        inferences_graph=None,
        include_details=None,
        include_conforms=None,
        only_shapes=None,
        deactivate_shapes=None,
        dry_run=None,
        max_rows=None,
        serial=None,
        max_iterations=100_000,
        debug_rules=None,
    ))]
    #[instrument(skip_all)]
    fn validate(
        &self,
        py: Python<'_>,
        shape_graph: Option<String>,
        data_graph: Option<String>,
        report_graph: Option<String>,
        inferences_graph: Option<String>,
        include_details: Option<bool>,
        include_conforms: Option<bool>,
        only_shapes: Option<Vec<String>>,
        deactivate_shapes: Option<Vec<String>>,
        dry_run: Option<bool>,
        max_rows: Option<usize>,
        serial: Option<bool>,
        max_iterations: Option<usize>,
        debug_rules: Option<bool>,
    ) -> PyResult<PyValidationReport> {
        let res = py.detach(move || {
            let mut inner = self.write_inner();
            validate_mutex(
                &mut inner,
                shape_graph,
                data_graph,
                report_graph,
                inferences_graph,
                include_details,
                include_conforms,
                only_shapes,
                deactivate_shapes,
                dry_run,
                max_rows,
                serial,
                max_iterations,
                debug_rules,
            )
        })?;
        Ok(res)
    }

    #[pyo3(signature = (
            query,
            parameters=None,
            solution_mappings=None,
            source_graph=None,
            target_graph=None,
            include_graphs=None,
            max_rows=None,
            debug=None,
    ))]
    #[instrument(skip_all)]
    fn insert(
        &self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        solution_mappings: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<HashMap<String, Py<PyAny>>> {
        let mapped_parameters = map_parameters(parameters, py)?;
        let source_graph = parse_optional_named_node(source_graph)?;
        let source_graph = NamedGraph::from_maybe_named_node(source_graph.as_ref());
        let target_graph = parse_optional_named_node(target_graph)?;
        let target_graph = NamedGraph::from_maybe_named_node(target_graph.as_ref());
        let (insert_result, cats) = py.detach(|| -> PyResult<(InsertResult, LockedCats)> {
            let mut inner = self.write_inner();

            let cats = inner.triplestore.global_cats.clone();

            let insert_result = insert_mutex(
                &mut inner,
                query,
                mapped_parameters,
                source_graph,
                target_graph,
                include_graphs,
                max_rows,
                debug,
            )?;
            Ok((insert_result, cats))
        })?;
        print_debug_if_exists(insert_result.debug.as_ref());

        new_triples_to_dict(
            insert_result.new_triples,
            solution_mappings.unwrap_or(false),
            cats,
            py,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        file_path,
        format=None,
        base_iri=None,
        parallel=None,
        checked=None,
        graph=None,
        replace_graph=None,
        triples_batch_size=None,
        known_contexts=None,
        geo=None,
        strict=false,
        storage_options=None,
    ))]
    #[instrument(skip_all)]
    fn read(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        base_iri: Option<String>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
        triples_batch_size: Option<usize>,
        known_contexts: Option<HashMap<String, String>>,
        geo: Option<bool>,
        strict: bool,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let source = ReadSource::extract(file_path)?;
        let storage_options = storage_options.unwrap_or_default();
        py.detach(move || {
            let mut inner = self.write_inner();
            read_mutex(
                &mut inner,
                source,
                format,
                base_iri,
                parallel,
                checked,
                graph,
                replace_graph,
                triples_batch_size,
                known_contexts.unwrap_or_default(),
                geo.unwrap_or(false),
                strict,
                &storage_options,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path, storage_options=None))]
    #[instrument(skip_all)]
    fn read_template(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let storage_options = storage_options.unwrap_or_default();
        py.detach(move || {
            let mut inner = self.write_inner();
            read_template_mutex(&mut inner, file_path, &storage_options)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        s,
        format,
        base_iri=None,
        parallel=None,
        checked=None,
        graph=None,
        replace_graph=None,
        triples_batch_size=None,
        known_contexts=None,
        geo=None,
        strict=false,
    ))]
    #[instrument(skip_all)]
    fn reads(
        &self,
        py: Python<'_>,
        s: &str,
        format: &str,
        base_iri: Option<String>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
        triples_batch_size: Option<usize>,
        known_contexts: Option<HashMap<String, String>>,
        geo: Option<bool>,
        strict: bool,
    ) -> PyResult<()> {
        py.detach(|| {
            let mut inner = self.write_inner();
            reads_mutex(
                &mut inner,
                s,
                format,
                base_iri,
                parallel,
                checked,
                graph,
                replace_graph,
                triples_batch_size,
                known_contexts.unwrap_or_default(),
                geo.unwrap_or(false),
                strict,
            )
        })
    }

    #[instrument(skip_all)]
    fn get_templates(&self, py: Python<'_>) -> PyResult<Vec<PyTemplate>> {
        py.detach(|| {
            let inner = self.read_inner();
            let templates = inner
                .get_templates()
                .into_iter()
                .map(|t| PyTemplate {
                    template: t.clone(),
                })
                .collect();
            Ok(templates)
        })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn templates_to_graph(&self, py: Python<'_>, graph: Option<String>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.write_inner();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            inner
                .templates_to_graph(&named_graph)
                .map_err(PyMaplibError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (file_path, format=None, graph=None, prefixes=None, storage_options=None))]
    #[instrument(skip_all)]
    fn write(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        graph: Option<Bound<'_, PyAny>>,
        prefixes: Option<Bound<'_, PyAny>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let target = WriteTarget::extract(file_path)?;
        let prefixes = create_prefix_map(prefixes)?;
        let graph = GraphArg::extract(graph.as_ref())?;
        let storage_options = storage_options.unwrap_or_default();

        py.detach(move || {
            let inner = self.read_inner();
            write_triples_mutex(&inner, target, format, graph, prefixes, &storage_options)
        })
    }

    #[pyo3(signature = (format=None, graph=None, prefixes=None))]
    #[instrument(skip_all)]
    fn write_bytes<'py>(
        &self,
        py: Python<'py>,
        format: Option<String>,
        graph: Option<Bound<'_, PyAny>>,
        prefixes: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let prefixes = create_prefix_map(prefixes)?;
        let graph = GraphArg::extract(graph.as_ref())?;
        let out = py.detach(move || {
            let inner = self.read_inner();
            write_bytes_mutex(&inner, format, graph, prefixes)
        })?;
        Ok(PyBytes::new(py, &out))
    }

    #[pyo3(signature = (
        file_path, profile_graph, model_iri=None, version=None, description=None, created=None,
        scenario_time=None, modeling_authority_set=None, prefixes=None, graph=None,
        storage_options=None))]
    #[instrument(skip_all)]
    fn write_cim_xml(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        profile_graph: String,
        model_iri: Option<String>,
        version: Option<String>,
        description: Option<String>, // pymethods non-critical functions
        created: Option<String>,
        scenario_time: Option<String>,
        modeling_authority_set: Option<String>,
        prefixes: Option<Bound<'_, PyAny>>,
        graph: Option<String>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let target = WriteTarget::extract(file_path)?;
        let use_prefixes = create_prefix_map(prefixes)?.unwrap_or(Default::default());
        let storage_options = storage_options.unwrap_or_default();

        py.detach(move || {
            let mut inner = self.write_inner();
            write_cim_xml_mutex(
                &mut inner,
                target,
                profile_graph,
                model_iri,
                version,
                description,
                created,
                scenario_time,
                modeling_authority_set,
                use_prefixes,
                graph,
                &storage_options,
            )
        })
    }

    #[pyo3(signature = (format=None, graph=None, prefixes=None))]
    #[instrument(skip_all)]
    fn writes(
        &self,
        py: Python<'_>,
        format: Option<String>,
        graph: Option<Bound<'_, PyAny>>,
        prefixes: Option<Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let use_prefixes = create_prefix_map(prefixes)?;
        let graph = GraphArg::extract(graph.as_ref())?;

        py.detach(|| {
            let inner = self.read_inner();
            writes_mutex(&inner, format, graph, use_prefixes)
        })
    }

    #[pyo3(signature = (graph=None, include_graphs=None))]
    #[instrument(skip_all)]
    fn get_predicate_iris(
        &self,
        py: Python<'_>,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
    ) -> PyResult<Vec<PyIRI>> {
        py.detach(move || {
            let inner = self.read_inner();
            get_predicate_iris_mutex(&inner, graph, include_graphs)
        })
    }

    #[pyo3(signature = (iri, graph=None, include_graphs=None))]
    #[instrument(skip_all)]
    fn get_predicate(
        &self,
        py: Python<'_>,
        iri: PyIRI,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let eager_sms = py.detach(|| {
            let inner = self.read_inner();
            get_predicate_mutex(&inner, iri, graph, include_graphs)
        })?;
        let mut out = vec![];
        for EagerSolutionMappings {
            mappings,
            rdf_node_types,
        } in eager_sms
        {
            let py_sm = df_to_py_df(mappings, rdf_node_types, None, None, true, py)?;
            out.push(py_sm);
        }
        Ok(out)
    }

    #[pyo3(signature = (
        rulesets,
        graph=None,
        max_iterations=100_000,
        include_graphs=None,
        max_rows=100_000_000,
        debug=None))]
    #[instrument(skip_all)]
    fn infer(
        &self,
        py: Python<'_>,
        rulesets: Py<PyAny>,
        graph: Option<String>,
        max_iterations: Option<usize>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<PyInferenceResult> {
        let rulesets = if let Ok(s) = rulesets.extract::<String>(py) {
            vec![s]
        } else if let Ok(ss) = rulesets.extract::<Vec<String>>(py) {
            ss
        } else {
            return Err(PyMaplibError::FunctionArgumentError(
                "ruleset should be str or List[str]".to_string(),
            )
            .into());
        };
        let graph = parse_optional_named_node(graph)?;
        let named_graph = graph.map(|graph| NamedGraph::from_maybe_named_node(Some(&graph)));
        let (res, ..) = py.detach(|| -> PyResult<(_, LockedCats)> {
            let mut inner = self.write_inner();

            let cats = inner.triplestore.global_cats.clone();

            let res = infer_mutex(
                &mut inner,
                rulesets,
                max_iterations,
                named_graph.as_ref(),
                parse_include_graphs(include_graphs, true)?,
                max_rows,
                debug,
            )?;

            Ok((res, cats))
        })?;
        Ok(PyInferenceResult { inner: res })
    }

    #[pyo3(signature = (
        profile="owl-rl".to_string(),
        graph=None,
        target_graph=None,
        include_graphs=None,
        max_violations=1000,
        max_rows=100_000_000))]
    #[instrument(skip_all)]
    fn reason(
        &self,
        py: Python<'_>,
        profile: String,
        graph: Option<String>,
        target_graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_violations: usize,
        max_rows: Option<usize>,
    ) -> PyResult<PyReasoningResult> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        let target_graph = if let Some(target_graph) = target_graph {
            NamedGraph::from_maybe_named_node(Some(&parse_named_node(target_graph)?))
        } else {
            NamedGraph::transient()
        };
        let include_graphs = parse_include_graphs(include_graphs, true)?;
        let res = py.detach(|| -> PyResult<_> {
            let mut inner = self.write_inner();
            Ok(reason_mutex(
                &mut inner,
                &profile,
                &graph,
                &target_graph,
                include_graphs,
                max_rows,
                max_violations,
            )?)
        })?;
        Ok(PyReasoningResult { inner: res })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn infer_rdfs(&self, py: Python<'_>, graph: Option<String>) -> PyResult<usize> {
        let graph = parse_optional_named_node(graph)?;
        let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        let res = py.detach(|| -> PyResult<usize> {
            let mut inner = self.write_inner();

            let res = infer_rdfs_mutex(&mut inner, &named_graph)?;

            Ok(res)
        })?;
        Ok(res)
    }

    #[pyo3(signature = ())]
    fn compact(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.write_inner();
            compact_mutex(&mut inner)
        })
    }

    #[pyo3(signature = (path = PathBuf::from("./model.maplib"), raw_db = false, single_file = true, storage_options = None))]
    fn serialize(
        &self,
        py: Python<'_>,
        path: PathBuf,
        raw_db: bool,
        single_file: bool,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let storage_options = storage_options.unwrap_or_default();
        py.detach(move || {
            let mut inner = self.write_inner();
            serialize_triples_mutex(&mut inner, path.as_ref(), raw_db, single_file, &storage_options)
        })
    }

    #[instrument(skip_all)]
    #[staticmethod]
    #[pyo3(signature = (path = PathBuf::from("./model.maplib"), storage_folder=None, storage=None, storage_options=None, cache_size=None))]
    fn deserialize(
        py: Python<'_>,
        path: PathBuf,
        storage_folder: Option<PathBuf>,
        storage: Option<String>,
        storage_options: Option<HashMap<String, String>>,
        cache_size: Option<u64>,
    ) -> PyResult<Self> {
        let storage = storage_options_from(storage, storage_folder)?;
        let remote_options = storage_options.unwrap_or_default();
        if let Some(cache_size) = cache_size {
            triplestore::storage::remote_segments::set_cache_limit(Some(cache_size));
        }

        let inner: Result<InnerModel, PyMaplibError> = py.detach(move || {
            let inner = match path.to_str().filter(|p| is_remote(p)) {
                Some(url) => InnerModel::deserialize_triples_remote(url, &remote_options, storage),
                None => InnerModel::deserialize_triples(&path, storage),
            }
            .map_err(PyMaplibError::from)?;
            Ok(inner)
        });
        Ok(PyModel::from_inner_mapping(inner?))
    }

    #[pyo3(signature = (iri, func, output_type, input_types=None))]
    fn add_udf(
        &self,
        py: Python<'_>,
        iri: String,
        func: Py<PyAny>,
        output_type: Bound<'_, PyAny>,
        input_types: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<()> {
        if !func.bind(py).is_callable() {
            return Err(PyMaplibError::UDFError(format!("UDF {} is not callable", iri)).into());
        }
        let iri = parse_named_node(iri)?;
        let output = resolve_rdf_node_type(&output_type)?;
        let inputs = if let Some(input_types) = input_types {
            let inputs: Vec<BaseRDFNodeType> = input_types
                .iter()
                .map(|t| resolve_rdf_node_type(t))
                .collect::<PyResult<Vec<_>>>()?;
            Some(inputs)
        } else {
            None
        };
        let _ = py.detach(|| -> PyResult<()> {
            let mut inner = self.write_inner();
            add_udf_mutex(&mut inner, iri, func, output, inputs)?;
            Ok(())
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        allowed_endpoints=None,
        headers=None,
        timeout=Some(300.0),
        max_rows=None,
        batch_size=1000,
        arrow_batch_size=1_000_000,
        concurrency=4,
        method="POST".to_string(),
        bind_join=true,
        allow_load=false,
        arrow=true,
    ))]
    fn set_federation(
        &self,
        py: Python<'_>,
        allowed_endpoints: Option<Vec<String>>,
        headers: Option<HashMap<String, HashMap<String, String>>>,
        timeout: Option<f64>,
        max_rows: Option<usize>,
        batch_size: usize,
        arrow_batch_size: usize,
        concurrency: usize,
        method: String,
        bind_join: bool,
        allow_load: bool,
        arrow: bool,
    ) -> PyResult<()> {
        let arg_error = |m: &str| -> PyErr {
            PyMaplibError::FunctionArgumentError(m.to_string()).into()
        };
        if batch_size == 0 || arrow_batch_size == 0 {
            return Err(arg_error("batch_size and arrow_batch_size must be at least 1"));
        }
        if concurrency == 0 {
            return Err(arg_error("concurrency must be at least 1"));
        }
        let timeout = match timeout {
            None => None,
            Some(t) if t > 0.0 && t.is_finite() => Some(std::time::Duration::from_secs_f64(t)),
            Some(_) => return Err(arg_error("timeout must be a positive number of seconds")),
        };
        let method = match method.to_ascii_uppercase().as_str() {
            "GET" => ServiceMethod::Get,
            "POST" => ServiceMethod::Post,
            _ => return Err(arg_error("method must be \"GET\" or \"POST\"")),
        };
        let mut header_list: Vec<(String, Vec<(String, String)>)> = headers
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();
        header_list.sort();
        let settings = FederationSettings {
            allowed_endpoints: match allowed_endpoints {
                None => AllowedEndpoints::Any,
                Some(l) => AllowedEndpoints::Only(l),
            },
            allow_load,
            headers: header_list,
            timeout,
            max_rows,
            batch_size,
            arrow_batch_size,
            concurrency,
            method,
            bind_join,
            arrow,
            ..FederationSettings::default()
        };
        py.detach(|| {
            let mut inner = self.write_inner();
            inner.triplestore.federation = Arc::new(settings);
        });
        Ok(())
    }

    fn list_udfs(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let udfs = py.detach(|| -> PyResult<Vec<String>> {
            let inner = self.read_inner();
            let udfs = list_udfs_mutex(&inner)?;
            Ok(udfs)
        });
        udfs
    }

    #[pyo3(signature = (other, source_graph=None, target_graph=None))]
    fn add_graph(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyModel>,
        source_graph: Option<String>,
        target_graph: Option<String>,
    ) -> PyResult<()> {
        let source_graph = parse_optional_named_node(source_graph)?;
        let target_graph = parse_optional_named_node(target_graph)?;
        let source_graph = NamedGraph::from_maybe_named_node(source_graph.as_ref());
        let target_graph = NamedGraph::from_maybe_named_node(target_graph.as_ref());
        let other = other.get();
        if std::ptr::eq(self, other) {
            return Err(PyMaplibError::FunctionArgumentError(
                "add_graph needs another Model, use insert to copy triples within a Model"
                    .to_string(),
            )
            .into());
        }
        py.detach(move || -> PyResult<()> {
            // Lock the two models in a fixed order, so that a.add_graph(b) and b.add_graph(a)
            // in two threads do not deadlock.
            let (mut inner, other_inner) = if (self as *const Self) < (other as *const Self) {
                let inner = self.write_inner();
                (inner, other.read_inner())
            } else {
                let other_inner = other.read_inner();
                (self.write_inner(), other_inner)
            };
            add_graph_mutex(
                &mut inner,
                &other_inner.triplestore,
                source_graph,
                target_graph,
            )?;
            Ok(())
        })
    }

    /// Sets the query result cache: QueryCacheOptions, True for the default options, or
    /// False / None to disable it. Results are kept until a write changes the triples they read
    /// (per named graph and predicate), they expire (ttl) or they are evicted.
    #[pyo3(signature = (options=None))]
    fn set_query_cache(&self, options: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let options = match options {
            Some(o) => options_from(&o)?,
            None => Default::default(),
        };
        self.query_cache.set_options(options);
        Ok(())
    }

    /// The options of the query result cache.
    fn get_query_cache(&self) -> PyQueryCacheOptions {
        PyQueryCacheOptions {
            inner: self.query_cache.options(),
        }
    }

    /// Counts of hits, misses and so on of the query result cache, and its size.
    fn query_cache_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        stats_dict(py, &self.query_cache.stats())
    }

    /// Removes all results from the query result cache.
    fn clear_query_cache(&self) {
        self.query_cache.clear();
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        host="127.0.0.1".to_string(),
        port=Some(7878),
        flight_port=Some(7879),
        allow_update=false,
        arrow_http=true,
        auth_token=None,
        cors_origins=None,
        graph=None,
        include_graphs=None,
        max_rows=None,
        max_request_bytes=None,
        batch_rows=None,
        result_ttl=None,
        arrow_compression=None,
        static_dir=None,
        static_path="/ui".to_string(),
        static_vars=None,
        query_log_size=1000,
        block=false,
        query_cache=None,
        service_endpoints=None,
        allow_load=false,
        query_timeout=None,
        max_query_timeout=None,
        max_concurrent=None,
        snapshot_url=None,
        snapshot_interval=Some(900.0),
        storage_options=None,
        snapshot_format="model".to_string(),
        refresh_interval=Some(10.0),
    ))]
    fn serve(
        &self,
        py: Python<'_>,
        host: String,
        port: Option<u16>,
        flight_port: Option<u16>,
        allow_update: bool,
        arrow_http: bool,
        auth_token: Option<String>,
        cors_origins: Option<Vec<String>>,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        max_request_bytes: Option<usize>,
        batch_rows: Option<usize>,
        result_ttl: Option<f64>,
        arrow_compression: Option<String>,
        static_dir: Option<PathBuf>,
        static_path: String,
        static_vars: Option<HashMap<String, String>>,
        query_log_size: usize,
        block: bool,
        query_cache: Option<Bound<'_, PyAny>>,
        service_endpoints: Option<Vec<String>>,
        allow_load: bool,
        query_timeout: Option<f64>,
        max_query_timeout: Option<f64>,
        max_concurrent: Option<usize>,
        snapshot_url: Option<String>,
        snapshot_interval: Option<f64>,
        storage_options: Option<HashMap<String, String>>,
        snapshot_format: String,
        refresh_interval: Option<f64>,
    ) -> PyResult<Py<PyServer>> {
        let query_timeout = parse_timeout(query_timeout)?;
        let snapshot = match snapshot_url {
            None => None,
            Some(url) => {
                let interval = match snapshot_interval {
                    None => None,
                    Some(t) if t.is_finite() && t > 0.0 => {
                        Some(std::time::Duration::from_secs_f64(t))
                    }
                    Some(t) => {
                        return Err(PyMaplibError::FunctionArgumentError(format!(
                            "snapshot_interval must be a positive number of seconds or None, not {t}"
                        ))
                        .into())
                    }
                };
                let format = match snapshot_format.as_str() {
                    "model" => SnapshotFormat::Model,
                    "graphs" => SnapshotFormat::Graphs {
                        refresh_interval: match refresh_interval {
                            None => None,
                            Some(t) if t.is_finite() && t > 0.0 => {
                                Some(std::time::Duration::from_secs_f64(t))
                            }
                            Some(t) => {
                                return Err(PyMaplibError::FunctionArgumentError(format!(
                                    "refresh_interval must be a positive number of seconds or None, not {t}"
                                ))
                                .into())
                            }
                        },
                    },
                    other => {
                        return Err(PyMaplibError::FunctionArgumentError(format!(
                            "snapshot_format must be \"model\" or \"graphs\", not {other:?}"
                        ))
                        .into())
                    }
                };
                Some(SnapshotOptions {
                    url,
                    interval,
                    storage_options: storage_options.unwrap_or_default(),
                    format,
                })
            }
        };
        let max_query_timeout = parse_timeout(max_query_timeout)?;
        if max_concurrent == Some(0) {
            return Err(PyMaplibError::FunctionArgumentError(
                "max_concurrent must be at least 1".to_string(),
            )
            .into());
        }
        // None keeps the model's setting, False makes the server not use the cache, True or
        // QueryCacheOptions set the model's cache (shared with Model.query).
        let mut server_cache = true;
        if let Some(qc) = &query_cache {
            if let Ok(false) = qc.extract::<bool>() {
                server_cache = false;
            } else {
                let options = options_from(qc)?;
                if !(qc.extract::<bool>().is_ok() && self.query_cache.enabled()) {
                    self.query_cache.set_options(options);
                }
            }
        }
        let mut options = ServerOptions {
            host,
            sparql_port: port,
            flight_port,
            allow_update,
            arrow_http,
            auth_token,
            graph: parse_optional_named_node(graph)?
                .map(|g| NamedGraph::from_maybe_named_node(Some(&g))),
            include_graphs: parse_include_graphs(include_graphs, true)?,
            max_rows,
            static_dir,
            static_path,
            static_vars: static_vars.unwrap_or_default(),
            query_log_size,
            query_cache: server_cache,
            service_endpoints: service_endpoints.unwrap_or_default(),
            allow_load,
            query_timeout,
            max_query_timeout,
            max_concurrent: max_concurrent.unwrap_or(ServerOptions::default().max_concurrent),
            snapshot,
            ..ServerOptions::default()
        };
        if let Some(cors_origins) = cors_origins {
            options.cors_origins = cors_origins;
        }
        if let Some(max_request_bytes) = max_request_bytes {
            options.max_request_bytes = max_request_bytes;
        }
        if let Some(batch_rows) = batch_rows {
            options.batch_rows = batch_rows;
        }
        if let Some(result_ttl) = result_ttl {
            if !(result_ttl >= 0.0 && result_ttl.is_finite()) {
                return Err(PyMaplibError::FunctionArgumentError(
                    "result_ttl must be a non-negative number of seconds".to_string(),
                )
                .into());
            }
            options.result_ttl = std::time::Duration::from_secs_f64(result_ttl);
        }
        if let Some(c) = arrow_compression {
            options.arrow_compression = ArrowCompression::parse(&c).map_err(server_error)?;
        }
        let model = self.inner.clone();
        let handle = py
            .detach(move || serve_model(model, options))
            .map_err(server_error)?;
        let server = Py::new(py, PyServer::new(handle))?;
        if block {
            server.get().wait(py, None)?;
        }
        Ok(server)
    }
}

/// Copies the triples of a graph of source into a graph of target, without going through Python.
/// Blank nodes of another model get new labels, so they stay distinct from those of target.
#[pyfunction]
#[pyo3(signature = (source, target, source_graph=None, target_graph=None))]
pub fn add_triples(
    py: Python<'_>,
    source: &Bound<'_, PyModel>,
    target: &Bound<'_, PyModel>,
    source_graph: Option<String>,
    target_graph: Option<String>,
) -> PyResult<()> {
    let source_graph = parse_optional_named_node(source_graph)?;
    let target_graph = parse_optional_named_node(target_graph)?;
    let source_graph = NamedGraph::from_maybe_named_node(source_graph.as_ref());
    let target_graph = NamedGraph::from_maybe_named_node(target_graph.as_ref());
    let source = source.get().inner.clone();
    let target = target.get().inner.clone();
    py.detach(move || -> PyResult<()> {
        if Arc::ptr_eq(&source, &target) {
            let mut inner = target.write().unwrap_or_else(|e| e.into_inner());
            inner
                .add_triples(None, &source_graph, &target_graph)
                .map_err(PyMaplibError::from)?;
            return Ok(());
        }
        // Lock the two models in a fixed order, so that copies in opposite directions in two
        // threads do not deadlock.
        let (mut inner, source_inner) = if Arc::as_ptr(&target) < Arc::as_ptr(&source) {
            let inner = target.write().unwrap_or_else(|e| e.into_inner());
            (inner, source.read().unwrap_or_else(|e| e.into_inner()))
        } else {
            let source_inner = source.read().unwrap_or_else(|e| e.into_inner());
            (target.write().unwrap_or_else(|e| e.into_inner()), source_inner)
        };
        inner
            .add_triples(Some(&source_inner.triplestore), &source_graph, &target_graph)
            .map_err(PyMaplibError::from)?;
        Ok(())
    })
}

fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

/// Resolves the `storage` and `storage_folder` arguments of `Model`, raising `ValueError` on an
/// unknown storage mode or a mode that does not fit the folder argument.
fn storage_options_from(
    storage: Option<String>,
    storage_folder: Option<PathBuf>,
) -> PyResult<StorageOptions> {
    let mode = storage
        .map(|s| StorageMode::from_str(&s))
        .transpose()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    StorageOptions::new(mode, storage_folder.map(path_to_string))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Mapping input of a stream, which is consumed when the stream is collected.
fn stream_data(data: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
    if let Some(data) = data {
        Ok(data.clone().unbind())
    } else {
        Err(
            PyMaplibError::FunctionArgumentError("stream=True requires data to map".to_string())
                .into(),
        )
    }
}

/// map_json and map_xml: maps a document, an iterable of documents, or returns a stream over them.
#[allow(clippy::too_many_arguments)]
fn map_documents(
    slf: &Bound<'_, PyModel>,
    py: Python<'_>,
    kind: DocumentKind,
    data: &Bound<'_, PyAny>,
    graph: Option<String>,
    uuid_namespace: Option<String>,
    stream: Option<bool>,
    documents_per_batch: Option<usize>,
    in_place: bool,
    compact_every: Option<usize>,
) -> PyResult<Option<Py<PyAny>>> {
    let this = slf.get();
    parse_optional_named_node(graph.clone())?;
    let documents = classify_document_data(data, kind.name())?;
    if stream.unwrap_or(false) {
        if documents_per_batch == Some(0) {
            return Err(PyMaplibError::FunctionArgumentError(
                "documents_per_batch should be at least 1".to_string(),
            )
            .into());
        }
        let stream = PyModelStream::new(
            slf.clone().unbind(),
            data.clone().unbind(),
            StreamSource::Documents {
                kind,
                graph,
                uuid_namespace,
                documents_per_batch: documents_per_batch.unwrap_or(1),
            },
            in_place,
            compact_every,
        )?;
        return Ok(Some(Py::new(py, stream)?.into_any()));
    }
    match documents {
        DocumentData::Single(doc) => py.detach(|| {
            let mut inner = this.write_inner();
            kind.map_mutex(&mut inner, doc, graph, uuid_namespace)
        })?,
        DocumentData::Many(docs) => for_each_document(&docs, kind.name(), &mut |py, i, doc| {
            py.detach(|| {
                let mut inner = this.write_inner();
                kind.map_mutex(
                    &mut inner,
                    doc,
                    graph.clone(),
                    document_uuid_namespace(&uuid_namespace, i),
                )
            })
        })?,
    }
    Ok(None)
}

fn osipi_auth(
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
) -> PyResult<OSIPIAuth> {
    match (username, password, token) {
        (Some(username), Some(password), None) => Ok(OSIPIAuth::Basic { username, password }),
        (None, None, Some(token)) => Ok(OSIPIAuth::Bearer { token }),
        (None, None, None) => Ok(OSIPIAuth::None),
        _ => Err(PyMaplibError::FunctionArgumentError(
            "Provide either both username and password, or a token, not both".to_string(),
        )
        .into()),
    }
}

fn check_osipi_limits(page_size: usize, max_concurrency: usize) -> PyResult<()> {
    if page_size == 0 || max_concurrency == 0 {
        return Err(PyMaplibError::FunctionArgumentError(
            "page_size and max_concurrency must be positive".to_string(),
        )
        .into());
    }
    Ok(())
}

/// Checks a host (a URL, host:port or host name) with the outbound policy of maplib.outbound,
/// raising its OutboundError.
fn check_outbound_host(py: Python<'_>, host: &str, scheme: &str, port: u16) -> PyResult<()> {
    py.import("maplib.outbound")?
        .call_method1("check_host", (host, scheme, port))?;
    Ok(())
}
