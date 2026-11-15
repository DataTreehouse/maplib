use file_io::remote::is_remote;
use crate::error::PyMaplibError;
use crate::mutexes::{
    add_graph_mutex, add_prefixes_mutex, add_template_mutex, add_udf_mutex,
    add_virtualization_mutex, compact_mutex, create_index_mutex, detach_graph_mutex,
    generate_template_mutex, get_predicate_iris_mutex, get_predicate_mutex, infer_mutex,
    infer_rdfs_mutex, insert_mutex, list_udfs_mutex, map_df_mutex, map_mutex, map_resolved_mutex,
    map_triples_mutex, query_external_mutex, query_mutex, read_mutex, read_template_mutex,
    reads_mutex, resolve_template_mutex, serialize_triples_mutex, size_mutex, truncate_graph_mutex,
    update_mutex, validate_mutex, write_bytes_mutex, write_cim_xml_mutex, write_triples_mutex,
    writes_mutex, GraphArg,
};
use crate::py_server::{server_error, PyServer};
use crate::py_io::{ReadSource, WriteTarget};
use crate::py_stream::{DocumentKind, PyModelStream, StreamSource};
use crate::shacl::PyValidationReport;
use crate::{
    classify_document_data, classify_mapping_data, create_prefix_map, data_to_mappings_types,
    document_uuid_namespace, for_each_document, for_each_mapping_batch, map_parameters,
    maybe_parse_bindings, new_triples_to_dict, parse_include_graphs, parse_named_node,
    parse_optional_named_node, print_debug_if_exists, query_to_result, resolve_rdf_node_type,
    DocumentData, MappingData, ParametersType, PyIndexingOptions,
};
use server::{serve as serve_model, ArrowCompression, ServerOptions};

use datalog::python::PyInferenceResult;
use external_sparql::maplib_endpoint::{query_maplib_endpoint, MaplibEndpointQuery};
use external_sparql::SparqlMethod;
use maplib::model::Model as InnerModel;
use cdf::map_cdf;
use opc_ua::mappings::map_opc_ua;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use pyo3::{pyclass, pymethods, Bound, Py, PyAny, PyRef, PyResult, Python};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::df_to_python::df_to_py_df;
use representation::python::PyIRI;
use representation::result::QueryResult;
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use templates::python::PyTemplate;
use tracing::instrument;
use triplestore::sparql::InsertResult;
use triplestore::{StorageMode, StorageOptions};
use virtualization::osipi::{OSIPIAuth, OSIPISettings, OSIPI_NAMESPACE};
use virtualization::python::VirtualizedPythonDatabase;

#[pyclass(name = "Model", frozen)]
pub struct PyModel {
    // Shared with the endpoints started by serve().
    pub inner: Arc<RwLock<InnerModel>>,
}

impl PyModel {
    pub fn from_inner_mapping(inner: InnerModel) -> PyModel {
        PyModel {
            inner: Arc::new(RwLock::new(inner)),
        }
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            add_template_mutex(&mut inner, template)
        })
    }

    #[instrument(skip_all)]
    fn add_prefixes(&self, py: Python<'_>, prefixes: Bound<'_, PyAny>) -> PyResult<()> {
        let use_prefixes = create_prefix_map(Some(prefixes))?.unwrap();
        py.detach(move || {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            add_prefixes_mutex(&mut inner, use_prefixes)
        })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn truncate_graph(&self, py: Python<'_>, graph: Option<String>) -> PyResult<()> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(move || {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            truncate_graph_mutex(&mut inner, &graph)
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            detach_graph_mutex(&mut inner, &graph, preserve_name.unwrap_or(false))
        })
    }

    #[pyo3(signature = (template, data=None, graph=None, validate_iris=None, stream=None))]
    #[instrument(skip_all)]
    fn map(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        template: Bound<'_, PyAny>,
        data: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let this = slf.get();
        let template = template.try_into()?;
        if stream.unwrap_or(false) {
            let data = stream_data(data)?;
            parse_optional_named_node(graph.clone())?;
            let template_iri = py.detach(|| {
                let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
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
            );
            return Ok(Some(Py::new(py, stream)?.into_any()));
        }
        let data = data.map(|x| classify_mapping_data(x, py)).transpose()?;
        match data {
            None => py.detach(move || {
                let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
                map_mutex(&mut inner, template, None, graph, None, validate_iris)
            }),
            Some(MappingData::Eager(mappings, types)) => py.detach(move || {
                let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
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
                    let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
                    resolve_template_mutex(&mut inner, template)
                })?;
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
            size_mutex(&inner, graph)
        })
    }

    #[pyo3(signature = (path_or_string, graph=None, uuid_namespace=None, stream=None, documents_per_batch=None))]
    #[instrument(skip_all)]
    fn map_json(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        path_or_string: &Bound<'_, PyAny>,
        graph: Option<String>,
        uuid_namespace: Option<String>,
        stream: Option<bool>,
        documents_per_batch: Option<usize>,
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
        )
    }

    #[pyo3(signature = (path_or_string, graph=None, uuid_namespace=None, stream=None, documents_per_batch=None))]
    #[instrument(skip_all)]
    fn map_xml(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        path_or_string: &Bound<'_, PyAny>,
        graph: Option<String>,
        uuid_namespace: Option<String>,
        stream: Option<bool>,
        documents_per_batch: Option<usize>,
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
        )
    }

    #[pyo3(signature = (data, predicate=None, graph=None, validate_iris=None, stream=None))]
    #[instrument(skip_all)]
    fn map_triples(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        predicate: Option<Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
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
            );
            return Ok(Some(Py::new(py, stream)?.into_any()));
        }
        match classify_mapping_data(data, py)? {
            MappingData::Eager(mappings, types) => py.detach(move || {
                let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
                map_triples_mutex(&mut inner, mappings, predicate, graph, types, validate_iris)
            }),
            MappingData::Batched(data) => {
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            map_opc_ua(&mut inner, &folder, &graph);
        });

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
        // The mapping adds triples through this Model, so the inner model must not be locked here.
        map_cdf(slf.as_any(), client, &options)
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
        let auth = match (username, password, token) {
            (Some(username), Some(password), None) => OSIPIAuth::Basic { username, password },
            (None, None, Some(token)) => OSIPIAuth::Bearer { token },
            (None, None, None) => OSIPIAuth::None,
            _ => {
                return Err(PyMaplibError::FunctionArgumentError(
                    "Provide either both username and password, or a token, not both".to_string(),
                )
                .into())
            }
        };
        if page_size == 0 || max_concurrency == 0 {
            return Err(PyMaplibError::FunctionArgumentError(
                "page_size and max_concurrency must be positive".to_string(),
            )
            .into());
        }
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
            let mut inner = self.inner.write().unwrap();
            inner
                .map_osipi(settings, &graph)
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
    ) -> PyResult<Py<PyAny>> {
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
            };
            let (kind, cats, prefixes) = py.detach(|| -> PyResult<_> {
                let kind = query_maplib_endpoint(&endpoint, &q)
                    .map_err(|e| PyMaplibError::RuntimeError(e.to_string()))?;
                // The results have no dictionary encoded columns, the model's dictionaries
                // are only used for formatting, and the model's prefixes for Turtle.
                let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
        let mapped_parameters = map_parameters(parameters, py)?;
        let graph = parse_optional_named_node(graph)?;
        let graph = graph.map(|graph| NamedGraph::from_maybe_named_node(Some(&graph)));
        let parsed_bindings = maybe_parse_bindings(bindings, py)?;
        let (res, cats, prefixes) = py.detach(|| -> PyResult<(_, LockedCats, _)> {
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
            let cats = inner.triplestore.global_cats.clone();
            let res = query_mutex(
                &inner,
                query,
                mapped_parameters,
                graph,
                include_graphs,
                max_rows,
                debug,
                parsed_bindings,
            )?;
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
    ) -> PyResult<()> {
        let mapped_parameters = map_parameters(parameters, py)?;
        let res = py.detach(|| {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            update_mutex(
                &mut inner,
                update,
                mapped_parameters,
                graph,
                include_graphs,
                max_rows,
                debug,
            )
        })?;
        print_debug_if_exists(res.debug.as_ref());
        Ok(())
    }

    #[pyo3(signature = (options=None))]
    #[instrument(skip_all)]
    fn create_index(&self, py: Python<'_>, options: Option<PyIndexingOptions>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());

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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());

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

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn infer_rdfs(&self, py: Python<'_>, graph: Option<String>) -> PyResult<usize> {
        let graph = parse_optional_named_node(graph)?;
        let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        let res = py.detach(|| -> PyResult<usize> {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());

            let res = infer_rdfs_mutex(&mut inner, &named_graph)?;

            Ok(res)
        })?;
        Ok(res)
    }

    #[pyo3(signature = ())]
    fn compact(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
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
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            add_udf_mutex(&mut inner, iri, func, output, inputs)?;
            Ok(())
        })?;
        Ok(())
    }

    fn list_udfs(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let udfs = py.detach(|| -> PyResult<Vec<String>> {
            let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
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
                let inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
                (inner, other.inner.read().unwrap_or_else(|e| e.into_inner()))
            } else {
                let other_inner = other.inner.read().unwrap_or_else(|e| e.into_inner());
                (self.inner.write().unwrap_or_else(|e| e.into_inner()), other_inner)
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
    ) -> PyResult<Py<PyServer>> {
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
        );
        return Ok(Some(Py::new(py, stream)?.into_any()));
    }
    match documents {
        DocumentData::Single(doc) => py.detach(|| {
            let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
            kind.map_mutex(&mut inner, doc, graph, uuid_namespace)
        })?,
        DocumentData::Many(docs) => for_each_document(&docs, kind.name(), &mut |py, i, doc| {
            py.detach(|| {
                let mut inner = this.inner.write().unwrap_or_else(|e| e.into_inner());
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
