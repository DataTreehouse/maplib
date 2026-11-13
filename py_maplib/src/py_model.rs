use crate::error::PyMaplibError;
use crate::mutexes::{
    add_graph_mutex, add_prefixes_mutex, add_template_mutex, add_udf_mutex,
    add_virtualization_mutex, compact_mutex, create_index_mutex, detach_graph_mutex,
    get_predicate_iris_mutex, get_predicate_mutex, infer_mutex, infer_rdfs_mutex, insert_mutex,
    list_udfs_mutex, map_default_batch_mutex, map_default_first_batch_mutex, map_default_mutex,
    map_df_mutex, map_mutex, map_resolved_mutex, map_triples_mutex, query_external_mutex,
    query_mutex, read_mutex, read_template_mutex, reads_mutex, resolve_template_mutex,
    serialize_triples_mutex, size_mutex, truncate_graph_mutex, update_mutex, validate_mutex,
    write_cim_xml_mutex, write_triples_mutex, writes_mutex,
};
use crate::py_stream::{DocumentKind, PyModelStream, StreamSource};
use crate::shacl::PyValidationReport;
use crate::{
    classify_document_data, classify_mapping_data, create_prefix_map, data_to_mappings_types,
    document_uuid_namespace, for_each_document, for_each_mapping_batch, map_parameters,
    maybe_parse_bindings, new_triples_to_dict, parse_include_graphs, parse_named_node,
    parse_optional_named_node, print_debug_if_exists, query_to_result, resolve_rdf_node_type,
    DocumentData, MappingData, ParametersType, PyIndexingOptions,
};

use datalog::python::PyInferenceResult;
use external_sparql::SparqlMethod;
use maplib::model::Model as InnerModel;
use opc_ua::mappings::map_opc_ua;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::{pyclass, pymethods, Bound, Py, PyAny, PyRef, PyResult, Python};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::df_to_python::df_to_py_df;
use representation::python::PyIRI;
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;
use templates::python::PyTemplate;
use tracing::instrument;
use triplestore::sparql::InsertResult;
use triplestore::{StorageMode, StorageOptions};
use virtualization::python::VirtualizedPythonDatabase;

#[pyclass(name = "Model", frozen)]
pub struct PyModel {
    pub inner: Mutex<InnerModel>,
}

impl PyModel {
    pub fn from_inner_mapping(inner: InnerModel) -> PyModel {
        PyModel {
            inner: Mutex::new(inner),
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
        let storage = storage_options(storage, storage_folder)?;
        let indexing = if let Some(indexing_options) = indexing_options {
            Some(indexing_options.inner)
        } else {
            None
        };
        Ok(PyModel {
            inner: Mutex::new(
                InnerModel::new(None, storage, indexing, None).map_err(PyMaplibError::from)?,
            ),
        })
    }

    #[instrument(skip_all)]
    fn add_template(&self, py: Python<'_>, template: Bound<'_, PyAny>) -> PyResult<()> {
        let template = template.try_into()?;
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            add_template_mutex(&mut inner, template)
        })
    }

    #[instrument(skip_all)]
    fn add_prefixes(&self, py: Python<'_>, prefixes: Bound<'_, PyAny>) -> PyResult<()> {
        let use_prefixes = create_prefix_map(Some(prefixes))?.unwrap();
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            add_prefixes_mutex(&mut inner, use_prefixes)
        })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn truncate_graph(&self, py: Python<'_>, graph: Option<String>) -> PyResult<()> {
        let graph = parse_optional_named_node(graph)?;
        let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
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
            let mut inner = self.inner.lock().unwrap();
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
                let mut inner = this.inner.lock().unwrap();
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
                let mut inner = this.inner.lock().unwrap();
                map_mutex(&mut inner, template, None, graph, None, validate_iris)
            }),
            Some(MappingData::Eager(mappings, types)) => py.detach(move || {
                let mut inner = this.inner.lock().unwrap();
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
                    let mut inner = this.inner.lock().unwrap();
                    resolve_template_mutex(&mut inner, template)
                })?;
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.inner.lock().unwrap();
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
            let mut inner = self.inner.lock().unwrap();
            size_mutex(&mut inner, graph)
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
                let mut inner = this.inner.lock().unwrap();
                map_triples_mutex(&mut inner, mappings, predicate, graph, types, validate_iris)
            }),
            MappingData::Batched(data) => {
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    if mappings.height() == 0 {
                        return Ok(());
                    }
                    py.detach(|| {
                        let mut inner = this.inner.lock().unwrap();
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

    #[pyo3(signature = (data, primary_key_column, graph=None, validate_iris=None, stream=None))]
    #[instrument(skip_all)]
    fn map_default(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        primary_key_column: String,
        graph: Option<String>,
        validate_iris: Option<bool>,
        stream: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.get();
        let graph = parse_optional_named_node(graph)?;
        let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        if stream.unwrap_or(false) {
            let stream = PyModelStream::new(
                slf.clone().unbind(),
                data.clone().unbind(),
                StreamSource::MapDefault {
                    primary_key_column,
                    graph: named_graph,
                    validate_iris,
                },
            );
            return Ok(Py::new(py, stream)?.into_any());
        }
        let template = match classify_mapping_data(data, py)? {
            MappingData::Eager(mappings, types) => py.detach(move || -> PyResult<String> {
                let mut inner = this.inner.lock().unwrap();
                map_default_mutex(
                    &mut inner,
                    mappings,
                    primary_key_column,
                    named_graph,
                    types,
                    validate_iris,
                )
            }),
            MappingData::Batched(data) => {
                let mut template = None;
                for_each_mapping_batch(&data, py, &mut |py, mappings, types| {
                    py.detach(|| -> PyResult<()> {
                        let mut inner = this.inner.lock().unwrap();
                        if let Some(template) = &template {
                            map_default_batch_mutex(
                                &mut inner,
                                template,
                                mappings,
                                &primary_key_column,
                                named_graph.clone(),
                                types,
                                validate_iris,
                            )
                        } else {
                            template = Some(map_default_first_batch_mutex(
                                &mut inner,
                                mappings,
                                &primary_key_column,
                                named_graph.clone(),
                                types,
                                validate_iris,
                            )?);
                            Ok(())
                        }
                    })
                })?;
                if let Some(template) = template {
                    Ok(format!("{template}"))
                } else {
                    Err(PyMaplibError::FunctionArgumentError(
                        "map_default received no batches, so no template could be created"
                            .to_string(),
                    )
                    .into())
                }
            }
        }?;
        Ok(PyString::new(py, &template).into_any().unbind())
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
            let mut inner = self.inner.lock().unwrap();
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
            let mut inner = self.inner.lock().unwrap();
            map_opc_ua(&mut inner, &folder, &graph);
        });

        Ok(())
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
            let mut inner = self.inner.lock().unwrap();
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
    ) -> PyResult<Py<PyAny>> {
        let mapped_parameters = map_parameters(parameters, py)?;
        let graph = parse_optional_named_node(graph)?;
        let graph = graph.map(|graph| NamedGraph::from_maybe_named_node(Some(&graph)));
        let parsed_bindings = maybe_parse_bindings(bindings, py)?;
        let (res, cats, prefixes) = py.detach(|| -> PyResult<(_, LockedCats, _)> {
            let mut inner = self.inner.lock().unwrap();
            let cats = inner.triplestore.global_cats.clone();
            let res = query_mutex(
                &mut inner,
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
            let mut inner = self.inner.lock().unwrap();
            let cats = inner.triplestore.global_cats.clone();
            let res = query_external_mutex(
                &mut inner,
                query,
                endpoint,
                method,
                parsed_bindings,
                headers,
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
            let mut inner = self.inner.lock().unwrap();
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
            let mut inner = self.inner.lock().unwrap();
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
        max_shape_constraint_results=None,
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
        max_shape_constraint_results: Option<usize>,
        only_shapes: Option<Vec<String>>,
        deactivate_shapes: Option<Vec<String>>,
        dry_run: Option<bool>,
        max_rows: Option<usize>,
        serial: Option<bool>,
        max_iterations: Option<usize>,
        debug_rules: Option<bool>,
    ) -> PyResult<PyValidationReport> {
        let res = py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            validate_mutex(
                &mut inner,
                shape_graph,
                data_graph,
                report_graph,
                inferences_graph,
                include_details,
                include_conforms,
                max_shape_constraint_results,
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
            let mut inner = self.inner.lock().unwrap();

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
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            read_mutex(
                &mut inner,
                file_path,
                format,
                base_iri,
                parallel,
                checked,
                graph,
                replace_graph,
                triples_batch_size,
                known_contexts.unwrap_or_default(),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path))]
    #[instrument(skip_all)]
    fn read_template(&self, py: Python<'_>, file_path: &Bound<'_, PyAny>) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            read_template_mutex(&mut inner, file_path)
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
    ) -> PyResult<()> {
        py.detach(|| {
            let mut inner = self.inner.lock().unwrap();
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
            )
        })
    }

    #[instrument(skip_all)]
    fn get_templates(&self, py: Python<'_>) -> PyResult<Vec<PyTemplate>> {
        py.detach(|| {
            let inner = self.inner.lock().unwrap();
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
            let mut inner = self.inner.lock().unwrap();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            inner
                .templates_to_graph(&named_graph)
                .map_err(PyMaplibError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (file_path, format=None, graph=None, prefixes=None))]
    #[instrument(skip_all)]
    fn write(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        graph: Option<String>,
        prefixes: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let prefixes = create_prefix_map(prefixes)?;

        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            write_triples_mutex(&mut inner, file_path, format, graph, prefixes)
        })
    }

    #[pyo3(signature = (
        file_path, profile_graph, model_iri=None, version=None, description=None, created=None,
        scenario_time=None, modeling_authority_set=None, prefixes=None, graph=None))]
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
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let use_prefixes = create_prefix_map(prefixes)?.unwrap_or(Default::default());

        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            write_cim_xml_mutex(
                &mut inner,
                file_path,
                profile_graph,
                model_iri,
                version,
                description,
                created,
                scenario_time,
                modeling_authority_set,
                use_prefixes,
                graph,
            )
        })
    }

    #[pyo3(signature = (format=None, graph=None, prefixes=None))]
    #[instrument(skip_all)]
    fn writes(
        &self,
        py: Python<'_>,
        format: Option<String>,
        graph: Option<String>,
        prefixes: Option<Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let use_prefixes = create_prefix_map(prefixes)?;

        py.detach(|| {
            let mut inner = self.inner.lock().unwrap();
            writes_mutex(&mut inner, format, graph, use_prefixes)
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
            let mut inner = self.inner.lock().unwrap();
            get_predicate_iris_mutex(&mut inner, graph, include_graphs)
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
            let mut inner = self.inner.lock().unwrap();
            get_predicate_mutex(&mut inner, iri, graph, include_graphs)
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
        max_results=10_000_000,
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
        max_results: Option<usize>,
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
            let mut inner = self.inner.lock().unwrap();

            let cats = inner.triplestore.global_cats.clone();

            let res = infer_mutex(
                &mut inner,
                rulesets,
                max_iterations,
                max_results,
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
            let mut inner = self.inner.lock().unwrap();

            let res = infer_rdfs_mutex(&mut inner, &named_graph)?;

            Ok(res)
        })?;
        Ok(res)
    }

    #[pyo3(signature = ())]
    fn compact(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            compact_mutex(&mut inner)
        })
    }

    #[pyo3(signature = (path = PathBuf::from("./serialized_triples"), raw_db = false))]
    fn serialize(&self, py: Python<'_>, path: PathBuf, raw_db: bool) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            serialize_triples_mutex(&mut inner, path.as_ref(), raw_db)
        })
    }

    #[instrument(skip_all)]
    #[staticmethod]
    #[pyo3(signature = (path = PathBuf::from("./serialized_triples"), storage_folder=None, storage=None))]
    fn deserialize(
        py: Python<'_>,
        path: PathBuf,
        storage_folder: Option<PathBuf>,
        storage: Option<String>,
    ) -> PyResult<Self> {
        let storage = storage_options(storage, storage_folder)?;

        let inner: Result<InnerModel, PyMaplibError> = py.detach(move || {
            let inner =
                InnerModel::deserialize_triples(&path, storage).map_err(PyMaplibError::from)?;
            Ok(inner)
        });
        Ok(Self {
            inner: Mutex::new(inner?),
        })
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
            let mut inner = self.inner.lock().unwrap();
            add_udf_mutex(&mut inner, iri, func, output, inputs)?;
            Ok(())
        })?;
        Ok(())
    }

    fn list_udfs(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let udfs = py.detach(|| -> PyResult<Vec<String>> {
            let mut inner = self.inner.lock().unwrap();
            let udfs = list_udfs_mutex(&mut inner)?;
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
        let other_borrow = other.borrow();
        let other_inner = other_borrow.inner.lock().unwrap();
        let borrow_other_triplestore = &other_inner.triplestore;
        let _ = py.detach(move || -> PyResult<()> {
            let mut inner = self.inner.lock().unwrap();
            add_graph_mutex(
                &mut inner,
                borrow_other_triplestore,
                source_graph,
                target_graph,
            )?;
            Ok(())
        })?;
        Ok(())
    }
}

fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

/// Resolves the `storage` and `storage_folder` arguments of `Model`, raising `ValueError` on an
/// unknown storage mode or a mode that does not fit the folder argument.
fn storage_options(
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
            let mut inner = this.inner.lock().unwrap();
            kind.map_mutex(&mut inner, doc, graph, uuid_namespace)
        })?,
        DocumentData::Many(docs) => for_each_document(&docs, kind.name(), &mut |py, i, doc| {
            py.detach(|| {
                let mut inner = this.inner.lock().unwrap();
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
