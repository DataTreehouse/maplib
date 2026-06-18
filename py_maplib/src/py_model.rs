use crate::error::PyMaplibError;
use crate::mutexes::{
    add_prefixes_mutex, add_template_mutex, add_virtualization_mutex, create_index_mutex,
    detach_graph_mutex, get_predicate_iris_mutex, get_predicate_mutex, infer_mutex, insert_mutex,
    map_default_mutex, map_df_mutex, map_json_mutex, map_mutex, map_triples_mutex, map_xml_mutex,
    query_mutex, read_mutex, read_template_mutex, reads_mutex, size_mutex, truncate_graph_mutex,
    update_mutex, validate_mutex, write_cim_xml_mutex, write_native_parquet_mutex,
    write_triples_mutex, writes_mutex,
};
use crate::shacl::{PyValidationReport, SHACL_RESULTS_QUERY};
use crate::{
    create_prefix_map, data_to_mappings_types, map_parameters, new_triples_to_dict,
    parse_named_node, parse_optional_named_node, print_debug_if_exists, query_to_result,
    ParametersType, PyIndexingOptions, StringOrPathBuf, DEFAULT_INCLUDE_TRANSIENT,
};

use datalog::python::PyInferenceResult;
use maplib::model::Model as InnerModel;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, Bound, Py, PyAny, PyRef, PyResult, Python};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::df_to_python::df_to_py_df;
use representation::python::PyIRI;
use representation::solution_mapping::EagerSolutionMappings;
use std::collections::HashMap;
use std::sync::Mutex;
use templates::python::PyTemplate;
use tracing::instrument;
use triplestore::sparql::InsertResult;
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
    #[pyo3(signature = (indexing_options=None, storage_folder=None))]
    #[instrument(skip_all)]
    fn new(
        indexing_options: Option<PyIndexingOptions>,
        storage_folder: Option<String>,
    ) -> PyResult<PyModel> {
        let indexing = if let Some(indexing_options) = indexing_options {
            Some(indexing_options.inner)
        } else {
            None
        };
        Ok(PyModel {
            inner: Mutex::new(
                InnerModel::new(None, storage_folder, indexing, None)
                    .map_err(PyMaplibError::from)?,
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

    #[instrument(skip_all)]
    fn truncate_graph(&self, py: Python<'_>, graph: Option<String>) -> PyResult<()> {
        let graph = parse_optional_named_node(graph)?;
        let graph = if let Some(graph) = graph {
            NamedGraph::NamedGraph(graph)
        } else {
            NamedGraph::DefaultGraph
        };
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
        let graph = if let Some(graph) = graph {
            NamedGraph::NamedGraph(graph)
        } else {
            NamedGraph::DefaultGraph
        };
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            detach_graph_mutex(&mut inner, &graph, preserve_name.unwrap_or(false))
        })
    }

    #[pyo3(signature = (template, data=None, graph=None, validate_iris=None))]
    #[instrument(skip_all)]
    fn map(
        &self,
        py: Python<'_>,
        template: Bound<'_, PyAny>,
        data: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let mtypes = data.map(|x| data_to_mappings_types(x, py)).transpose()?;
        let (mappings, types) = mtypes.unzip();
        let template = template.try_into()?;
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            map_mutex(
                &mut inner,
                template,
                mappings,
                graph,
                types.flatten(),
                validate_iris,
            )
        })
    }

    #[pyo3(signature = (graph=None))]
    #[instrument(skip_all)]
    fn size(&self, py: Python<'_>, graph: Option<String>) -> PyResult<usize> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            size_mutex(&mut inner, graph)
        })
    }

    #[pyo3(signature = (path_or_string, graph=None, transient=None))]
    #[instrument(skip_all)]
    fn map_json(
        &self,
        py: Python<'_>,
        path_or_string: StringOrPathBuf,
        graph: Option<String>,
        transient: Option<bool>,
    ) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            map_json_mutex(&mut inner, path_or_string, graph, transient)
        })
    }

    #[pyo3(signature = (path_or_string, graph=None, transient=None))]
    #[instrument(skip_all)]
    fn map_xml(
        &self,
        py: Python<'_>,
        path_or_string: StringOrPathBuf,
        graph: Option<String>,
        transient: Option<bool>,
    ) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            map_xml_mutex(&mut inner, path_or_string, graph, transient)
        })
    }

    #[pyo3(signature = (data, predicate=None, graph=None, validate_iris=None))]
    #[instrument(skip_all)]
    fn map_triples(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        predicate: Option<Bound<'_, PyAny>>,
        graph: Option<String>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<Py<PyAny>>> {
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
        let (mappings, types) = data_to_mappings_types(data, py)?;
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            map_triples_mutex(&mut inner, mappings, predicate, graph, types, validate_iris)
        })
    }

    #[pyo3(signature = (data, primary_key_column, dry_run=None, graph=None, validate_iris=None))]
    #[instrument(skip_all)]
    fn map_default(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        primary_key_column: String,
        dry_run: Option<bool>,
        graph: Option<String>,
        validate_iris: Option<bool>,
    ) -> PyResult<String> {
        let (mappings, types) = data_to_mappings_types(data, py)?;
        py.detach(move || -> PyResult<String> {
            let mut inner = self.inner.lock().unwrap();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            map_default_mutex(
                &mut inner,
                mappings,
                primary_key_column,
                dry_run,
                named_graph,
                types,
                validate_iris,
            )
        })
    }

    #[pyo3(signature = (df, graph=None))]
    #[instrument(skip_all)]
    fn map_df(&self, py: Python<'_>, df: &Bound<'_, PyAny>, graph: Option<String>) -> PyResult<()> {
        let (df, _) = data_to_mappings_types(df, py)?;
        py.detach(move || -> PyResult<()> {
            let mut inner = self.inner.lock().unwrap();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            map_df_mutex(&mut inner, df, named_graph)
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

    /// :param host: The hostname that we will point the browser to.
    /// :param port: The port where the graph explorer webserver listens on.
    /// :param bind: Bind to the following host / ip.
    /// :param popup: Pop up the browser window.
    /// :param fts: Enable full text search indexing
    /// :param fts_path: Path to the fts index.
    /// :param graph: The named graph to query, None is the default graph.
    /// :param new: Use new graph explorer?
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

    #[pyo3(signature = (*args, **kwargs), text_signature = "(/)")]
    fn map_opc_ua(
        self_: PyRef<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, pyo3::types::PyTuple>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let module = py.import("maplib")?;
        let func = module.getattr("_map_opc_ua")?;

        let mut old_args: Vec<_> = args.iter().collect();
        let mut new_args = Vec::new();
        new_args.push(self_.into_pyobject(py)?.into_any());
        new_args.append(&mut old_args);

        let new_args = pyo3::types::PyTuple::new(py, new_args)?;

        let res = func.call(new_args, kwargs)?;
        Ok(res.into())
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
        streaming=None,
        return_json=None,
        include_transient=None,
        max_rows=None,
        debug=None))]
    #[instrument(skip_all)]
    fn query(
        &self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        solution_mappings: Option<bool>,
        graph: Option<String>,
        streaming: Option<bool>,
        return_json: Option<bool>,
        include_transient: Option<bool>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let mapped_parameters = map_parameters(parameters, py)?;
        let graph = parse_optional_named_node(graph)?;
        let graph = if let Some(graph) = graph {
            Some(NamedGraph::NamedGraph(graph))
        } else {
            None
        };
        let (res, cats) = py.detach(|| -> PyResult<(_, LockedCats)> {
            let mut inner = self.inner.lock().unwrap();
            let cats = inner.triplestore.global_cats.clone();
            let res = query_mutex(
                &mut inner,
                query,
                mapped_parameters,
                graph,
                streaming,
                include_transient,
                max_rows,
                debug,
            )?;
            Ok((res, cats))
        })?;
        print_debug_if_exists(res.debug.as_ref());
        query_to_result(
            res,
            solution_mappings.unwrap_or(false),
            solution_mappings.unwrap_or(false),
            return_json.unwrap_or(false),
            cats,
            py,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
            update,
            parameters=None,
            graph=None,
            streaming=None,
            include_transient=None,
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
        streaming: Option<bool>,
        include_transient: Option<bool>,
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
                streaming,
                include_transient,
                max_rows,
                debug,
            )
        })?;
        print_debug_if_exists(res.debug.as_ref());
        Ok(())
    }

    #[pyo3(signature = (options=None, all=None, graph=None))]
    #[instrument(skip_all)]
    fn create_index(
        &self,
        py: Python<'_>,
        options: Option<PyIndexingOptions>,
        all: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            create_index_mutex(&mut inner, options, all, graph)
        })
    }

    #[pyo3(signature = (
        shape_graph=None,
        data_graph=None,
        report_graph=None,
        inferences_graph=None,
        include_details=None,
        include_conforms=None,
        streaming=None,
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
        streaming: Option<bool>,
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
                streaming,
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

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (graph=None, streaming=None))]
    #[instrument(skip_all)]
    fn shacl_report(
        &self,
        py: Python<'_>,
        graph: Option<String>,
        streaming: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let graph = if let Some(graph) = graph {
            NamedGraph::NamedGraph(parse_named_node(graph)?)
        } else {
            let inner = self.inner.lock().unwrap();
            if let Some(latest_report_graph) = &inner.latest_report_graph {
                latest_report_graph.clone()
            } else {
                return Err(PyMaplibError::FunctionArgumentError(
                    "Either run the validation first or supply a graph".to_string(),
                )
                .into());
            }
        };

        let (res, cats) = py.detach(|| -> PyResult<(_, LockedCats)> {
            let mut inner = self.inner.lock().unwrap();
            let cats = inner.triplestore.global_cats.clone();
            let res = query_mutex(
                &mut inner,
                SHACL_RESULTS_QUERY.to_string(),
                None,
                Some(graph),
                streaming,
                Some(false),
                None,
                None,
            )?;
            Ok((res, cats))
        })?;
        print_debug_if_exists(res.debug.as_ref());
        query_to_result(res, false, false, false, cats, py)
    }

    #[pyo3(signature = (
            query,
            parameters=None,
            solution_mappings=None,
            transient=None,
            streaming=None,
            source_graph=None,
            target_graph=None,
            include_transient=None,
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
        transient: Option<bool>,
        streaming: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        include_transient: Option<bool>,
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
                transient,
                streaming,
                source_graph,
                target_graph,
                include_transient,
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
        transient=None,
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
        transient: Option<bool>,
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
                transient,
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
        transient=None,
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
        transient: Option<bool>,
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
                transient,
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

    #[pyo3(signature = (folder_path, graph=None))]
    #[instrument(skip_all)]
    fn write_native_parquet(
        &self,
        py: Python<'_>,
        folder_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let folder_path = folder_path.str()?.to_string();
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            write_native_parquet_mutex(&mut inner, folder_path, graph)
        })
    }

    #[pyo3(signature = (graph=None, include_transient=None))]
    #[instrument(skip_all)]
    fn get_predicate_iris(
        &self,
        py: Python<'_>,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<PyIRI>> {
        py.detach(move || {
            let mut inner = self.inner.lock().unwrap();
            get_predicate_iris_mutex(&mut inner, graph, include_transient)
        })
    }

    #[pyo3(signature = (iri, graph=None, include_transient=None))]
    #[instrument(skip_all)]
    fn get_predicate(
        &self,
        py: Python<'_>,
        iri: PyIRI,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let include_transient = include_transient.unwrap_or(false);
        let eager_sms = py.detach(|| {
            let mut inner = self.inner.lock().unwrap();
            get_predicate_mutex(&mut inner, iri, graph, include_transient)
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
        include_transient=None,
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
        include_transient: Option<bool>,
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
        let named_graph = if let Some(graph) = graph {
            Some(NamedGraph::NamedGraph(graph))
        } else {
            None
        };
        let (res, ..) = py.detach(|| -> PyResult<(_, LockedCats)> {
            let mut inner = self.inner.lock().unwrap();

            let cats = inner.triplestore.global_cats.clone();

            let res = infer_mutex(
                &mut inner,
                rulesets,
                max_iterations,
                max_results,
                named_graph.as_ref(),
                include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
                max_rows,
                debug,
            )?;

            Ok((res, cats))
        })?;
        Ok(PyInferenceResult { inner: res })
    }
}
