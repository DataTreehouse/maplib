extern crate core;
mod error;
mod shacl;
use crate::error::*;
use polars::frame::DataFrame;
use pydf_io::to_rust::polars_df_to_rust_df;

use tracing::{info, instrument, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{filter, prelude::*};

use crate::shacl::PyValidationReport;
use maplib::errors::MaplibError;
use maplib::model::{MapOptions, Model as InnerModel};

use chrono::Utc;
use cimxml::export::FullModelDetails;
use pydf_io::to_python::{df_to_py_df, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use triplestore::sparql::{
    QueryResult, QueryResultKind as SparqlQueryResult, QueryResultKind, QuerySettings, UpdateResult,
};

//The below snippet controlling alloc-library is from https://github.com/pola-rs/polars/blob/main/py-polars/src/lib.rs
//And has a MIT license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deapub pub l
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#[cfg(target_os = "linux")]
use tikv_jemallocator::Jemalloc;

use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::{col, lit, IntoLazy};
use pyo3::types::{PyList, PyString};
use pyo3::IntoPyObjectExt;
use representation::python::{
    PyBlankNode, PyIRI, PyLiteral, PyPrefix, PyRDFType, PySolutionMappings, PyVariable,
};
use representation::solution_mapping::EagerSolutionMappings;

use datalog::inference::InferenceResult;
use datalog::python::PyInferenceResult;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::debug::DebugOutputs;
use representation::formatting::format_native_columns;
use representation::polars_to_rdf::XSD_DATETIME_WITH_TZ_FORMAT;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use templates::python::owl::PyOWL;
use templates::python::rdf::PyRDF;
use templates::python::rdfs::PyRDFS;
use templates::python::xsd::PyXSD;
use templates::python::{py_triple, PyArgument, PyInstance, PyParameter, PyTemplate};
use templates::MappingColumnType;
use triplestore::{IndexingOptions, NewTriples, Triplestore};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DEFAULT_STREAMING: bool = false;
const DEFAULT_INCLUDE_TRANSIENT: bool = true;
const DEFAULT_DEBUG_NO_RESULTS: bool = false;

#[pyclass(name = "Model", frozen)]
pub struct PyModel {
    inner: Mutex<InnerModel>,
}

impl PyModel {
    pub fn from_inner_mapping(inner: InnerModel) -> PyModel {
        PyModel {
            inner: Mutex::new(inner),
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "IndexingOptions")]
pub struct PyIndexingOptions {
    inner: IndexingOptions,
}

#[pymethods]
impl PyIndexingOptions {
    #[new]
    #[instrument(skip_all)]
    #[pyo3(signature = (object_sort_all=None, object_sort_some=None, fts_path=None, subject_object_index=None))]
    pub fn new(
        object_sort_all: Option<bool>,
        object_sort_some: Option<Vec<PyIRI>>,
        fts_path: Option<String>,
        subject_object_index: Option<bool>,
    ) -> PyIndexingOptions {
        let fts_path = fts_path.map(|fts_path| Path::new(&fts_path).to_owned());
        let subject_object_index = subject_object_index.unwrap_or(false);
        let inner = if object_sort_all.is_none() && object_sort_some.is_none() {
            let mut opts = IndexingOptions::new_default_object_sort(fts_path, subject_object_index);
            opts
        } else {
            let object_sort_all = object_sort_all.unwrap_or(false);
            if object_sort_all && object_sort_some.is_none() {
                IndexingOptions::default()
            } else {
                let object_sort_some: Option<HashSet<_>> =
                    object_sort_some.map(|object_sort_some| {
                        object_sort_some
                            .into_iter()
                            .map(|x| x.into_inner())
                            .collect()
                    });
                IndexingOptions::new(
                    object_sort_all,
                    object_sort_some,
                    fts_path,
                    subject_object_index,
                )
            }
        };
        PyIndexingOptions { inner }
    }
}

type ParametersType<'a> = HashMap<String, (Bound<'a, PyAny>, HashMap<String, PyRDFType>)>;

#[pymethods]
impl PyModel {
    #[new]
    #[pyo3(signature = (indexing_options=None))]
    #[instrument(skip_all)]
    fn new(indexing_options: Option<PyIndexingOptions>) -> PyResult<PyModel> {
        let indexing = if let Some(indexing_options) = indexing_options {
            Some(indexing_options.inner)
        } else {
            None
        };
        Ok(PyModel {
            inner: Mutex::new(
                InnerModel::new(None, None, indexing, None).map_err(PyMaplibError::from)?,
            ),
        })
    }

    #[instrument(skip_all)]
    fn add_template(&self, py: Python<'_>, template: Bound<'_, PyAny>) -> PyResult<()> {
        let template = template.try_into()?;
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            add_template_mutex(&mut inner, template)
        })
    }

    #[instrument(skip_all)]
    fn add_prefixes(&self, py: Python<'_>, prefixes: Bound<'_, PyAny>) -> PyResult<()> {
        let mut use_prefixes = HashMap::new();
        if let Ok(prefixes) = prefixes.extract::<HashMap<String, String>>() {
            for (k, v) in prefixes {
                let nn = NamedNode::new(v).map_err(|x| {
                    PyMaplibError::FunctionArgumentError(format!(
                        "Error parsing prefix {}:{}",
                        k,
                        x.to_string()
                    ))
                })?;
                use_prefixes.insert(k, nn);
            }
        } else if let Ok(prefixes) = prefixes.extract::<HashMap<String, PyIRI>>() {
            for (k, v) in prefixes {
                use_prefixes.insert(k, v.into_inner());
            }
        } else if let Ok(prefixes) = prefixes.extract::<HashMap<String, PyPrefix>>() {
            for (k, v) in prefixes {
                use_prefixes.insert(k, v.iri.clone());
            }
        } else {
            return Err(PyMaplibError::FunctionArgumentError(format!(
                "Prefixes should be Dict[str,str]"
            ))
            .into());
        };
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            add_prefixes_mutex(&mut inner, use_prefixes)
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
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            detach_graph_mutex(&mut inner, &graph, preserve_name.unwrap_or(false))
        })
    }

    #[pyo3(signature = (template, df=None, graph=None, types=None, validate_iris=None))]
    #[instrument(skip_all)]
    fn map(
        &self,
        py: Python<'_>,
        template: Bound<'_, PyAny>,
        df: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let df = df.map(polars_df_to_rust_df).transpose()?;
        let template = template.try_into()?;
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            map_mutex(&mut inner, template, df, graph, types, validate_iris)
        })
    }

    #[pyo3(signature = (df, predicate=None, graph=None, types=None, validate_iris=None))]
    #[instrument(skip_all)]
    fn map_triples(
        &self,
        py: Python<'_>,
        df: &Bound<'_, PyAny>,
        predicate: Option<PyObject>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let predicate = if let Some(predicate) = predicate {
            Some(if let Ok(predicate) = predicate.extract::<PyIRI>(py) {
                predicate.iri.as_str().to_string()
            } else if let Ok(predicate) = predicate.extract::<String>(py) {
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
        let df = polars_df_to_rust_df(df)?;
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            map_triples_mutex(&mut inner, df, predicate, graph, types, validate_iris)
        })
    }

    #[pyo3(signature = (df, primary_key_column, dry_run=None, graph=None, types=None,
                            validate_iris=None))]
    #[instrument(skip_all)]
    fn map_default(
        &self,
        py: Python<'_>,
        df: &Bound<'_, PyAny>,
        primary_key_column: String,
        dry_run: Option<bool>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<String> {
        let df = polars_df_to_rust_df(df)?;
        py.allow_threads(move || -> PyResult<String> {
            let mut inner = self.inner.lock().unwrap();
            let graph = parse_optional_named_node(graph)?;
            let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
            map_default_mutex(
                &mut inner,
                df,
                primary_key_column,
                dry_run,
                named_graph,
                types,
                validate_iris,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
    graph=None, streaming=None, return_json=None, include_transient=None, max_rows=None, debug=None))]
    #[instrument(skip_all)]
    fn query(
        &self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        graph: Option<String>,
        streaming: Option<bool>,
        return_json: Option<bool>,
        include_transient: Option<bool>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<PyObject> {
        let mapped_parameters = map_parameters(parameters)?;
        let graph = parse_optional_named_node(graph)?;
        let graph = if let Some(graph) = graph {
            Some(NamedGraph::NamedGraph(graph))
        } else {
            None
        };
        let (res, cats) = py.allow_threads(|| -> PyResult<(_, LockedCats)> {
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
        query_to_result(
            res,
            native_dataframe.unwrap_or(false),
            include_datatypes.unwrap_or(false),
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
        let mapped_parameters = map_parameters(parameters)?;
        let res = py.allow_threads(|| {
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
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            create_index_mutex(&mut inner, options, all, graph)
        })
    }

    #[pyo3(signature = (
        shape_graph,
        data_graph=None,
        include_details=None,
        include_conforms=None,
        include_shape_graph=None,
        streaming=None,
        max_shape_constraint_results=None,
        only_shapes=None,
        deactivate_shapes=None,
        dry_run=None,
        max_rows=None,
        serial=None,
    ))]
    #[instrument(skip_all)]
    fn validate(
        &self,
        py: Python<'_>,
        shape_graph: String,
        data_graph: Option<String>,
        include_details: Option<bool>,
        include_conforms: Option<bool>,
        include_shape_graph: Option<bool>,
        streaming: Option<bool>,
        max_shape_constraint_results: Option<usize>,
        only_shapes: Option<Vec<String>>,
        deactivate_shapes: Option<Vec<String>>,
        dry_run: Option<bool>,
        max_rows: Option<usize>,
        serial: Option<bool>,
    ) -> PyResult<PyValidationReport> {
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            validate_mutex(
                &mut inner,
                shape_graph,
                data_graph,
                include_details,
                include_conforms,
                include_shape_graph,
                streaming,
                max_shape_constraint_results,
                only_shapes,
                deactivate_shapes,
                dry_run,
                max_rows,
                serial,
            )
        })
    }

    #[pyo3(signature = (
            query,
            parameters=None,
            include_datatypes=None,
            native_dataframe=None,
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
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        transient: Option<bool>,
        streaming: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        include_transient: Option<bool>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<HashMap<String, PyObject>> {
        let mapped_parameters = map_parameters(parameters)?;
        let source_graph = parse_optional_named_node(source_graph)?;
        let source_graph = NamedGraph::from_maybe_named_node(source_graph.as_ref());
        let target_graph = parse_optional_named_node(target_graph)?;
        let target_graph = NamedGraph::from_maybe_named_node(target_graph.as_ref());
        let (new_triples, cats) =
            py.allow_threads(|| -> PyResult<(Vec<NewTriples>, LockedCats)> {
                let mut inner = self.inner.lock().unwrap();

                let cats = inner.triplestore.global_cats.clone();

                let new_triples = insert_mutex(
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

                Ok((new_triples, cats))
            })?;
        new_triples_to_dict(
            new_triples,
            native_dataframe.unwrap_or(false),
            include_datatypes.unwrap_or(false),
            cats,
            py,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path, format=None, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
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
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.allow_threads(move || {
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
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path))]
    #[instrument(skip_all)]
    fn read_template(&self, py: Python<'_>, file_path: &Bound<'_, PyAny>) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            read_template_mutex(&mut inner, file_path)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (s, format, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
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
    ) -> PyResult<()> {
        py.allow_threads(|| {
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
            )
        })
    }

    #[pyo3(signature = (file_path, format=None, graph=None))]
    #[instrument(skip_all)]
    fn write(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.allow_threads(move || {
            let mut inner = self.inner.lock().unwrap();
            write_triples_mutex(&mut inner, file_path, format, graph)
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
        prefixes: Option<HashMap<String, String>>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        py.allow_threads(move || {
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
                prefixes,
                graph,
            )
        })
    }

    #[pyo3(signature = (format=None, graph=None))]
    #[instrument(skip_all)]
    fn writes(
        &self,
        py: Python<'_>,
        format: Option<String>,
        graph: Option<String>,
    ) -> PyResult<String> {
        py.allow_threads(|| {
            let mut inner = self.inner.lock().unwrap();
            writes_mutex(&mut inner, format, graph)
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
        py.allow_threads(move || {
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
        py.allow_threads(move || {
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
    ) -> PyResult<Vec<PyObject>> {
        let include_transient = include_transient.unwrap_or(false);
        let eager_sms = py.allow_threads(|| {
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
        include_datatypes=None,
        native_dataframe=None,
        max_iterations=100_000,
        max_results=10_000_000,
        include_transient=None,
        max_rows=100_000_000,
        debug=None))]
    #[instrument(skip_all)]
    fn infer(
        &self,
        py: Python<'_>,
        rulesets: PyObject,
        graph: Option<String>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
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
        let (res, ..) = py.allow_threads(|| -> PyResult<(_, LockedCats)> {
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

// pymethods non-critical functions

enum TemplateType {
    TemplateIRI(PyIRI),
    TemplateString(String),
    TemplatePyTemplate(PyTemplate),
}

impl TryFrom<Bound<'_, PyAny>> for TemplateType {
    type Error = PyMaplibError;

    fn try_from(value: Bound<'_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(i) = value.extract::<PyIRI>() {
            return Ok(Self::TemplateIRI(i));
        } else if let Ok(s) = value.extract::<String>() {
            return Ok(Self::TemplateString(s));
        } else if let Ok(t) = value.extract::<PyTemplate>() {
            return Ok(Self::TemplatePyTemplate(t));
        } else {
            return Err(PyMaplibError::FunctionArgumentError(
                "Expected template to be Template or stOTTR document string".to_string(),
            )
            .into());
        }
    }
}

fn add_template_mutex(inner: &mut MutexGuard<InnerModel>, template: TemplateType) -> PyResult<()> {
    match template {
        TemplateType::TemplateString(s) => {
            inner
                .add_templates_from_string(&s)
                .map_err(PyMaplibError::from)?;
        }
        TemplateType::TemplatePyTemplate(t) => {
            inner
                .add_template(t.into_inner())
                .map_err(PyMaplibError::from)?;
        }
        TemplateType::TemplateIRI(_) => {
            return Err(PyMaplibError::FunctionArgumentError(
                "Adding IRIs templates is not supported".to_string(),
            )
            .into())
        }
    }
    Ok(())
}

fn add_prefixes_mutex(
    inner: &mut MutexGuard<InnerModel>,
    prefixes: HashMap<String, NamedNode>,
) -> PyResult<()> {
    inner.prefixes.extend(prefixes.into_iter());
    Ok(())
}

fn detach_graph_mutex(
    model: &mut MutexGuard<InnerModel>,
    graph: &NamedGraph,
    preserve_name: bool,
) -> PyResult<PyModel> {
    let sprout = model
        .detach_graph(graph, preserve_name)
        .map_err(PyMaplibError::from)?;
    let m = PyModel {
        inner: Mutex::new(sprout),
    };
    Ok(m)
}

fn map_mutex(
    inner: &mut MutexGuard<InnerModel>,
    template: TemplateType,
    df: Option<DataFrame>,
    graph: Option<String>,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<PyObject>> {
    let template = match template {
        TemplateType::TemplateIRI(i) => i.into_inner().to_string(),
        TemplateType::TemplateString(s) => {
            if let Ok(nn) = NamedNode::new(&s) {
                nn.as_str().to_string()
            } else if s.len() < 200 {
                s
            } else {
                let s = inner
                    .add_templates_from_string(&s)
                    .map_err(PyMaplibError::from)?;
                let s = if let Some(s) = s {
                    s
                } else {
                    return Err(PyMaplibError::FunctionArgumentError(
                        "Template stOTTR document contained no templates".to_string(),
                    )
                    .into());
                };
                s.as_str().to_string()
            }
        }
        TemplateType::TemplatePyTemplate(t) => {
            let t_string = t.template.signature.iri.as_str().to_string();
            inner
                .add_template(t.into_inner())
                .map_err(PyMaplibError::from)?;
            t_string
        }
    };

    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let options = MapOptions::from_args(named_graph, validate_iris);
    let types = map_types(types);

    if let Some(df) = df {
        if df.height().gt(&0) {
            let _report = inner
                .expand(&template, Some(df), types, options)
                .map_err(PyMaplibError::from)?;
        } else {
            warn!("Template expansion of {template} with empty DataFrame");
        }
    } else {
        let _report = inner
            .expand(&template, None, None, options)
            .map_err(PyMaplibError::from)?;
    }

    Ok(None)
}

fn map_triples_mutex(
    inner: &mut MutexGuard<InnerModel>,
    df: DataFrame,
    predicate: Option<String>,
    graph: Option<String>,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<PyObject>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let options = MapOptions::from_args(named_graph, validate_iris);
    let types = map_types(types);
    let predicate = if let Some(predicate) = predicate {
        Some(NamedNode::new(predicate).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
    } else {
        None
    };
    inner
        .expand_triples(df, types, predicate, options)
        .map_err(PyMaplibError::from)?;
    Ok(None)
}

fn map_default_mutex(
    inner: &mut MutexGuard<InnerModel>,
    df: DataFrame,
    primary_key_column: String,
    dry_run: Option<bool>,
    graph: NamedGraph,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<String> {
    let options = MapOptions::from_args(graph, validate_iris);
    let dry_run = dry_run.unwrap_or(false);
    let types = map_types(types);
    let tmpl = inner
        .expand_default(df, primary_key_column, vec![], dry_run, types, options)
        .map_err(PyMaplibError::from)?;
    if dry_run {
        info!("Produced template:\n\n {tmpl}");
    }
    Ok(format!("{tmpl}"))
}

fn query_mutex(
    inner: &mut MutexGuard<InnerModel>,
    query: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    graph: Option<NamedGraph>,
    streaming: Option<bool>,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<QueryResult> {
    let res = inner
        .query(
            &query,
            &mapped_parameters,
            graph.as_ref(),
            streaming.unwrap_or(DEFAULT_STREAMING),
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;
    Ok(res)
}

fn update_mutex(
    inner: &mut MutexGuard<InnerModel>,
    update: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    graph: Option<String>,
    streaming: Option<bool>,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<UpdateResult> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = if let Some(graph) = graph {
        Some(NamedGraph::NamedGraph(graph))
    } else {
        None
    };
    let res = inner
        .update(
            &update,
            &mapped_parameters,
            named_graph.as_ref(),
            streaming.unwrap_or(DEFAULT_STREAMING),
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;
    Ok(res)
}

fn create_index_mutex(
    inner: &mut MutexGuard<InnerModel>,
    options: Option<PyIndexingOptions>,
    all: Option<bool>,
    graph: Option<String>,
) -> PyResult<()> {
    let named_graph = if all.unwrap_or(true) {
        None
    } else {
        let graph = parse_optional_named_node(graph)?;
        let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        Some(named_graph)
    };
    let options = if let Some(options) = options {
        options.inner
    } else {
        IndexingOptions::default()
    };
    inner
        .create_index(options, named_graph.as_ref())
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn validate_mutex(
    inner: &mut MutexGuard<InnerModel>,
    shape_graph: String,
    data_graph: Option<String>,
    include_details: Option<bool>,
    include_conforms: Option<bool>,
    include_shape_graph: Option<bool>,
    streaming: Option<bool>,
    max_shape_constraint_results: Option<usize>,
    only_shapes: Option<Vec<String>>,
    deactivate_shapes: Option<Vec<String>>,
    dry_run: Option<bool>,
    max_rows: Option<usize>,
    serial: Option<bool>,
) -> PyResult<PyValidationReport> {
    let data_graph = parse_optional_named_node(data_graph)?;
    let data_graph = NamedGraph::from_maybe_named_node(data_graph.as_ref());
    let shape_graph = parse_named_node(shape_graph)?;
    let shape_graph = NamedGraph::NamedGraph(shape_graph);

    if only_shapes.is_some() && deactivate_shapes.is_some() {
        return Err(PyMaplibError::FunctionArgumentError(
            "only_shapes and deactivate_shapes cannot both be set".to_string(),
        )
        .into());
    }
    let only_shapes = if let Some(only_shapes) = only_shapes {
        let only_shapes: Result<Vec<_>, _> = only_shapes
            .into_iter()
            .map(|x| NamedNode::new(x).map_err(|x| PyMaplibError::from(MaplibError::from(x))))
            .collect();
        Some(only_shapes?)
    } else {
        None
    };

    let deactivate_shapes = if let Some(deactivate_shapes) = deactivate_shapes {
        let deactivate_shapes: Result<Vec<_>, _> = deactivate_shapes
            .into_iter()
            .map(|x| NamedNode::new(x).map_err(|x| PyMaplibError::from(MaplibError::from(x))))
            .collect();
        deactivate_shapes?
    } else {
        vec![]
    };

    let report = inner
        .validate(
            &data_graph,
            &shape_graph,
            include_details.unwrap_or(false),
            include_conforms.unwrap_or(false),
            streaming.unwrap_or(false),
            max_shape_constraint_results,
            false, //TODO: Needs more work before can be exposed
            max_rows,
            only_shapes,
            deactivate_shapes,
            dry_run.unwrap_or(false),
            serial.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;

    let ts = if include_shape_graph.unwrap_or(true) {
        let mut ts = Triplestore::new(None, None)
            .map_err(|x| PyMaplibError::MaplibError(MaplibError::TriplestoreError(x)))?;
        let qs = QuerySettings {
            include_transient: false,
            max_rows: None,
            strict_project: false,
        };
        let res = inner
            .triplestore
            .query(
                "CONSTRUCT { ?subject ?predicate ?object } WHERE {?subject ?predicate ?object}",
                &None,
                false,
                &qs,
                Some(&shape_graph),
                None,
                false,
            )
            .map_err(|x| PyMaplibError::MaplibError(MaplibError::SparqlError(x)))?;
        if let QueryResultKind::Construct(sms) = res.kind {
            let mut new_sms = Vec::with_capacity(sms.len());
            for (
                EagerSolutionMappings {
                    mut mappings,
                    mut rdf_node_types,
                },
                pred,
            ) in sms
            {
                mappings = format_native_columns(
                    mappings.lazy(),
                    &mut rdf_node_types,
                    inner.triplestore.global_cats.clone(),
                )
                .collect()
                .unwrap();
                new_sms.push((EagerSolutionMappings::new(mappings, rdf_node_types), pred));
            }
            ts.insert_construct_result(new_sms, false, &NamedGraph::DefaultGraph)
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
        } else {
            todo!("Handle this error..")
        };
        Some(ts)
    } else {
        None
    };
    Ok(PyValidationReport::new(report, ts))
}

fn insert_mutex(
    inner: &mut MutexGuard<InnerModel>,
    query: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    transient: Option<bool>,
    streaming: Option<bool>,
    source_graph: NamedGraph,
    target_graph: NamedGraph,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<Vec<NewTriples>> {
    let res = inner
        .query(
            &query,
            &mapped_parameters,
            Some(&source_graph),
            streaming.unwrap_or(DEFAULT_STREAMING),
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;
    print_debug_if_exists(res.debug.as_ref());
    let new_triples = if let QueryResultKind::Construct(dfs_and_dts) = res.kind {
        inner
            .insert_construct_result(dfs_and_dts, transient.unwrap_or(false), &target_graph)
            .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?
    } else {
        todo!("Handle this error..")
    };

    Ok(new_triples)
}

fn read_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    format: Option<String>,
    base_iri: Option<String>,
    transient: Option<bool>,
    parallel: Option<bool>,
    checked: Option<bool>,
    graph: Option<String>,
    replace_graph: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let path = Path::new(&file_path);
    let format = format.map(|format| resolve_format(&format));
    inner
        .read_triples(
            path,
            format,
            base_iri,
            transient.unwrap_or(false),
            parallel,
            checked.unwrap_or(true),
            &named_graph,
            replace_graph.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn read_template_mutex(inner: &mut MutexGuard<InnerModel>, file_path: String) -> PyResult<()> {
    let path = Path::new(&file_path);
    inner.read_template(path).map_err(PyMaplibError::from)?;
    Ok(())
}

fn reads_mutex(
    inner: &mut MutexGuard<InnerModel>,
    s: &str,
    format: &str,
    base_iri: Option<String>,
    transient: Option<bool>,
    parallel: Option<bool>,
    checked: Option<bool>,
    graph: Option<String>,
    replace_graph: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let format = resolve_format(format);
    inner
        .reads(
            s,
            format,
            base_iri,
            transient.unwrap_or(false),
            parallel,
            checked.unwrap_or(true),
            &named_graph,
            replace_graph.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn write_triples_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    format: Option<String>,
    graph: Option<String>,
) -> PyResult<()> {
    let format = if let Some(format) = format {
        resolve_format(&format)
    } else {
        RdfFormat::NTriples
    };
    let path_buf = PathBuf::from(file_path);
    let mut actual_file = File::create(path_buf.as_path())
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    inner
        .write_triples(&mut actual_file, &named_graph, format)
        .unwrap();
    Ok(())
}

fn write_cim_xml_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    profile_graph: String,
    model_iri: Option<String>,
    version: Option<String>,
    description: Option<String>,
    created: Option<String>,
    scenario_time: Option<String>,
    modeling_authority_set: Option<String>,
    prefixes: Option<HashMap<String, String>>,
    graph: Option<String>,
) -> PyResult<()> {
    let mut named_node_prefixes = HashMap::new();
    if let Some(prefixes) = prefixes {
        for (k, v) in prefixes {
            let v_nn = parse_named_node(v)?;
            named_node_prefixes.insert(k, v_nn);
        }
    }
    if !named_node_prefixes.contains_key("cim") {
        named_node_prefixes.insert(
            "cim".to_string(),
            NamedNode::new_unchecked("http://iec.ch/TC57/CIM100#"),
        );
    }
    let model_iri = parse_optional_named_node(model_iri)?.unwrap_or(NamedNode::new_unchecked(
        format!("urn:uuid:{}", uuid::Uuid::new_v4()),
    ));
    let version = version.map(oxrdf::Literal::new_simple_literal);
    let description = description.map(oxrdf::Literal::new_simple_literal);
    let profile_graph = parse_named_node(profile_graph)?;
    let profile_graph = NamedGraph::NamedGraph(profile_graph);
    let created = oxrdf::Literal::new_typed_literal(
        created.unwrap_or(Utc::now().format(XSD_DATETIME_WITH_TZ_FORMAT).to_string()),
        xsd::DATE_TIME,
    );
    let scenario_time = oxrdf::Literal::new_typed_literal(
        scenario_time.unwrap_or(Utc::now().format(XSD_DATETIME_WITH_TZ_FORMAT).to_string()),
        xsd::DATE_TIME,
    );
    let modeling_authority_set = modeling_authority_set.map(oxrdf::Literal::new_simple_literal);
    let graph = parse_optional_named_node(graph)?;
    let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let path_buf = PathBuf::from(file_path);
    let mut actual_file = File::create(path_buf.as_path())
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
    let fullmodel_details = FullModelDetails::new(
        model_iri,
        description,
        version,
        created,
        scenario_time,
        modeling_authority_set,
    )
    .map_err(|x| PyMaplibError::from(MaplibError::CIMXMLError(x)))?;
    inner
        .write_cim_xml(
            &mut actual_file,
            fullmodel_details,
            named_node_prefixes,
            &graph,
            &profile_graph,
        )
        .unwrap();
    Ok(())
}

fn writes_mutex(
    inner: &mut MutexGuard<InnerModel>,
    format: Option<String>,
    graph: Option<String>,
) -> PyResult<String> {
    let format = if let Some(format) = format {
        resolve_format(&format)
    } else {
        RdfFormat::NTriples
    };
    let mut out = vec![];
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    inner.write_triples(&mut out, &named_graph, format).unwrap();
    Ok(String::from_utf8(out).unwrap())
}

fn write_native_parquet_mutex(
    inner: &mut MutexGuard<InnerModel>,
    folder_path: String,
    graph: Option<String>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    inner
        .write_native_parquet(&folder_path, &named_graph)
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn get_predicate_iris_mutex(
    inner: &mut MutexGuard<InnerModel>,
    graph: Option<String>,
    include_transient: Option<bool>,
) -> PyResult<Vec<PyIRI>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let nns = inner
        .get_predicate_iris(&named_graph, include_transient.unwrap_or(false))
        .map_err(PyMaplibError::from)?;
    Ok(nns.into_iter().map(PyIRI::from).collect())
}

fn get_predicate_mutex(
    inner: &mut MutexGuard<InnerModel>,
    iri: PyIRI,
    graph: Option<String>,
    include_transient: bool,
) -> PyResult<Vec<EagerSolutionMappings>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let eager_sms = inner
        .get_predicate(&iri.into_inner(), &named_graph, include_transient)
        .map_err(PyMaplibError::from)?;
    let global_cats = &inner.triplestore.global_cats;
    let mut out_eager_sms = Vec::with_capacity(eager_sms.len());
    for EagerSolutionMappings {
        mut mappings,
        mut rdf_node_types,
    } in eager_sms
    {
        (mappings, rdf_node_types) =
            fix_cats_and_multicolumns(mappings, rdf_node_types, true, global_cats.clone());
        out_eager_sms.push(EagerSolutionMappings::new(mappings, rdf_node_types));
    }
    Ok(out_eager_sms)
}

fn infer_mutex(
    inner: &mut MutexGuard<InnerModel>,
    rulesets: Vec<String>,
    max_iterations: Option<usize>,
    max_results: Option<usize>,
    graph: Option<&NamedGraph>,
    include_transient: bool,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> Result<InferenceResult, PyMaplibError> {
    inner
        .infer(
            rulesets,
            max_iterations,
            max_results,
            graph,
            include_transient,
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::MaplibError)
}

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // The python log writer is too slow, needs a logging object cache similar to pyo3-log
    // let make_writer = tracing_pyo3_logger::PythonLogMakeWriter::new(py)?;
    // let pyo3_log_layer = tracing_subscriber::fmt::layer()
    //     .with_writer(make_writer)
    //     .with_target(false)
    //     .with_level(false)
    //     .without_time()
    //     .with_filter(tracing_subscriber::filter::LevelFilter::INFO);

    // let subscriber = tracing_subscriber::registry().with(pyo3_log_layer);
    // subscriber.init();

    let fmt = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(filter::LevelFilter::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    fmt.init();

    m.add_class::<PyIndexingOptions>()?;
    m.add_class::<PyModel>()?;
    m.add_class::<PyValidationReport>()?;
    m.add_class::<PySolutionMappings>()?;
    m.add_class::<PyInferenceResult>()?;
    m.add_class::<PyRDFType>()?;
    m.add_class::<PyPrefix>()?;
    m.add_class::<PyVariable>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyBlankNode>()?;
    m.add_class::<PyIRI>()?;
    m.add_class::<PyXSD>()?;
    m.add_class::<PyRDF>()?;
    m.add_class::<PyRDFS>()?;
    m.add_class::<PyOWL>()?;
    m.add_class::<PyParameter>()?;
    m.add_class::<PyArgument>()?;
    m.add_class::<PyTemplate>()?;
    m.add_class::<PyInstance>()?;
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add("MaplibException", py.get_type::<MaplibException>())?;
    m.add(
        "FunctionArgumentException",
        py.get_type::<FunctionArgumentException>(),
    )?;
    Ok(())
}

fn query_to_result(
    res: QueryResult,
    native_dataframe: bool,
    include_details: bool,
    return_json: bool,
    global_cats: LockedCats,
    py: Python<'_>,
) -> PyResult<PyObject> {
    if return_json {
        let json = res.kind.json(global_cats.clone());
        return Ok(PyString::new(py, &json).into());
    }
    let QueryResult { kind, debug } = res;
    match kind {
        SparqlQueryResult::Select(EagerSolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) => {
            (mappings, rdf_node_types) =
                fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe, global_cats);
            let pydf = df_to_py_df(mappings, rdf_node_types, debug, None, include_details, py)?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (
                EagerSolutionMappings {
                    mut mappings,
                    mut rdf_node_types,
                },
                predicate,
            ) in dfs
            {
                if let Some(predicate) = predicate {
                    mappings = mappings
                        .lazy()
                        .with_column(
                            lit(rdf_named_node_to_polars_literal_value(&predicate))
                                .alias(PREDICATE_COL_NAME),
                        )
                        .select([
                            col(SUBJECT_COL_NAME),
                            col(PREDICATE_COL_NAME),
                            col(OBJECT_COL_NAME),
                        ])
                        .collect()
                        .unwrap();
                    rdf_node_types.insert(
                        PREDICATE_COL_NAME.to_string(),
                        BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                    );
                }
                (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                    mappings,
                    rdf_node_types,
                    native_dataframe,
                    global_cats.clone(),
                );
                let pydf = df_to_py_df(
                    mappings,
                    rdf_node_types,
                    debug.clone(),
                    None,
                    include_details,
                    py,
                )?;
                query_results.push(pydf);
            }
            PyList::new(py, query_results)?.into_py_any(py)
        }
    }
}

//Allowing complex type as it is not used anywhere else.
#[allow(clippy::type_complexity)]
fn map_parameters(
    parameters: Option<HashMap<String, (Bound<'_, PyAny>, HashMap<String, PyRDFType>)>>,
) -> PyResult<Option<HashMap<String, EagerSolutionMappings>>> {
    if let Some(parameters) = parameters {
        let mut mapped_parameters = HashMap::new();
        for (k, (pydf, map)) in parameters {
            let df = polars_df_to_rust_df(&pydf)?;
            let mut rdf_node_types = HashMap::new();
            for (k, v) in map {
                let t = v.as_rdf_node_state();
                rdf_node_types.insert(k, t);
            }
            let m = EagerSolutionMappings {
                mappings: df,
                rdf_node_types,
            };
            mapped_parameters.insert(k, m);
        }

        Ok(Some(mapped_parameters))
    } else {
        Ok(None)
    }
}

fn resolve_format(format: &str) -> RdfFormat {
    match format.to_lowercase().as_str() {
        "ntriples" => RdfFormat::NTriples,
        "turtle" => RdfFormat::Turtle,
        "rdf/xml" | "xml" | "rdfxml" => RdfFormat::RdfXml,
        _ => unimplemented!("Unknown format {}", format),
    }
}

fn parse_named_node(s: String) -> PyResult<NamedNode> {
    Ok(NamedNode::new(s).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
}

fn parse_optional_named_node(s: Option<String>) -> PyResult<Option<NamedNode>> {
    let nn = if let Some(s) = s {
        Some(parse_named_node(s)?)
    } else {
        None
    };
    Ok(nn)
}

fn map_types(
    types: Option<HashMap<String, PyRDFType>>,
) -> Option<HashMap<String, MappingColumnType>> {
    if let Some(types) = types {
        let mut new_types = HashMap::new();
        for (k, v) in types {
            new_types.insert(k, MappingColumnType::Flat(v.as_rdf_node_state()));
        }
        Some(new_types)
    } else {
        None
    }
}

fn new_triples_to_dict(
    new_triples: Vec<NewTriples>,
    native_dataframe: bool,
    include_datatypes: bool,
    global_cats: LockedCats,
    py: Python<'_>,
) -> PyResult<HashMap<String, PyObject>> {
    let mut map = HashMap::new();
    //TODO: Handle case where same predicate occurs multiple times
    for NewTriples {
        df,
        graph: _,
        predicate,
        subject_type,
        object_type,
    } in new_triples
    {
        if let Some(mut df) = df {
            let mut types = HashMap::new();
            types.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.into_default_stored_rdf_node_state(),
            );
            types.insert(
                OBJECT_COL_NAME.to_string(),
                object_type.into_default_stored_rdf_node_state(),
            );
            (df, types) =
                fix_cats_and_multicolumns(df, types, native_dataframe, global_cats.clone());
            let py_sm = df_to_py_df(df, types, None, None, include_datatypes, py)?;
            map.insert(predicate.as_str().to_string(), py_sm);
        }
    }
    Ok(map)
}

fn print_debug_if_exists(debug_outputs: Option<&DebugOutputs>) {
    if let Some(debug_outputs) = debug_outputs {
        print!("{}", debug_outputs);
    }
}
