extern crate core;
mod error;
mod shacl;
use crate::error::*;
use pydf_io::to_rust::polars_df_to_rust_df;

use crate::shacl::PyValidationReport;
use log::warn;
use maplib::errors::MaplibError;
use maplib::mapping::{ExpandOptions, Mapping as InnerMapping};

use chrono::Utc;
use cimxml::export::FullModelDetails;
use pydf_io::to_python::{df_to_py_df, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use triplestore::sparql::{QueryResult as SparqlQueryResult, QueryResult};

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
use jemallocator::Jemalloc;
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

#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;
use representation::cats::Cats;
use representation::polars_to_rdf::XSD_DATETIME_WITH_TZ_FORMAT;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use templates::python::{a, py_triple, PyArgument, PyInstance, PyParameter, PyTemplate, PyXSD};
use templates::MappingColumnType;
use triplestore::{IndexingOptions, NewTriples};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[pyclass(name = "Mapping", frozen)]
pub struct PyMapping {
    inner: Mutex<InnerMapping>,
    sprout: Mutex<Option<InnerMapping>>,
}

impl PyMapping {
    pub fn from_inner_mapping(inner: InnerMapping) -> PyMapping {
        PyMapping {
            inner: Mutex::new(inner),
            sprout: Mutex::new(None),
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
    #[pyo3(signature = (object_sort_all=None, object_sort_some=None, fts_path=None))]
    pub fn new(
        object_sort_all: Option<bool>,
        object_sort_some: Option<Vec<PyIRI>>,
        fts_path: Option<String>,
    ) -> PyIndexingOptions {
        let fts_path = fts_path.map(|fts_path| Path::new(&fts_path).to_owned());
        let inner = if object_sort_all.is_none() && object_sort_some.is_none() {
            let mut opts = IndexingOptions::default();
            opts.set_fts_path(fts_path);
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
                IndexingOptions {
                    object_sort_all,
                    object_sort_some,
                    fts_path,
                }
            }
        };
        PyIndexingOptions { inner }
    }
}

type ParametersType<'a> = HashMap<String, (Bound<'a, PyAny>, HashMap<String, PyRDFType>)>;

#[pymethods]
impl PyMapping {
    #[new]
    #[pyo3(signature = (documents=None, indexing_options=None))]
    fn new(
        documents: Option<&Bound<'_, PyAny>>,
        //storage_folder: Option<String>,
        indexing_options: Option<PyIndexingOptions>,
    ) -> PyResult<PyMapping> {
        let documents = if let Some(documents) = documents {
            if documents.is_instance_of::<PyList>() {
                let mut strs = vec![];
                for doc in documents.try_iter()? {
                    let docstr = doc?.str()?.to_string();
                    strs.push(docstr);
                }
                Some(strs)
            } else {
                let docstr = documents.str()?.to_string();
                Some(vec![docstr])
            }
        } else {
            None
        };
        let mut parsed_documents = vec![];
        if let Some(documents) = documents {
            for ds in documents {
                let parsed_doc = document_from_str(&ds)
                    .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
                parsed_documents.push(parsed_doc);
            }
        }
        let template_dataset = TemplateDataset::from_documents(parsed_documents)
            .map_err(MaplibError::from)
            .map_err(PyMaplibError::from)?;

        let indexing = if let Some(indexing_options) = indexing_options {
            Some(indexing_options.inner)
        } else {
            None
        };
        Ok(PyMapping {
            inner: Mutex::new(
                InnerMapping::new(&template_dataset, None, indexing)
                    .map_err(PyMaplibError::from)?,
            ),
            sprout: Mutex::new(None),
        })
    }

    fn add_template(&self, template: Bound<'_, PyAny>) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        add_template_mutex(&mut inner, template)
    }

    fn create_sprout(&self) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let mut sprout = self.sprout.lock().unwrap();
        create_sprout_mutex(&mut inner, &mut sprout)
    }

    fn detach_sprout(&self) -> PyResult<Option<PyMapping>> {
        let sprout = self.sprout.lock().unwrap();
        detach_sprout_mutex(sprout)
    }

    #[pyo3(signature = (template, df=None, graph=None, types=None, validate_iris=None))]
    fn expand(
        &self,
        template: &Bound<'_, PyAny>,
        df: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let mut inner = self.inner.lock().unwrap();
        expand_mutex(&mut inner, template, df, graph, types, validate_iris)
    }

    #[pyo3(signature = (df, verb=None, graph=None, types=None, validate_iris=None))]
    fn expand_triples(
        &self,
        df: &Bound<'_, PyAny>,
        verb: Option<String>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let mut inner = self.inner.lock().unwrap();
        expand_triples_mutex(&mut inner, df, verb, graph, types, validate_iris)
    }

    #[pyo3(signature = (df, primary_key_column, dry_run=None, graph=None, types=None,
                            validate_iris=None))]
    fn expand_default(
        &self,
        df: &Bound<'_, PyAny>,
        primary_key_column: String,
        dry_run: Option<bool>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
    ) -> PyResult<String> {
        let mut inner = self.inner.lock().unwrap();
        expand_default_mutex(
            &mut inner,
            df,
            primary_key_column,
            dry_run,
            graph,
            types,
            validate_iris,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
    graph=None, streaming=None, return_json=None, include_transient=None))]
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
    ) -> PyResult<PyObject> {
        match self.inner.lock() {
            Ok(mut inner) => Ok(query_mutex(
                &mut inner,
                py,
                query,
                parameters,
                include_datatypes,
                native_dataframe,
                graph,
                streaming,
                return_json,
                include_transient,
            )?),
            Err(err) => {
                let s = if let Some(err) = err.source() {
                    format!("{}", err)
                } else {
                    format!("{}", err)
                };
                Err(PyMaplibError::RuntimeError(s).into())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (update, parameters=None, graph=None, streaming=None,
        include_transient=None))]
    fn update(
        &self,
        py: Python<'_>,
        update: String,
        parameters: Option<ParametersType>,
        graph: Option<String>,
        streaming: Option<bool>,
        include_transient: Option<bool>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        update_mutex(
            &mut inner,
            py,
            update,
            parameters,
            graph,
            streaming,
            include_transient,
        )
    }

    #[pyo3(signature = (options=None, all=None, graph=None))]
    fn create_index(
        &self,
        options: Option<PyIndexingOptions>,
        all: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        create_index_mutex(&mut inner, options, all, graph)
    }

    #[pyo3(signature = (
        shape_graph,
        include_details=None,
        include_conforms=None,
        include_shape_graph=None,
        streaming=None,
        max_shape_constraint_results=None,
        result_storage=None,
        only_shapes=None,
        deactivate_shapes=None,
        dry_run=None,
    ))]
    fn validate(
        &self,
        py: Python<'_>,
        shape_graph: String,
        include_details: Option<bool>,
        include_conforms: Option<bool>,
        include_shape_graph: Option<bool>,
        streaming: Option<bool>,
        max_shape_constraint_results: Option<usize>,
        result_storage: Option<&str>,
        only_shapes: Option<Vec<String>>,
        deactivate_shapes: Option<Vec<String>>,
        dry_run: Option<bool>,
    ) -> PyResult<PyValidationReport> {
        let mut inner = self.inner.lock().unwrap();
        validate_mutex(
            &mut inner,
            py,
            shape_graph,
            include_details,
            include_conforms,
            include_shape_graph,
            streaming,
            max_shape_constraint_results,
            result_storage,
            only_shapes,
            deactivate_shapes,
            dry_run,
        )
    }

    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
                               transient=None, streaming=None, source_graph=None, target_graph=None,
                               include_transient=None))]
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
    ) -> PyResult<HashMap<String, PyObject>> {
        let mut inner = self.inner.lock().unwrap();
        let mut sprout = self.sprout.lock().unwrap();
        insert_mutex(
            &mut inner,
            &mut sprout,
            py,
            query,
            parameters,
            include_datatypes,
            native_dataframe,
            transient,
            streaming,
            source_graph,
            target_graph,
            include_transient,
        )
    }

    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
                        transient=None, streaming=None, source_graph=None, target_graph=None,
                        include_transient=None))]
    fn insert_sprout(
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
    ) -> PyResult<HashMap<String, PyObject>> {
        let mut inner = self.inner.lock().unwrap();
        let mut sprout = self.sprout.lock().unwrap();
        insert_sprout_mutex(
            &mut inner,
            &mut sprout,
            py,
            query,
            parameters,
            include_datatypes,
            native_dataframe,
            transient,
            streaming,
            source_graph,
            target_graph,
            include_transient,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path, format=None, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
    fn read_triples(
        &self,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        read_triples_mutex(
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
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (s, format, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
    fn read_triples_string(
        &self,
        s: &str,
        format: &str,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        read_triples_string_mutex(
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
    }

    #[pyo3(signature = (file_path, graph=None))]
    fn write_ntriples(&self, file_path: &Bound<'_, PyAny>, graph: Option<String>) -> PyResult<()> {
        warn!("use write_triples with format=\"ntriples\" instead");
        self.write_triples(file_path, Some("ntriples".to_string()), graph)
    }

    #[pyo3(signature = (file_path, format=None, graph=None))]
    fn write_triples(
        &self,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        write_triples_mutex(&mut inner, file_path, format, graph)
    }

    #[pyo3(signature = (
        file_path, profile_graph, model_iri=None, version=None, description=None, created=None,
        scenario_time=None, modeling_authority_set=None, prefixes=None, graph=None))]
    fn write_cim_xml(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
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
        let mut inner = self.inner.lock().unwrap();
        write_cim_xml_mutex(
            &mut inner,
            py,
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
    }

    #[pyo3(signature = (graph=None))]
    fn write_ntriples_string(&self, graph: Option<String>) -> PyResult<String> {
        warn!("use write_triples_string with format=\"ntriples\" instead");
        self.write_triples_string(Some("ntriples".to_string()), graph)
    }

    #[pyo3(signature = (format=None, graph=None))]
    fn write_triples_string(
        &self,
        format: Option<String>,
        graph: Option<String>,
    ) -> PyResult<String> {
        let mut inner = self.inner.lock().unwrap();
        write_triples_string_mutex(&mut inner, format, graph)
    }

    #[pyo3(signature = (folder_path, graph=None))]
    fn write_native_parquet(
        &self,
        folder_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        write_native_parquet_mutex(&mut inner, folder_path, graph)
    }

    #[pyo3(signature = (graph=None, include_transient=None))]
    fn get_predicate_iris(
        &self,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<PyIRI>> {
        let mut inner = self.inner.lock().unwrap();
        get_predicate_iris_mutex(&mut inner, graph, include_transient)
    }

    #[pyo3(signature = (iri, graph=None, include_transient=None))]
    fn get_predicate(
        &self,
        py: Python<'_>,
        iri: PyIRI,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<PyObject>> {
        let mut inner = self.inner.lock().unwrap();
        get_predicate_mutex(&mut inner, py, iri, graph, include_transient)
    }

    #[pyo3(signature = (ruleset,))]
    fn add_ruleset(&self, ruleset: &str) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .add_ruleset(ruleset)
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    fn drop_ruleset(&self) {
        self.inner.lock().unwrap().drop_ruleset();
    }

    #[pyo3(signature = (insert=None, include_datatypes=None, native_dataframe=None, max_iterations=None))]
    fn infer(
        &self,
        py: Python<'_>,
        insert: Option<bool>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        max_iterations: Option<usize>,
    ) -> PyResult<Option<HashMap<String, PyObject>>> {
        let mut inner = self.inner.lock().unwrap();
        infer_mutex(
            &mut inner,
            py,
            insert,
            include_datatypes,
            native_dataframe,
            max_iterations,
        )
    }
}

// pymethods mutex functions

fn add_template_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    template: Bound<'_, PyAny>,
) -> PyResult<()> {
    if let Ok(s) = template.extract::<String>() {
        inner
            .add_templates_from_string(&s)
            .map_err(PyMaplibError::from)?;
    } else if let Ok(s) = template.extract::<PyTemplate>() {
        inner
            .add_template(s.into_inner())
            .map_err(PyMaplibError::from)?;
    } else {
        return Err(PyMaplibError::FunctionArgumentError(
            "Expected template to be Template or stOTTR document string".to_string(),
        )
        .into());
    }
    Ok(())
}

fn create_sprout_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    sprout: &mut MutexGuard<Option<InnerMapping>>,
) -> PyResult<()> {
    let mut new_sprout =
        InnerMapping::new(&inner.template_dataset, None, Some(inner.indexing.clone()))
            .map_err(PyMaplibError::from)?;
    new_sprout.blank_node_counter = inner.blank_node_counter;
    **sprout = Some(new_sprout);
    Ok(())
}

fn detach_sprout_mutex(
    mut sprout: MutexGuard<Option<InnerMapping>>,
) -> PyResult<Option<PyMapping>> {
    if let Some(sprout) = sprout.take() {
        let m = PyMapping {
            inner: Mutex::new(sprout),
            sprout: Mutex::new(None),
        };
        Ok(Some(m))
    } else {
        Ok(None)
    }
}

fn expand_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    template: &Bound<'_, PyAny>,
    df: Option<&Bound<'_, PyAny>>,
    graph: Option<String>,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<PyObject>> {
    let template = if let Ok(i) = template.extract::<PyIRI>() {
        i.into_inner().to_string()
    } else if let Ok(t) = template.extract::<PyTemplate>() {
        let t_string = t.template.signature.template_name.as_str().to_string();
        inner
            .add_template(t.into_inner())
            .map_err(PyMaplibError::from)?;
        t_string
    } else if let Ok(s) = template.extract::<String>() {
        if s.len() < 100 {
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
    } else {
        return Err(PyMaplibError::FunctionArgumentError(
            "Template must be Template, IRI or str".to_string(),
        )
        .into());
    };
    let graph = parse_optional_named_node(graph)?;
    let options = ExpandOptions::from_args(graph, validate_iris);
    let types = map_types(types);

    if let Some(df) = df {
        if df.getattr("height")?.gt(0).unwrap() {
            let df = polars_df_to_rust_df(df)?;

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

fn expand_triples_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    df: &Bound<'_, PyAny>,
    verb: Option<String>,
    graph: Option<String>,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<PyObject>> {
    let graph = parse_optional_named_node(graph)?;
    let df = polars_df_to_rust_df(df)?;
    let options = ExpandOptions::from_args(graph, validate_iris);
    let types = map_types(types);
    let verb = if let Some(verb) = verb {
        Some(NamedNode::new(verb).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
    } else {
        None
    };
    inner
        .expand_triples(df, types, verb, options)
        .map_err(PyMaplibError::from)?;
    Ok(None)
}

fn expand_default_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    df: &Bound<'_, PyAny>,
    primary_key_column: String,
    dry_run: Option<bool>,
    graph: Option<String>,
    types: Option<HashMap<String, PyRDFType>>,
    validate_iris: Option<bool>,
) -> PyResult<String> {
    let df = polars_df_to_rust_df(df)?;
    let graph = parse_optional_named_node(graph)?;
    let options = ExpandOptions::from_args(graph, validate_iris);
    let dry_run = dry_run.unwrap_or(false);
    let types = map_types(types);
    let tmpl = inner
        .expand_default(df, primary_key_column, vec![], dry_run, types, options)
        .map_err(PyMaplibError::from)?;
    if dry_run {
        println!("Produced template:\n\n {tmpl}");
    }
    Ok(format!("{tmpl}"))
}

fn query_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    query: String,
    parameters: Option<ParametersType>,
    include_datatypes: Option<bool>,
    native_dataframe: Option<bool>,
    graph: Option<String>,
    streaming: Option<bool>,
    return_json: Option<bool>,
    include_transient: Option<bool>,
) -> PyResult<PyObject> {
    let graph = parse_optional_named_node(graph)?;
    let mapped_parameters = map_parameters(parameters)?;
    let res = inner
        .query(
            &query,
            &mapped_parameters,
            graph.clone(),
            streaming.unwrap_or(false),
            include_transient.unwrap_or(true),
            py,
        )
        .map_err(PyMaplibError::from)?;
    query_to_result(
        res,
        native_dataframe.unwrap_or(false),
        include_datatypes.unwrap_or(false),
        return_json.unwrap_or(false),
        inner.get_triplestore(&graph).cats.clone(),
        py,
    )
}

fn update_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    update: String,
    parameters: Option<ParametersType>,
    graph: Option<String>,
    streaming: Option<bool>,
    include_transient: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let mapped_parameters = map_parameters(parameters)?;
    inner
        .update(
            &update,
            &mapped_parameters,
            graph,
            streaming.unwrap_or(false),
            include_transient.unwrap_or(true),
            py,
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn create_index_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    options: Option<PyIndexingOptions>,
    all: Option<bool>,
    graph: Option<String>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let options = if let Some(options) = options {
        options.inner
    } else {
        IndexingOptions::default()
    };
    inner
        .create_index(options, all.unwrap_or(true), graph)
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn validate_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    shape_graph: String,
    include_details: Option<bool>,
    include_conforms: Option<bool>,
    include_shape_graph: Option<bool>,
    streaming: Option<bool>,
    max_shape_constraint_results: Option<usize>,
    result_storage: Option<&str>,
    only_shapes: Option<Vec<String>>,
    deactivate_shapes: Option<Vec<String>>,
    dry_run: Option<bool>,
) -> PyResult<PyValidationReport> {
    let shape_graph =
        NamedNode::new(shape_graph).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
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

    let path = result_storage.map(|v| Path::new(v).to_owned());
    let report = inner
        .validate(
            &shape_graph,
            include_details.unwrap_or(false),
            include_conforms.unwrap_or(false),
            streaming.unwrap_or(false),
            max_shape_constraint_results,
            path.as_ref(),
            false, //TODO: Needs more work before can be exposed
            only_shapes,
            deactivate_shapes,
            dry_run.unwrap_or(false),
            py,
        )
        .map_err(PyMaplibError::from)?;
    let shape_graph_triplestore = if include_shape_graph.unwrap_or(true) {
        Some(inner.triplestores_map.get(&shape_graph).unwrap().clone())
    } else {
        None
    };
    Ok(PyValidationReport::new(
        report,
        shape_graph_triplestore,
        inner.indexing.clone(),
    ))
}

fn insert_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    sprout: &mut MutexGuard<Option<InnerMapping>>,
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
) -> PyResult<HashMap<String, PyObject>> {
    let mapped_parameters = map_parameters(parameters)?;
    let source_graph = parse_optional_named_node(source_graph)?;
    let target_graph = parse_optional_named_node(target_graph)?;
    let res = inner
        .query(
            &query,
            &mapped_parameters,
            source_graph.clone(),
            streaming.unwrap_or(false),
            include_transient.unwrap_or(true),
            py,
        )
        .map_err(PyMaplibError::from)?;
    let out_dict = if let QueryResult::Construct(dfs_and_dts) = res {
        let new_triples = inner
            .insert_construct_result(dfs_and_dts, transient.unwrap_or(false), target_graph)
            .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
        new_triples_to_dict(
            new_triples,
            native_dataframe.unwrap_or(false),
            include_datatypes.unwrap_or(false),
            inner.get_triplestore(&source_graph).cats.clone(),
            py,
        )?
    } else {
        todo!("Handle this error..")
    };
    if sprout.is_some() {
        sprout.as_mut().unwrap().blank_node_counter = inner.blank_node_counter;
    }
    Ok(out_dict)
}

fn insert_sprout_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    sprout: &mut MutexGuard<Option<InnerMapping>>,
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
) -> PyResult<HashMap<String, PyObject>> {
    let mapped_parameters = map_parameters(parameters)?;
    let source_graph = parse_optional_named_node(source_graph)?;
    let target_graph = parse_optional_named_node(target_graph)?;
    if sprout.is_none() {
        create_sprout_mutex(inner, sprout)?;
    }
    let res = inner
        .query(
            &query,
            &mapped_parameters,
            source_graph.clone(),
            streaming.unwrap_or(false),
            include_transient.unwrap_or(true),
            py,
        )
        .map_err(PyMaplibError::from)?;
    let out_dict = if let QueryResult::Construct(dfs_and_dts) = res {
        let new_triples = sprout
            .as_mut()
            .unwrap()
            .insert_construct_result(dfs_and_dts, transient.unwrap_or(false), target_graph)
            .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
        new_triples_to_dict(
            new_triples,
            native_dataframe.unwrap_or(false),
            include_datatypes.unwrap_or(false),
            inner.get_triplestore(&source_graph).cats.clone(),
            py,
        )?
    } else {
        todo!("Handle this error..")
    };
    inner.blank_node_counter = sprout.as_ref().unwrap().blank_node_counter;

    Ok(out_dict)
}

fn read_triples_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    file_path: &Bound<'_, PyAny>,
    format: Option<String>,
    base_iri: Option<String>,
    transient: Option<bool>,
    parallel: Option<bool>,
    checked: Option<bool>,
    graph: Option<String>,
    replace_graph: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let file_path = file_path.str()?.to_string();
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
            graph,
            replace_graph.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn read_triples_string_mutex(
    inner: &mut MutexGuard<InnerMapping>,
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
    let format = resolve_format(format);
    inner
        .read_triples_string(
            s,
            format,
            base_iri,
            transient.unwrap_or(false),
            parallel,
            checked.unwrap_or(true),
            graph,
            replace_graph.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn write_triples_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    file_path: &Bound<'_, PyAny>,
    format: Option<String>,
    graph: Option<String>,
) -> PyResult<()> {
    let format = if let Some(format) = format {
        resolve_format(&format)
    } else {
        RdfFormat::NTriples
    };
    let file_path = file_path.str()?.to_string();
    let path_buf = PathBuf::from(file_path);
    let mut actual_file = File::create(path_buf.as_path())
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
    let graph = parse_optional_named_node(graph)?;
    inner
        .write_triples(&mut actual_file, graph, format)
        .unwrap();
    Ok(())
}

fn write_cim_xml_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    file_path: &Bound<'_, PyAny>,
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
    let file_path = file_path.str()?.to_string();
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
            graph,
            profile_graph,
            py,
        )
        .unwrap();
    Ok(())
}

fn write_triples_string_mutex(
    inner: &mut MutexGuard<InnerMapping>,
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
    inner.write_triples(&mut out, graph, format).unwrap();
    Ok(String::from_utf8(out).unwrap())
}

fn write_native_parquet_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    folder_path: &Bound<'_, PyAny>,
    graph: Option<String>,
) -> PyResult<()> {
    let folder_path = folder_path.str()?.to_string();
    let graph = parse_optional_named_node(graph)?;
    inner
        .write_native_parquet(&folder_path, graph)
        .map_err(PyMaplibError::from)?;
    Ok(())
}

fn get_predicate_iris_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    graph: Option<String>,
    include_transient: Option<bool>,
) -> PyResult<Vec<PyIRI>> {
    let graph = parse_optional_named_node(graph)?;
    let nns = inner
        .get_predicate_iris(&graph, include_transient.unwrap_or(false))
        .map_err(PyMaplibError::from)?;
    Ok(nns.into_iter().map(PyIRI::from).collect())
}

fn get_predicate_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    iri: PyIRI,
    graph: Option<String>,
    include_transient: Option<bool>,
) -> PyResult<Vec<PyObject>> {
    let graph = parse_optional_named_node(graph)?;
    let eager_sms = inner
        .get_predicate(&iri.into_inner(), graph, include_transient.unwrap_or(false))
        .map_err(PyMaplibError::from)?;
    let mut out = vec![];
    for EagerSolutionMappings {
        mappings,
        rdf_node_types,
    } in eager_sms
    {
        let py_sm = df_to_py_df(mappings, rdf_node_types, None, true, py)?;
        out.push(py_sm);
    }
    Ok(out)
}

fn infer_mutex(
    inner: &mut MutexGuard<InnerMapping>,
    py: Python<'_>,
    insert: Option<bool>,
    include_datatypes: Option<bool>,
    native_dataframe: Option<bool>,
    max_iterations: Option<usize>,
) -> PyResult<Option<HashMap<String, PyObject>>> {
    let res = inner
        .infer(insert.unwrap_or(true), max_iterations)
        .map_err(PyMaplibError::MaplibError)?;
    if let Some(res) = res {
        let mut py_res = HashMap::new();
        for (
            nn,
            EagerSolutionMappings {
                mut mappings,
                mut rdf_node_types,
            },
        ) in res
        {
            let include_datatypes = include_datatypes.unwrap_or(false);
            let native_dataframe = native_dataframe.unwrap_or(false);
            (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                mappings,
                rdf_node_types,
                native_dataframe,
                inner.base_triplestore.cats.clone(),
            );
            let pydf = df_to_py_df(mappings, rdf_node_types, None, include_datatypes, py)?;
            py_res.insert(nn.as_str().to_string(), pydf);
        }
        Ok(Some(py_res))
    } else {
        Ok(None)
    }
}

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Currently deadlocks, likely need to change all above with allow threads: https://docs.rs/pyo3-log/latest/pyo3_log/
    // pyo3_log::init();
    m.add_class::<PyIndexingOptions>()?;
    m.add_class::<PyMapping>()?;
    m.add_class::<PyValidationReport>()?;
    m.add_class::<PySolutionMappings>()?;
    m.add_class::<PyRDFType>()?;
    m.add_class::<PyPrefix>()?;
    m.add_class::<PyVariable>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyBlankNode>()?;
    m.add_class::<PyIRI>()?;
    m.add_class::<PyXSD>()?;
    m.add_class::<PyParameter>()?;
    m.add_class::<PyArgument>()?;
    m.add_class::<PyTemplate>()?;
    m.add_class::<PyInstance>()?;
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add_function(wrap_pyfunction!(a, m)?)?;
    m.add("MaplibException", py.get_type::<MaplibException>())?;
    m.add(
        "FunctionArgumentException",
        py.get_type::<FunctionArgumentException>(),
    )?;
    Ok(())
}

fn query_to_result(
    res: SparqlQueryResult,
    native_dataframe: bool,
    include_details: bool,
    return_json: bool,
    global_cats: Arc<Cats>,
    py: Python<'_>,
) -> PyResult<PyObject> {
    if return_json {
        let json = res.json(global_cats.clone());
        return Ok(PyString::new(py, &json).into());
    }
    match res {
        SparqlQueryResult::Select(EagerSolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) => {
            (mappings, rdf_node_types) =
                fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe, global_cats);
            let pydf = df_to_py_df(mappings, rdf_node_types, None, include_details, py)?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (
                EagerSolutionMappings {
                    mut mappings,
                    mut rdf_node_types,
                },
                verb,
            ) in dfs
            {
                if let Some(verb) = verb {
                    mappings = mappings
                        .lazy()
                        .with_column(
                            lit(rdf_named_node_to_polars_literal_value(&verb))
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
                let pydf = df_to_py_df(mappings, rdf_node_types, None, include_details, py)?;
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
    global_cats: Arc<Cats>,
    py: Python<'_>,
) -> PyResult<HashMap<String, PyObject>> {
    let mut map = HashMap::new();
    //TODO: Handle case where same predicate occurs multiple times
    for NewTriples {
        df,
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
            let py_sm = df_to_py_df(df, types, None, include_datatypes, py)?;
            map.insert(predicate.as_str().to_string(), py_sm);
        }
    }
    Ok(map)
}
