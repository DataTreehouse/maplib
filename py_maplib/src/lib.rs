use polars::enable_string_cache;
extern crate core;

mod error;
mod shacl;

use crate::error::*;
use pydf_io::to_rust::polars_df_to_rust_df;

use crate::shacl::PyValidationReport;
use log::warn;
use maplib::errors::MaplibError;
use maplib::mapping::{ExpandOptions, Mapping as InnerMapping};

use pydf_io::to_python::{df_to_py_df, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
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
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::{RDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use templates::python::{a, py_triple, PyArgument, PyInstance, PyParameter, PyTemplate, PyXSD};
use templates::MappingColumnType;
use triplestore::{IndexingOptions, NewTriples};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[pyclass(name = "Mapping")]
pub struct PyMapping {
    inner: InnerMapping,
    sprout: Option<InnerMapping>,
}

impl PyMapping {
    pub fn from_inner_mapping(inner: InnerMapping) -> PyMapping {
        PyMapping {
            inner,
            sprout: None,
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
            inner: InnerMapping::new(&template_dataset, None, indexing)
                .map_err(PyMaplibError::from)?,
            sprout: None,
        })
    }

    fn add_template(&mut self, template: Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(s) = template.extract::<String>() {
            self.inner
                .add_templates_from_string(&s)
                .map_err(PyMaplibError::from)?;
        } else if let Ok(s) = template.extract::<PyTemplate>() {
            self.inner
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

    fn create_sprout(&mut self) -> PyResult<()> {
        let mut sprout = InnerMapping::new(
            &self.inner.template_dataset,
            None,
            Some(self.inner.indexing.clone()),
        )
        .map_err(PyMaplibError::from)?;
        sprout.blank_node_counter = self.inner.blank_node_counter;
        self.sprout = Some(sprout);
        Ok(())
    }

    fn detach_sprout(&mut self) -> PyResult<Option<PyMapping>> {
        if let Some(sprout) = self.sprout.take() {
            let m = PyMapping {
                inner: sprout,
                sprout: None,
            };
            Ok(Some(m))
        } else {
            Ok(None)
        }
    }

    #[pyo3(signature = (template, df=None, graph=None, types=None, validate_iris=None, delay_index=None))]
    fn expand(
        &mut self,
        template: &Bound<'_, PyAny>,
        df: Option<&Bound<'_, PyAny>>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
        delay_index: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let template = if let Ok(i) = template.extract::<PyIRI>() {
            i.into_inner().to_string()
        } else if let Ok(t) = template.extract::<PyTemplate>() {
            let t_string = t.template.signature.template_name.as_str().to_string();
            self.inner
                .add_template(t.into_inner())
                .map_err(PyMaplibError::from)?;
            t_string
        } else if let Ok(s) = template.extract::<String>() {
            if s.len() < 100 {
                s
            } else {
                let s = self
                    .inner
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
        let graph = parse_optional_graph(graph)?;
        let options = ExpandOptions::from_args(graph, validate_iris, delay_index);
        let types = map_types(types);

        if let Some(df) = df {
            if df.getattr("height")?.gt(0).unwrap() {
                let df = polars_df_to_rust_df(df)?;

                let _report = self
                    .inner
                    .expand(&template, Some(df), types, options)
                    .map_err(PyMaplibError::from)?;
            } else {
                warn!("Template expansion of {} with empty DataFrame", template);
            }
        } else {
            let _report = self
                .inner
                .expand(&template, None, None, options)
                .map_err(PyMaplibError::from)?;
        }

        Ok(None)
    }

    #[pyo3(signature = (df, verb=None, graph=None, types=None, validate_iris=None, delay_index=None))]
    fn expand_triples(
        &mut self,
        df: &Bound<'_, PyAny>,
        verb: Option<String>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
        delay_index: Option<bool>,
    ) -> PyResult<Option<PyObject>> {
        let graph = parse_optional_graph(graph)?;
        let df = polars_df_to_rust_df(df)?;
        let options = ExpandOptions::from_args(graph, validate_iris, delay_index);
        let types = map_types(types);
        let verb = if let Some(verb) = verb {
            Some(NamedNode::new(verb).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
        } else {
            None
        };
        self.inner
            .expand_triples(df, types, verb, options)
            .map_err(PyMaplibError::from)?;
        Ok(None)
    }

    #[pyo3(signature = (df, primary_key_column, dry_run=None, graph=None, types=None,
                            validate_iris=None, delay_index=None))]
    fn expand_default(
        &mut self,
        df: &Bound<'_, PyAny>,
        primary_key_column: String,
        dry_run: Option<bool>,
        graph: Option<String>,
        types: Option<HashMap<String, PyRDFType>>,
        validate_iris: Option<bool>,
        delay_index: Option<bool>,
    ) -> PyResult<String> {
        let df = polars_df_to_rust_df(df)?;
        let graph = parse_optional_graph(graph)?;
        let options = ExpandOptions::from_args(graph, validate_iris, delay_index);
        let dry_run = dry_run.unwrap_or(false);
        let types = map_types(types);
        let tmpl = self
            .inner
            .expand_default(df, primary_key_column, vec![], dry_run, types, options)
            .map_err(PyMaplibError::from)?;
        if dry_run {
            println!("Produced template:\n\n {}", tmpl);
        }
        Ok(format!("{}", tmpl))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
    graph=None, streaming=None, return_json=None, include_transient=None))]
    fn query(
        &mut self,
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
        let graph = parse_optional_graph(graph)?;
        let mapped_parameters = map_parameters(parameters)?;
        let res = self
            .inner
            .query(
                &query,
                &mapped_parameters,
                graph,
                streaming.unwrap_or(false),
                include_transient.unwrap_or(true),
            )
            .map_err(PyMaplibError::from)?;
        query_to_result(
            res,
            native_dataframe.unwrap_or(false),
            include_datatypes.unwrap_or(false),
            return_json.unwrap_or(false),
            py,
        )
    }

    #[pyo3(signature = (options=None, all=None, graph=None))]
    fn create_index(
        &mut self,
        options: Option<PyIndexingOptions>,
        all: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let options = if let Some(options) = options {
            options.inner
        } else {
            IndexingOptions::default()
        };
        self.inner
            .create_index(options, all.unwrap_or(true), graph)
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    #[pyo3(signature = (
        shape_graph,
        include_details=None,
        include_conforms=None,
        include_shape_graph=None,
        streaming=None,
        max_shape_results=None,
        result_storage=None,
        only_shapes=None,
        deactivate_shapes=None,
        dry_run=None,
    ))]
    fn validate(
        &mut self,
        shape_graph: String,
        include_details: Option<bool>,
        include_conforms: Option<bool>,
        include_shape_graph: Option<bool>,
        streaming: Option<bool>,
        max_shape_results: Option<usize>,
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
        let report = self
            .inner
            .validate(
                &shape_graph,
                include_details.unwrap_or(false),
                include_conforms.unwrap_or(false),
                streaming.unwrap_or(false),
                max_shape_results,
                path.as_ref(),
                false, //TODO: Needs more work before can be exposed
                only_shapes,
                deactivate_shapes,
                dry_run.unwrap_or(false),
            )
            .map_err(PyMaplibError::from)?;
        let shape_graph_triplestore = if include_shape_graph.unwrap_or(true) {
            Some(
                self.inner
                    .triplestores_map
                    .get(&shape_graph)
                    .unwrap()
                    .clone(),
            )
        } else {
            None
        };
        Ok(PyValidationReport::new(
            report,
            shape_graph_triplestore,
            self.inner.indexing.clone(),
        ))
    }

    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
                               transient=None, streaming=None, source_graph=None, target_graph=None,
                               delay_index=None, include_transient=None))]
    fn insert(
        &mut self,
        query: String,
        parameters: Option<ParametersType>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        transient: Option<bool>,
        streaming: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        delay_index: Option<bool>,
        include_transient: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<HashMap<String, PyObject>> {
        let mapped_parameters = map_parameters(parameters)?;
        let source_graph = parse_optional_graph(source_graph)?;
        let target_graph = parse_optional_graph(target_graph)?;
        let res = self
            .inner
            .query(
                &query,
                &mapped_parameters,
                source_graph,
                streaming.unwrap_or(false),
                include_transient.unwrap_or(true),
            )
            .map_err(PyMaplibError::from)?;
        let out_dict = if let QueryResult::Construct(dfs_and_dts) = res {
            let new_triples = self
                .inner
                .insert_construct_result(
                    dfs_and_dts,
                    transient.unwrap_or(false),
                    target_graph,
                    delay_index.unwrap_or(true),
                )
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            new_triples_to_dict(
                new_triples,
                native_dataframe.unwrap_or(false),
                include_datatypes.unwrap_or(false),
                py,
            )?
        } else {
            todo!("Handle this error..")
        };
        if self.sprout.is_some() {
            self.sprout.as_mut().unwrap().blank_node_counter = self.inner.blank_node_counter;
        }
        Ok(out_dict)
    }

    #[pyo3(signature = (query, parameters=None, include_datatypes=None, native_dataframe=None,
                        transient=None, streaming=None, source_graph=None, target_graph=None,
                        delay_index=None, include_transient=None))]
    fn insert_sprout(
        &mut self,
        query: String,
        parameters: Option<ParametersType>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        transient: Option<bool>,
        streaming: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        delay_index: Option<bool>,
        include_transient: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<HashMap<String, PyObject>> {
        let mapped_parameters = map_parameters(parameters)?;
        let source_graph = parse_optional_graph(source_graph)?;
        let target_graph = parse_optional_graph(target_graph)?;
        if self.sprout.is_none() {
            self.create_sprout()?;
        }
        let res = self
            .inner
            .query(
                &query,
                &mapped_parameters,
                source_graph,
                streaming.unwrap_or(false),
                include_transient.unwrap_or(true),
            )
            .map_err(PyMaplibError::from)?;
        let out_dict = if let QueryResult::Construct(dfs_and_dts) = res {
            let new_triples = self
                .sprout
                .as_mut()
                .unwrap()
                .insert_construct_result(
                    dfs_and_dts,
                    transient.unwrap_or(false),
                    target_graph,
                    delay_index.unwrap_or(true),
                )
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            new_triples_to_dict(
                new_triples,
                native_dataframe.unwrap_or(false),
                include_datatypes.unwrap_or(false),
                py,
            )?
        } else {
            todo!("Handle this error..")
        };
        self.inner.blank_node_counter = self.sprout.as_ref().unwrap().blank_node_counter;

        Ok(out_dict)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (file_path, format=None, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
    fn read_triples(
        &mut self,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let file_path = file_path.str()?.to_string();
        let path = Path::new(&file_path);
        let format = format.map(|format| resolve_format(&format));
        self.inner
            .read_triples(
                path,
                format,
                base_iri,
                transient.unwrap_or(false),
                parallel.unwrap_or(false),
                checked.unwrap_or(true),
                graph,
                replace_graph.unwrap_or(false),
            )
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (s, format, base_iri=None, transient=None, parallel=None, checked=None, graph=None, replace_graph=None))]
    fn read_triples_string(
        &mut self,
        s: &str,
        format: &str,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        graph: Option<String>,
        replace_graph: Option<bool>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let format = resolve_format(format);
        self.inner
            .read_triples_string(
                s,
                format,
                base_iri,
                transient.unwrap_or(false),
                parallel.unwrap_or(false),
                checked.unwrap_or(true),
                graph,
                replace_graph.unwrap_or(false),
            )
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    #[pyo3(signature = (file_path, graph=None))]
    fn write_ntriples(
        &mut self,
        file_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        warn!("use write_triples with format=\"ntriples\" instead");
        self.write_triples(file_path, Some("ntriples".to_string()), graph)
    }

    #[pyo3(signature = (file_path, format=None, graph=None))]
    fn write_triples(
        &mut self,
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
        let graph = parse_optional_graph(graph)?;
        self.inner
            .write_triples(&mut actual_file, graph, format)
            .unwrap();
        Ok(())
    }

    #[pyo3(signature = (graph=None))]
    fn write_ntriples_string(&mut self, graph: Option<String>) -> PyResult<String> {
        warn!("use write_triples_string with format=\"ntriples\" instead");
        self.write_triples_string(Some("ntriples".to_string()), graph)
    }

    #[pyo3(signature = (format=None, graph=None))]
    fn write_triples_string(
        &mut self,
        format: Option<String>,
        graph: Option<String>,
    ) -> PyResult<String> {
        let format = if let Some(format) = format {
            resolve_format(&format)
        } else {
            RdfFormat::NTriples
        };
        let mut out = vec![];
        let graph = parse_optional_graph(graph)?;
        self.inner.write_triples(&mut out, graph, format).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    #[pyo3(signature = (folder_path, graph=None))]
    fn write_native_parquet(
        &mut self,
        folder_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let folder_path = folder_path.str()?.to_string();
        let graph = parse_optional_graph(graph)?;
        self.inner
            .write_native_parquet(&folder_path, graph)
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    #[pyo3(signature = (graph=None, include_transient=None))]
    fn get_predicate_iris(
        &mut self,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<PyIRI>> {
        let graph = parse_optional_graph(graph)?;
        let nns = self
            .inner
            .get_predicate_iris(&graph, include_transient.unwrap_or(false))
            .map_err(PyMaplibError::from)?;
        Ok(nns.into_iter().map(PyIRI::from).collect())
    }

    #[pyo3(signature = (iri, graph=None, include_transient=None))]
    fn get_predicate(
        &mut self,
        py: Python<'_>,
        iri: PyIRI,
        graph: Option<String>,
        include_transient: Option<bool>,
    ) -> PyResult<Vec<PyObject>> {
        let graph = parse_optional_graph(graph)?;
        let eager_sms = self
            .inner
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

    #[pyo3(signature = (ruleset,))]
    fn add_ruleset(&mut self, ruleset: &str) -> PyResult<()> {
        self.inner
            .add_ruleset(ruleset)
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    fn drop_ruleset(&mut self) {
        self.inner.drop_ruleset();
    }

    #[pyo3(signature = (insert=None, include_datatypes=None, native_dataframe=None))]
    fn infer(
        &mut self,
        insert: Option<bool>,
        include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<HashMap<String, PyObject>>> {
        let res = self
            .inner
            .infer(insert.unwrap_or(true))
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
                (mappings, rdf_node_types) =
                    fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe);
                let pydf = df_to_py_df(mappings, rdf_node_types, None, include_datatypes, py)?;
                py_res.insert(nn.as_str().to_string(), pydf);
            }
            Ok(Some(py_res))
        } else {
            Ok(None)
        }
    }
}

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    enable_string_cache();
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
    py: Python<'_>,
) -> PyResult<PyObject> {
    if return_json {
        let json = res.json();
        return Ok(PyString::new(py, &json).into());
    }
    match res {
        SparqlQueryResult::Select(EagerSolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) => {
            (mappings, rdf_node_types) =
                fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe);
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
                            lit(rdf_named_node_to_polars_literal_value(&verb)).alias(VERB_COL_NAME),
                        )
                        .select([
                            col(SUBJECT_COL_NAME),
                            col(VERB_COL_NAME),
                            col(OBJECT_COL_NAME),
                        ])
                        .collect()
                        .unwrap();
                    rdf_node_types.insert(VERB_COL_NAME.to_string(), RDFNodeType::IRI);
                }
                (mappings, rdf_node_types) =
                    fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe);
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
                let t = v.as_rdf_node_type();
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

fn parse_optional_graph(graph: Option<String>) -> PyResult<Option<NamedNode>> {
    let graph = if let Some(graph) = graph {
        Some(NamedNode::new(graph).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
    } else {
        None
    };
    Ok(graph)
}

fn map_types(
    types: Option<HashMap<String, PyRDFType>>,
) -> Option<HashMap<String, MappingColumnType>> {
    if let Some(types) = types {
        let mut new_types = HashMap::new();
        for (k, v) in types {
            new_types.insert(k, MappingColumnType::Flat(v.as_rdf_node_type()));
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
                subject_type.as_rdf_node_type(),
            );
            types.insert(OBJECT_COL_NAME.to_string(), object_type.as_rdf_node_type());
            (df, types) = fix_cats_and_multicolumns(df, types, native_dataframe);
            let py_sm = df_to_py_df(df, types, None, include_datatypes, py)?;
            map.insert(predicate.as_str().to_string(), py_sm);
        }
    }
    Ok(map)
}
