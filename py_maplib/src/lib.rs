use polars::enable_string_cache;
extern crate core;

mod error;
mod shacl;

use crate::error::PyMaplibError;
use pydf_io::to_rust::polars_df_to_rust_df;

use crate::shacl::ValidationReport;
use log::warn;
use maplib::errors::MaplibError;
use maplib::mapping::errors::MappingError;
use maplib::mapping::ExpandOptions as RustExpandOptions;
use maplib::mapping::Mapping as InnerMapping;
use pydf_io::to_python::{df_to_py_df, fix_cats_and_multicolumns};
use pyo3::prelude::*;
use std::collections::HashMap;
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
use oxrdf::vocab::rdf;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use pyo3::types::PyList;
use representation::python::PyRDFType;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;

#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

use templates::python::{
    py_triple, PyArgument, PyIRI, PyInstance, PyLiteral, PyParameter, PyPrefix, PyTemplate,
    PyVariable, PyXSD,
};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[pyclass]
pub struct Mapping {
    inner: InnerMapping,
    sprout: Option<InnerMapping>,
}

impl Mapping {
    pub fn from_inner_mapping(inner: InnerMapping) -> Mapping {
        Mapping {
            inner,
            sprout: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpandOptions {
    pub unique_subsets: Option<Vec<Vec<String>>>,
}

impl ExpandOptions {
    fn to_rust_expand_options(self) -> RustExpandOptions {
        RustExpandOptions {
            unique_subsets: self.unique_subsets,
        }
    }
}

type ParametersType<'a> = HashMap<String, (Bound<'a, PyAny>, HashMap<String, PyRDFType>)>;

#[pymethods]
impl Mapping {
    #[new]
    fn new(
        documents: Option<&Bound<'_, PyAny>>,
        caching_folder: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Mapping> {
        let documents = if let Some(documents) = documents {
            if documents.is_instance_of::<PyList>() {
                let mut strs = vec![];
                for doc in documents.iter()? {
                    let docstr = doc?.str()?.to_str()?.to_string();
                    strs.push(docstr);
                }
                Some(strs)
            } else {
                let docstr = documents.str()?.to_str()?.to_string();
                Some(vec![docstr])
            }
        } else {
            None
        };
        let mut parsed_documents = vec![];
        if let Some(documents) = documents {
            for ds in documents {
                let parsed_doc = document_from_str(&ds).map_err(PyMaplibError::from)?;
                parsed_documents.push(parsed_doc);
            }
        }
        let template_dataset = TemplateDataset::from_documents(parsed_documents)
            .map_err(MaplibError::from)
            .map_err(PyMaplibError::from)?;
        let caching_folder = if let Some(c) = caching_folder {
            Some(c.str()?.to_string())
        } else {
            None
        };
        Ok(Mapping {
            inner: InnerMapping::new(&template_dataset, caching_folder)
                .map_err(PyMaplibError::from)?,
            sprout: None,
        })
    }

    fn add_template(&mut self, template: PyTemplate) -> PyResult<()> {
        self.inner
            .add_template(template.into_inner())
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    fn create_sprout(&mut self) -> PyResult<()> {
        let mut sprout =
            InnerMapping::new(&self.inner.template_dataset, None).map_err(PyMaplibError::from)?;
        sprout.blank_node_counter = self.inner.blank_node_counter;
        self.sprout = Some(sprout);
        Ok(())
    }

    fn detach_sprout(&mut self) -> PyResult<Option<Mapping>> {
        if let Some(sprout) = self.sprout.take() {
            let m = Mapping {
                inner: sprout,
                sprout: None,
            };
            Ok(Some(m))
        } else {
            Ok(None)
        }
    }

    fn expand(
        &mut self,
        template: &Bound<'_, PyAny>,
        df: Option<&Bound<'_, PyAny>>,
        unique_subset: Option<Vec<String>>,
    ) -> PyResult<Option<PyObject>> {
        let template = if let Ok(i) = template.extract::<PyIRI>() {
            i.into_inner().to_string()
        } else if let Ok(t) = template.extract::<PyTemplate>() {
            let t_string = t.template.signature.template_name.as_str().to_string();
            self.add_template(t)?;
            t_string
        } else if let Ok(s) = template.extract::<String>() {
            s
        } else {
            return Err(PyMaplibError::FunctionArgumentError(
                "Template must be IRI or str".to_string(),
            )
            .into());
        };

        let unique_subsets = if let Some(unique_subset) = unique_subset {
            Some(vec![unique_subset.into_iter().collect()])
        } else {
            None
        };
        let options = ExpandOptions { unique_subsets };

        if let Some(df) = df {
            if df.getattr("height")?.gt(0).unwrap() {
                let df = polars_df_to_rust_df(&df)?;

                let _report = self
                    .inner
                    .expand(&template, Some(df), None, options.to_rust_expand_options())
                    .map_err(MaplibError::from)
                    .map_err(PyMaplibError::from)?;
            } else {
                warn!("Template expansion of {} with empty DataFrame", template);
            }
        } else {
            let _report = self
                .inner
                .expand(&template, None, None, options.to_rust_expand_options())
                .map_err(MaplibError::from)
                .map_err(PyMaplibError::from)?;
        }

        Ok(None)
    }

    fn expand_default(
        &mut self,
        df: &Bound<'_, PyAny>,
        primary_key_column: String,
        template_prefix: Option<String>,
        predicate_uri_prefix: Option<String>,
    ) -> PyResult<String> {
        let df = polars_df_to_rust_df(&df)?;
        let options = ExpandOptions {
            unique_subsets: Some(vec![vec![primary_key_column.clone()]]),
        };

        let tmpl = self
            .inner
            .expand_default(
                df,
                primary_key_column,
                vec![],
                template_prefix,
                predicate_uri_prefix,
                options.to_rust_expand_options(),
            )
            .map_err(MaplibError::from)
            .map_err(PyMaplibError::from)?;
        return Ok(format!("{}", tmpl));
    }

    fn query(
        &mut self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        _include_datatypes: Option<bool>,
        native_dataframe: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<PyObject> {
        let graph = parse_optional_graph(graph)?;
        let mapped_parameters = map_parameters(parameters)?;
        let res = self
            .inner
            .query(&query, &mapped_parameters, graph)
            .map_err(PyMaplibError::from)?;
        query_to_result(res, native_dataframe.unwrap_or(false), py)
    }

    fn validate(
        &mut self,
        shape_graph: String,
        include_details: Option<bool>,
        include_conforms: Option<bool>,
    ) -> PyResult<ValidationReport> {
        let shape_graph = NamedNode::new(shape_graph).map_err(PyMaplibError::from)?;
        let report = self
            .inner
            .validate(
                &shape_graph,
                include_details.unwrap_or(false),
                include_conforms.unwrap_or(false),
            )
            .map_err(PyMaplibError::from)?;
        Ok(ValidationReport::new(report))
    }

    fn insert(
        &mut self,
        query: String,
        parameters: Option<ParametersType>,
        transient: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
    ) -> PyResult<()> {
        let mapped_parameters = map_parameters(parameters)?;
        let source_graph = parse_optional_graph(source_graph)?;
        let target_graph = parse_optional_graph(target_graph)?;
        let res = self
            .inner
            .query(&query, &mapped_parameters, source_graph)
            .map_err(PyMaplibError::from)?;
        if let QueryResult::Construct(dfs_and_dts) = res {
            self.inner
                .insert_construct_result(dfs_and_dts, transient.unwrap_or(false), target_graph)
                .map_err(PyMaplibError::from)?;
        } else {
            todo!("Handle this error..")
        }
        if self.sprout.is_some() {
            self.sprout.as_mut().unwrap().blank_node_counter = self.inner.blank_node_counter;
        }
        Ok(())
    }

    fn insert_sprout(
        &mut self,
        query: String,
        parameters: Option<ParametersType>,
        transient: Option<bool>,
        source_graph: Option<String>,
        target_graph: Option<String>,
    ) -> PyResult<()> {
        let mapped_parameters = map_parameters(parameters)?;
        let source_graph = parse_optional_graph(source_graph)?;
        let target_graph = parse_optional_graph(target_graph)?;
        if self.sprout.is_none() {
            self.create_sprout()?;
        }
        let res = self
            .inner
            .query(&query, &mapped_parameters, source_graph)
            .map_err(PyMaplibError::from)?;
        if let QueryResult::Construct(dfs_and_dts) = res {
            self.sprout
                .as_mut()
                .unwrap()
                .insert_construct_result(dfs_and_dts, transient.unwrap_or(false), target_graph)
                .map_err(PyMaplibError::from)?;
        } else {
            todo!("Handle this error..")
        }
        self.inner.blank_node_counter = self.sprout.as_ref().unwrap().blank_node_counter;

        Ok(())
    }

    fn read_triples(
        &mut self,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        deduplicate: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let file_path = file_path.str()?.to_string();
        let path = Path::new(&file_path);
        let format = if let Some(format) = format {
            Some(resolve_format(&format))
        } else {
            None
        };
        self.inner
            .read_triples(
                path,
                format,
                base_iri,
                transient.unwrap_or(false),
                parallel.unwrap_or(false),
                checked.unwrap_or(true),
                deduplicate.unwrap_or(true),
                graph,
            )
            .map_err(|x| PyMaplibError::from(x))?;
        Ok(())
    }

    fn read_triples_string(
        &mut self,
        s: &str,
        format: &str,
        base_iri: Option<String>,
        transient: Option<bool>,
        parallel: Option<bool>,
        checked: Option<bool>,
        deduplicate: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let format = resolve_format(&format);
        self.inner
            .read_triples_string(
                s,
                format,
                base_iri,
                transient.unwrap_or(false),
                parallel.unwrap_or(false),
                checked.unwrap_or(true),
                deduplicate.unwrap_or(true),
                graph,
            )
            .map_err(|x| PyMaplibError::from(x))?;
        Ok(())
    }

    fn write_ntriples(
        &mut self,
        file_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let path_buf = PathBuf::from(file_path);
        let mut actual_file = File::create(path_buf.as_path())
            .map_err(|x| PyMaplibError::from(MappingError::FileCreateIOError(x)))?;
        let graph = parse_optional_graph(graph)?;
        self.inner.write_n_triples(&mut actual_file, graph).unwrap();
        Ok(())
    }

    fn write_ntriples_string(&mut self, graph: Option<String>) -> PyResult<String> {
        let mut out = vec![];
        let graph = parse_optional_graph(graph)?;
        self.inner.write_n_triples(&mut out, graph).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    fn write_native_parquet(
        &mut self,
        folder_path: &Bound<'_, PyAny>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let folder_path = folder_path.str()?.to_string();
        let graph = parse_optional_graph(graph)?;
        self.inner
            .write_native_parquet(&folder_path, graph)
            .map_err(|x| PyMaplibError::MappingError(x))?;
        Ok(())
    }
}

#[pyfunction]
fn a() -> PyIRI {
    PyIRI::new(rdf::TYPE.as_str().to_string()).unwrap()
}

#[pyfunction]
fn xsd() -> PyXSD {
    PyXSD::new()
}

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    enable_string_cache();
    m.add_class::<Mapping>()?;
    m.add_class::<ValidationReport>()?;
    m.add_class::<PyRDFType>()?;
    m.add_class::<PyPrefix>()?;
    m.add_class::<PyVariable>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyIRI>()?;
    m.add_class::<PyParameter>()?;
    m.add_class::<PyArgument>()?;
    m.add_class::<PyTemplate>()?;
    m.add_class::<PyInstance>()?;
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add_function(wrap_pyfunction!(a, m)?)?;
    m.add_function(wrap_pyfunction!(xsd, m)?)?;
    Ok(())
}

fn query_to_result(
    res: SparqlQueryResult,
    native_dataframe: bool,
    py: Python<'_>,
) -> PyResult<PyObject> {
    match res {
        SparqlQueryResult::Select(mut df, mut datatypes) => {
            (df, datatypes) = fix_cats_and_multicolumns(df, datatypes, native_dataframe);
            let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (mut df, mut datatypes) in dfs {
                (df, datatypes) = fix_cats_and_multicolumns(df, datatypes, native_dataframe);
                let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
                query_results.push(pydf);
            }
            Ok(PyList::new_bound(py, query_results).into())
        }
    }
}

fn dtypes_map(map: HashMap<String, RDFNodeType>) -> HashMap<String, String> {
    map.into_iter().map(|(x, y)| (x, y.to_string())).collect()
}

fn map_parameters(
    parameters: Option<HashMap<String, (Bound<'_, PyAny>, HashMap<String, PyRDFType>)>>,
) -> PyResult<Option<HashMap<String, EagerSolutionMappings>>> {
    if let Some(parameters) = parameters {
        let mut mapped_parameters = HashMap::new();
        for (k, (pydf, map)) in parameters {
            let df = polars_df_to_rust_df(&pydf)?;
            let mut rdf_node_types = HashMap::new();
            for (k, v) in map {
                let t = v.as_rdf_node_type().unwrap();
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
        Some(NamedNode::new(graph).map_err(PyMaplibError::from)?)
    } else {
        None
    };
    Ok(graph)
}
