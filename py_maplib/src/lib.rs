use polars::enable_string_cache;
extern crate core;

mod error;

use crate::error::PyMaplibError;
use pydf_io::to_rust::polars_df_to_rust_df;

use log::{info, warn};
use maplib::document::document_from_str;
use maplib::errors::MaplibError;
use maplib::mapping::errors::MappingError;
use maplib::mapping::ExpandOptions as RustExpandOptions;
use maplib::mapping::Mapping as InnerMapping;
use maplib::templates::TemplateDataset;
use pydf_io::to_python::df_to_py_df;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use triplestore::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use triplestore::sparql::{QueryResult as SparqlQueryResult, QueryResult};

//The below snippet controlling alloc-library is from https://github.com/pola-rs/polars/blob/main/py-polars/src/lib.rs
//And has a MIT license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
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
use polars::prelude::{col, DataFrame, IntoLazy, lit};
use pyo3::types::PyList;
use representation::formatting::format_iris_and_blank_nodes;
use representation::multitype::{
    compress_actual_multitypes, lf_column_from_categorical, multi_columns_to_string_cols,
};
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::python::RDFType;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeType};

#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[pyclass]
#[derive(Debug, Clone)]
pub struct ValidationReport {
    #[pyo3(get)]
    pub conforms: bool,
    #[pyo3(get)]
    pub report: Option<PyObject>,
    #[pyo3(get)]
    pub details: Option<PyObject>,
}

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

type ParametersType<'a> = HashMap<String, (Bound<'a, PyAny>, HashMap<String, RDFType>)>;


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
        let template_dataset = TemplateDataset::new(parsed_documents)
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
        template: &str,
        df: Option<&Bound<'_, PyAny>>,
        unique_subset: Option<Vec<String>>,
    ) -> PyResult<Option<PyObject>> {
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
                    .expand(template, Some(df), options.to_rust_expand_options())
                    .map_err(MaplibError::from)
                    .map_err(PyMaplibError::from)?;
            } else {
                warn!("Template expansion of {} with empty DataFrame", template);
            }
        } else {
            let _report = self
                .inner
                .expand(template, None, options.to_rust_expand_options())
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
        include_datatypes: Option<bool>,
        multi_as_strings: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<PyObject> {
        let graph = parse_optional_graph(graph)?;
        let mapped_parameters = map_parameters(parameters)?;
        let res = self
            .inner
            .query(&query, &mapped_parameters, graph)
            .map_err(PyMaplibError::from)?;
        query_to_result(res, multi_as_strings.unwrap_or(true), py)
    }

    fn validate(
        &mut self,
        py: Python<'_>,
        shape_graph: String,
        multi_as_strings: Option<bool>,
        include_details: Option<bool>,
    ) -> PyResult<ValidationReport> {
        let shape_graph = NamedNode::new(shape_graph).map_err(PyMaplibError::from)?;
        let shacl::ValidationReport {
            conforms,
            df,
            rdf_node_types,
            details,
        } = self
            .inner
            .validate(&shape_graph, include_details.unwrap_or(false))
            .map_err(PyMaplibError::from)?;
        finish_report(conforms, df, rdf_node_types, multi_as_strings, details, py)
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
            .read_triples(path, format, base_iri, transient.unwrap_or(false), graph)
            .map_err(|x| PyMaplibError::from(x))?;
        Ok(())
    }

    fn read_triples_string(
        &mut self,
        s: &str,
        format: &str,
        base_iri: Option<String>,
        transient: Option<bool>,
        graph: Option<String>,
    ) -> PyResult<()> {
        let graph = parse_optional_graph(graph)?;
        let format = resolve_format(&format);
        self.inner
            .read_triples_string(s, format, base_iri, transient.unwrap_or(false), graph)
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

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    enable_string_cache();
    m.add_class::<Mapping>()?;
    m.add_class::<ValidationReport>()?;
    m.add_class::<RDFType>()?;
    Ok(())
}

fn finish_report(
    conforms: bool,
    df: Option<DataFrame>,
    rdf_node_types: Option<HashMap<String, RDFNodeType>>,
    multi_as_strings: Option<bool>,
    details: Option<EagerSolutionMappings>,
    py: Python<'_>,
) -> PyResult<ValidationReport> {
    let report = if let Some(mut df) = df {
        (df, _) = fix_cats_and_multicolumns(
            df,
            rdf_node_types.unwrap(),
            multi_as_strings.unwrap_or(true),
        );
        Some(df_to_py_df(df, HashMap::new(), py)?)
    } else {
        None
    };

    let details = if let Some(EagerSolutionMappings {
        mut mappings,
        rdf_node_types,
    }) = details
    {
        (mappings, _) =
            fix_cats_and_multicolumns(mappings, rdf_node_types, multi_as_strings.unwrap_or(true));
        Some(df_to_py_df(mappings, HashMap::new(), py)?)
    } else {
        None
    };

    Ok(ValidationReport {
        conforms,
        report,
        details,
    })
}

fn fix_cats_and_multicolumns(
    mut df: DataFrame,
    mut dts: HashMap<String, RDFNodeType>,
    multi_to_strings: bool,
) -> (DataFrame, HashMap<String, RDFNodeType>) {
    let column_ordering: Vec<_> = df
        .get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect();
    //Important that column compression happen before decisions are made based on column type.
    (df, dts) = compress_actual_multitypes(df, dts);
    let mut lf = df.lazy();
    for (c, _) in &dts {
        lf = lf_column_from_categorical(lf.lazy(), c, &dts);
    }
    lf = format_iris_and_blank_nodes(lf, &dts, !multi_to_strings);
    df = lf.collect().unwrap();
    if multi_to_strings {
        df = multi_columns_to_string_cols(df.lazy(), &dts)
            .collect()
            .unwrap();
    }
    df = df.select(column_ordering.as_slice()).unwrap();
    (df, dts)
}

fn query_to_result(
    res: SparqlQueryResult,
    multi_as_strings: bool,
    py: Python<'_>,
) -> PyResult<PyObject> {
    match res {
        SparqlQueryResult::Select(mut df, mut datatypes) => {
            (df, datatypes) = fix_cats_and_multicolumns(df, datatypes, multi_as_strings);
            let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (mut df, mut datatypes) in dfs {
                (df, datatypes) = fix_cats_and_multicolumns(df, datatypes, multi_as_strings);
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
    parameters: Option<HashMap<String, (Bound<'_, PyAny>, HashMap<String, RDFType>),>>,
) -> PyResult<Option<HashMap<String, EagerSolutionMappings>>> {
    if let Some(parameters) = parameters {
        let mut mapped_parameters = HashMap::new();
        for (k, (pydf, map)) in parameters {
            let df = polars_df_to_rust_df(&pydf)?;
            let mut rdf_node_types = HashMap::new();
            let mut lf = df.lazy();
            for (k,v) in map {
                let t = v.to_rust().unwrap();
                match &t {
                    BaseRDFNodeType::IRI => {
                        lf = lf.with_column(col(&k).str().strip_prefix(lit("<")).str().strip_suffix(lit(">")));
                    }
                    BaseRDFNodeType::BlankNode => {
                        lf = lf.with_column(col(&k).str().strip_prefix(lit("_:")));
                    }
                    _ => {}
                }
                rdf_node_types.insert(k, t.as_rdf_node_type());
            }
            let m = EagerSolutionMappings {
                mappings: lf.collect().unwrap(),
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
