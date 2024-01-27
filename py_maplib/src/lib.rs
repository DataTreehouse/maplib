extern crate core;

mod error;

use crate::error::PyMaplibError;
use pydf_io::to_rust::polars_df_to_rust_df;

use log::warn;
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
use oxrdf::vocab::xsd;
use polars_core::frame::DataFrame;
use polars_lazy::frame::IntoLazy;
use pyo3::types::PyList;
use representation::multitype::multi_col_to_string_col;
use representation::polars_to_sparql::primitive_polars_type_to_literal_type;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;

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

#[pymethods]
impl Mapping {
    #[new]
    fn new(documents: Option<&PyAny>, caching_folder: Option<&PyAny>) -> PyResult<Mapping> {
        let documents = if let Some(documents) = documents {
            if documents.is_instance_of::<PyList>() {
                let mut strs = vec![];
                for doc in documents.iter()? {
                    strs.push(doc?.str()?.to_str()?);
                }
                Some(strs)
            } else {
                Some(vec![documents.str()?.to_str()?])
            }
        } else {
            None
        };
        let mut parsed_documents = vec![];
        if let Some(documents) = documents {
            for ds in documents {
                let parsed_doc = document_from_str(ds).map_err(PyMaplibError::from)?;
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
        df: Option<&PyAny>,
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
        df: &PyAny,
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
        parameters: Option<HashMap<String, &PyAny>>,
    ) -> PyResult<PyObject> {
        let mapped_parameters = map_parameters(parameters)?;
        let res = self
            .inner
            .triplestore
            .query(&query, &mapped_parameters)
            .map_err(PyMaplibError::from)?;
        query_to_result(res, py)
    }

    fn validate(&mut self, py: Python<'_>) -> PyResult<ValidationReport> {
        let shacl::ValidationReport { conforms, df } =
            self.inner.validate().map_err(PyMaplibError::from)?;

        let report = if let Some(df) = df {
            Some(df_to_py_df(df, HashMap::new(), py)?)
        } else {
            None
        };

        Ok(ValidationReport { conforms, report })
    }

    fn insert(&mut self, query: String, parameters: Option<HashMap<String, &PyAny>>, transient: Option<bool>) -> PyResult<()> {
        let mapped_parameters = map_parameters(parameters)?;
        self.inner
            .triplestore
            .insert(&query, &mapped_parameters, transient.unwrap_or(false))
            .map_err(PyMaplibError::from)?;
        if self.sprout.is_some() {
            self.sprout.as_mut().unwrap().blank_node_counter = self.inner.blank_node_counter;
        }
        Ok(())
    }

    fn insert_sprout(&mut self, query: String, parameters: Option<HashMap<String, &PyAny>>, transient: Option<bool>) -> PyResult<()> {
        let mapped_parameters = map_parameters(parameters)?;
        if self.sprout.is_none() {
            self.create_sprout()?;
        }
        let res = self
            .inner
            .triplestore
            .query(&query, &mapped_parameters)
            .map_err(PyMaplibError::from)?;
        if let QueryResult::Construct(dfs_and_dts) = res {
            self.sprout
                .as_mut()
                .unwrap()
                .triplestore
                .insert_construct_result(dfs_and_dts, transient.unwrap_or(false))
                .map_err(PyMaplibError::from)?;
        } else {
            todo!("Handle this error..")
        }
        self.inner.blank_node_counter = self.sprout.as_ref().unwrap().blank_node_counter;

        Ok(())
    }

    fn read_triples(
        &mut self,
        file_path: &PyAny,
        base_iri: Option<String>,
        transient: Option<bool>,
    ) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let path = Path::new(&file_path);
        self.inner
            .read_triples(path, base_iri, transient.unwrap_or(false))
            .map_err(|x| PyMaplibError::from(x))?;
        Ok(())
    }

    fn write_ntriples(&mut self, file_path: &PyAny) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let path_buf = PathBuf::from(file_path);
        let mut actual_file = File::create(path_buf.as_path())
            .map_err(|x| PyMaplibError::from(MappingError::FileCreateIOError(x)))?;
        self.inner.write_n_triples(&mut actual_file).unwrap();
        Ok(())
    }

    fn write_native_parquet(&mut self, folder_path: &PyAny) -> PyResult<()> {
        let folder_path = folder_path.str()?.to_string();
        self.inner
            .write_native_parquet(&folder_path)
            .map_err(|x| PyMaplibError::MappingError(x))?;
        Ok(())
    }
}

#[pymodule]
fn _maplib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Mapping>()?;
    m.add_class::<ValidationReport>()?;
    Ok(())
}

fn fix_multicolumns(df: DataFrame, dts: &HashMap<String, RDFNodeType>) -> DataFrame {
    let mut lf = df.lazy();
    for (c, v) in dts {
        if v == &RDFNodeType::MultiType {
            lf = multi_col_to_string_col(lf, c);
        }
    }
    lf.collect().unwrap()
}

fn query_to_result(res: SparqlQueryResult, py: Python<'_>) -> PyResult<PyObject> {
    match res {
        SparqlQueryResult::Select(mut df, datatypes) => {
            df = fix_multicolumns(df, &datatypes);
            let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (df, subj_type, obj_type) in dfs {
                let datatypes: HashMap<_, _> = [
                    (SUBJECT_COL_NAME.to_string(), subj_type),
                    (OBJECT_COL_NAME.to_string(), obj_type),
                ]
                .into();
                let df = fix_multicolumns(df, &datatypes);
                let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
                query_results.push(pydf);
            }
            Ok(PyList::new(py, query_results).into())
        }
    }
}

fn dtypes_map(map: HashMap<String, RDFNodeType>) -> HashMap<String, String> {
    map.into_iter().map(|(x, y)| (x, y.to_string())).collect()
}

fn map_parameters(
    parameters: Option<HashMap<String, &PyAny>>,
) -> PyResult<Option<HashMap<String, EagerSolutionMappings>>> {
    if let Some(parameters) = parameters {
        let mut mapped_parameters = HashMap::new();
        for (k, pydf) in parameters {
            let mut rdf_node_types = HashMap::new();
            let df = polars_df_to_rust_df(pydf)?;
            let names = df.get_column_names();
            for c in df.columns(names).unwrap() {
                let dt = primitive_polars_type_to_literal_type(c.dtype()).unwrap();

                let mut rdf_node_type = None;

                if dt == xsd::STRING {
                    let ch = c.utf8().unwrap();
                    if let Some(s) = ch.first_non_null() {
                        let f = ch.get(s).unwrap();
                        if f.starts_with("<") {
                            rdf_node_type = Some(RDFNodeType::IRI);
                        }
                    }
                }

                if rdf_node_type.is_none() {
                    rdf_node_type = Some(RDFNodeType::Literal(dt.into_owned()))
                }
                rdf_node_types.insert(c.name().to_string(), rdf_node_type.unwrap());
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
