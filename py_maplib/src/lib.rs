extern crate core;

mod error;

use crate::error::PyMaplibError;
use pydf_io::to_rust::polars_df_to_rust_df;

use pydf_io::to_python::df_to_py_df;
use log::warn;
use maplib::document::document_from_str;
use maplib::errors::MaplibError;
use maplib::mapping::errors::MappingError;
use maplib::mapping::ExpandOptions as RustExpandOptions;
use maplib::mapping::Mapping as InnerMapping;
use maplib::templates::TemplateDataset;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use triplestore::sparql::QueryResult as SparqlQueryResult;

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
use polars_core::frame::DataFrame;
use polars_lazy::frame::IntoLazy;
use pyo3::types::PyList;
use representation::RDFNodeType;
use representation::multitype::multi_col_to_string_col;

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
#[derive(Debug, Clone)]
pub struct QueryResult {
    #[pyo3(get)]
    pub df: PyObject,
    #[pyo3(get)]
    pub types: HashMap<String, String>,
}

#[pyclass]
pub struct Mapping {
    inner: InnerMapping,
}

impl Mapping {
    pub fn from_inner_mapping(inner: InnerMapping) -> Mapping {
        Mapping { inner }
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
    /// Create a new Mapping object from a stOTTR document (a string) or list of stOTTR documents.
    /// Usage:
    /// import polars as pl
    /// from maplib import Mapping
    /// doc = """
    ///     @prefix ex:<http://example.net/ns#>.
    ///     ex:ExampleTemplate [?MyValue] :: {
    ///     ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    ///     } .
    ///     """
    /// m = Mapping(doc)
    /// m.expand("ex:ExampleTemplate", df)
    /// Optionally, a caching folder (a string or Path) can be specified which offloads the constructed graph to disk in Parquet format.
    #[new]
    #[pyo3(signature=(/,documents=None,caching_folder=None),
    text_signature="(documents:Union[str,List[str]]=None, caching_folder:str=None))")]
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
        })
    }

    /// Expand a template (prefix:name or full IRI) using a DataFrame where the columns have the same names as the template arguments.
    /// Usage:
    /// m.expand("ex:ExampleTemplate", df)
    /// DataFrame columns known to be unique may be specified, e.g. unique_subsets=["colA", "colB"], for a performance boost (reduce costly deduplication)
    /// Usage:
    /// m.expand("ex:ExampleTemplate", df, unique_subsets=["MyValue"])
    /// If the template has no arguments, the df argument is not necessary.
    #[pyo3(signature = (template, /, df=None, unique_subset=None),
        text_signature = "(template:str, df:DataFrame=None, unique_subset:List[str]=None)"
    )]
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
        let options = ExpandOptions {
            unique_subsets,
        };

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

    /// Create a default template and expand it based on a dataframe.
    /// The primary key column must be specified
    /// The types of other columns are inferred.
    /// Other columns which are URI-columns can also be specified, otherwise they are assumed to be xsd:string
    /// Usage:
    /// template_string = m.expand_default(df, "myKeyCol", ["otherURICol1", "otherURICol1"])
    /// print(template_string)
    #[pyo3(signature = (df, primary_key_column, /, template_prefix=None, predicate_uri_prefix=None),
    text_signature = "(df:DataFrame, primary_key_column:str, template_prefix:str=None, predicate_uri_prefix:str=None)"
    )]
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

    /// Query the mapped knowledge graph using SPARQL
    /// Currently, SELECT, CONSTRUCT and INSERT are supported.
    /// res = mapping.query("""
    /// PREFIX ex:<http://example.net/ns#>
    /// SELECT ?obj1 ?obj2 WHERE {
    /// ?obj1 ex:hasObj ?obj2
    /// }""")
    /// print(res.df)
    #[pyo3(signature = (query),
    text_signature = "(query:str)"
    )]
    fn query(&mut self, py: Python<'_>, query: String) -> PyResult<PyObject> {
        let res = self
            .inner
            .triplestore
            .query(&query)
            .map_err(PyMaplibError::from)?;
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
                        ("subject".to_string(), subj_type),
                        ("object".to_string(), obj_type),
                    ]
                    .into();
                    let df = fix_multicolumns(df, &datatypes);
                    let pydf = df_to_py_df(df, dtypes_map(datatypes), py)?;
                    query_results.push(
                        pydf,
                    );
                }
                Ok(PyList::new(py, query_results).into())
            }
        }
    }

    /// Validate the contained knowledge graph using SHACL
    /// Assumes that the contained knowledge graph also contains SHACL Shapes.
    #[pyo3(signature = ())]
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

    /// Utility method for running a CONSTRUCT query as if it were an INSERT query.
    /// Useful when you want to first check the result of CONSTRUCT and then insert that exact result.
    /// Specify transient if you only want the results to be available for further querying and validation, but not persisted using write-methods.
    /// Usage:
    /// res = m.query(my_construct_query)
    /// print(res[0])
    /// # I am happy with these results!
    /// m.insert(my_construct_query, transient=True)
    #[pyo3(signature = (query,/, transient=false),
    text_signature = "(query:str, transient:bool=False)"
    )]
    fn insert(&mut self, query: String, transient: Option<bool>) -> PyResult<()> {
        self.inner
            .triplestore
            .insert(&query, transient.unwrap_or(false))
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    /// Reads triples from a path.
    /// File format is derived using file extension, e.g. .ttl or .nt.
    /// Specify transient if you only want the triples to be available for further querying and validation, but not persisted using write-methods.
    /// Usage:
    /// m.read_triples("my_triples.ttl", transient=True)
    #[pyo3(signature = (file_path, /,base_iri=None, transient=false),
    text_signature = "(file_path:Union[str, Path], base_iri:str=None, transient:bool=False)"
    )]
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

    /// Write triples to an NTriples file.
    /// Will not write triples that have been added using transient=True
    /// Usage:
    /// m.write_ntriples("my_triples.nt")
    #[pyo3(signature = (file_path),
    text_signature = "(file_path:Union[str, Path])"
    )]
    fn write_ntriples(&mut self, file_path: &PyAny) -> PyResult<()> {
        let file_path = file_path.str()?.to_string();
        let path_buf = PathBuf::from(file_path);
        let mut actual_file = File::create(path_buf.as_path())
            .map_err(|x| PyMaplibError::from(MappingError::FileCreateIOError(x)))?;
        self.inner.write_n_triples(&mut actual_file).unwrap();
        Ok(())
    }

    /// Write triples using the internal native Parquet format.
    /// Will not write triples that have been added using transient=True
    /// Usage:
    /// m.write_native_parquet("native_parquet_path")
    #[pyo3(signature = (folder_path),
    text_signature = "(folder_path:Union[str,Path])"
    )]
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
    m.add_class::<QueryResult>()?;
    m.add_class::<ValidationReport>()?;
    Ok(())
}

fn fix_multicolumns(mut df: DataFrame, dts: &HashMap<String, RDFNodeType>) -> DataFrame {
    let mut lf = df.lazy();
    for (c, v) in dts {
        if v == &RDFNodeType::MultiType {
            lf = multi_col_to_string_col(lf, c);
        }
    }
    lf.collect().unwrap()
}

fn dtypes_map(map: HashMap<String, RDFNodeType>) -> HashMap<String, String> {
    map.into_iter().map(|(x, y)| (x, y.to_string())).collect()
}
