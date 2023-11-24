extern crate core;

mod error;

use crate::error::PyMaplibError;
use arrow_python_utils::to_rust::polars_df_to_rust_df;

use arrow_python_utils::to_python::{df_to_py_df, df_vec_to_py_df_list};
use log::warn;
use maplib::document::document_from_str;
use maplib::errors::MaplibError;
use maplib::mapping::errors::MappingError;
use maplib::mapping::ExpandOptions as RustExpandOptions;
use maplib::mapping::Mapping as InnerMapping;
use maplib::templates::TemplateDataset;
use oxrdf::NamedNode;
use pyo3::basic::CompareOp;
use pyo3::prelude::PyModule;
use pyo3::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use triplestore::sparql::QueryResult;

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
use polars_core::prelude::{DataType, NamedFrom};
use triplestore::sparql::multitype::{multi_series_to_string_series, MULTI_TYPE_NAME};

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
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct BlankNode {
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl BlankNode {
    fn __repr__(&self) -> String {
        format!("_:{}", self.name)
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct IRI {
    #[pyo3(get)]
    pub iri: String,
}

#[pymethods]
impl IRI {
    fn __repr__(&self) -> String {
        format!("<{}>", self.iri)
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct TripleSubject {
    #[pyo3(get)]
    pub iri: Option<IRI>,
    #[pyo3(get)]
    pub blank_node: Option<BlankNode>,
}

impl TripleSubject {
    pub fn __repr__(&self) -> String {
        if let Some(iri) = &self.iri {
            iri.__repr__()
        } else if let Some(blank_node) = &self.blank_node {
            blank_node.__repr__()
        } else {
            panic!("TripleSubject in invalid state: {:?}", self);
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct Literal {
    #[pyo3(get)]
    pub lexical_form: String,
    #[pyo3(get)]
    pub language_tag: Option<String>,
    #[pyo3(get)]
    pub datatype_iri: Option<IRI>,
}

#[pymethods]
impl Literal {
    pub fn __repr__(&self) -> String {
        if let Some(tag) = &self.language_tag {
            format!("\"{}\"@{}", self.lexical_form.to_owned(), tag)
        } else if let Some(dt) = &self.datatype_iri {
            format!("\"{}\"^^{}", &self.lexical_form, dt.__repr__())
        } else {
            panic!("Literal in invalid state {:?}", self)
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct TripleObject {
    #[pyo3(get)]
    pub iri: Option<IRI>,
    #[pyo3(get)]
    pub blank_node: Option<BlankNode>,
    #[pyo3(get)]
    pub literal: Option<Literal>,
}

#[pymethods]
impl TripleObject {
    pub fn __repr__(&self) -> String {
        if let Some(iri) = &self.iri {
            iri.__repr__()
        } else if let Some(blank_node) = &self.blank_node {
            blank_node.__repr__()
        } else if let Some(literal) = &self.literal {
            literal.__repr__()
        } else {
            panic!("TripleObject in invalid state: {:?}", self);
        }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self < other),
            CompareOp::Le => Ok(self <= other),
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            CompareOp::Gt => Ok(self > other),
            CompareOp::Ge => Ok(self >= other),
        }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct Triple {
    #[pyo3(get)]
    pub subject: TripleSubject,
    #[pyo3(get)]
    pub verb: IRI,
    #[pyo3(get)]
    pub object: TripleObject,
}

#[pymethods]
impl Triple {
    pub fn __repr__(&self) -> String {
        format!(
            "{} {} {}",
            self.subject.__repr__(),
            self.verb.__repr__(),
            self.object.__repr__()
        )
    }
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
    pub language_tags: Option<HashMap<String, String>>,
    pub unique_subsets: Option<Vec<Vec<String>>>,
    pub caching_folder: Option<String>,
}

impl ExpandOptions {
    fn to_rust_expand_options(self) -> RustExpandOptions {
        RustExpandOptions {
            language_tags: self.language_tags,
            unique_subsets: self.unique_subsets,
        }
    }
}

#[pymethods]
impl Mapping {
    #[new]
    pub fn new(documents: Option<Vec<&str>>, caching_folder: Option<String>) -> PyResult<Mapping> {
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
        Ok(Mapping {
            inner: InnerMapping::new(&template_dataset, caching_folder)
                .map_err(PyMaplibError::from)?,
        })
    }

    #[pyo3(text_signature = "(template, df, unique_subset, language_tags, caching_folder)")]
    pub fn expand(
        &mut self,
        template: &str,
        df: Option<&PyAny>,
        unique_subset: Option<Vec<String>>,
        language_tags: Option<HashMap<String, String>>,
        caching_folder: Option<String>,
    ) -> PyResult<Option<PyObject>> {
        let unique_subsets = if let Some(unique_subset) = unique_subset {
            Some(vec![unique_subset.into_iter().collect()])
        } else {
            None
        };
        let options = ExpandOptions {
            language_tags,
            unique_subsets,
            caching_folder,
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

    #[pyo3(
        text_signature = "(template, primary_key_column, foreign_key_column, template_prefix, predicate_uri_prefix, language_tags, caching_folder)"
    )]
    pub fn expand_default(
        &mut self,
        df: &PyAny,
        primary_key_column: String,
        foreign_key_columns: Option<Vec<String>>,
        template_prefix: Option<String>,
        predicate_uri_prefix: Option<String>,
        language_tags: Option<HashMap<String, String>>,
        caching_folder: Option<String>,
    ) -> PyResult<String> {
        let df = polars_df_to_rust_df(&df)?;
        let options = ExpandOptions {
            language_tags,
            unique_subsets: Some(vec![vec![primary_key_column.clone()]]),
            caching_folder,
        };

        let fk_cols = if let Some(fk_cols) = foreign_key_columns {
            fk_cols
        } else {
            vec![]
        };

        let tmpl = self
            .inner
            .expand_default(
                df,
                primary_key_column,
                fk_cols,
                template_prefix,
                predicate_uri_prefix,
                options.to_rust_expand_options(),
            )
            .map_err(MaplibError::from)
            .map_err(PyMaplibError::from)?;
        return Ok(format!("{}", tmpl));
    }

    #[pyo3(text_signature = "(query)")]
    pub fn query(&mut self, py: Python<'_>, query: String) -> PyResult<PyObject> {
        let res = self
            .inner
            .triplestore
            .query(&query)
            .map_err(PyMaplibError::from)?;
        match res {
            QueryResult::Select(mut df, datatypes) => {
                df = fix_multicolumns(df);
                df_to_py_df(df, py)
            },
            QueryResult::Construct(dfs) => {
                let dfs = dfs.into_iter().map(|(df, subj_type, obj_type)| fix_multicolumns(df)).collect();
                Ok(df_vec_to_py_df_list(dfs, py)?.into())
            }
        }
    }

    #[pyo3(text_signature = "()")]
    pub fn validate(&mut self, py: Python<'_>) -> PyResult<ValidationReport> {
        let shacl::ValidationReport{ conforms, df } = self
            .inner
            .validate()
            .map_err(PyMaplibError::from)?;

        let report = if let Some(df) = df {
            Some(df_to_py_df(df, py)?)
        } else {
            None
        };

        Ok(ValidationReport{
            conforms,
            report
        })
    }

    #[pyo3(text_signature = "(query, transient)")]
    pub fn insert(&mut self, query: String, transient:Option<bool>) -> PyResult<()> {
        self.inner
            .triplestore
            .insert(&query, transient.unwrap_or(false))
            .map_err(PyMaplibError::from)?;
        Ok(())
    }

    pub fn to_triples(&mut self) -> PyResult<Vec<Triple>> {
        let mut triples = vec![];

        fn create_subject(s: &str) -> TripleSubject {
            if is_blank_node(s) {
                TripleSubject {
                    iri: None,
                    blank_node: Some(BlankNode {
                        name: s.to_string(),
                    }),
                }
            } else {
                TripleSubject {
                    iri: Some(IRI { iri: s.to_string() }),
                    blank_node: None,
                }
            }
        }
        fn create_nonliteral_object(s: &str) -> TripleObject {
            if is_blank_node(s) {
                TripleObject {
                    iri: None,
                    blank_node: Some(BlankNode {
                        name: s.to_string(),
                    }),
                    literal: None,
                }
            } else {
                TripleObject {
                    iri: Some(IRI { iri: s.to_string() }),
                    blank_node: None,
                    literal: None,
                }
            }
        }
        fn create_literal(lex: &str, ltag_opt: Option<&str>, dt: Option<&str>) -> Literal {
            Literal {
                lexical_form: lex.to_string(),
                language_tag: if let Some(ltag) = ltag_opt {
                    Some(ltag.to_string())
                } else {
                    None
                },
                datatype_iri: if let Some(dt) = dt {
                    Some(IRI {
                        iri: dt.to_string(),
                    })
                } else {
                    None
                },
            }
        }

        fn to_python_object_triple(s: &str, v: &str, o: &str) -> Triple {
            let subject = create_subject(s);
            let verb = IRI { iri: v.to_string() };
            let object = create_nonliteral_object(o);
            Triple {
                subject,
                verb,
                object,
            }
        }
        fn to_python_string_literal_triple(
            s: &str,
            v: &str,
            lex: &str,
            ltag_opt: Option<&str>,
        ) -> Triple {
            let subject = create_subject(s);
            let verb = IRI { iri: v.to_string() };
            let literal = create_literal(lex, ltag_opt, None);
            let object = TripleObject {
                iri: None,
                blank_node: None,
                literal: Some(literal),
            };
            Triple {
                subject,
                verb,
                object,
            }
        }

        fn to_python_nonstring_literal_triple(
            s: &str,
            v: &str,
            lex: &str,
            dt: &NamedNode,
        ) -> Triple {
            let subject = create_subject(s);
            let verb = IRI { iri: v.to_string() };
            let literal = create_literal(lex, None, Some(dt.as_str()));
            let object = TripleObject {
                iri: None,
                blank_node: None,
                literal: Some(literal),
            };
            Triple {
                subject,
                verb,
                object,
            }
        }

        self.inner
            .triplestore
            .deduplicate()
            .map_err(PyMaplibError::from)?;
        self.inner
            .triplestore
            .object_property_triples(to_python_object_triple, &mut triples)
            .map_err(PyMaplibError::from)?;
        self.inner
            .triplestore
            .string_data_property_triples(to_python_string_literal_triple, &mut triples)
            .map_err(PyMaplibError::from)?;
        self.inner
            .triplestore
            .nonstring_data_property_triples(to_python_nonstring_literal_triple, &mut triples)
            .map_err(PyMaplibError::from)?;
        Ok(triples)
    }

    #[pyo3(text_signature = "(file_path, base_iri)")]
    pub fn read_triples(&mut self, file_path:&str, base_iri:Option<String>) -> PyResult<()> {
        let path = Path::new(file_path);
        self.inner.read_triples(path, base_iri).map_err(|x| PyMaplibError::from(x))?;
        Ok(())
    }

    #[pyo3(text_signature = "(file_path)")]
    pub fn write_ntriples(&mut self, file_path: &str) -> PyResult<()> {
        let path_buf = PathBuf::from(file_path);
        let mut actual_file = File::create(path_buf.as_path())
            .map_err(|x| PyMaplibError::from(MappingError::FileCreateIOError(x)))?;
        self.inner.write_n_triples(&mut actual_file).unwrap();
        Ok(())
    }

    #[pyo3(text_signature = "(folder_path)")]
    pub fn write_native_parquet(&mut self, path: &str) -> PyResult<()> {
        self.inner
            .write_native_parquet(path)
            .map_err(|x| PyMaplibError::MappingError(x))?;
        Ok(())
    }
}

#[pymodule]
fn _maplib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Mapping>()?;

    Ok(())
}

fn is_blank_node(s: &str) -> bool {
    s.starts_with("_:")
}

fn fix_multicolumns(mut df: DataFrame) -> DataFrame {
    let columns:Vec<_> = df.get_column_names().iter().map(|x|x.to_string()).collect();
    for c in columns {
        if df.column(&c).unwrap().dtype() == &DataType::Object(MULTI_TYPE_NAME) {
            let ser = df.column(&c).unwrap();
            let new_ser = multi_series_to_string_series(ser);
            df.with_column(new_ser).unwrap();
        }
    }
    df
}