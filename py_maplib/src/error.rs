// Adapted from: https://raw.githubusercontent.com/pola-rs/polars/master/py-polars/src/error.rs
// Original licence:
//
// Copyright (c) 2020 Ritchie Vink
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

use maplib::errors::MaplibError;
use maplib::mapping::errors::MappingError;
use oxrdf::IriParseError;
use polars::prelude::PolarsError;
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use shacl::errors::ShaclError;
use std::fmt::Debug;
use templates::dataset::errors::TemplateError;
use thiserror::Error;
use triplestore::errors::TriplestoreError;
use triplestore::sparql::errors::SparqlError;

#[derive(Error, Debug)]
pub enum PyMaplibError {
    #[error(transparent)]
    MaplibError(#[from] MaplibError),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error(transparent)]
    SparqlError(#[from] SparqlError),
    #[error(transparent)]
    TemplateError(#[from] TemplateError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
    #[error(transparent)]
    TriplestoreError(#[from] TriplestoreError),
    #[error(transparent)]
    ShaclError(#[from] ShaclError),
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error("Function argument error: `{0}`")]
    FunctionArgumentError(String),
}

impl std::convert::From<PyMaplibError> for PyErr {
    fn from(err: PyMaplibError) -> PyErr {
        match &err {
            PyMaplibError::MaplibError(err) => MaplibErrorException::new_err(format!("{}", err)),
            PyMaplibError::PolarsError(err) => PolarsErrorException::new_err(format!("{}", err)),
            PyMaplibError::SparqlError(err) => SparqlErrorException::new_err(format!("{}", err)),
            PyMaplibError::TemplateError(err) => {
                TemplateErrorException::new_err(format!("{}", err))
            }
            PyMaplibError::MappingError(err) => MappingErrorException::new_err(format!("{}", err)),
            PyMaplibError::TriplestoreError(err) => {
                TriplestoreErrorException::new_err(format!("{}", err))
            }
            PyMaplibError::ShaclError(err) => ShaclErrorException::new_err(format!("{}", err)),
            PyMaplibError::IriParseError(err) => {
                IriParseErrorException::new_err(format!("{}", err))
            }
            PyMaplibError::FunctionArgumentError(s) => {
                FunctionArgumentErrorException::new_err(s.clone())
            }
        }
    }
}

create_exception!(exceptions, MaplibErrorException, PyException);
create_exception!(exceptions, PolarsErrorException, PyException);
create_exception!(exceptions, MappingErrorException, PyException);
create_exception!(exceptions, SparqlErrorException, PyException);
create_exception!(exceptions, TemplateErrorException, PyException);
create_exception!(exceptions, TriplestoreErrorException, PyException);
create_exception!(exceptions, ShaclErrorException, PyException);
create_exception!(exceptions, IriParseErrorException, PyException);
create_exception!(exceptions, FunctionArgumentErrorException, PyException);
