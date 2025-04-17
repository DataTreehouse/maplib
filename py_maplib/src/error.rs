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
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum PyMaplibError {
    #[error(transparent)]
    MaplibError(#[from] MaplibError),
    #[error("Function argument error: `{0}`")]
    FunctionArgumentError(String),
}

impl std::convert::From<PyMaplibError> for PyErr {
    fn from(err: PyMaplibError) -> PyErr {
        match &err {
            PyMaplibError::MaplibError(err) => MaplibException::new_err(format!("{}", err)),
            PyMaplibError::FunctionArgumentError(s) => {
                FunctionArgumentException::new_err(s.clone())
            }
        }
    }
}

create_exception!(exceptions, MaplibException, PyException);
create_exception!(exceptions, FunctionArgumentException, PyException);
