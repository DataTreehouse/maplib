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
    #[error("Runtime error `{0}`")]
    RuntimeError(String),
    #[error("UDF error `{0}`")]
    UDFError(String),
}

impl std::convert::From<PyMaplibError> for PyErr {
    fn from(err: PyMaplibError) -> PyErr {
        match &err {
            PyMaplibError::MaplibError(err) => MaplibException::new_err(format!("{err}")),
            PyMaplibError::FunctionArgumentError(s) => {
                FunctionArgumentException::new_err(s.clone())
            }
            PyMaplibError::RuntimeError(s) => MaplibRuntimeError::new_err(s.clone()),
            PyMaplibError::UDFError(s) => MaplibUDFError::new_err(s.to_string()),
        }
    }
}

create_exception!(exceptions, MaplibException, PyException);
create_exception!(exceptions, MaplibRuntimeError, PyException);
create_exception!(exceptions, FunctionArgumentException, PyException);
create_exception!(exceptions, MaplibUDFError, PyException);
// Raised when a query or update runs longer than its timeout. A MaplibException, so code that
// catches those catches timeouts too.
create_exception!(exceptions, QueryTimeoutError, MaplibException);

/// The `timeout` argument of a query, in seconds.
pub fn parse_timeout(timeout: Option<f64>) -> PyResult<Option<std::time::Duration>> {
    match timeout {
        None => Ok(None),
        Some(t) if t.is_finite() && t >= 0.0 => Ok(Some(std::time::Duration::from_secs_f64(t))),
        Some(t) => Err(FunctionArgumentException::new_err(format!(
            "timeout must be a number of seconds, 0 or more, not {t}"
        ))),
    }
}

/// A QueryTimeoutError when the query stopped because its token timed out, a MemoryError when it
/// stopped because the process used more memory than queries may, otherwise `err`.
pub fn timeout_error(err: PyErr, token: Option<&utils::cancel::CancelToken>) -> PyErr {
    match token.and_then(|t| t.stopped()) {
        Some(c @ utils::cancel::Cancelled::Timeout(_)) => QueryTimeoutError::new_err(c.to_string()),
        Some(c @ utils::cancel::Cancelled::MemoryLimit(_)) => {
            pyo3::exceptions::PyMemoryError::new_err(c.to_string())
        }
        _ => err,
    }
}
