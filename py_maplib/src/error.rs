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
// Raised when a query or update uses more memory than its memory limit (`max_memory_mb`, or the
// default set with maplib.set_memory_limit(max_query_memory_mb=)). A MaplibException.
create_exception!(exceptions, QueryMemoryLimitError, MaplibException);

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
/// stopped because the process used more memory than queries may, a QueryMemoryLimitError when it
/// went over its own memory limit, otherwise `err`.
pub fn timeout_error(err: PyErr, token: Option<&utils::cancel::CancelToken>) -> PyErr {
    match token.and_then(|t| t.stopped()).and_then(stopped_error) {
        Some(e) => e,
        None => err,
    }
}

/// The Python error for a query that stopped for `c`, None when it was cancelled by its caller.
pub fn stopped_error(c: utils::cancel::Cancelled) -> Option<PyErr> {
    use utils::cancel::Cancelled;
    match c {
        Cancelled::Timeout(_) => Some(QueryTimeoutError::new_err(c.to_string())),
        Cancelled::MemoryLimit(_) => Some(pyo3::exceptions::PyMemoryError::new_err(c.to_string())),
        Cancelled::QueryMemoryLimit(_) => Some(QueryMemoryLimitError::new_err(c.to_string())),
        Cancelled::Cancelled => None,
    }
}

/// The `max_memory_mb` argument of a query in bytes, or the default memory limit of queries.
pub fn parse_max_memory(max_memory_mb: Option<f64>) -> PyResult<Option<u64>> {
    match max_memory_mb {
        None => Ok(utils::cancel::default_query_memory_limit()),
        Some(m) if m.is_finite() && m > 0.0 => Ok(Some((m * 1e6) as u64)),
        Some(m) => Err(FunctionArgumentException::new_err(format!(
            "max_memory_mb must be a positive number of megabytes, not {m}"
        ))),
    }
}

/// The token of a query with `timeout` and a memory limit of `max_memory` bytes. None when it has
/// neither and the process has no memory limit for queries
/// (`utils::memory::query_memory_limit`). With a memory limit, a token also lets a large join
/// run on the streaming engine, where it can be stopped.
pub fn query_token(
    timeout: Option<std::time::Duration>,
    max_memory: Option<u64>,
) -> Option<utils::cancel::CancelToken> {
    if timeout.is_none()
        && max_memory.is_none()
        && utils::memory::query_memory_limit().is_none()
    {
        return None;
    }
    Some(utils::cancel::CancelToken::with_memory_limit(
        timeout,
        timeout.map(|t| std::time::Instant::now() + t),
        max_memory,
    ))
}
