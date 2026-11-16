//! Raw bytes in object storage and local files, for `maplib.storage`. The public API, its
//! documentation and its exceptions are in `maplib/storage.py`, which wraps these functions.

use bytes::Bytes;
use file_io::remote::{ObjectInfo, PutCondition, RemoteError, RemoteLocation, RemoteOptions};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyBytes;
use std::collections::HashMap;

/// (name, size, last modified in microseconds since the epoch, version token)
type PyObjectInfo = (String, u64, i64, Option<String>);

fn info_tuple(i: ObjectInfo) -> PyObjectInfo {
    (i.name, i.size, i.last_modified_micros, i.version_token)
}

/// Raises the exception class `name` of `maplib.storage`.
fn storage_error(py: Python<'_>, name: &str, message: String) -> PyErr {
    match py
        .import("maplib.storage")
        .and_then(|m| m.getattr(name))
        .and_then(|cls| cls.call1((message.clone(),)))
    {
        Ok(e) => PyErr::from_value(e),
        Err(e) => e,
    }
}

fn to_py_err(py: Python<'_>, e: RemoteError) -> PyErr {
    let message = e.to_string();
    let name = match &e {
        RemoteError::ObjectStore(_, object_store::Error::NotFound { .. }) => "NotFound",
        RemoteError::ObjectStore(_, object_store::Error::Precondition { .. }) => {
            "PreconditionFailed"
        }
        RemoteError::ObjectStore(_, object_store::Error::AlreadyExists { .. }) => "AlreadyExists",
        RemoteError::ObjectStore(_, object_store::Error::NotImplemented { .. }) => {
            "NotSupported"
        }
        RemoteError::Io(_, io) if io.kind() == std::io::ErrorKind::NotFound => "NotFound",
        _ => "StorageError",
    };
    storage_error(py, name, message)
}

fn location(
    py: Python<'_>,
    url: &str,
    options: Option<HashMap<String, String>>,
) -> PyResult<RemoteLocation> {
    let options: RemoteOptions = options.unwrap_or_default();
    RemoteLocation::parse_path_or_url(url, &options).map_err(|e| to_py_err(py, e))
}

#[pyfunction]
#[pyo3(signature = (url, data, if_match=None, if_none_match=false, storage_options=None))]
pub fn _storage_put(
    py: Python<'_>,
    url: String,
    data: PyBackedBytes,
    if_match: Option<String>,
    if_none_match: bool,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Option<String>> {
    let condition = match (if_match, if_none_match) {
        (Some(_), true) => {
            return Err(PyValueError::new_err(
                "if_match and if_none_match can not be used together",
            ))
        }
        (Some(t), false) => PutCondition::IfMatch(t),
        (None, true) => PutCondition::IfNoneMatch,
        (None, false) => PutCondition::Overwrite,
    };
    let loc = location(py, &url, storage_options)?;
    let bytes = Bytes::copy_from_slice(&data);
    py.detach(|| loc.put_with(bytes, &condition))
        .map_err(|e| to_py_err(py, e))
}

#[pyfunction]
#[pyo3(signature = (url, storage_options=None))]
pub fn _storage_get<'py>(
    py: Python<'py>,
    url: String,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<(Bound<'py, PyBytes>, Option<String>)> {
    let loc = location(py, &url, storage_options)?;
    let (bytes, token) = py
        .detach(|| loc.get_with_version())
        .map_err(|e| to_py_err(py, e))?;
    Ok((PyBytes::new(py, &bytes), token))
}

#[pyfunction]
#[pyo3(signature = (url, storage_options=None))]
pub fn _storage_head(
    py: Python<'_>,
    url: String,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Option<PyObjectInfo>> {
    let loc = location(py, &url, storage_options)?;
    Ok(py
        .detach(|| loc.info())
        .map_err(|e| to_py_err(py, e))?
        .map(info_tuple))
}

#[pyfunction]
#[pyo3(signature = (url, storage_options=None))]
pub fn _storage_delete(
    py: Python<'_>,
    url: String,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let loc = location(py, &url, storage_options)?;
    py.detach(|| loc.delete()).map_err(|e| to_py_err(py, e))
}

#[pyfunction]
#[pyo3(signature = (url, storage_options=None))]
pub fn _storage_list(
    py: Python<'_>,
    url: String,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Vec<PyObjectInfo>> {
    let loc = location(py, &url, storage_options)?;
    Ok(py
        .detach(|| loc.list_info())
        .map_err(|e| to_py_err(py, e))?
        .into_iter()
        .map(info_tuple)
        .collect())
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_storage_put, m)?)?;
    m.add_function(wrap_pyfunction!(_storage_get, m)?)?;
    m.add_function(wrap_pyfunction!(_storage_head, m)?)?;
    m.add_function(wrap_pyfunction!(_storage_delete, m)?)?;
    m.add_function(wrap_pyfunction!(_storage_list, m)?)?;
    Ok(())
}
