//! Sources for the read methods and targets for the write methods: a file path, bytes in memory, or
//! a file-like Python object (anything with a `read` method, or a `write` method accepting bytes,
//! e.g. `io.BytesIO`, an open file or a socket file).

use crate::error::PyMaplibError;
use maplib::errors::MaplibError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyByteArray, PyBytes, PyMemoryView, PyString};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Writes to the Python object are buffered so that the GIL is taken once per chunk.
const PY_WRITE_BUFFER_SIZE: usize = 1 << 20;

pub(crate) enum ReadSource {
    Path(String),
    /// Bytes from a bytes-like object, or everything read from a file-like object.
    /// Bytes objects are used without copying.
    Bytes(PyBackedBytes),
}

impl ReadSource {
    /// Bytes, bytearray and memoryview objects are data, objects with a `read` method (that are not
    /// os.PathLike) are read to the end, and anything else is converted to a path with str().
    pub(crate) fn extract(obj: &Bound<'_, PyAny>) -> PyResult<ReadSource> {
        if obj.is_instance_of::<PyString>() || obj.hasattr("__fspath__")? {
            return Ok(ReadSource::Path(obj.str()?.to_string()));
        }
        if obj.is_instance_of::<PyBytes>() || obj.is_instance_of::<PyByteArray>() {
            return Ok(ReadSource::Bytes(obj.extract()?));
        }
        if obj.is_instance_of::<PyMemoryView>() {
            return Ok(ReadSource::Bytes(obj.call_method0("tobytes")?.extract()?));
        }
        if obj.hasattr("read")? {
            let data = obj.call_method0("read")?;
            // Text streams give str, which is read as UTF-8
            let data = if data.is_instance_of::<PyString>() {
                data.call_method1("encode", ("utf-8",))?
            } else {
                data
            };
            return Ok(ReadSource::Bytes(data.extract().map_err(|_| {
                PyMaplibError::FunctionArgumentError(
                    "The read method of the file-like object should return bytes or str"
                        .to_string(),
                )
            })?));
        }
        Ok(ReadSource::Path(obj.str()?.to_string()))
    }
}

#[derive(Clone)]
pub(crate) enum WriteTarget {
    Path(PathBuf),
    Stream(Py<PyAny>),
}

impl WriteTarget {
    /// Paths are str or os.PathLike objects, other objects need a `write` method.
    pub(crate) fn extract(obj: &Bound<'_, PyAny>) -> PyResult<WriteTarget> {
        if obj.is_instance_of::<PyString>() || obj.hasattr("__fspath__")? {
            let path: PathBuf = obj.extract()?;
            return Ok(WriteTarget::Path(path));
        }
        if obj.hasattr("write")? {
            return Ok(WriteTarget::Stream(obj.clone().unbind()));
        }
        Err(PyMaplibError::FunctionArgumentError(format!(
            "Expected a file path or a binary file-like object with a write method, got {}",
            obj.get_type().name()?
        ))
        .into())
    }

    /// Creates (truncates) the file, or wraps the Python object in a buffered writer.
    pub(crate) fn open(&self) -> PyResult<Box<dyn Write + Send>> {
        match self {
            WriteTarget::Path(path) => {
                let file = File::create(path)
                    .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
                Ok(Box::new(file))
            }
            WriteTarget::Stream(obj) => Ok(Box::new(BufWriter::with_capacity(
                PY_WRITE_BUFFER_SIZE,
                PyStreamWriter {
                    obj: Python::attach(|py| obj.clone_ref(py)),
                },
            ))),
        }
    }
}

/// Flushes the writer, turning errors (e.g. from the Python object) into Python exceptions.
pub(crate) fn finish_writer(w: &mut dyn Write) -> PyResult<()> {
    w.flush()
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)).into())
}

/// Adapts a binary file-like Python object to std::io::Write. The GIL is acquired for each call,
/// so it can be used from code that runs with the GIL released.
pub(crate) struct PyStreamWriter {
    obj: Py<PyAny>,
}

fn py_err_to_io(e: PyErr) -> std::io::Error {
    std::io::Error::other(format!("Error writing to the Python object: {e}"))
}

impl Write for PyStreamWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            let res = self
                .obj
                .call_method1(py, "write", (PyBytes::new(py, buf),))
                .map_err(py_err_to_io)?;
            // Raw (unbuffered) streams may write fewer bytes and return the count; buffered
            // streams write everything and return the count or None.
            if res.is_none(py) {
                Ok(buf.len())
            } else {
                let n: usize = res.extract(py).map_err(py_err_to_io)?;
                Ok(n.min(buf.len()))
            }
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Python::attach(|py| {
            let obj = self.obj.bind(py);
            if obj.hasattr("flush").map_err(py_err_to_io)? {
                obj.call_method0("flush").map_err(py_err_to_io)?;
            }
            Ok(())
        })
    }
}
