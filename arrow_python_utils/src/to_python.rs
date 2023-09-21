// From: https://github.com/pola-rs/polars/blob/master/py-polars/src/arrow_interop/to_py.rs
// Edited to remove dependencies on py-polars
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

use polars_core::frame::{ArrowChunk, DataFrame};
use polars_core::prelude::{ArrayRef, ArrowField};
use polars_core::utils::arrow::ffi;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;

/// Arrow array to Python.
pub(crate) fn to_py_array(array: ArrayRef, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&ArrowField::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

/// RecordBatch to Python.
pub(crate) fn to_py_rb(
    rb: &ArrowChunk,
    names: &[&str],
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(rb.len());

    for array in rb.columns() {
        let array_object = to_py_array(array.clone(), py, pyarrow)?;
        arrays.push(array_object);
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names.to_vec()))?;

    Ok(record.to_object(py))
}
fn to_py_df(
    rb: &ArrowChunk,
    names: &[&str],
    py: Python,
    pyarrow: &PyModule,
    polars: &PyModule,
) -> PyResult<PyObject> {
    let py_rb = to_py_rb(rb, names, py, pyarrow)?;
    let py_rb_list = PyList::empty(py);
    py_rb_list.append(py_rb)?;
    let py_table = pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_rb_list,))?;
    let py_table = py_table.to_object(py);
    let df = polars.call_method1("from_arrow", (py_table,))?;
    Ok(df.to_object(py))
}

pub fn df_to_py_df(mut df: DataFrame, py: Python) -> PyResult<PyObject> {
    let names_vec: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|x| x.to_string())
        .collect();
    let names: Vec<&str> = names_vec.iter().map(|x| x.as_str()).collect();
    let chunk = df.as_single_chunk().iter_chunks().next().unwrap();
    let pyarrow = PyModule::import(py, "pyarrow")?;
    let polars = PyModule::import(py, "polars")?;
    to_py_df(&chunk, names.as_slice(), py, pyarrow, polars)
}

pub fn df_vec_to_py_df_list(dfs: Vec<DataFrame>, py: Python) -> PyResult<&PyList> {
    let mut py_dfs = vec![];
    for df in dfs {
        py_dfs.push(df_to_py_df(df, py)?)
    }

    Ok(PyList::new(py, py_dfs))
}
