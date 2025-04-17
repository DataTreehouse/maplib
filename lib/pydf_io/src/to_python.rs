// From: https://github.com/pola-rs/polars/blob/master/py-polars/src/arrow_interop/to_py.rs
// Edited to remove dependencies on py-polars, and added specific functionality for RDF.
// Original licence in ../licensing/POLARS_LICENSE

use polars::prelude::{col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::{ArrayRef, ArrowField, CompatLevel};
use polars_core::utils::arrow::ffi;
use polars_core::utils::arrow::record_batch::RecordBatch;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::IntoPyObjectExt;
use representation::formatting::format_columns;
use representation::multitype::{compress_actual_multitypes, lf_column_from_categorical};
use representation::python::PySolutionMappings;
use representation::query_context::Context;
use representation::RDFNodeType;
use std::collections::HashMap;

/// Arrow array to Python.
pub(crate) fn to_py_array(
    array: ArrayRef,
    py: Python,
    pyarrow: &Bound<'_, PyModule>,
) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&ArrowField::new(
        "".into(),
        array.dtype().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    array.into_py_any(py)
}

/// RecordBatch to Python.
pub(crate) fn to_py_rb(
    rb: &RecordBatch,
    names: &[&str],
    py: Python,
    pyarrow: &Bound<'_, PyModule>,
) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(rb.len());

    for array in rb.columns() {
        let array_object = to_py_array(array.clone(), py, pyarrow)?;
        arrays.push(array_object);
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names.to_vec()))?;

    record.into_py_any(py)
}
pub fn to_py_df(
    rb: &RecordBatch,
    names: &[&str],
    py: Python,
    pyarrow: &Bound<'_, PyModule>,
    polars: &Bound<'_, PyModule>,
) -> PyResult<PyObject> {
    let py_rb = to_py_rb(rb, names, py, pyarrow)?;
    let py_rb_list = PyList::empty(py);
    py_rb_list.append(py_rb)?;
    let py_table = pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_rb_list,))?;
    let py_table = py_table.into_py_any(py)?;
    let df = polars.call_method1("from_arrow", (py_table,))?;
    df.into_py_any(py)
}

pub fn df_to_py_df(
    mut df: DataFrame,
    rdf_node_types: HashMap<String, RDFNodeType>,
    pushdown_paths: Option<Vec<Context>>,
    include_datatypes: bool,
    py: Python,
) -> PyResult<PyObject> {
    let names_vec: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|x| x.to_string())
        .collect();
    let names: Vec<&str> = names_vec.iter().map(|x| x.as_str()).collect();
    let chunk = df
        .as_single_chunk()
        .iter_chunks(CompatLevel::oldest(), true)
        .next()
        .unwrap();
    let pyarrow = PyModule::import(py, "pyarrow")?;
    let polars = PyModule::import(py, "polars")?;
    let py_df = to_py_df(&chunk, names.as_slice(), py, &pyarrow, &polars)?;
    if include_datatypes {
        Py::new(
            py,
            PySolutionMappings {
                mappings: py_df.into_any(),
                rdf_node_types,
                pushdown_paths,
            },
        )?
        .into_py_any(py)
    } else {
        Ok(py_df)
    }
}

pub fn fix_cats_and_multicolumns(
    mut df: DataFrame,
    mut dts: HashMap<String, RDFNodeType>,
    native_dataframe: bool,
) -> (DataFrame, HashMap<String, RDFNodeType>) {
    let column_ordering: Vec<_> = df
        .get_column_names()
        .iter()
        .map(|x| col(x.as_str()))
        .collect();
    //Important that column compression happen before decisions are made based on column type.
    (df, dts) = compress_actual_multitypes(df, dts);
    let mut lf = df.lazy();
    for c in dts.keys() {
        lf = lf_column_from_categorical(lf.lazy(), c, &dts);
    }
    if !native_dataframe {
        lf = format_columns(lf, &dts)
    }
    df = lf.select(column_ordering).collect().unwrap();
    (df, dts)
}
