// From: https://github.com/pola-rs/polars/blob/master/py-polars/src/arrow_interop/to_py.rs
// Edited to remove dependencies on py-polars, and added specific functionality for RDF.
// Original licence in ../licensing/POLARS_LICENSE

use polars::prelude::{col, ArrayRef, ArrowField, CompatLevel, IntoLazy};
use pyo3::ffi::{Py_uintptr_t};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::IntoPyObjectExt;
use std::collections::HashMap;
use polars_core::frame::DataFrame;
use polars_core::utils::arrow::ffi;
use polars_core::utils::arrow::record_batch::RecordBatch;
use crate::cats::LockedCats;
use crate::debug::DebugOutputs;
use crate::formatting::{format_columns, format_native_columns};
use crate::multitype::compress_actual_multitypes;
use crate::python::PySolutionMappings;
use crate::query_context::Context;
use crate::RDFNodeState;

/// Arrow array to Python.
pub(crate) fn to_py_array(
    array: ArrayRef,
    py: Python,
    pyarrow: &Bound<'_, PyModule>,
) -> PyResult<Py<PyAny>> {
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
) -> PyResult<Py<PyAny>> {
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
) -> PyResult<Py<PyAny>> {
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
    rdf_node_states: HashMap<String, RDFNodeState>,
    debug_outputs: Option<DebugOutputs>,
    pushdown_paths: Option<Vec<Context>>,
    include_datatypes: bool,
    py: Python,
) -> PyResult<Py<PyAny>> {
    let names_vec: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|x| x.to_string())
        .collect();
    let names: Vec<&str> = names_vec.iter().map(|x| x.as_str()).collect();
    let chunk = df
        .rechunk_mut()
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
                debug: debug_outputs,
                rdf_node_states,
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
    mut dts: HashMap<String, RDFNodeState>,
    native_dataframe: bool,
    global_cats: LockedCats,
) -> (DataFrame, HashMap<String, RDFNodeState>) {
    let column_ordering: Vec<_> = df
        .get_column_names()
        .iter()
        .map(|x| col(x.as_str()))
        .collect();
    //Important that column compression happen before decisions are made based on column type.
    (df, dts) = compress_actual_multitypes(df, dts);
    let mut lf = df.lazy();
    if !native_dataframe {
        lf = format_columns(lf, &dts, global_cats)
    } else {
        lf = format_native_columns(lf, &mut dts, global_cats)
    }
    df = lf.select(column_ordering).collect().unwrap();
    (df, dts)
}
