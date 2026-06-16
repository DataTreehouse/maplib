// From: https://github.com/pola-rs/polars/blob/main/crates/polars-python/src/interop/arrow/to_py.rs
// Edited to remove dependencies on py-polars, and added specific functionality for RDF.
// Edited to support newer version of pyarrow
// Original license in ../licensing/POLARS_LICENSE

use std::collections::HashMap;
use std::ffi::CString;

use crate::cats::LockedCats;
use crate::debug::DebugOutputs;
use crate::errors::RepresentationError;
use crate::formatting::{format_columns, format_native_columns};
use crate::multitype::compress_actual_multitypes;
use crate::python::PySolutionMappings;
use crate::query_context::Context;
use crate::RDFNodeState;
use arrow::datatypes::ArrowDataType;
use polars::datatypes::CompatLevel;
use polars::frame::DataFrame;
use polars::prelude::{col, ArrayRef, ArrowField, IntoLazy, PlSmallStr};
use polars::series::Series;
use polars_arrow::record_batch::RecordBatch;
use polars_core::utils::arrow;
use polars_core::utils::arrow::array::Array;
use polars_core::utils::arrow::datatypes::Field;
use polars_core::utils::arrow::ffi;
use polars_core::utils::arrow::ffi::{
    export_array_to_c, export_field_to_c, ArrowArray, ArrowSchema,
};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::IntoPyObjectExt;

/// Arrow array to Python.
pub(crate) fn to_py_array(
    array: ArrayRef,
    field: &ArrowField,
    pyarrow: &Bound<PyModule>,
) -> PyResult<Py<PyAny>> {
    let schema = Box::new(ffi::export_field_to_c(field));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.unbind())
}

/// RecordBatch to Python.
pub(crate) fn to_py_rb(
    rb: &RecordBatch,
    py: Python<'_>,
    pyarrow: &Bound<PyModule>,
) -> PyResult<Py<PyAny>> {
    let mut arrays = Vec::with_capacity(rb.width());

    for (array, field) in rb.columns().iter().zip(rb.schema().iter_values()) {
        let array_object = to_py_array(array.clone(), field, pyarrow)?;
        arrays.push(array_object);
    }

    let schema = Box::new(ffi::export_field_to_c(&ArrowField {
        name: PlSmallStr::EMPTY,
        dtype: ArrowDataType::Struct(rb.schema().iter_values().cloned().collect()),
        is_nullable: false,
        metadata: None,
    }));
    let schema_ptr: *const ffi::ArrowSchema = &*schema;

    let schema = pyarrow
        .getattr("Schema")?
        .call_method1("_import_from_c", (schema_ptr as Py_uintptr_t,))?;
    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, py.None(), schema))?;

    Ok(record.unbind())
}

#[allow(unused)]
unsafe extern "C" fn get_next(iter: *mut ArrowArrayStream, array: *mut ArrowArray) -> i32 {
    if iter.is_null() {
        return 2001;
    }
    let private = &mut *((*iter).private_data as *mut PrivateData);

    match private.iter.next() {
        Some(Ok(item)) => {
            // check that the array has the same dtype as field
            let item_dt = item.dtype();
            let expected_dt = private.field.dtype();
            if item_dt != expected_dt {
                private.error = Some(CString::new(format!("The iterator produced an item of data type {item_dt:?} but the producer expects data type {expected_dt:?}").as_bytes().to_vec()).unwrap());
                return 2001; // custom application specific error (since this is never a result of this interface)
            }

            std::ptr::write(array, export_array_to_c(item));

            private.error = None;
            0
        }
        Some(Err(err)) => {
            private.error = Some(CString::new(err.to_string().as_bytes().to_vec()).unwrap());
            2001 // custom application specific error (since this is never a result of this interface)
        }
        None => {
            let a = ArrowArray::empty();
            std::ptr::write_unaligned(array, a);
            private.error = None;
            0
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct ArrowArrayStream {
    pub(super) get_schema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut ArrowArrayStream,
            out: *mut ArrowSchema,
        ) -> ::std::os::raw::c_int,
    >,
    pub(super) get_next: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut ArrowArrayStream,
            out: *mut ArrowArray,
        ) -> ::std::os::raw::c_int,
    >,
    pub(super) get_last_error: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut ArrowArrayStream) -> *const ::std::os::raw::c_char,
    >,
    pub(super) release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut ArrowArrayStream)>,
    pub(super) private_data: *mut ::std::os::raw::c_void,
}

unsafe impl Send for ArrowArrayStream {}

impl ArrowArrayStream {
    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

#[allow(unused)]
unsafe extern "C" fn get_schema(iter: *mut ArrowArrayStream, schema: *mut ArrowSchema) -> i32 {
    if iter.is_null() {
        return 2001;
    }
    let private = &mut *((*iter).private_data as *mut PrivateData);

    std::ptr::write(schema, export_field_to_c(&private.field));
    0
}

#[allow(unused)]
unsafe extern "C" fn get_last_error(iter: *mut ArrowArrayStream) -> *const ::std::os::raw::c_char {
    if iter.is_null() {
        return std::ptr::null();
    }
    let private = &mut *((*iter).private_data as *mut PrivateData);

    private
        .error
        .as_ref()
        .map(|x| x.as_ptr())
        .unwrap_or(std::ptr::null())
}

#[allow(unused)]
unsafe extern "C" fn release(iter: *mut ArrowArrayStream) {
    if iter.is_null() {
        return;
    }
    let _ = Box::from_raw((*iter).private_data as *mut PrivateData);
    (*iter).release = None;
    // private drops automatically
}

fn export_iterator(
    iter: Box<dyn Iterator<Item = Result<Box<dyn Array>, RepresentationError>>>,
    field: Field,
) -> ArrowArrayStream {
    let private_data = Box::new(PrivateData {
        iter,
        field,
        error: None,
    });

    ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
    }
}

struct PrivateData {
    iter: Box<dyn Iterator<Item = Result<Box<dyn Array>, RepresentationError>>>,
    field: Field,
    error: Option<CString>,
}

pub struct DataFrameStreamIterator {
    columns: Vec<Series>,
    dtype: ArrowDataType,
    idx: usize,
    n_chunks: usize,
    height: usize,
}

impl Iterator for DataFrameStreamIterator {
    type Item = Result<ArrayRef, RepresentationError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.n_chunks {
            None
        } else {
            // create a batch of the columns with the same chunk no.
            let batch_cols = self
                .columns
                .iter()
                .map(|s| s.to_arrow(self.idx, CompatLevel::newest()))
                .collect::<Vec<_>>();
            self.idx += 1;

            let col_len = batch_cols.first().map_or(self.height, |c| c.len());
            let array =
                arrow::array::StructArray::new(self.dtype.clone(), col_len, batch_cols, None);
            Some(Ok(Box::new(array)))
        }
    }
}

pub fn to_py_df(
    rb: &RecordBatch,
    py: Python,
    pyarrow: &Bound<'_, PyModule>,
    polars: &Bound<'_, PyModule>,
) -> PyResult<Py<PyAny>> {
    let py_rb = to_py_rb(rb, py, pyarrow)?;
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
    let chunk = df
        .rechunk_mut()
        .iter_chunks(CompatLevel::oldest(), true)
        .next()
        .unwrap();
    let pyarrow = PyModule::import(py, "pyarrow")?;
    let polars = PyModule::import(py, "polars")?;
    let py_df = to_py_df(&chunk, py, &pyarrow, &polars)?;
    if include_datatypes {
        Py::new(
            py,
            PySolutionMappings {
                mappings: py_df.into_any(),
                debug: debug_outputs,
                rdf_node_states,
                pushdown_paths: pushdown_paths.unwrap_or(Vec::new()),
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
