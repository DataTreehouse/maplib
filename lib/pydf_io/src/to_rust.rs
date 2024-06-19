// From: https://github.com/pola-rs/polars/blob/master/py-polars/src/arrow_interop/to_rust.rs
// Edited to remove dependencies on py-polars, remove unused functionality
// Original licence in ../licensing/POLARS_LICENSE

use polars_core::error::PolarsError;
use polars_core::prelude::{ArrayRef, ArrowDataType, DataFrame, Series};
use polars_core::utils::accumulate_dataframes_vertical;
use polars_core::utils::arrow::ffi;
use polars_core::utils::rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use polars_core::POOL;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ToRustError {
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error("Empty table")]
    EmptyTableError,
}

pub fn array_to_rust(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).map_err(ToRustError::from)?;
        let array = ffi::import_array_from_c(*array, field.data_type).map_err(ToRustError::from)?;
        Ok(array)
    }
}

pub fn polars_df_to_rust_df(df: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    let arr = df.call_method0("to_arrow")?;
    let batches = arr.call_method1("to_batches", (u32::MAX,))?;
    let batches_len = batches.call_method0("__len__")?;
    let l: u32 = batches_len.extract()?;
    let mut batches_vec = vec![];
    for i in 0..l {
        let batch = batches.call_method1("__getitem__", (i,))?;
        batches_vec.push(batch);
    }
    array_to_rust_df(batches_vec.as_slice())
}

pub fn array_to_rust_df(rb: &[Bound<'_, PyAny>]) -> PyResult<DataFrame> {
    let schema = rb
        .first()
        .ok_or_else(|| ToRustError::EmptyTableError)?
        .getattr("schema")?;
    let names = schema.getattr("names")?.extract::<Vec<String>>()?;

    let dfs = rb
        .iter()
        .map(|rb| {
            let mut run_parallel = false;

            let columns = (0..names.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let arr = array_to_rust(&array)?;
                    run_parallel |= matches!(
                        arr.data_type(),
                        ArrowDataType::Utf8 | ArrowDataType::Dictionary(_, _, _)
                    );
                    Ok(arr)
                })
                .collect::<PyResult<Vec<_>>>()?;

            // we parallelize this part because we can have dtypes that are not zero copy
            // for instance String -> large-String
            // dict encoded to categorical
            let columns = if run_parallel {
                POOL.install(|| {
                    columns
                        .into_par_iter()
                        .enumerate()
                        .map(|(i, arr)| {
                            let s = Series::try_from((names[i].as_str(), arr))
                                .map_err(ToRustError::from)?;
                            Ok(s)
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
            } else {
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, arr)| {
                        let s = Series::try_from((names[i].as_str(), arr))
                            .map_err(ToRustError::from)?;
                        Ok(s)
                    })
                    .collect::<PyResult<Vec<_>>>()
            }?;

            Ok(DataFrame::new(columns).map_err(ToRustError::from)?)
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok(accumulate_dataframes_vertical(dfs).map_err(ToRustError::from)?)
}

impl std::convert::From<ToRustError> for PyErr {
    fn from(err: ToRustError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));

        default()
    }
}

create_exception!(exceptions, ArrowErrorException, PyException);
