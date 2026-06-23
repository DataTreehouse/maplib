// From: https://github.com/pola-rs/polars/blob/main/crates/polars-python/src/interop/arrow/to_rust.rs
// Edited to remove dependencies on py-polars, remove unused functionality
// Edited to support newer version of pyarrow
// Original license in ../licensing/POLARS_LICENSE

use polars_core::prelude::*;
use polars_core::runtime::RAYON;
use polars_core::utils::accumulate_dataframes_vertical_unchecked;
use polars_core::utils::arrow::ffi;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rayon::prelude::*;

use crate::python::PyRepresentationError;

pub fn field_to_rust_arrow(obj: Bound<'_, PyAny>) -> PyResult<ArrowField> {
    let mut schema = Box::new(ffi::ArrowSchema::empty());
    let schema_ptr = schema.as_mut() as *mut ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    obj.call_method1("_export_to_c", (schema_ptr as Py_uintptr_t,))?;
    let field = unsafe {
        ffi::import_field_from_c(schema.as_ref())
            .map_err(|_| PyRepresentationError::ImportFieldError(String::from("FFI error")))?
    };
    Ok(field)
}

pub fn field_to_rust(obj: Bound<'_, PyAny>) -> PyResult<Field> {
    field_to_rust_arrow(obj).map(|f| (&f).into())
}

// PyList<Field> which you get by calling `list(schema)`
pub fn pyarrow_schema_to_rust(obj: &Bound<'_, PyList>) -> PyResult<Schema> {
    obj.into_iter().map(field_to_rust).collect()
}

pub fn array_to_rust(obj: &Bound<PyAny>) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let mut array = Box::new(ffi::ArrowArray::empty());
    let mut schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = array.as_mut() as *mut ffi::ArrowArray;
    let schema_ptr = schema.as_mut() as *mut ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref())
            .map_err(|_| PyRepresentationError::ImportFieldError(String::from("FFI error")))?;
        let array = ffi::import_array_from_c(*array, field.dtype)
            .map_err(|_| PyRepresentationError::ImportArrayError(String::from("FFI error")))?;
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
    if rb.is_empty() {
        return Ok(DataFrame::empty());
    }
    let schema = rb.first().unwrap().getattr("schema")?;
    to_rust_df(rb, schema)
}

pub fn to_rust_df(rb: &[Bound<PyAny>], schema: Bound<PyAny>) -> PyResult<DataFrame> {
    let ArrowDataType::Struct(fields) = field_to_rust_arrow(schema)?.dtype else {
        return Err(PyRepresentationError::InvalidTopLevelSchemaError(
            String::from("Invalid top-level schema"),
        ))?;
    };

    let schema = ArrowSchema::from_iter(fields.iter().cloned());

    // Verify that field names are not duplicated. Arrow permits duplicate field names, we do not.
    // Required to uphold safety invariants for unsafe block below.
    if schema.len() != fields.len() {
        let mut field_map: PlHashMap<PlSmallStr, u64> = PlHashMap::with_capacity(fields.len());
        fields.iter().for_each(|field| {
            field_map
                .entry(field.name.clone())
                .and_modify(|c| {
                    *c += 1;
                })
                .or_insert(1);
        });
        let duplicate_fields: Vec<_> = field_map
            .into_iter()
            .filter_map(|(k, v)| (v > 1).then_some(k))
            .collect();

        Err(PyRepresentationError::DuplicateColumnError(format!(
            "column appears more than once; names must be unique: {duplicate_fields:?}"
        )))?;

        // return Err(PyPolarsErr::Polars(PolarsError::Duplicate(
        //     format!("column appears more than once; names must be unique: {duplicate_fields:?}")
        //         .into(),
        // ))
        // .into());
    }

    if rb.is_empty() {
        let columns = schema
            .iter_values()
            .map(|field| {
                let field = Field::from(field);
                Series::new_empty(field.name, &field.dtype).into_column()
            })
            .collect::<Vec<_>>();

        // no need to check as a record batch has the same guarantees
        return Ok(unsafe { DataFrame::new_unchecked_infer_height(columns) });
    }

    let dfs = rb
        .iter()
        .map(|rb| {
            let mut run_parallel = false;

            let columns = (0..schema.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let mut arr = array_to_rust(&array)?;

                    // Only the schema contains extension type info, restore.
                    // TODO: nested?
                    let dtype = schema.get_at_index(i).unwrap().1.dtype();
                    if let ArrowDataType::Extension(ext) = dtype {
                        if *arr.dtype() == ext.inner {
                            *arr.dtype_mut() = dtype.clone();
                        }
                    }

                    run_parallel |= matches!(
                        arr.dtype(),
                        ArrowDataType::Utf8 | ArrowDataType::Dictionary(_, _, _)
                    );
                    Ok(arr)
                })
                .collect::<PyResult<Vec<_>>>()?;

            // we parallelize this part because we can have dtypes that are not zero copy
            // for instance string -> large-utf8
            // dict encoded to categorical
            let columns = if run_parallel {
                // py.enter_polars(|| {
                RAYON.install(|| {
                    columns
                        .into_par_iter()
                        .enumerate()
                        .map(|(i, arr)| {
                            let (_, field) = schema.get_at_index(i).unwrap();
                            let s = unsafe {
                                Series::_try_from_arrow_unchecked_with_md(
                                    field.name.clone(),
                                    vec![arr],
                                    field.dtype(),
                                    field.metadata.as_deref(),
                                )
                            }
                            .map_err(|_| {
                                PyRepresentationError::NewArrowSeriesError(String::from(
                                    "New arrow series error",
                                ))
                            })?
                            .into_column();
                            Ok(s)
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
                // })
            } else {
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, arr)| {
                        let (_, field) = schema.get_at_index(i).unwrap();
                        let s = unsafe {
                            Series::_try_from_arrow_unchecked_with_md(
                                field.name.clone(),
                                vec![arr],
                                field.dtype(),
                                field.metadata.as_deref(),
                            )
                        }
                        .map_err(|_| {
                            PyRepresentationError::NewArrowSeriesError(String::from(
                                "New arrow series error",
                            ))
                        })?
                        .into_column();
                        Ok(s)
                    })
                    .collect::<PyResult<Vec<_>>>()
            }?;

            // no need to check as a record batch has the same guarantees
            Ok(unsafe { DataFrame::new_unchecked_infer_height(columns) })
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok(accumulate_dataframes_vertical_unchecked(dfs))
}
