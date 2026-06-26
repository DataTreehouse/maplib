use crate::errors::QueryProcessingError;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::{IpcReader, IpcWriter};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::{Py, PyAny, PyErr, Python};
use representation::BaseRDFNodeType;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Clone)]
pub struct UdfEntry {
    func: Py<PyAny>,
    input_types: Option<Vec<BaseRDFNodeType>>,
    output_type: BaseRDFNodeType,
}

#[derive(Clone)]
pub struct UdfRegistry {
    udfs: HashMap<NamedNode, UdfEntry>,
}

impl UdfRegistry {
    pub fn new() -> UdfRegistry {
        UdfRegistry {
            udfs: Default::default(),
        }
    }
    pub fn has(&self, iri: &NamedNode) -> bool {
        self.udfs.contains_key(iri)
    }
    pub fn input_types(&self, iri: &NamedNode) -> Option<&[BaseRDFNodeType]> {
        if let Some(udf) = self.udfs.get(iri) {
            udf.input_types.as_ref().map(|x| x.as_slice())
        } else {
            None
        }
    }
    pub fn output_type(&self, iri: &NamedNode) -> Option<&BaseRDFNodeType> {
        self.udfs.get(iri).map(|e| &e.output_type)
    }
    pub fn call(
        &self,
        iri: &NamedNode,
        args: DataFrame,
    ) -> Result<DataFrame, QueryProcessingError> {
        let entry = self
            .udfs
            .get(iri)
            .ok_or_else(|| QueryProcessingError::UDFError(format!("UDF {} not found", iri)))?;
        Python::attach(|py| {
            let mut buf = Vec::new();
            let mut args_clone = args.clone();
            IpcWriter::new(&mut buf)
                .finish(&mut args_clone)
                .map_err(|e| {
                    QueryProcessingError::UDFError(format!(
                        "UDF '{}': failed to serialize input: {}",
                        iri, e
                    ))
                })?;
            let py_bytes = PyBytes::new(py, &buf);
            let io = py.import("io").map_err(|e| {
                QueryProcessingError::UDFError(format!("UDF '{}': failed to import io: {}", iri, e))
            })?;
            let reader = io.call_method1("BytesIO", (py_bytes,)).map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to create reader: {}",
                    iri, e
                ))
            })?;
            let pl = py.import("polars").map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to import polars: {}",
                    iri, e
                ))
            })?;
            let py_df = pl.call_method1("read_ipc", (reader,)).map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to deserialize input: {}",
                    iri, e
                ))
            })?;
            let result = entry.func.call1(py, (py_df,)).map_err(|e| {
                QueryProcessingError::UDFError(format!("UDF '{}': failed to call UDF: {}", iri, e))
            })?;
            let result_bound = result.bind(py);
            let result_df = if result_bound
                .is_instance(
                    &pl.getattr("DataFrame")
                        .map_err(|e| QueryProcessingError::UDFError(e.to_string()))?,
                )
                .unwrap_or(false)
            {
                result_bound.clone()
            } else {
                result_bound
                    .call_method0("to_frame")
                    .map_err(|e| {
                        QueryProcessingError::UDFError(format!(
                            "UDF '{}': failed to convert result to DataFrame: {}",
                            iri, e
                        ))
                    })?
                    .clone()
            };
            let write_buf = io.call_method1("BytesIO", ()).map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to create writer: {}",
                    iri, e
                ))
            })?;
            result_df
                .call_method1("write_ipc", (&write_buf,))
                .map_err(|e| {
                    QueryProcessingError::UDFError(format!(
                        "UDF '{}': failed to serialize result: {}",
                        iri, e
                    ))
                })?;
            write_buf.call_method1("seek", (0i64,)).map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to seek in writer: {}",
                    iri, e
                ))
            })?;
            let bytes: Vec<u8> = write_buf
                .call_method0("getvalue")
                .map_err(|e| {
                    QueryProcessingError::UDFError(format!(
                        "UDF '{}': failed to get value from writer: {}",
                        iri, e
                    ))
                })?
                .extract()
                .map_err(|e: PyErr| {
                    QueryProcessingError::UDFError(format!(
                        "UDF '{}': failed to extract value from writer: {}",
                        iri, e
                    ))
                })?;
            let cursor = Cursor::new(bytes);
            IpcReader::new(cursor).finish().map_err(|e| {
                QueryProcessingError::UDFError(format!(
                    "UDF '{}': failed to deserialize result: {}",
                    iri, e
                ))
            })
        })
    }

    pub fn add_udf(
        &mut self,
        iri: NamedNode,
        f: Py<PyAny>,
        output_type: BaseRDFNodeType,
        input_types: Option<Vec<BaseRDFNodeType>>,
    ) {
        self.udfs.insert(
            iri,
            UdfEntry {
                func: f,
                input_types,
                output_type,
            },
        );
    }

    pub fn list_udfs(&self) -> Vec<NamedNode> {
        self.udfs.keys().map(|x| x.clone()).collect()
    }
}
