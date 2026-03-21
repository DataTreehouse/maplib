use crate::error::PyMaplibError;
use crate::{PyModel};
use maplib::errors::MaplibError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use report_mapping::report_to_model;
use representation::solution_mapping::EagerSolutionMappings;
use shacl::ValidationReport as RustValidationReport;
use std::collections::HashMap;
use representation::df_to_python::{df_to_py_df, fix_cats_and_multicolumns};
use triplestore::Triplestore;

#[derive(Clone)]
#[pyclass(name = "ValidationReport", from_py_object)]
pub struct PyValidationReport {
    shape_graph: Option<Triplestore>,
    inner: RustValidationReport,
}

impl PyValidationReport {
    pub fn new(
        inner: RustValidationReport,
        shape_graph: Option<Triplestore>,
    ) -> PyValidationReport {
        PyValidationReport { shape_graph, inner }
    }
}

#[pymethods]
impl PyValidationReport {
    #[getter]
    pub fn conforms(&self) -> Option<bool> {
        self.inner.conforms
    }

    #[getter]
    pub fn shape_targets(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let df = py.detach(|| self.inner.shape_targets_df());
        df_to_py_df(df, HashMap::new(), None, None, false, py)
    }

    #[getter]
    pub fn performance(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let df = py.detach(|| self.inner.performance_df());
        df_to_py_df(df, HashMap::new(), None, None, false, py)
    }

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None, streaming=None))]
    pub fn results(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        streaming: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let streaming = streaming.unwrap_or(false);
        let report = py.detach(|| -> Result<_, PyMaplibError> {
            let sm = self
                .inner
                .concatenated_results()
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            match sm {
                Some(sm) => {
                    let cats = self.inner.cats.as_ref().unwrap().clone();
                    let EagerSolutionMappings {
                        mut mappings,
                        mut rdf_node_types,
                    } = sm.as_eager(streaming);
                    (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                        mappings,
                        rdf_node_types,
                        native_dataframe.unwrap_or(false),
                        cats,
                    );
                    Ok(Some((mappings, rdf_node_types)))
                }
                None => Ok(None),
            }
        })?;
        let res = match report {
            Some((mappings, rdf_node_types)) => Ok(Some(df_to_py_df(
                mappings,
                rdf_node_types,
                None,
                None,
                include_datatypes.unwrap_or(false),
                py,
            )?)),
            None => Ok(None),
        };
        res
    }

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None, streaming=None))]
    pub fn details(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        streaming: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let streaming = streaming.unwrap_or(false);
        let native_dataframe = native_dataframe.unwrap_or(false);
        let include_datatypes = include_datatypes.unwrap_or(false);
        let details = py.detach(|| -> Result<_, PyMaplibError> {
            let sm = self
                .inner
                .concatenated_details()
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            let cats = self.inner.cats.as_ref().unwrap().clone();
            match sm {
                Some(sm) => {
                    let EagerSolutionMappings {
                        mut mappings,
                        mut rdf_node_types,
                    } = sm.as_eager(streaming);
                    (mappings, rdf_node_types) =
                        fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe, cats);
                    Ok(Some((mappings, rdf_node_types)))
                }
                None => Ok(None),
            }
        })?;
        match details {
            Some((mappings, rdf_node_types)) => Ok(Some(df_to_py_df(
                mappings,
                rdf_node_types,
                None,
                None,
                include_datatypes,
                py,
            )?)),
            None => Ok(None),
        }
    }

    pub fn graph(&self, py: Python<'_>) -> PyResult<PyModel> {
        let m = py.detach(|| {
            report_to_model(&self.inner, &self.shape_graph)
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))
        })?;
        Ok(PyModel::from_inner_mapping(m))
    }
}
