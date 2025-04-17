use crate::error::PyMaplibError;
use crate::{fix_cats_and_multicolumns, PyIndexingOptions, PyMapping};
use maplib::errors::MaplibError;
use pydf_io::to_python::df_to_py_df;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use report_mapping::report_to_mapping;
use representation::solution_mapping::EagerSolutionMappings;
use shacl::ValidationReport as RustValidationReport;
use std::collections::HashMap;
use triplestore::{IndexingOptions, Triplestore};

#[derive(Clone)]
#[pyclass(name = "ValidationReport")]
pub struct PyValidationReport {
    shape_graph: Option<Triplestore>,
    inner: RustValidationReport,
    indexing: IndexingOptions,
}

impl PyValidationReport {
    pub fn new(
        inner: RustValidationReport,
        shape_graph: Option<Triplestore>,
        indexing: IndexingOptions,
    ) -> PyValidationReport {
        PyValidationReport {
            shape_graph,
            inner,
            indexing,
        }
    }
}

#[pymethods]
impl PyValidationReport {
    #[getter]
    pub fn conforms(&self) -> Option<bool> {
        self.inner.conforms
    }

    #[getter]
    pub fn shape_targets(&self, py: Python<'_>) -> PyResult<PyObject> {
        let df = self.inner.shape_targets_df();
        df_to_py_df(df, HashMap::new(), None, false, py)
    }

    #[getter]
    pub fn performance(&self, py: Python<'_>) -> PyResult<PyObject> {
        let df = self.inner.performance_df();
        df_to_py_df(df, HashMap::new(), None, false, py)
    }

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None, streaming=None))]
    pub fn results(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        streaming: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let streaming = streaming.unwrap_or(false);
        let report = if let Some(sm) = self
            .inner
            .concatenated_results()
            .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?
        {
            let EagerSolutionMappings {
                mut mappings,
                mut rdf_node_types,
            } = sm.as_eager(streaming);
            (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                mappings,
                rdf_node_types,
                native_dataframe.unwrap_or(false),
            );
            Some(df_to_py_df(
                mappings,
                rdf_node_types,
                None,
                include_datatypes.unwrap_or(false),
                py,
            )?)
        } else {
            None
        };
        Ok(report)
    }

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None, streaming=None))]
    pub fn details(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        streaming: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let streaming = streaming.unwrap_or(false);
        let details = if let Some(sm) = self
            .inner
            .concatenated_details()
            .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?
        {
            let EagerSolutionMappings {
                mut mappings,
                mut rdf_node_types,
            } = sm.as_eager(streaming);
            (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                mappings,
                rdf_node_types,
                native_dataframe.unwrap_or(false),
            );
            Some(df_to_py_df(
                mappings,
                rdf_node_types,
                None,
                include_datatypes.unwrap_or(false),
                py,
            )?)
        } else {
            None
        };
        Ok(details)
    }

    #[pyo3(signature = (indexing=None))]
    pub fn graph(&self, indexing: Option<PyIndexingOptions>) -> PyResult<PyMapping> {
        let indexing = if let Some(indexing) = indexing {
            Some(indexing.inner)
        } else {
            None
        };
        let m = report_to_mapping(
            &self.inner,
            &self.shape_graph,
            Some(indexing.unwrap_or(self.indexing.clone())),
        )
        .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
        Ok(PyMapping::from_inner_mapping(m))
    }
}
