use crate::{fix_cats_and_multicolumns, PyIndexingOptions, PyMapping};
use pydf_io::to_python::df_to_py_df;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use report_mapping::report_to_mapping;
use representation::solution_mapping::EagerSolutionMappings;
use shacl::ValidationReport as RustValidationReport;
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
    pub fn conforms(&self) -> bool {
        self.inner.conforms
    }

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None))]
    pub fn results(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let report = if let Some(df) = self.inner.df.clone() {
            let (df, rdf_node_types) = fix_cats_and_multicolumns(
                df,
                self.inner.rdf_node_types.as_ref().unwrap().clone(),
                native_dataframe.unwrap_or(false),
            );
            Some(df_to_py_df(
                df,
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

    #[pyo3(signature = (native_dataframe=None, include_datatypes=None))]
    pub fn details(
        &self,
        native_dataframe: Option<bool>,
        include_datatypes: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let details = if let Some(EagerSolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) = self.inner.details.clone()
        {
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
    pub fn graph(&self, indexing: Option<PyIndexingOptions>) -> PyMapping {
        let indexing = if let Some(indexing) = indexing {
            Some(indexing.inner)
        } else {
            None
        };
        let m = report_to_mapping(
            &self.inner,
            &self.shape_graph,
            Some(indexing.unwrap_or(self.indexing.clone())),
        );
        PyMapping::from_inner_mapping(m)
    }
}
