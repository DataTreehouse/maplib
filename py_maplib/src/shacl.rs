use crate::{fix_cats_and_multicolumns, PyMapping};
use pydf_io::to_python::df_to_py_df;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use report_mapping::report_to_mapping;
use representation::solution_mapping::EagerSolutionMappings;
use shacl::ValidationReport as RustValidationReport;
use triplestore::Triplestore;

#[derive(Clone)]
#[pyclass(name = "ValidationReport")]
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
    pub fn conforms(&self) -> bool {
        self.inner.conforms
    }

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

    pub fn graph(&self) -> PyMapping {
        let m = report_to_mapping(&self.inner, &self.shape_graph);
        PyMapping::from_inner_mapping(m)
    }
}
