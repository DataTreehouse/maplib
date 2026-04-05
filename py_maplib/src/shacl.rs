use crate::error::PyMaplibError;
use maplib::errors::MaplibError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::df_to_python::{df_to_py_df, fix_cats_and_multicolumns};
use representation::solution_mapping::EagerSolutionMappings;
use shacl::ValidationReport as RustValidationReport;
use std::collections::HashMap;

pub const SHACL_RESULTS_QUERY: &str = r#"
PREFIX sh: <http://www.w3.org/ns/shacl#>
SELECT 
    ?focus_node 
    ?result_path 
    ?result_severity 
    ?source_constraint_component 
    ?source_shape 
    ?message 
    ?value 
WHERE {
    ?report a sh:ValidationReport .
    ?report sh:conforms ?conforms .
    ?report sh:result ?result .
    ?result a sh:ValidationResult .
    ?result sh:focusNode ?focus_node .
    ?result sh:resultSeverity ?result_severity .
    ?result sh:sourceConstraintComponent ?source_constraint_component .
    ?result sh:sourceShape ?source_shape .
    OPTIONAL {
        ?result sh:resultPath ?result_path .
        }
    OPTIONAL {
        ?result sh:resultMessage ?message .
    }
    OPTIONAL {
        ?result sh:value ?value.
    }
}
"#;

#[derive(Clone)]
#[pyclass(name = "ValidationReport", from_py_object)]
pub struct PyValidationReport {
    inner: RustValidationReport,
    cats: LockedCats,
    pub report_graph: Option<NamedGraph>,
}

impl PyValidationReport {
    pub fn new(
        inner: RustValidationReport,
        cats: LockedCats,
        report_graph: Option<NamedGraph>,
    ) -> PyValidationReport {
        PyValidationReport {
            inner,
            cats,
            report_graph,
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
    pub fn shape_targets(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let df = py.detach(|| self.inner.shape_targets_df());
        df_to_py_df(df, HashMap::new(), None, None, false, py)
    }

    #[getter]
    pub fn performance(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let df = py.detach(|| self.inner.performance_df());
        df_to_py_df(df, HashMap::new(), None, None, false, py)
    }

    #[getter]
    pub fn report_graph(&self) -> PyResult<Option<String>> {
        if let Some(NamedGraph::NamedGraph(graph)) = &self.report_graph {
            Ok(Some(graph.as_str().to_string()))
        } else {
            Ok(None)
        }
    }

    #[getter]
    pub fn rule_log(&self) -> PyResult<String> {
        Ok(self.inner.rules_result.to_string())
    }

    #[pyo3(signature = (streaming=None))]
    pub fn results(&self, streaming: Option<bool>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let streaming = streaming.unwrap_or(false);
        let report = py.detach(|| -> Result<_, PyMaplibError> {
            let sm = self
                .inner
                .concatenated_results(self.cats.clone())
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            match sm {
                Some(sm) => {
                    let EagerSolutionMappings {
                        mut mappings,
                        mut rdf_node_types,
                    } = sm.as_eager(streaming);
                    (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                        mappings,
                        rdf_node_types,
                        false,
                        self.cats.clone(),
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
                false,
                py,
            )?)),
            None => Ok(None),
        };
        res
    }

    #[pyo3(signature = (streaming=None))]
    pub fn details(&self, streaming: Option<bool>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let streaming = streaming.unwrap_or(false);

        let details = py.detach(|| -> Result<_, PyMaplibError> {
            let sm = self
                .inner
                .concatenated_details(self.cats.clone())
                .map_err(|x| PyMaplibError::from(MaplibError::from(x)))?;
            match sm {
                Some(sm) => {
                    let EagerSolutionMappings {
                        mut mappings,
                        mut rdf_node_types,
                    } = sm.as_eager(streaming);
                    (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                        mappings,
                        rdf_node_types,
                        false,
                        self.cats.clone(),
                    );
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
                false,
                py,
            )?)),
            None => Ok(None),
        }
    }
}
