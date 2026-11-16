use crate::inference::InferenceResult;
use crate::reasoning::ReasoningResult;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::{pyclass, pymethods, PyResult};

#[derive(Clone)]
#[pyclass(name = "InferenceResult", from_py_object)]
pub struct PyInferenceResult {
    pub inner: InferenceResult,
}

#[pymethods]
impl PyInferenceResult {
    fn __repr__(&self) -> PyResult<String> {
        Ok("Contact Data Treehouse to try".to_string())
    }

    fn __str__(&self) -> PyResult<String> {
        Ok("Contact Data Treehouse to try".to_string())
    }
}

#[derive(Clone)]
#[pyclass(name = "ReasoningResult", from_py_object)]
pub struct PyReasoningResult {
    pub inner: ReasoningResult,
}

#[pymethods]
impl PyReasoningResult {
    #[getter]
    fn profile(&self) -> String {
        self.inner.profile.clone()
    }

    #[getter]
    fn inferred_triples(&self) -> usize {
        self.inner.inferred_triples
    }

    #[getter]
    fn consistent(&self) -> bool {
        self.inner.consistent()
    }

    #[getter]
    fn violations<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let mut out = vec![];
        for v in &self.inner.violations {
            let d = PyDict::new(py);
            d.set_item("rule", &v.rule)?;
            d.set_item("message", &v.message)?;
            d.set_item("nodes", v.nodes.clone())?;
            out.push(d);
        }
        Ok(out)
    }

    #[getter]
    fn violations_truncated(&self) -> bool {
        self.inner.violations_truncated
    }

    #[getter]
    fn rounds(&self) -> usize {
        self.inner.rounds
    }

    #[getter]
    fn rules(&self) -> usize {
        self.inner.rules
    }

    #[getter]
    fn rule_executions(&self) -> usize {
        self.inner.rule_executions
    }

    #[getter]
    fn seconds(&self) -> f64 {
        self.inner.seconds
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(self.inner.to_string())
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(self.inner.to_string())
    }
}
