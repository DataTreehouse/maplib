use crate::ShaclInferenceResult;
use pyo3::{pyclass, pymethods, PyResult};

#[derive(Clone)]
#[pyclass(name = "InferenceResult", from_py_object)]
pub struct PyShaclInferenceResult {
    pub inner: ShaclInferenceResult,
}

#[pymethods]
impl PyShaclInferenceResult {
    fn __repr__(&self) -> PyResult<String> {
        Ok("Contact Data Treehouse to try".to_string())
    }

    fn __str__(&self) -> PyResult<String> {
        Ok("Contact Data Treehouse to try".to_string())
    }
}
