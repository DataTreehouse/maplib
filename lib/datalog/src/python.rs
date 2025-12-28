use pyo3::{pyclass, pymethods, PyResult};
use crate::inference::InferenceResult;

#[derive(Clone)]
#[pyclass(name = "InferenceResult")]
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