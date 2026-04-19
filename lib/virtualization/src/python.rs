use pyo3::prelude::*;
#[derive(Clone, Debug)]
#[pyclass(name = "VirtualizedDatabase", from_py_object)]
pub struct VirtualizedPythonDatabase {
}