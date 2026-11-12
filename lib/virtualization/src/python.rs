use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
#[derive(Clone, Debug)]
#[pyclass(name = "VirtualizedDatabase", from_py_object)]
pub struct VirtualizedPythonDatabase {}

#[pymethods]
impl VirtualizedPythonDatabase {
    #[new]
    #[pyo3(signature = (database, resource_sql_map=None, sql_dialect=None))]
    pub fn new(
        database: Py<PyAny>,
        resource_sql_map: Option<Py<PyAny>>,
        sql_dialect: Option<String>,
    ) -> PyResult<VirtualizedPythonDatabase> {
        let _ = (database, resource_sql_map, sql_dialect);
        Err(PyNotImplementedError::new_err(
            "Contact Data Treehouse to try",
        ))
    }
}
