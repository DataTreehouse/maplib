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
        sql_dialect: Option<Py<PyAny>>,
    ) -> PyResult<VirtualizedPythonDatabase> {
        let _ = (database, resource_sql_map, sql_dialect);
        Err(PyNotImplementedError::new_err(
            "Contact Data Treehouse to try",
        ))
    }

    #[staticmethod]
    #[pyo3(name = "Cognite", signature = (client, resource_cognite_map=None))]
    pub fn cognite(
        client: Py<PyAny>,
        resource_cognite_map: Option<Py<PyAny>>,
    ) -> PyResult<VirtualizedPythonDatabase> {
        let _ = (client, resource_cognite_map);
        Err(PyNotImplementedError::new_err(
            "Contact Data Treehouse to try",
        ))
    }
}

/// Add a maplib.virtualization module that explains that virtualization is not in this build.
pub fn register_python_modules(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::from_code(
        py,
        c"def __getattr__(name):\n    if name.startswith('__'):\n        raise AttributeError(name)\n    raise NotImplementedError('Contact Data Treehouse to try')\n",
        c"maplib/virtualization.py",
        c"maplib.virtualization",
    )?;
    parent.setattr("virtualization", &module)?;
    Ok(())
}
