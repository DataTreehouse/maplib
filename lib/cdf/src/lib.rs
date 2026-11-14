use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub fn map_cdf(
    _model: &Bound<'_, PyAny>,
    _client: &Bound<'_, PyAny>,
    _options: &Bound<'_, PyDict>,
) -> PyResult<()> {
    Err(PyNotImplementedError::new_err(
        "Contact Data Treehouse to try",
    ))
}
