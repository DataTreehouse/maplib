use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Maps a Microsoft Fabric ontology to RDF. Not available in the open source build.
pub fn map_fabric<'py>(
    _model: &Bound<'py, PyAny>,
    _ontology: &Bound<'py, PyAny>,
    _options: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyAny>> {
    Err(PyNotImplementedError::new_err(
        "Contact Data Treehouse to try",
    ))
}
