use crate::error::PyMaplibError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::time::Duration;
use triplestore::query_cache::{QueryCacheOptions, QueryCacheStats};

/// Settings of a model's query result cache, see Model.set_query_cache().
#[derive(Clone)]
#[pyclass(name = "QueryCacheOptions", from_py_object)]
pub struct PyQueryCacheOptions {
    pub inner: QueryCacheOptions,
}

fn seconds(name: &str, v: f64) -> PyResult<Duration> {
    if v.is_finite() && v >= 0.0 {
        Ok(Duration::from_secs_f64(v))
    } else {
        Err(PyMaplibError::FunctionArgumentError(format!(
            "{name} must be a non-negative number of seconds"
        ))
        .into())
    }
}

#[pymethods]
impl PyQueryCacheOptions {
    #[new]
    #[pyo3(signature = (
        enabled=true,
        max_entries=None,
        max_bytes=None,
        max_entry_bytes=None,
        ttl=None,
        min_run_time=None,
        cache_virtualized=false,
    ))]
    pub fn new(
        enabled: bool,
        max_entries: Option<usize>,
        max_bytes: Option<usize>,
        max_entry_bytes: Option<usize>,
        ttl: Option<f64>,
        min_run_time: Option<f64>,
        cache_virtualized: bool,
    ) -> PyResult<PyQueryCacheOptions> {
        let d = QueryCacheOptions::default();
        Ok(PyQueryCacheOptions {
            inner: QueryCacheOptions {
                enabled,
                max_entries: max_entries.unwrap_or(d.max_entries),
                max_bytes: max_bytes.unwrap_or(d.max_bytes),
                max_entry_bytes: max_entry_bytes.unwrap_or(d.max_entry_bytes),
                ttl: ttl.map(|t| seconds("ttl", t)).transpose()?,
                min_run_time: min_run_time
                    .map(|t| seconds("min_run_time", t))
                    .transpose()?
                    .unwrap_or(d.min_run_time),
                cache_virtualized,
            },
        })
    }

    #[getter]
    fn enabled(&self) -> bool {
        self.inner.enabled
    }

    #[getter]
    fn max_entries(&self) -> usize {
        self.inner.max_entries
    }

    #[getter]
    fn max_bytes(&self) -> usize {
        self.inner.max_bytes
    }

    #[getter]
    fn max_entry_bytes(&self) -> usize {
        self.inner.max_entry_bytes
    }

    #[getter]
    fn ttl(&self) -> Option<f64> {
        self.inner.ttl.map(|t| t.as_secs_f64())
    }

    #[getter]
    fn min_run_time(&self) -> f64 {
        self.inner.min_run_time.as_secs_f64()
    }

    #[getter]
    fn cache_virtualized(&self) -> bool {
        self.inner.cache_virtualized
    }

    fn __eq__(&self, other: &PyQueryCacheOptions) -> bool {
        self.inner == other.inner
    }

    fn __repr__(&self) -> String {
        let o = &self.inner;
        format!(
            "QueryCacheOptions(enabled={}, max_entries={}, max_bytes={}, max_entry_bytes={}, ttl={}, min_run_time={}, cache_virtualized={})",
            if o.enabled { "True" } else { "False" },
            o.max_entries,
            o.max_bytes,
            o.max_entry_bytes,
            o.ttl
                .map(|t| t.as_secs_f64().to_string())
                .unwrap_or_else(|| "None".to_string()),
            o.min_run_time.as_secs_f64(),
            if o.cache_virtualized { "True" } else { "False" },
        )
    }
}

/// The options from what Model.set_query_cache() and Model.serve() accept: options, True for
/// the defaults with the cache enabled, False or None to disable.
pub(crate) fn options_from(value: &Bound<'_, PyAny>) -> PyResult<QueryCacheOptions> {
    if value.is_none() {
        return Ok(QueryCacheOptions::default());
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(QueryCacheOptions {
            enabled: b,
            ..QueryCacheOptions::default()
        });
    }
    match value.extract::<PyQueryCacheOptions>() {
        Ok(o) => Ok(o.inner),
        Err(_) => Err(PyMaplibError::FunctionArgumentError(
            "Expected QueryCacheOptions, True, False or None".to_string(),
        )
        .into()),
    }
}

pub(crate) fn stats_dict<'py>(py: Python<'py>, s: &QueryCacheStats) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("enabled", s.enabled)?;
    d.set_item("entries", s.entries)?;
    d.set_item("bytes", s.bytes)?;
    d.set_item("hits", s.hits)?;
    d.set_item("misses", s.misses)?;
    d.set_item("bypassed", s.bypassed)?;
    d.set_item("inserts", s.inserts)?;
    d.set_item("evictions", s.evictions)?;
    d.set_item("invalidations", s.invalidations)?;
    d.set_item("expirations", s.expirations)?;
    d.set_item("stamp", s.stamp)?;
    d.set_item("version_cells", s.version_cells)?;
    Ok(d)
}
