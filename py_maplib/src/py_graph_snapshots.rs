use crate::py_model::PyModel;
use crate::py_server::server_error;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use server::{GraphSnapshotOptions, GraphSnapshots, WriteLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A Python callable giving a float, for the clock and random options.
fn float_fn(f: Py<PyAny>) -> Arc<dyn Fn() -> f64 + Send + Sync> {
    Arc::new(move || {
        Python::attach(|py| {
            f.call0(py).and_then(|t| t.extract::<f64>(py)).unwrap_or_else(|e| {
                e.print(py);
                0.0
            })
        })
    })
}

/// A Python lock as a WriteLock. One with acquire and release (a threading.Lock or RLock) is tried
/// for the given time (zero: without waiting); other context managers are entered, waiting as
/// long as they do. The function runs without the GIL. Errors are printed and count as taken.
fn write_lock(lock: Py<PyAny>) -> WriteLock {
    Arc::new(move |timeout: Duration, f: &mut (dyn FnMut() + Send)| {
        Python::attach(|py| {
            let l = lock.bind(py);
            let report = |e: PyErr| {
                e.print(py);
                false
            };
            if l.hasattr("acquire").unwrap_or(false) && l.hasattr("release").unwrap_or(false) {
                let kwargs = PyDict::new(py);
                let set = if timeout.is_zero() {
                    kwargs.set_item("blocking", false)
                } else {
                    kwargs.set_item("timeout", timeout.as_secs_f64())
                };
                if let Err(e) = set {
                    return report(e);
                }
                match l.call_method("acquire", (), Some(&kwargs)).and_then(|g| g.extract::<bool>()) {
                    Ok(true) => {}
                    Ok(false) => return false,
                    Err(e) => return report(e),
                }
                py.detach(|| f());
                if let Err(e) = l.call_method0("release") {
                    e.print(py);
                }
                true
            } else {
                if let Err(e) = l.call_method0("__enter__") {
                    return report(e);
                }
                py.detach(|| f());
                if let Err(e) = l.call_method1("__exit__", (py.None(), py.None(), py.None())) {
                    e.print(py);
                }
                true
            }
        })
    })
}

/// The default of on_event: slow snapshots and the background loop's backoffs (one per failed step)
/// as warnings on the maplib.graph_snapshots logger, where the platform's log collection sees them;
/// a failed snapshot or refresh at info (a direct call raises), the rest at debug.
fn log_event(py: Python<'_>, e: &serde_json::Value) -> PyResult<()> {
    let logger = py.import("logging")?.call_method1("getLogger", ("maplib.graph_snapshots",))?;
    let f = |k: &str| e.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let kind = e.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let err = e.get("error").and_then(|v| v.as_str());
    if kind == "backoff" {
        logger.call_method1(
            "warning",
            (format!(
                "The model's snapshot in {} failed ({} in a row), retrying in {:.1} s: {}",
                e.get("url").and_then(|v| v.as_str()).unwrap_or(""),
                f("failures") as u64,
                f("retry_in_s"),
                err.unwrap_or("")
            ),),
        )?;
    } else if let Some(err) = err {
        logger.call_method1("info", (format!("A {kind} of the model's snapshot failed: {err}"),))?;
    } else if e.get("slow").and_then(|v| v.as_bool()).unwrap_or(false) {
        logger.call_method1(
            "warning",
            (format!(
                "A snapshot of {} took {:.1} s ({:.1} s waiting for a refresh, {} attempts, {} lost puts, \
                 {} files, {} bytes), longer than {} s: the snapshot on SIGTERM may not finish within \
                 the platform's grace period (Cloud Run 10 s), so keep a replica running \
                 (MIN_REPLICAS=1) or the model's graphs smaller",
                e.get("url").and_then(|v| v.as_str()).unwrap_or(""),
                f("s"),
                f("wait_s"),
                f("attempts") as u64,
                f("conflicts") as u64,
                f("files") as u64,
                f("bytes") as u64,
                f("warn_after_s"),
            ),),
        )?;
    } else {
        logger.call_method1("debug", (e.to_string(),))?;
    }
    Ok(())
}

fn seconds(name: &str, value: Option<f64>) -> PyResult<Option<Duration>> {
    match value {
        None => Ok(None),
        Some(t) if t.is_finite() && t > 0.0 => Ok(Some(Duration::from_secs_f64(t))),
        Some(t) => Err(PyValueError::new_err(format!(
            "{name} must be a positive number of seconds, not {t}"
        ))),
    }
}

/// A model kept in storage as one HDT file per named graph, with a manifest, which several
/// writers (e.g. replicas of a writable Model portal) can share (maplib Enterprise Edition).
/// See Model.serve(snapshot_format="graphs") for the same through the endpoints.
#[pyclass(name = "GraphSnapshots", frozen)]
pub struct PyGraphSnapshots {
    inner: Arc<GraphSnapshots>,
}

#[pymethods]
impl PyGraphSnapshots {
    #[new]
    #[pyo3(signature = (model, url, storage_options=None, writer=None, grace=3600.0, snapshot_interval=Some(300.0), refresh_interval=Some(10.0), debounce=2.0, gc_interval=None, warn_after=10.0, clock=None, max_delay=5.0, jitter=1.0, write_lock=None, random=None, lock_timeout=0.2, on_event=None, backoff_min=1.0, backoff_max=300.0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        model: &PyModel,
        url: String,
        storage_options: Option<HashMap<String, String>>,
        writer: Option<String>,
        grace: f64,
        snapshot_interval: Option<f64>,
        refresh_interval: Option<f64>,
        debounce: f64,
        gc_interval: Option<f64>,
        warn_after: f64,
        clock: Option<Py<PyAny>>,
        max_delay: f64,
        jitter: f64,
        write_lock: Option<Py<PyAny>>,
        random: Option<Py<PyAny>>,
        lock_timeout: f64,
        on_event: Option<Py<PyAny>>,
        backoff_min: f64,
        backoff_max: f64,
    ) -> PyResult<PyGraphSnapshots> {
        let non_negative = |name: &str, t: f64| -> PyResult<Duration> {
            if t.is_finite() && t >= 0.0 {
                Ok(Duration::from_secs_f64(t))
            } else {
                Err(PyValueError::new_err(format!("{name} must be 0 or more seconds, not {t}")))
            }
        };
        let mut o = GraphSnapshotOptions::new(url);
        o.storage_options = storage_options.unwrap_or_default();
        o.writer = writer;
        o.grace = seconds("grace", Some(grace))?.unwrap_or_default();
        o.snapshot_interval = seconds("snapshot_interval", snapshot_interval)?;
        o.refresh_interval = seconds("refresh_interval", refresh_interval)?;
        o.debounce = non_negative("debounce", debounce)?;
        o.max_delay = non_negative("max_delay", max_delay)?;
        o.jitter = non_negative("jitter", jitter)?;
        o.lock_timeout = non_negative("lock_timeout", lock_timeout)?;
        o.warn_after = non_negative("warn_after", warn_after)?;
        o.backoff_min = seconds("backoff_min", Some(backoff_min))?.unwrap_or_default();
        o.backoff_max = seconds("backoff_max", Some(backoff_max))?.unwrap_or_default().max(o.backoff_min);
        o.gc_interval = seconds("gc_interval", gc_interval)?;

        o.clock = clock.map(float_fn);
        o.on_event = Some(Arc::new(move |e: &serde_json::Value| {
            Python::attach(|py| {
                let done = match &on_event {
                    Some(f) => py
                        .import("json")
                        .and_then(|j| j.call_method1("loads", (e.to_string(),)))
                        .and_then(|d| f.call1(py, (d,)))
                        .map(|_| ()),
                    None => log_event(py, e),
                };
                if let Err(err) = done {
                    err.print(py);
                }
            })
        }));
        o.random = random.map(float_fn);
        let inner = GraphSnapshots::new(model.inner.clone(), o).map_err(server_error)?;
        inner.set_write_lock(write_lock.map(self::write_lock));
        Ok(PyGraphSnapshots { inner })
    }

    #[getter]
    fn url(&self) -> String {
        self.inner.url().to_string()
    }

    /// The name of this writer in the manifest.
    #[getter]
    fn writer(&self) -> String {
        self.inner.writer().to_string()
    }

    /// The manifest last read or published, as JSON text.
    #[getter]
    fn manifest_json(&self) -> String {
        self.inner.manifest_json()
    }

    /// Whether the model has changes that are not published.
    #[getter]
    fn pending(&self, py: Python<'_>) -> bool {
        py.detach(|| self.inner.pending())
    }

    /// The model's non-empty graphs (excluding the transient graph), the default graph as "".
    fn graphs(&self, py: Python<'_>) -> Vec<String> {
        py.detach(|| self.inner.graph_keys())
    }

    /// Reads the snapshot at the URL (if any) into the model and returns its generation (0 when
    /// there is none). A graph in the snapshot adds to a graph of the same name in the model.
    fn restore(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| self.inner.restore()).map_err(server_error)
    }

    /// Writes the graphs that changed since the last snapshot and publishes the manifest, merging
    /// with other writers' snapshots when they published first. Returns whether it published.
    fn snapshot(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.snapshot()).map_err(server_error)
    }

    /// Reads what other writers published, merging the graphs that changed here too. Returns
    /// whether the manifest had changed.
    fn refresh(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.refresh()).map_err(server_error)
    }

    /// Applies what the last refresh could not because the write lock was taken, without reading
    /// the manifest again. Returns whether the model is now up to date.
    fn catch_up(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.catch_up()).map_err(server_error)
    }

    /// Whether a graph of the last manifest read is not yet applied to the model.
    #[getter]
    fn behind(&self, py: Python<'_>) -> bool {
        py.detach(|| self.inner.behind())
    }

    /// The last snapshots and refreshes (at most 1000), oldest first, as dicts; only those with an
    /// id greater than since when it is given. See GraphSnapshots in the stubs for the keys.
    #[pyo3(signature = (since=None))]
    fn events<'py>(&self, py: Python<'py>, since: Option<u64>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let events = py.detach(|| self.inner.events(since));
        let json = py.import("json")?;
        events
            .into_iter()
            .map(|e| json.call_method1("loads", (e.to_string(),)))
            .collect()
    }

    /// Deletes graph files no manifest names, after the grace period. Returns their URLs.
    fn gc(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.inner.gc()).map_err(server_error)
    }

    /// The lock (a context manager, e.g. the Model portal's write lock) held while other writers'
    /// graphs are loaded into the model, None for none.
    #[pyo3(signature = (lock))]
    fn set_write_lock(&self, lock: Option<Py<PyAny>>) {
        self.inner.set_write_lock(lock.map(write_lock));
    }

    /// Snapshots, refreshes and collects garbage in a background thread.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.start()).map_err(server_error)
    }

    /// Stops the background thread and, with snapshot, takes a last snapshot.
    #[pyo3(signature = (snapshot=true))]
    fn stop(&self, py: Python<'_>, snapshot: bool) -> PyResult<()> {
        py.detach(|| self.inner.stop(snapshot)).map_err(server_error)
    }
}
