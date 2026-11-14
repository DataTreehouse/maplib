use crate::error::PyMaplibError;
use pyo3::prelude::*;
use server::{ServerError, ServerHandle};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A running SPARQL protocol and Arrow Flight server, see Model.serve().
#[pyclass(name = "Server", frozen)]
pub struct PyServer {
    handle: Mutex<ServerHandle>,
}

impl PyServer {
    pub fn new(handle: ServerHandle) -> PyServer {
        PyServer {
            handle: Mutex::new(handle),
        }
    }

    fn sparql_addr(&self) -> Option<SocketAddr> {
        self.lock().sparql_addr()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ServerHandle> {
        self.handle.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn http_url(&self, path: &str) -> Option<String> {
        self.sparql_addr()
            .map(|a| format!("http://{}{}", display_addr(a), path))
    }
}

/// Connectable address, a server listening on all interfaces is reached through localhost.
fn display_addr(addr: SocketAddr) -> String {
    let ip = addr.ip();
    if ip.is_unspecified() {
        if ip.is_ipv4() {
            format!("127.0.0.1:{}", addr.port())
        } else {
            format!("[::1]:{}", addr.port())
        }
    } else {
        addr.to_string()
    }
}

pub(crate) fn server_error(e: ServerError) -> PyErr {
    match e {
        ServerError::InvalidOption(s) => PyMaplibError::FunctionArgumentError(s).into(),
        e => PyMaplibError::RuntimeError(e.to_string()).into(),
    }
}

#[pymethods]
impl PyServer {
    /// The SPARQL query and update endpoint.
    #[getter]
    fn url(&self) -> Option<String> {
        self.http_url("/sparql")
    }

    #[getter]
    fn query_url(&self) -> Option<String> {
        self.http_url("/query")
    }

    #[getter]
    fn update_url(&self) -> Option<String> {
        self.http_url("/update")
    }

    /// The Graph Store Protocol endpoint.
    #[getter]
    fn store_url(&self) -> Option<String> {
        self.http_url("/store")
    }

    #[getter]
    fn port(&self) -> Option<u16> {
        self.sparql_addr().map(|a| a.port())
    }

    /// The Arrow Flight endpoint, for pyarrow.flight.connect().
    #[getter]
    fn flight_url(&self) -> Option<String> {
        self.lock()
            .flight_addr()
            .map(|a| format!("grpc://{}", display_addr(a)))
    }

    #[getter]
    fn flight_port(&self) -> Option<u16> {
        self.lock().flight_addr().map(|a| a.port())
    }

    #[getter]
    fn running(&self) -> bool {
        self.lock().is_running()
    }

    /// Stops the server, running requests are allowed to finish.
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.lock().stop()).map_err(server_error)
    }

    /// Blocks until the server stops or timeout seconds have passed, returns whether it stopped.
    /// Ctrl-C stops the server.
    #[pyo3(signature = (timeout=None))]
    pub(crate) fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t.max(0.0)));
        loop {
            let step = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return Ok(!self.running());
                    }
                    (d - now).min(Duration::from_millis(100))
                }
                None => Duration::from_millis(100),
            };
            if py.detach(|| self.lock().wait_timeout(step)) {
                return Ok(true);
            }
            if let Err(e) = py.check_signals() {
                self.stop(py)?;
                return Err(e);
            }
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.stop(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let state = if self.running() { "running" } else { "stopped" };
        let mut parts = vec![];
        if let Some(u) = self.url() {
            parts.push(format!("sparql={u}"));
        }
        if let Some(u) = self.flight_url() {
            parts.push(format!("flight={u}"));
        }
        format!("Server({}, {state})", parts.join(", "))
    }
}
