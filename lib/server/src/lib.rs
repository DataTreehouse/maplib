//! SPARQL 1.1 Protocol, Graph Store Protocol and Arrow Flight endpoints for a maplib Model.
//!
//! The endpoints are part of maplib Enterprise Edition. This crate has the same API as the
//! enterprise implementation, and `serve` returns `ServerError::NotAvailable`.

use maplib::model::Model;
use representation::dataset::NamedGraph;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(
        "Model.serve() is part of maplib Enterprise Edition, contact Data Treehouse to try it"
    )]
    NotAvailable,
    #[error("Invalid server option: {0}")]
    InvalidOption(String),
    #[error("Could not listen on {0}: {1}")]
    Bind(String, std::io::Error),
    #[error("Server error: {0}")]
    Runtime(String),
}

/// Options for serving a model. Both endpoints can run at the same time, on separate ports.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// The address to listen on.
    pub host: String,
    /// The port of the SPARQL protocol endpoints, None disables them. 0 picks a free port.
    pub sparql_port: Option<u16>,
    /// The port of the Arrow Flight endpoint, None disables it. 0 picks a free port.
    pub flight_port: Option<u16>,
    /// Allow SPARQL Update and writes through the Graph Store Protocol and the Arrow API.
    pub allow_update: bool,
    /// Serve the Arrow API (queries, graphs and mapping uploads, as with Flight) over plain HTTP
    /// at /arrow on the SPARQL port, for networks where gRPC does not get through. It is also
    /// used by `Model.query(endpoint=)`.
    pub arrow_http: bool,
    /// When set, every request must carry `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
    /// Origins allowed by CORS, e.g. "https://yasgui.org", "*" allows all. Empty (the default)
    /// disables CORS, so web pages of other origins can not read results or send updates.
    pub cors_origins: Vec<String>,
    /// The graph queried when the request gives no dataset. None is the default graph.
    pub graph: Option<NamedGraph>,
    /// Graphs merged into the default graph of queries, as with `Model.query(include_graphs=)`.
    pub include_graphs: Vec<NamedGraph>,
    /// Queries with more result rows than this fail.
    pub max_rows: Option<usize>,
    /// The largest request body accepted.
    pub max_request_bytes: usize,
    /// The number of rows in each Arrow record batch sent.
    pub batch_rows: usize,
    /// How long results prepared by Flight GetFlightInfo are kept for DoGet.
    pub result_ttl: Duration,
    /// The largest number of prepared Flight results kept at once.
    pub result_capacity: usize,
    /// Compression of the record batches in Arrow results (Flight and Arrow over HTTP), when
    /// the request does not choose. Readers decompress them transparently.
    pub arrow_compression: ArrowCompression,
    /// A web application to serve from this folder, read from disk on every request.
    pub static_dir: Option<std::path::PathBuf>,
    /// Where the application is served, e.g. "/ui".
    pub static_path: String,
    /// Values for the `<!--#echo var='name' -->` directives (nginx SSI) in its HTML pages.
    pub static_vars: std::collections::HashMap<String, String>,
    /// How many queries and updates the query log (`/query-log`) keeps, 0 for no log.
    pub query_log_size: usize,
    /// Answer queries from the model's query result cache (see `Model.set_query_cache`), when
    /// it is enabled.
    pub query_cache: bool,
    /// Endpoints that SERVICE in queries to the server may reach (prefixes, as in
    /// `FederationSettings`), "*" for any. Empty (the default) refuses SERVICE, so that clients
    /// can not make the server send requests, e.g. to internal hosts. The other federation
    /// settings (headers, timeouts, batching) are those of the model.
    pub service_endpoints: Vec<String>,
    /// Allow SPARQL Update LOAD (with allow_update) of the service endpoints' URLs.
    pub allow_load: bool,
    /// How long a query or update may take, from when its request arrives. None for no limit.
    /// Requests can ask for less (or, up to `max_query_timeout`, more). A query that times out
    /// is stopped and answered with 503 Service Unavailable.
    pub query_timeout: Option<Duration>,
    /// The longest timeout a request can ask for. None uses `query_timeout`.
    pub max_query_timeout: Option<Duration>,
    /// How many operations on the model (queries, updates, graph reads and writes) run at the
    /// same time. Others wait for their turn.
    pub max_concurrent: usize,
    /// Snapshot the model (serialize it) every interval when it changed, and when the server
    /// stops. None takes no snapshots.
    pub snapshot: Option<SnapshotOptions>,
}

/// Where and how often a served model is snapshotted.
#[derive(Debug, Clone)]
pub struct SnapshotOptions {
    /// A path or URL (gs://, az://, s3://, ...) to serialize the model to, as `Model.serialize`
    /// does. A URL ending in `.maplib` is written as a single file, others as a folder.
    pub url: String,
    /// How often to check for changes. None snapshots only when the server stops.
    pub interval: Option<Duration>,
    /// Object storage options, as for `Model.serialize(storage_options=)`.
    pub storage_options: std::collections::HashMap<String, String>,
    /// How the model is kept at the URL.
    pub format: SnapshotFormat,
}

/// How a served model is snapshotted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFormat {
    /// The whole model with `Model.serialize`, for a single writer.
    Model,
    /// One HDT file per named graph and a manifest, see `GraphSnapshots`: only the changed
    /// graphs are written, several writers can share the URL, and the snapshot at the URL is
    /// restored into the model when the server starts.
    Graphs {
        /// How often other writers' snapshots are read. None never reads them.
        refresh_interval: Option<Duration>,
    },
}

/// Options of `GraphSnapshots`.
#[derive(Clone)]
pub struct GraphSnapshotOptions {
    /// Where the snapshot is kept: a path or URL (gs://, az://, s3://, file://).
    pub url: String,
    /// Object storage options, as for `Model.serialize(storage_options=)`.
    pub storage_options: std::collections::HashMap<String, String>,
    /// The name of this writer in the manifest, by default host-pid-random.
    pub writer: Option<String>,
    /// How long a file no manifest names is kept (another writer may still merge against it);
    /// tombstones of removed graphs are kept twice as long.
    pub grace: Duration,
    /// The background loop: a snapshot `debounce` after the last change and at least every
    /// `snapshot_interval`.
    pub snapshot_interval: Option<Duration>,
    pub debounce: Duration,
    /// The background loop publishes changes at most this long after the first unpublished
    /// one, so that steady writes are still published.
    pub max_delay: Duration,
    /// Each writer waits up to this much longer (drawn per wait), and starts its periodic work at
    /// a random point in its interval, so that writers changing the model together do not all
    /// publish the manifest at the same moment.
    pub jitter: Duration,
    /// How often the background loop reads other writers' snapshots.
    pub refresh_interval: Option<Duration>,
    /// How often the background loop collects garbage, by default max(60 s, grace / 4).
    pub gc_interval: Option<Duration>,
    /// Snapshots slower than this are logged as warnings.
    pub warn_after: Duration,
    /// How long a refresh waits for the write lock before it leaves the model to catch up later.
    pub lock_timeout: Duration,
    /// When the store fails (a missing bucket or container, auth or network errors), the
    /// background loop waits before its next try: `backoff_min`, doubling with each failure in a
    /// row up to `backoff_max`, with up to half as much again of jitter. A success resets it.
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Seconds since the Unix epoch, for the times in the manifest and garbage collection.
    pub clock: Option<std::sync::Arc<dyn Fn() -> f64 + Send + Sync>>,
    /// Random numbers in [0, 1) for the jitter, for tests.
    pub random: Option<std::sync::Arc<dyn Fn() -> f64 + Send + Sync>>,
    /// Called with each event (see `GraphSnapshots::events`) after the snapshot or refresh, without
    /// any of the snapshots' locks held, e.g. to log slow snapshots where the platform collects logs.
    pub on_event: Option<std::sync::Arc<dyn Fn(&serde_json::Value) + Send + Sync>>,
}

/// Runs a function while holding a lock of the caller's, e.g. the Model portal's write lock, so
/// that other writers' graphs are loaded into the model between the portal's requests: it waits
/// at most the given time for the lock (zero: only tries it) and returns whether it ran the
/// function. It must not be one that is held while waiting for the snapshots.
pub type WriteLock = std::sync::Arc<dyn Fn(Duration, &mut (dyn FnMut() + Send)) -> bool + Send + Sync>;

impl GraphSnapshotOptions {
    pub fn new(url: String) -> GraphSnapshotOptions {
        GraphSnapshotOptions {
            url,
            storage_options: Default::default(),
            writer: None,
            grace: Duration::from_secs(3600),
            snapshot_interval: Some(Duration::from_secs(300)),
            debounce: Duration::from_secs(2),
            max_delay: Duration::from_secs(5),
            jitter: Duration::from_secs(1),
            refresh_interval: Some(Duration::from_secs(10)),
            gc_interval: None,
            warn_after: Duration::from_secs(10),
            lock_timeout: Duration::from_millis(200),
            backoff_min: Duration::from_secs(1),
            backoff_max: Duration::from_secs(300),
            clock: None,
            random: None,
            on_event: None,
        }
    }
}

/// A model kept in storage as one HDT file per named graph (maplib Enterprise Edition).
pub struct GraphSnapshots {
    _private: (),
}

impl GraphSnapshots {
    pub fn new(
        _model: Arc<RwLock<Model>>,
        _options: GraphSnapshotOptions,
    ) -> Result<Arc<GraphSnapshots>, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn url(&self) -> &str {
        ""
    }
    pub fn writer(&self) -> &str {
        ""
    }
    pub fn restore(&self) -> Result<u64, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn snapshot(&self) -> Result<bool, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn refresh(&self) -> Result<bool, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn gc(&self) -> Result<Vec<String>, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn pending(&self) -> bool {
        false
    }
    pub fn manifest_json(&self) -> String {
        "{}".to_string()
    }
    pub fn graph_keys(&self) -> Vec<String> {
        vec![]
    }
    pub fn start(self: &Arc<Self>) -> Result<(), ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn stop(&self, _snapshot: bool) -> Result<(), ServerError> {
        Ok(())
    }
    pub fn set_write_lock(&self, _lock: Option<WriteLock>) {}
    pub fn catch_up(&self) -> Result<bool, ServerError> {
        Err(ServerError::NotAvailable)
    }
    pub fn behind(&self) -> bool {
        false
    }
    pub fn events(&self, _since: Option<u64>) -> Vec<serde_json::Value> {
        vec![]
    }
}

/// Compression of Arrow record batch buffers, as defined by the Arrow IPC format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowCompression {
    None,
    Zstd,
    /// zstd for clients on other hosts, none for clients on the same host, where compressing
    /// costs more time than it saves.
    Auto,
}

impl ArrowCompression {
    /// Parses "zstd", "none" (or "uncompressed") and "auto".
    pub fn parse(s: &str) -> Result<ArrowCompression, ServerError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "zstd" => Ok(ArrowCompression::Zstd),
            "none" | "uncompressed" => Ok(ArrowCompression::None),
            "auto" => Ok(ArrowCompression::Auto),
            other => Err(ServerError::InvalidOption(format!(
                "Unknown Arrow compression {other}, use zstd, none or auto"
            ))),
        }
    }

    /// Whether to compress for a client at `peer`.
    pub fn compress_for(self, peer: Option<SocketAddr>) -> bool {
        match self {
            ArrowCompression::None => false,
            ArrowCompression::Zstd => true,
            ArrowCompression::Auto => peer.is_some_and(|p| !p.ip().is_loopback()),
        }
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            host: "127.0.0.1".to_string(),
            sparql_port: Some(7878),
            flight_port: Some(7879),
            allow_update: false,
            arrow_http: true,
            auth_token: None,
            cors_origins: vec![],
            graph: None,
            include_graphs: vec![NamedGraph::transient()],
            max_rows: None,
            max_request_bytes: 256 << 20,
            batch_rows: 65_536,
            result_ttl: Duration::from_secs(600),
            static_dir: None,
            static_path: "/ui".to_string(),
            static_vars: Default::default(),
            query_log_size: 1000,
            query_cache: true,
            service_endpoints: vec![],
            allow_load: false,
            result_capacity: 64,
            arrow_compression: ArrowCompression::Auto,
            query_timeout: None,
            max_query_timeout: None,
            max_concurrent: 64,
            snapshot: None,
        }
    }
}

/// A running server. Dropping the handle leaves the server running, call `stop` to stop it.
pub struct ServerHandle {
    _private: (),
}

impl ServerHandle {
    pub fn sparql_addr(&self) -> Option<SocketAddr> {
        None
    }

    pub fn flight_addr(&self) -> Option<SocketAddr> {
        None
    }

    pub fn is_running(&self) -> bool {
        false
    }

    /// Waits for the server to stop, at most `timeout`. Returns true if it has stopped.
    pub fn wait_timeout(&self, _timeout: Duration) -> bool {
        true
    }

    pub fn stop(&mut self) -> Result<(), ServerError> {
        Ok(())
    }
}

/// Starts the endpoints in the background and returns when they are listening.
pub fn serve(
    _model: Arc<RwLock<Model>>,
    _options: ServerOptions,
) -> Result<ServerHandle, ServerError> {
    Err(ServerError::NotAvailable)
}
