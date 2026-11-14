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
            result_capacity: 64,
            arrow_compression: ArrowCompression::Auto,
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
