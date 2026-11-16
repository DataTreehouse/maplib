//! The client of the Arrow query endpoint of a maplib server (`Model.serve()`), used by
//! `Model.query(endpoint=)`. Results come in the solution mappings format of
//! `representation::arrow_wire`, so they are the same as the results of a local query.

use representation::arrow_wire::{decode_query_result, SOLUTION_MAPPINGS_MEDIA_TYPE};
use representation::result::QueryResultKind;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Builder;

#[derive(Debug, Error)]
pub enum MaplibEndpointError {
    #[error("Could not reach the maplib endpoint: {0}")]
    Request(reqwest::Error),
    #[error("The maplib endpoint answered {0}: {1}")]
    Status(u16, String),
    #[error("Invalid result from the maplib endpoint: {0}")]
    InvalidResult(String),
    #[error("Invalid endpoint {0}, give the address of a server started with Model.serve(), e.g. http://127.0.0.1:7878")]
    InvalidEndpoint(String),
}

/// A query for a maplib endpoint.
#[derive(Debug, Default, Clone)]
pub struct MaplibEndpointQuery {
    pub query: String,
    pub graph: Option<String>,
    pub include_graphs: Option<Vec<String>>,
    pub max_rows: Option<usize>,
    /// Variable names and the N-Triples syntax of the IRIs or literals they are bound to.
    pub bindings: Option<HashMap<String, String>>,
    pub auth_token: Option<String>,
    /// Seconds the server may spend on the query before it stops it.
    pub timeout: Option<f64>,
}

/// The URL of the Arrow query endpoint, from the server address or its SPARQL endpoint URL.
pub fn arrow_query_url(endpoint: &str) -> Result<String, MaplibEndpointError> {
    let e = endpoint.trim().trim_end_matches('/');
    if !(e.starts_with("http://") || e.starts_with("https://")) {
        return Err(MaplibEndpointError::InvalidEndpoint(endpoint.to_string()));
    }
    let base = e
        .strip_suffix("/arrow/query")
        .or_else(|| e.strip_suffix("/sparql"))
        .or_else(|| e.strip_suffix("/query"))
        .unwrap_or(e);
    Ok(format!("{base}/arrow/query"))
}

pub fn query_maplib_endpoint(
    endpoint: &str,
    q: &MaplibEndpointQuery,
) -> Result<QueryResultKind, MaplibEndpointError> {
    let url = arrow_query_url(endpoint)?;
    let mut command = serde_json::json!({
        "query": q.query,
        "solution_mappings": true,
        // The server compresses when this client is on another host.
        "compression": "auto",
    });
    if let Some(g) = &q.graph {
        command["graph"] = serde_json::json!(g);
    }
    if let Some(g) = &q.include_graphs {
        command["include_graphs"] = serde_json::json!(g);
    }
    if let Some(m) = q.max_rows {
        command["max_rows"] = serde_json::json!(m);
    }
    if let Some(b) = &q.bindings {
        command["bindings"] = serde_json::json!(b);
    }
    if let Some(t) = q.timeout {
        command["timeout"] = serde_json::json!(t);
    }
    // The server answers when the query times out, the client gives it some more time to do so.
    // Giving up disconnects, which cancels the query on the server.
    let request_timeout = q
        .timeout
        .filter(|t| t.is_finite() && *t >= 0.0)
        .map(|t| Duration::from_secs_f64(t) + Duration::from_secs(30));
    let body = command.to_string();
    let token = q.auth_token.clone();
    let bytes = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| MaplibEndpointError::InvalidResult(e.to_string()))?
        .block_on(async move {
            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(30))
                .build()
                .map_err(MaplibEndpointError::Request)?;
            let mut req = client.post(&url);
            if let Some(t) = request_timeout {
                req = req.timeout(t);
            }
            let mut req = req
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, SOLUTION_MAPPINGS_MEDIA_TYPE)
                .header(USER_AGENT, format!("maplib/{}", env!("CARGO_PKG_VERSION")))
                .body(body);
            if let Some(t) = token {
                req = req.header(AUTHORIZATION, format!("Bearer {t}"));
            }
            let r = req.send().await.map_err(MaplibEndpointError::Request)?;
            let status = r.status();
            let bytes = r.bytes().await.map_err(MaplibEndpointError::Request)?;
            if !status.is_success() {
                return Err(MaplibEndpointError::Status(
                    status.as_u16(),
                    String::from_utf8_lossy(&bytes).trim().to_string(),
                ));
            }
            Ok(bytes)
        })?;
    decode_query_result(&bytes).map_err(|e| MaplibEndpointError::InvalidResult(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::arrow_query_url;

    #[test]
    fn finds_the_arrow_query_url() {
        for e in [
            "http://127.0.0.1:7878",
            "http://127.0.0.1:7878/",
            "http://127.0.0.1:7878/sparql",
            "http://127.0.0.1:7878/query",
            "http://127.0.0.1:7878/arrow/query",
        ] {
            assert_eq!(
                arrow_query_url(e).unwrap(),
                "http://127.0.0.1:7878/arrow/query"
            );
        }
        assert!(arrow_query_url("grpc://127.0.0.1:7879").is_err());
    }
}
