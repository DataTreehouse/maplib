//! The client side of SPARQL 1.1 Federated Query (`SERVICE`) and of SPARQL Update `LOAD`.
//!
//! [`FederationSettings`] says which endpoints and URLs may be reached, with which headers,
//! timeouts and limits. [`run_service_calls`] sends the queries of one `SERVICE` evaluation (the
//! batches of a bind join) to an endpoint, at most `concurrency` at a time, and parses the
//! results. Queries are sent with the SPARQL 1.1 Protocol, asking for maplib's solution
//! mappings format first, so that a maplib server (`Model.serve()`) answers with Arrow, and other
//! endpoints with SPARQL JSON, XML or TSV results. Once an endpoint has answered with solution
//! mappings, it is remembered as a maplib endpoint, and bind join batches are sent to its Arrow
//! API (`/arrow/query`) as Arrow parameters of the query instead of as VALUES text.
//!
//! The requests run on their own thread with their own Tokio runtime, so that they can be made
//! from synchronous code that runs within another runtime (e.g. the server or chrontext).

use crate::maplib_endpoint::arrow_query_url;
use futures::{StreamExt, TryStreamExt};
use polars::frame::DataFrame;
use polars::prelude::{col, Column, IntoLazy};
use representation::arrow_wire::{
    decode_query_result, encode_query_result, SOLUTION_MAPPINGS_MEDIA_TYPE,
};
use representation::cats::LockedCats;
use representation::result::QueryResultKind;
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use reqwest::header::{HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QuerySolution, SliceQueryResultsParserOutput,
};
use spargebra::term::Variable;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use thiserror::Error;
use url::Url;

/// The name of the PVALUES parameter holding the bindings sent to a maplib endpoint.
pub const SERVICE_BINDINGS_PARAMETER: &str = "maplib_service_bindings";

/// The media type of an Arrow API query command followed by its parameters, see
/// [`encode_parameterized_command`].
pub const PARAMETERIZED_QUERY_MEDIA_TYPE: &str = "application/vnd.maplib.parameterized-query";

const SPARQL_RESULTS_ACCEPT_NO_ARROW: &str = "application/sparql-results+json, application/sparql-results+xml;q=0.8, text/tab-separated-values;q=0.5";
const SPARQL_RESULTS_ACCEPT: &str = "application/vnd.maplib.solution-mappings, application/sparql-results+json;q=0.9, application/sparql-results+xml;q=0.8, text/tab-separated-values;q=0.5";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceMethod {
    Get,
    Post,
}

/// Which endpoints and URLs `SERVICE` and `LOAD` may reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowedEndpoints {
    /// Any http(s) URL (and object storage for LOAD), but not file URLs.
    Any,
    /// URLs that start with one of these, matched by scheme, host, port and then path
    /// segments, e.g. `http://localhost:7878` allows every path on that server and
    /// `https://example.org/sparql` allows `https://example.org/sparql` and
    /// `https://example.org/sparql/x`, but not `https://example.org/sparqlx`. Empty allows nothing.
    Only(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct FederationSettings {
    pub allowed_endpoints: AllowedEndpoints,
    /// SPARQL Update LOAD is refused unless this is set.
    pub allow_load: bool,
    /// Headers sent to the endpoints (and LOAD URLs) that start with the key, matched as the
    /// allowed endpoints are, e.g. `Authorization: Bearer ...`.
    pub headers: Vec<(String, Vec<(String, String)>)>,
    /// Timeout of each request, from sending it to having read the whole response.
    pub timeout: Option<Duration>,
    pub connect_timeout: Duration,
    /// The most rows that one SERVICE evaluation may receive in total, an error beyond.
    pub max_rows: Option<usize>,
    /// Rows of bound values per VALUES block sent to a SPARQL endpoint.
    pub batch_size: usize,
    /// Rows of bound values per request to a maplib endpoint, sent as Arrow.
    pub arrow_batch_size: usize,
    /// The most requests to one endpoint that are in flight at the same time.
    pub concurrency: usize,
    pub method: ServiceMethod,
    /// Send the solutions bound so far with the pattern (a bind join). Otherwise the pattern is
    /// sent alone and the results are joined locally.
    pub bind_join: bool,
    /// Ask for maplib's Arrow format, so that maplib endpoints answer with it, and send them
    /// the bindings as Arrow. Otherwise maplib endpoints are queried as any other endpoint.
    pub arrow: bool,
}

impl Default for FederationSettings {
    fn default() -> Self {
        FederationSettings {
            allowed_endpoints: AllowedEndpoints::Any,
            allow_load: false,
            headers: vec![],
            timeout: Some(Duration::from_secs(300)),
            connect_timeout: Duration::from_secs(30),
            max_rows: None,
            batch_size: 1000,
            arrow_batch_size: 1_000_000,
            concurrency: 4,
            method: ServiceMethod::Post,
            bind_join: true,
            arrow: true,
        }
    }
}

impl FederationSettings {
    /// Settings that allow no SERVICE and no LOAD, as used by `Model.serve()` by default.
    pub fn deny_all() -> Self {
        FederationSettings {
            allowed_endpoints: AllowedEndpoints::Only(vec![]),
            allow_load: false,
            ..Default::default()
        }
    }

    /// Is the URL allowed by the allow-list? `load` also allows object storage URLs, and file
    /// URLs when they are listed explicitly.
    pub fn is_allowed(&self, url: &str, load: bool) -> bool {
        let Ok(u) = Url::parse(url) else {
            return false;
        };
        let scheme = u.scheme();
        let web = matches!(scheme, "http" | "https");
        let object_storage = matches!(scheme, "s3" | "s3a" | "gs" | "az" | "abfs" | "abfss");
        if !(web || (load && (object_storage || scheme == "file"))) {
            return false;
        }
        match &self.allowed_endpoints {
            AllowedEndpoints::Any => scheme != "file",
            AllowedEndpoints::Only(prefixes) => prefixes
                .iter()
                .any(|p| p.trim() == "*" && scheme != "file" || url_matches_prefix(&u, p)),
        }
    }

    pub fn check_allowed(&self, url: &str, load: bool) -> Result<(), ServiceError> {
        if self.is_allowed(url, load) {
            return Ok(());
        }
        // A URL that cannot be fetched at all is an error of the endpoint (which SILENT
        // ignores), not a refusal by the policy.
        match Url::parse(url) {
            Err(e) => Err(ServiceError::InvalidEndpoint(url.to_string(), e.to_string())),
            Ok(u) => {
                let scheme = u.scheme();
                let supported = matches!(scheme, "http" | "https")
                    || (load
                        && matches!(
                            scheme,
                            "s3" | "s3a" | "gs" | "az" | "abfs" | "abfss" | "file"
                        ));
                if supported {
                    Err(ServiceError::NotAllowed(url.to_string()))
                } else {
                    Err(ServiceError::InvalidEndpoint(
                        url.to_string(),
                        format!("the scheme {scheme} is not supported"),
                    ))
                }
            }
        }
    }

    /// The headers for the URL.
    pub fn headers_for(&self, url: &str) -> Vec<(String, String)> {
        let Ok(u) = Url::parse(url) else {
            return vec![];
        };
        let mut out = vec![];
        for (p, hs) in &self.headers {
            if p.trim() == "*" || url_matches_prefix(&u, p) {
                out.extend(hs.iter().cloned());
            }
        }
        out
    }
}

fn default_port(u: &Url) -> Option<u16> {
    u.port_or_known_default()
}

/// Does the URL start with the prefix, by scheme, host, port and whole path segments?
pub fn url_matches_prefix(u: &Url, prefix: &str) -> bool {
    let Ok(p) = Url::parse(prefix.trim()) else {
        return false;
    };
    if u.scheme() != p.scheme()
        || u.host_str().map(|h| h.to_ascii_lowercase()) != p.host_str().map(|h| h.to_ascii_lowercase())
        || default_port(u) != default_port(&p)
        || !u.username().is_empty() && u.username() != p.username()
    {
        return false;
    }
    let pp = p.path().trim_end_matches('/');
    let up = u.path();
    if pp.is_empty() {
        return true;
    }
    if up == pp {
        // A prefix with a query only allows that query.
        return p.query().is_none() || p.query() == u.query();
    }
    up.starts_with(pp) && up[pp.len()..].starts_with('/')
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("The endpoint <{0}> is not allowed. Allow it with Model.set_federation(allowed_endpoints=[...]), or with Model.serve(service_endpoints=[...]) for queries to a served model")]
    NotAllowed(String),
    #[error("LOAD is not allowed, enable it with Model.set_federation(allow_load=True), or with Model.serve(allow_load=True)")]
    LoadNotAllowed,
    #[error("Invalid endpoint <{0}>: {1}")]
    InvalidEndpoint(String, String),
    #[error("Could not reach the endpoint <{0}>: {1}")]
    Request(String, String),
    #[error("The endpoint <{0}> did not answer within the timeout")]
    Timeout(String),
    #[error("The endpoint <{0}> answered {1}: {2}")]
    Status(String, u16, String),
    #[error("Invalid results from the endpoint <{0}>: {1}")]
    InvalidResults(String, String),
    #[error("The endpoint <{0}> answered with the unsupported media type {1}")]
    UnsupportedMediaType(String, String),
    #[error("The SERVICE <{0}> gave more than the maximum of {1} rows (max_rows of the federation settings)")]
    TooManyRows(String, usize),
    #[error("Could not run the requests: {0}")]
    Runtime(String),
}

impl ServiceError {
    /// Errors that SERVICE SILENT does not hide, since they come from maplib's own settings
    /// and not from the remote endpoint.
    pub fn is_policy_error(&self) -> bool {
        matches!(
            self,
            ServiceError::NotAllowed(_)
                | ServiceError::LoadNotAllowed
                | ServiceError::TooManyRows(..)
        )
    }
}

fn maplib_endpoints() -> &'static Mutex<HashSet<String>> {
    static M: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Has the endpoint answered with maplib's solution mappings format, so that it is a maplib
/// server with the Arrow API?
pub fn is_known_maplib_endpoint(endpoint: &str) -> bool {
    maplib_endpoints().lock().unwrap().contains(endpoint)
}

fn remember_maplib_endpoint(endpoint: &str) {
    maplib_endpoints()
        .lock()
        .unwrap()
        .insert(endpoint.to_string());
}

/// Forgets which endpoints are maplib servers (for tests and benchmarks).
pub fn forget_maplib_endpoints() {
    maplib_endpoints().lock().unwrap().clear();
}

/// One request of a SERVICE evaluation.
pub enum ServiceCall {
    /// A SELECT query sent with the SPARQL 1.1 Protocol. The variables are those projected,
    /// in order.
    Sparql {
        query: String,
        variables: Vec<Variable>,
    },
    /// A SELECT query sent to the Arrow API of a maplib server, with the bindings as the PVALUES
    /// parameter [`SERVICE_BINDINGS_PARAMETER`].
    Maplib {
        query: String,
        variables: Vec<Variable>,
        bindings: EagerSolutionMappings,
    },
}

/// Runs the future to completion on a new thread with its own Tokio runtime.
pub fn block_on_own_runtime<F, T>(threads: usize, f: F) -> Result<T, ServiceError>
where
    F: Future<Output = T> + Send,
    T: Send,
{
    std::thread::scope(|s| {
        s.spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads.clamp(1, 16))
                .enable_all()
                .build()
                .map_err(|e| ServiceError::Runtime(e.to_string()))?;
            Ok(rt.block_on(f))
        })
        .join()
        .map_err(|_| ServiceError::Runtime("The request thread panicked".to_string()))?
    })
}

/// A client that only follows redirects to allowed URLs.
pub fn http_client(
    settings: &Arc<FederationSettings>,
    load: bool,
) -> Result<reqwest::Client, ServiceError> {
    let redirect_settings = settings.clone();
    let policy = reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= 10 {
            attempt.error("Too many redirects")
        } else if redirect_settings.is_allowed(attempt.url().as_str(), load) {
            attempt.follow()
        } else {
            let url = attempt.url().to_string();
            attempt.error(format!("Redirected to <{url}>, which is not allowed"))
        }
    });
    let mut b = reqwest::Client::builder()
        .connect_timeout(settings.connect_timeout)
        .redirect(policy);
    if let Some(t) = settings.timeout {
        b = b.timeout(t);
    }
    b.build()
        .map_err(|e| ServiceError::Runtime(e.to_string()))
}

fn request_error(endpoint: &str, e: reqwest::Error) -> ServiceError {
    if e.is_timeout() {
        ServiceError::Timeout(endpoint.to_string())
    } else {
        let mut msg = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(s) = source {
            msg.push_str(": ");
            msg.push_str(&s.to_string());
            source = s.source();
        }
        ServiceError::Request(endpoint.to_string(), msg)
    }
}

fn add_headers(
    mut rb: reqwest::RequestBuilder,
    headers: &[(String, String)],
) -> Result<reqwest::RequestBuilder, ServiceError> {
    for (n, v) in headers {
        let name = HeaderName::from_bytes(n.as_bytes())
            .map_err(|e| ServiceError::Runtime(format!("Invalid header name {n}: {e}")))?;
        let value = HeaderValue::from_str(v)
            .map_err(|e| ServiceError::Runtime(format!("Invalid value of header {n}: {e}")))?;
        rb = rb.header(name, value);
    }
    Ok(rb)
}

fn user_agent() -> String {
    format!("maplib/{}", env!("CARGO_PKG_VERSION"))
}

/// Sends the calls to the endpoint, at most `settings.concurrency` at a time, and returns
/// their results. Stops at the first error.
pub fn run_service_calls(
    settings: &Arc<FederationSettings>,
    endpoint: &str,
    calls: Vec<ServiceCall>,
    cats: LockedCats,
) -> Result<Vec<EagerSolutionMappings>, ServiceError> {
    settings.check_allowed(endpoint, false)?;
    if calls.is_empty() {
        return Ok(vec![]);
    }
    let client = http_client(settings, false)?;
    let headers = settings.headers_for(endpoint);
    let rows = AtomicUsize::new(0);
    let concurrency = settings.concurrency.max(1);
    let fut = async {
        futures::stream::iter(calls.into_iter().map(|c| {
            let client = &client;
            let headers = &headers;
            let rows = &rows;
            let cats = cats.clone();
            async move {
                let sm = match c {
                    ServiceCall::Sparql { query, variables } => {
                        sparql_call(client, settings, endpoint, headers, query, variables).await?
                    }
                    ServiceCall::Maplib {
                        query,
                        variables,
                        bindings,
                    } => {
                        maplib_call(
                            client, settings, endpoint, headers, query, variables, bindings, cats,
                        )
                        .await?
                    }
                };
                let n = rows.fetch_add(sm.mappings.height(), Ordering::Relaxed)
                    + sm.mappings.height();
                if let Some(max) = settings.max_rows {
                    if n > max {
                        return Err(ServiceError::TooManyRows(endpoint.to_string(), max));
                    }
                }
                Ok(sm)
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await
    };
    block_on_own_runtime(concurrency, fut)?
}

async fn sparql_call(
    client: &reqwest::Client,
    settings: &FederationSettings,
    endpoint: &str,
    headers: &[(String, String)],
    query: String,
    variables: Vec<Variable>,
) -> Result<EagerSolutionMappings, ServiceError> {
    let rb = match settings.method {
        ServiceMethod::Get => client.get(endpoint).query(&[("query", query.as_str())]),
        ServiceMethod::Post => client
            .post(endpoint)
            .header(CONTENT_TYPE, "application/sparql-query")
            .body(query),
    };
    let accept = if settings.arrow {
        SPARQL_RESULTS_ACCEPT
    } else {
        SPARQL_RESULTS_ACCEPT_NO_ARROW
    };
    let rb = rb
        .header(ACCEPT, accept)
        .header(USER_AGENT, user_agent());
    let rb = add_headers(rb, headers)?;
    let response = rb.send().await.map_err(|e| request_error(endpoint, e))?;
    let status = response.status();
    let media_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").trim().to_ascii_lowercase())
        .unwrap_or_default();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| request_error(endpoint, e))?;
    if !status.is_success() {
        return Err(status_error(endpoint, status.as_u16(), &bytes));
    }
    let endpoint = endpoint.to_string();
    tokio::task::spawn_blocking(move || {
        if media_type == SOLUTION_MAPPINGS_MEDIA_TYPE {
            remember_maplib_endpoint(&endpoint);
            decode_solution_mappings(&endpoint, &bytes, &variables)
        } else {
            parse_sparql_results(&endpoint, &media_type, &bytes, &variables)
        }
    })
    .await
    .map_err(|e| ServiceError::Runtime(e.to_string()))?
}

fn status_error(endpoint: &str, status: u16, body: &[u8]) -> ServiceError {
    let mut msg = String::from_utf8_lossy(body).trim().to_string();
    if msg.len() > 2000 {
        let mut cut = 2000;
        while !msg.is_char_boundary(cut) {
            cut -= 1;
        }
        msg.truncate(cut);
        msg.push_str("...");
    }
    ServiceError::Status(endpoint.to_string(), status, msg)
}

#[allow(clippy::too_many_arguments)]
async fn maplib_call(
    client: &reqwest::Client,
    settings: &FederationSettings,
    endpoint: &str,
    headers: &[(String, String)],
    query: String,
    variables: Vec<Variable>,
    bindings: EagerSolutionMappings,
    cats: LockedCats,
) -> Result<EagerSolutionMappings, ServiceError> {
    let url = arrow_query_url(endpoint)
        .map_err(|e| ServiceError::InvalidEndpoint(endpoint.to_string(), e.to_string()))?;
    let max_rows = settings.max_rows;
    let body = tokio::task::spawn_blocking(move || {
        let mut command = serde_json::json!({
            "query": query,
            "solution_mappings": true,
            "compression": "auto",
            "parameters": [SERVICE_BINDINGS_PARAMETER],
        });
        if let Some(m) = max_rows {
            command["max_rows"] = serde_json::json!(m);
        }
        encode_parameterized_command(&command.to_string(), vec![bindings], cats)
    })
    .await
    .map_err(|e| ServiceError::Runtime(e.to_string()))?
    .map_err(|e| ServiceError::Runtime(format!("Could not encode the bindings: {e}")))?;
    let rb = client
        .post(&url)
        .header(CONTENT_TYPE, PARAMETERIZED_QUERY_MEDIA_TYPE)
        .header(ACCEPT, SOLUTION_MAPPINGS_MEDIA_TYPE)
        .header(USER_AGENT, user_agent())
        .body(body);
    let rb = add_headers(rb, headers)?;
    let response = rb.send().await.map_err(|e| request_error(endpoint, e))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| request_error(endpoint, e))?;
    if !status.is_success() {
        return Err(status_error(endpoint, status.as_u16(), &bytes));
    }
    let endpoint = endpoint.to_string();
    tokio::task::spawn_blocking(move || decode_solution_mappings(&endpoint, &bytes, &variables))
        .await
        .map_err(|e| ServiceError::Runtime(e.to_string()))?
}

/// The body of a parameterized Arrow API query: the JSON command as a frame (an 8 byte little
/// endian length and the bytes), followed by each parameter in the solution mappings format, in
/// the order of the command's `parameters`.
pub fn encode_parameterized_command(
    command: &str,
    parameters: Vec<EagerSolutionMappings>,
    cats: LockedCats,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.extend_from_slice(&(command.len() as u64).to_le_bytes());
    out.extend_from_slice(command.as_bytes());
    for p in parameters {
        let b = encode_query_result(QueryResultKind::Select(p), cats.clone(), false, 1 << 20)
            .map_err(|e| e.to_string())?;
        out.extend_from_slice(&b);
    }
    Ok(out)
}

/// Splits a parameterized query body into the JSON command and the encoded parameters.
pub fn decode_parameterized_command(
    body: &[u8],
) -> Result<(String, Vec<EagerSolutionMappings>), String> {
    let frame = |b: &[u8], at: usize| -> Result<(usize, usize), String> {
        if b.len() < at + 8 {
            return Err("Truncated parameterized query".to_string());
        }
        let n = u64::from_le_bytes(b[at..at + 8].try_into().unwrap()) as usize;
        if b.len() < at + 8 + n {
            return Err("Truncated parameterized query".to_string());
        }
        Ok((at + 8, at + 8 + n))
    };
    let (s, e) = frame(body, 0)?;
    let command = std::str::from_utf8(&body[s..e])
        .map_err(|_| "The query command is not UTF-8".to_string())?
        .to_string();
    let mut params = vec![];
    let mut at = e;
    while at < body.len() {
        let (_, e) = frame(body, at)?;
        match decode_query_result(&body[at..e]).map_err(|e| e.to_string())? {
            QueryResultKind::Select(sm) => params.push(sm),
            _ => return Err("A parameter must be solution mappings".to_string()),
        }
        at = e;
    }
    Ok((command, params))
}

fn decode_solution_mappings(
    endpoint: &str,
    bytes: &[u8],
    variables: &[Variable],
) -> Result<EagerSolutionMappings, ServiceError> {
    match decode_query_result(bytes)
        .map_err(|e| ServiceError::InvalidResults(endpoint.to_string(), e.to_string()))?
    {
        QueryResultKind::Select(sm) => Ok(project_variables(sm, variables)),
        _ => Err(ServiceError::InvalidResults(
            endpoint.to_string(),
            "expected solutions".to_string(),
        )),
    }
}

/// Parses SPARQL JSON, XML or TSV results (by media type, or by sniffing when the endpoint
/// gives none or a generic one) into solution mappings with exactly the variables.
pub fn parse_sparql_results(
    endpoint: &str,
    media_type: &str,
    bytes: &[u8],
    variables: &[Variable],
) -> Result<EagerSolutionMappings, ServiceError> {
    let format = match media_type {
        "application/sparql-results+json" | "application/json" | "text/javascript"
        | "application/javascript" => QueryResultsFormat::Json,
        "application/sparql-results+xml" | "application/xml" | "text/xml" => {
            QueryResultsFormat::Xml
        }
        "text/tab-separated-values" => QueryResultsFormat::Tsv,
        _ => {
            let start = bytes
                .iter()
                .find(|b| !b.is_ascii_whitespace())
                .copied()
                .unwrap_or(b'{');
            match start {
                b'{' => QueryResultsFormat::Json,
                b'<' => QueryResultsFormat::Xml,
                b'?' => QueryResultsFormat::Tsv,
                _ => {
                    return Err(ServiceError::UnsupportedMediaType(
                        endpoint.to_string(),
                        media_type.to_string(),
                    ))
                }
            }
        }
    };
    let invalid = |e: String| ServiceError::InvalidResults(endpoint.to_string(), e);
    let parser = QueryResultsParser::from_format(format);
    let solutions: Vec<QuerySolution> =
        match parser.for_slice(bytes).map_err(|e| invalid(e.to_string()))? {
            SliceQueryResultsParserOutput::Solutions(s) => s
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| invalid(e.to_string()))?,
            SliceQueryResultsParserOutput::Boolean(_) => {
                return Err(invalid("expected solutions, got a boolean".to_string()))
            }
        };
    solutions_to_solution_mappings(&solutions, variables).map_err(invalid)
}

/// The solutions as solution mappings with exactly the given variables (in order): variables
/// that are never bound are columns of nulls, and other variables are left out.
pub fn solutions_to_solution_mappings(
    solutions: &[QuerySolution],
    variables: &[Variable],
) -> Result<EagerSolutionMappings, String> {
    if variables.is_empty() {
        return Ok(EagerSolutionMappings::new(
            DataFrame::empty_with_height(solutions.len()),
            HashMap::new(),
        ));
    }
    let sm = if solutions.is_empty() {
        EagerSolutionMappings::new(DataFrame::empty(), HashMap::new())
    } else {
        EagerSolutionMappings::from_query_solutions(solutions).map_err(|e| e.to_string())?
    };
    Ok(project_variables(sm, variables))
}

/// Keeps the variables, in order, adding columns of nulls for missing ones.
pub fn project_variables(
    mut sm: EagerSolutionMappings,
    variables: &[Variable],
) -> EagerSolutionMappings {
    if variables.is_empty() {
        let h = sm.mappings.height();
        return EagerSolutionMappings::new(DataFrame::empty_with_height(h), HashMap::new());
    }
    let height = sm.mappings.height();
    let mut columns = Vec::with_capacity(variables.len());
    let mut states = HashMap::with_capacity(variables.len());
    for v in variables {
        let name = v.as_str();
        if let (Ok(c), Some(s)) = (sm.mappings.column(name), sm.rdf_node_types.remove(name)) {
            columns.push(c.clone());
            states.insert(name.to_string(), s);
        } else {
            columns.push(Column::full_null(
                name.into(),
                height,
                &BaseRDFNodeType::None.default_input_polars_data_type(),
            ));
            states.insert(
                name.to_string(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            );
        }
    }
    let df = DataFrame::new(height, columns).unwrap();
    // Keeps the column order of the variables.
    let df = df
        .lazy()
        .select(variables.iter().map(|v| col(v.as_str())).collect::<Vec<_>>())
        .collect()
        .unwrap();
    EagerSolutionMappings::new(df, states)
}

/// The RDF formats that LOAD accepts, in the Accept header.
pub const RDF_ACCEPT: &str = "text/turtle, application/n-triples;q=0.9, application/rdf+xml;q=0.9, application/trig;q=0.8, application/n-quads;q=0.8, application/ld+json;q=0.7, text/plain;q=0.1, */*;q=0.05";

/// Fetches the http(s) URL for LOAD, with the RDF formats in the Accept header. Returns the
/// bytes and the media type of the response.
pub fn fetch_for_load(
    settings: &Arc<FederationSettings>,
    url: &str,
) -> Result<(Vec<u8>, Option<String>), ServiceError> {
    if !settings.allow_load {
        return Err(ServiceError::LoadNotAllowed);
    }
    settings.check_allowed(url, true)?;
    let client = http_client(settings, true)?;
    let headers = settings.headers_for(url);
    let fut = async {
        let rb = client
            .get(url)
            .header(ACCEPT, RDF_ACCEPT)
            .header(USER_AGENT, user_agent());
        let rb = add_headers(rb, &headers)?;
        let response = rb.send().await.map_err(|e| request_error(url, e))?;
        let status = response.status();
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(';').next().unwrap_or("").trim().to_ascii_lowercase());
        let bytes = response.bytes().await.map_err(|e| request_error(url, e))?;
        if !status.is_success() {
            return Err(status_error(url, status.as_u16(), &bytes));
        }
        Ok((bytes.to_vec(), media_type))
    };
    block_on_own_runtime(1, fut)?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn only(v: &[&str]) -> FederationSettings {
        FederationSettings {
            allowed_endpoints: AllowedEndpoints::Only(v.iter().map(|x| x.to_string()).collect()),
            ..Default::default()
        }
    }

    #[test]
    fn allow_list_matches_by_origin_and_path_segments() {
        let s = only(&["http://localhost:7878", "https://example.org/sparql"]);
        assert!(s.is_allowed("http://localhost:7878/sparql", false));
        assert!(s.is_allowed("http://LOCALHOST:7878/arrow/query", false));
        assert!(!s.is_allowed("http://localhost:7879/sparql", false));
        assert!(!s.is_allowed("https://localhost:7878/sparql", false));
        assert!(s.is_allowed("https://example.org/sparql", false));
        assert!(s.is_allowed("https://example.org:443/sparql/x", false));
        assert!(!s.is_allowed("https://example.org/sparqlx", false));
        assert!(!s.is_allowed("https://example.org/", false));
        assert!(!s.is_allowed("https://example.org.evil.com/sparql", false));
        assert!(!s.is_allowed("ftp://example.org/sparql", false));
        assert!(!only(&[]).is_allowed("http://localhost:7878/sparql", false));
    }

    #[test]
    fn any_allows_web_but_not_files() {
        let s = FederationSettings::default();
        assert!(s.is_allowed("http://10.0.0.1/sparql", false));
        assert!(!s.is_allowed("file:///etc/passwd", true));
        assert!(!s.is_allowed("s3://bucket/x.ttl", false));
        assert!(s.is_allowed("s3://bucket/x.ttl", true));
        assert!(only(&["file:///data"]).is_allowed("file:///data/x.ttl", true));
        assert!(!only(&["*"]).is_allowed("file:///data/x.ttl", true));
        assert!(!FederationSettings::deny_all().is_allowed("http://localhost/sparql", false));
    }

    #[test]
    fn headers_by_prefix() {
        let s = FederationSettings {
            headers: vec![(
                "https://graphdb.example.org/repositories".to_string(),
                vec![("Authorization".to_string(), "Bearer x".to_string())],
            )],
            ..Default::default()
        };
        assert_eq!(
            s.headers_for("https://graphdb.example.org/repositories/r1").len(),
            1
        );
        assert!(s.headers_for("https://graphdb.example.org/other").is_empty());
    }
}
