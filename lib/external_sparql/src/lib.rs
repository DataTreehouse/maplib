pub mod federation;
pub mod maplib_endpoint;

use representation::CollectAuto;
use polars::frame::DataFrame;
use polars::prelude::{col, Column, IntoLazy};
use query_processing::bindings::maybe_replace_bindings;
use representation::cats::{Cats, LockedCats};
use representation::errors::RepresentationError;
use representation::result::{template_to_solution_mappings, QueryResult, QueryResultKind};
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use reqwest::header::{ACCEPT, CONTENT_TYPE, USER_AGENT};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QueryResultsSyntaxError, QuerySolution,
    SliceQueryResultsParserOutput,
};
use spargebra::algebra::GraphPattern;
use spargebra::term::{
    GraphNamePattern, GroundTerm, NamedNode, NamedNodePattern, QuadPattern, TermPattern,
};
use spargebra::{Query, SparqlSyntaxError};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tokio::runtime::Builder;

#[derive(Debug, Error)]
pub enum SparqlEndpointQueryError {
    #[error(transparent)]
    RequestError(reqwest::Error),
    #[error(transparent)]
    Outbound(outbound::OutboundError),
    #[error("Bad status code `{0}`")]
    BadStatusCode(String),
    #[error("Results parse error `{0}`")]
    ResultsParseError(QueryResultsSyntaxError),
    #[error("Solution parse error `{0}`")]
    SolutionParseError(QueryResultsSyntaxError),
    #[error("Wrong result type, expected solutions")]
    WrongResultType,
    #[error("Wrong result type, expected a boolean")]
    WrongResultTypeExpectedBoolean,
    #[error("{0} queries are not supported against SPARQL endpoints")]
    UnsupportedQueryForm(String),
    #[error("Invalid query results: `{0}`")]
    InvalidResults(RepresentationError),
    #[error("SPARQL parse error: `{0}`")]
    SPARQLSyntaxError(SparqlSyntaxError),
    #[error(transparent)]
    BindingReplacementError(RepresentationError),
    #[error("Error creating quads from pattern: `{0}`")]
    CreateQuadsFromPatternError(RepresentationError),
}

pub enum SparqlMethod {
    GET,
    POST,
}

pub struct SparqlEndpoint {
    pub endpoint: String,
    pub method: SparqlMethod,
}

impl SparqlEndpoint {
    pub fn new(endpoint: &str, method: SparqlMethod) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            method,
        }
    }

    pub fn query_blocking(
        &self,
        query: &str,
        prefixes: Option<&HashMap<String, NamedNode>>,
        bindings: Option<&HashMap<String, GroundTerm>>,
        headers: Option<&Vec<(String, String)>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let qr = builder
            .build()
            .unwrap()
            .block_on(self.async_query(query, prefixes, bindings, headers));
        qr
    }

    pub async fn async_query(
        &self,
        query: &str,
        prefixes: Option<&HashMap<String, NamedNode>>,
        bindings: Option<&HashMap<String, GroundTerm>>,
        headers: Option<&Vec<(String, String)>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let query = Query::parse(query, None, prefixes)
            .map_err(SparqlEndpointQueryError::SPARQLSyntaxError)?;
        let query = maybe_replace_bindings(query, bindings)
            .map_err(SparqlEndpointQueryError::BindingReplacementError)?;
        self.async_query_parsed(&query, headers).await
    }

    pub async fn async_query_parsed(
        &self,
        query: &Query,
        headers: Option<&Vec<(String, String)>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let use_query = match query {
            Query::Select { .. } => query.clone(),
            Query::Construct {
                template,
                graph_template,
                dataset,
                pattern,
                base_iri,
            } => {
                let mut used_variables = HashSet::new();
                let parts = template
                    .iter()
                    .map(|p| (&p.subject, &p.predicate, &p.object, None))
                    .chain(
                        graph_template
                            .iter()
                            .map(|q| (&q.subject, &q.predicate, &q.object, Some(&q.graph_name))),
                    );
                for (subject, predicate, object, graph_name) in parts {
                    if let TermPattern::Variable(v) = subject {
                        used_variables.insert(v);
                    }
                    if let TermPattern::Variable(v) = object {
                        used_variables.insert(v);
                    }
                    if let NamedNodePattern::Variable(v) = predicate {
                        used_variables.insert(v);
                    }
                    if let Some(GraphNamePattern::Variable(v)) = graph_name {
                        used_variables.insert(v);
                    }
                }
                let use_pattern = GraphPattern::Distinct {
                    inner: Box::new(GraphPattern::Project {
                        inner: Box::new(pattern.clone()),
                        variables: used_variables.into_iter().cloned().collect(),
                    }),
                };
                Query::Select {
                    pattern: use_pattern,
                    dataset: dataset.clone(),
                    base_iri: base_iri.clone(),
                }
            }
            Query::Ask { .. } => query.clone(),
            Query::Describe { .. } => {
                return Err(SparqlEndpointQueryError::UnsupportedQueryForm(
                    "DESCRIBE".to_string(),
                ))
            }
        };
        outbound::check_url(&self.endpoint).map_err(SparqlEndpointQueryError::Outbound)?;
        let client = outbound::guard_client(reqwest::Client::builder())
            .build()
            .map_err(SparqlEndpointQueryError::RequestError)?;

        let response = match &self.method {
            SparqlMethod::GET => {
                let mut rb =
                    client
                        .get(&self.endpoint)
                        .header(ACCEPT, "application/sparql-results+json,application/json,text/javascript,application/javascript")
                        .header(USER_AGENT, "maplib");

                if let Some(headers) = headers {
                    for (n, v) in headers {
                        rb = rb.header(n, v);
                    }
                }

                rb.query(&[("query", use_query.to_string())])
                    .query(&[("format", "json"), ("output", "json"), ("results", "json")])
                    .send()
                    .await
            }
            SparqlMethod::POST => {
                let mut rb = client
                    .post(&self.endpoint)
                    .header(CONTENT_TYPE, "application/sparql-query")
                    .header(ACCEPT, "application/sparql-results+json,application/json,text/javascript,application/javascript")
                    .header(USER_AGENT, "maplib");

                if let Some(headers) = headers {
                    for (n, v) in headers {
                        rb = rb.header(n, v);
                    }
                }

                rb.body(use_query.to_string())
                    .query(&[("format", "json"), ("output", "json"), ("results", "json")])
                    .send()
                    .await
            }
        };
        let text = match response {
            Ok(proper_response) => {
                if proper_response.status().as_u16() != 200 {
                    return Err(SparqlEndpointQueryError::BadStatusCode(
                        proper_response.status().to_string(),
                    ));
                } else {
                    proper_response
                        .text()
                        .await
                        .map_err(SparqlEndpointQueryError::RequestError)?
                }
            }
            Err(error) => {
                return Err(match outbound::refusal(&error) {
                    Some(o) => SparqlEndpointQueryError::Outbound(o),
                    None => SparqlEndpointQueryError::RequestError(error),
                })
            }
        };
        if let Query::Ask { .. } = use_query {
            return Ok(QueryResult {
                kind: QueryResultKind::Ask(parse_json_boolean(&text)?),
                debug: None,
                pushdown_paths: vec![],
            });
        }
        let solutions = parse_json_text(&text)?;
        let sm = match use_query {
            Query::Select { pattern, .. } => {
                let mut vars = Vec::new();
                pattern.on_in_scope_variable(|x| vars.push(x));
                let sm = if !solutions.is_empty() {
                    let mut sm = EagerSolutionMappings::from_query_solutions(solutions.as_slice())
                        .map_err(SparqlEndpointQueryError::InvalidResults)?;
                    let cols: Vec<_> = vars.iter().map(|x| col(x.as_str())).collect();
                    sm.mappings = sm.mappings.lazy().select(cols).collect_auto().unwrap();
                    sm
                } else {
                    let mut columns = Vec::with_capacity(vars.len());
                    let mut states = HashMap::new();
                    for v in vars {
                        columns.push(Column::new_empty(
                            v.as_str().into(),
                            &BaseRDFNodeType::None.default_input_polars_data_type(),
                        ));
                        states.insert(
                            v.as_str().to_string(),
                            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                        );
                    }

                    let df = DataFrame::new(0, columns).unwrap();

                    EagerSolutionMappings::new(df, states)
                };
                sm
            }
            _ => unreachable!(),
        };
        match query {
            Query::Select { .. } => Ok(QueryResult {
                kind: QueryResultKind::Select(sm),
                debug: None,
                pushdown_paths: vec![],
            }),
            Query::Construct {
                template,
                graph_template,
                ..
            } => {
                let quad_template: Vec<_> = template
                    .iter()
                    .map(|x| QuadPattern {
                        subject: x.subject.clone(),
                        predicate: x.predicate.clone(),
                        object: x.object.clone(),
                        graph_name: GraphNamePattern::DefaultGraph,
                    })
                    .chain(graph_template.iter().cloned())
                    .collect();
                let sms = template_to_solution_mappings(
                    sm.mappings,
                    sm.rdf_node_types,
                    &quad_template,
                    LockedCats::new(Cats::new_empty(None, None)),
                )
                .map_err(SparqlEndpointQueryError::CreateQuadsFromPatternError)?;
                Ok(QueryResult {
                    kind: QueryResultKind::Construct(sms),
                    debug: None,
                    pushdown_paths: vec![],
                })
            }
            Query::Ask { .. } | Query::Describe { .. } => {
                unreachable!("ASK returns early and DESCRIBE is refused")
            }
        }
    }
}

fn parse_json_boolean(text: &str) -> Result<bool, SparqlEndpointQueryError> {
    let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
    match json_parser.for_slice(text.as_bytes()) {
        Ok(SliceQueryResultsParserOutput::Boolean(b)) => Ok(b),
        Ok(_) => Err(SparqlEndpointQueryError::WrongResultTypeExpectedBoolean),
        Err(parse_error) => Err(SparqlEndpointQueryError::ResultsParseError(parse_error)),
    }
}

fn parse_json_text(text: &str) -> Result<Vec<QuerySolution>, SparqlEndpointQueryError> {
    let json_parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
    let parsed_results = json_parser.for_slice(text.as_bytes());
    match parsed_results {
        Ok(reader) => {
            let mut solns = vec![];
            if let SliceQueryResultsParserOutput::Solutions(solutions) = reader {
                for s in solutions {
                    match s {
                        Ok(query_solution) => solns.push(query_solution),
                        Err(syntax_error) => {
                            return Err(SparqlEndpointQueryError::SolutionParseError(syntax_error))
                        }
                    }
                }
                Ok(solns)
            } else {
                Err(SparqlEndpointQueryError::WrongResultType)
            }
        }
        Err(parse_error) => Err(SparqlEndpointQueryError::ResultsParseError(parse_error)),
    }
}
