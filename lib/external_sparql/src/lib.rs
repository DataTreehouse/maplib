use representation::errors::RepresentationError;
use representation::result::{QueryResult, QueryResultKind};
use representation::solution_mapping::EagerSolutionMappings;
use reqwest::header::{ACCEPT, USER_AGENT};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QueryResultsSyntaxError, QuerySolution,
    SliceQueryResultsParserOutput,
};
use spargebra::term::NamedNode;
use spargebra::{Query, SparqlSyntaxError};
use std::collections::HashMap;
use thiserror::Error;
use tokio::runtime::Builder;

#[derive(Debug, Error)]
pub enum SparqlEndpointQueryError {
    #[error(transparent)]
    RequestError(reqwest::Error),
    #[error("Bad status code `{0}`")]
    BadStatusCode(String),
    #[error("Results parse error `{0}`")]
    ResultsParseError(QueryResultsSyntaxError),
    #[error("Solution parse error `{0}`")]
    SolutionParseError(QueryResultsSyntaxError),
    #[error("Wrong result type, expected solutions")]
    WrongResultType,
    #[error("Invalid query results: `{0}`")]
    InvalidResults(RepresentationError),
    #[error("SPARQL parse error: `{0}`")]
    SPARQLSyntaxError(SparqlSyntaxError),
}

pub enum SparqlMethod {
    GET,
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
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let qr = builder
            .build()
            .unwrap()
            .block_on(self.async_query(query, prefixes));
        qr
    }

    pub async fn async_query(
        &self,
        query: &str,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let query = Query::parse(query, None, prefixes)
            .map_err(SparqlEndpointQueryError::SPARQLSyntaxError)?;
        self.async_query_parsed(&query).await
    }

    pub async fn async_query_parsed(
        &self,
        query: &Query,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let client = reqwest::Client::new();
        let response = match &self.method {
            SparqlMethod::GET => client
                .get(&self.endpoint)
                .header(ACCEPT, "application/sparql-results+json,application/json,text/javascript,application/javascript")
                .header(USER_AGENT, "maplib")
                .query(&[("query", query.to_string())])
                .query(&[("format", "json"), ("output", "json"), ("results", "json")])
                .send()
                .await
        };
        let solutions = match response {
            Ok(proper_response) => {
                if proper_response.status().as_u16() != 200 {
                    return Err(SparqlEndpointQueryError::BadStatusCode(
                        proper_response.status().to_string(),
                    ));
                } else {
                    parse_json_text(&proper_response.text().await.expect("Read text error"))?
                }
            }
            Err(error) => return Err(SparqlEndpointQueryError::RequestError(error)),
        };
        let sm = EagerSolutionMappings::from_query_solutions(solutions.as_slice())
            .map_err(SparqlEndpointQueryError::InvalidResults)?;
        Ok(QueryResult {
            kind: QueryResultKind::Select(sm),
            debug: None,
            pushdown_paths: vec![],
        })
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
