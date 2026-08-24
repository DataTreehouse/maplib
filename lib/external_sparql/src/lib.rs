use polars::frame::DataFrame;
use polars::prelude::{col, Column, IntoLazy};
use query_processing::bindings::maybe_replace_bindings;
use representation::errors::RepresentationError;
use representation::result::{QueryResult, QueryResultKind};
use representation::solution_mapping::EagerSolutionMappings;
use representation::BaseRDFNodeType;
use reqwest::header::{ACCEPT, USER_AGENT};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QueryResultsSyntaxError, QuerySolution,
    SliceQueryResultsParserOutput,
};
use spargebra::term::{GroundTerm, NamedNode};
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
    #[error(transparent)]
    BindingReplacementError(RepresentationError),
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
        bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let qr = builder
            .build()
            .unwrap()
            .block_on(self.async_query(query, prefixes, bindings));
        qr
    }

    pub async fn async_query(
        &self,
        query: &str,
        prefixes: Option<&HashMap<String, NamedNode>>,
        bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let query = Query::parse(query, None, prefixes)
            .map_err(SparqlEndpointQueryError::SPARQLSyntaxError)?;
        let query = maybe_replace_bindings(query, bindings)
            .map_err(SparqlEndpointQueryError::BindingReplacementError)?;
        self.async_query_parsed(&query).await
    }

    pub async fn async_query_parsed(
        &self,
        query: &Query,
    ) -> Result<QueryResult, SparqlEndpointQueryError> {
        let client = reqwest::Client::new();
        let response = match &self.method {
            SparqlMethod::GET => {
                client
                    .get(&self.endpoint)
                    .header(ACCEPT, "application/sparql-results+json,application/json,text/javascript,application/javascript")
                    .header(USER_AGENT, "maplib")
                    .query(&[("query", query.to_string())])
                    .query(&[("format", "json"), ("output", "json"), ("results", "json")])
                    .send()
                    .await
            }
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
        if let Query::Select { pattern, .. } = query {
            let mut vars = Vec::new();
            pattern.on_in_scope_variable(|x| vars.push(x));
            let sm = if !solutions.is_empty() {
                let mut sm = EagerSolutionMappings::from_query_solutions(solutions.as_slice())
                    .map_err(SparqlEndpointQueryError::InvalidResults)?;
                let cols: Vec<_> = vars.iter().map(|x| col(x.as_str())).collect();
                sm.mappings = sm.mappings.lazy().select(cols).collect().unwrap();
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
                let sm = EagerSolutionMappings::new(df, states);
                sm
            };
            Ok(QueryResult {
                kind: QueryResultKind::Select(sm),
                debug: None,
                pushdown_paths: vec![],
            })
        } else {
            todo!()
        }
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
