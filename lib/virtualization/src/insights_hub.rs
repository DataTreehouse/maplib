//! Virtualization of Siemens Insights Hub (formerly MindSphere) asset models and time series.
//! Not available in the open source build.

use crate::python::VirtualizedPythonDatabase;
use templates::ast::Template;
use thiserror::Error;

pub const INSIGHTS_HUB_RESOURCE: &str = "insights_hub";
pub const INSIGHTS_HUB_NAMESPACE: &str = "urn:maplib_insights_hub:";

#[derive(Debug, Clone)]
pub enum InsightsHubAuth {
    ServiceCredentials {
        client_id: String,
        client_secret: String,
        token_url: Option<String>,
    },
    AppCredentials {
        client_id: String,
        client_secret: String,
        app_name: String,
        app_version: String,
        user_tenant: Option<String>,
    },
    Token {
        token: String,
    },
}

#[derive(Debug, Clone)]
pub struct InsightsHubSettings {
    pub host: String,
    pub tenant: String,
    pub auth: InsightsHubAuth,
    pub root_asset: Option<String>,
    pub base_iri: String,
    pub default_start_time: Option<String>,
    pub default_end_time: Option<String>,
    pub aggregate_pushdown: bool,
    pub page_size: usize,
    pub max_concurrency: usize,
    pub timeout_seconds: Option<f64>,
    pub verify_ssl: bool,
}

pub struct InsightsHubAssetModel {
    pub ntriples: String,
    pub database: VirtualizedPythonDatabase,
    pub assets: usize,
    pub asset_types: usize,
    pub aspects: usize,
    pub variables: usize,
    pub timeseries: usize,
}

#[derive(Debug, Error)]
pub enum InsightsHubError {
    #[error("Siemens Insights Hub virtualization is not available in the open source build. Contact Data Treehouse to try")]
    NotAvailable,
}

pub fn read_insights_hub(
    _settings: &InsightsHubSettings,
) -> Result<InsightsHubAssetModel, InsightsHubError> {
    Err(InsightsHubError::NotAvailable)
}

pub fn insights_hub_resource_template() -> Result<Template, InsightsHubError> {
    Err(InsightsHubError::NotAvailable)
}
