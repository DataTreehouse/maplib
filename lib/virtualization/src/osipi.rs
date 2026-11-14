//! Virtualization of OSIsoft PI (AVEVA PI) data through the PI Web API.
//! Not available in the open source build.

use templates::ast::Template;
use thiserror::Error;

pub const OSIPI_RESOURCE: &str = "osipi";
pub const OSIPI_NAMESPACE: &str = "urn:maplib_osipi:";

#[derive(Debug, Clone)]
pub enum OSIPIAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}

#[derive(Debug, Clone)]
pub struct OSIPISettings {
    pub url: String,
    pub auth: OSIPIAuth,
    pub verify_ssl: bool,
    pub asset_server: Option<String>,
    pub asset_database: Option<String>,
    pub root_element: Option<String>,
    pub base_iri: String,
    pub read_static_values: bool,
    pub default_start_time: Option<String>,
    pub default_end_time: Option<String>,
    pub page_size: usize,
    pub max_concurrency: usize,
    pub timeout_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OSIPIAssetModel {
    pub ntriples: String,
    pub databases: usize,
    pub elements: usize,
    pub attributes: usize,
    pub timeseries: usize,
}

#[derive(Debug, Error)]
pub enum OSIPIError {
    #[error("OSIsoft PI virtualization is not available in the open source build. Contact Data Treehouse to try")]
    NotAvailable,
}

pub fn read_asset_framework(_settings: &OSIPISettings) -> Result<OSIPIAssetModel, OSIPIError> {
    Err(OSIPIError::NotAvailable)
}

pub fn osipi_resource_template() -> Result<Template, OSIPIError> {
    Err(OSIPIError::NotAvailable)
}

#[derive(Debug, Clone)]
pub struct OSIPIDatabase {}

impl OSIPIDatabase {
    pub fn new(_settings: OSIPISettings) -> Result<OSIPIDatabase, OSIPIError> {
        Err(OSIPIError::NotAvailable)
    }
}
