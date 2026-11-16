//! SPARQL 1.1 Update `LOAD <url> [INTO GRAPH <g>]` and `LOAD SILENT`.
//!
//! LOAD is refused unless the federation settings allow it, and only reaches allowed URLs:
//! http(s) URLs are fetched with content negotiation (the format is taken from the media type of
//! the response, else from the extension of the URL), and object storage (s3://, gs://, az://)
//! and file URLs are read as `Model.read` reads them, with the format from the extension.

use super::errors::SparqlError;
use super::QuerySettings;
use crate::triples_read::ExtendedRdfFormat;
use crate::{NewTriples, Triplestore};
use external_sparql::federation::{fetch_for_load, ServiceError};
use file_io::remote::{RemoteLocation, RemoteOptions};
use oxrdfio::{JsonLdProfileSet, RdfFormat};
use representation::dataset::NamedGraph;
use std::collections::HashMap;
use tracing::warn;

const LOAD_TRIPLES_BATCH_SIZE: usize = 10_000_000;

fn format_from_media_type(media_type: &str) -> Option<ExtendedRdfFormat> {
    let f = match media_type {
        "text/turtle" | "application/x-turtle" | "application/turtle" => RdfFormat::Turtle,
        "application/n-triples" => RdfFormat::NTriples,
        "application/rdf+xml" => RdfFormat::RdfXml,
        "application/trig" | "application/x-trig" => RdfFormat::TriG,
        "application/n-quads" | "text/x-nquads" | "text/nquads" => RdfFormat::NQuads,
        "application/ld+json" => RdfFormat::JsonLd {
            profile: JsonLdProfileSet::empty(),
        },
        "application/vnd.hdt" => return Some(ExtendedRdfFormat::HDT),
        _ => return None,
    };
    Some(ExtendedRdfFormat::Normal(f))
}

fn format_from_extension(url: &str) -> Option<ExtendedRdfFormat> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    let ext = name.rsplit_once('.').map(|(_, e)| e)?;
    let f = match ext {
        "ttl" => RdfFormat::Turtle,
        "nt" => RdfFormat::NTriples,
        "rdf" | "xml" | "owl" => RdfFormat::RdfXml,
        "trig" => RdfFormat::TriG,
        "nq" => RdfFormat::NQuads,
        "jsonld" | "json" => RdfFormat::JsonLd {
            profile: JsonLdProfileSet::empty(),
        },
        "hdt" => return Some(ExtendedRdfFormat::HDT),
        _ => return None,
    };
    Some(ExtendedRdfFormat::Normal(f))
}

impl Triplestore {
    /// Runs `LOAD <url> INTO GRAPH <target>`. With `new_triples` the triples that were not
    /// already present are added to it (not for HDT).
    pub(crate) fn load(
        &mut self,
        url: &str,
        target: &NamedGraph,
        silent: bool,
        query_settings: &QuerySettings,
        new_triples: Option<&mut Vec<NewTriples>>,
    ) -> Result<(), SparqlError> {
        match self.load_inner(url, target, query_settings, new_triples) {
            Ok(()) => Ok(()),
            Err(SparqlError::ServiceError(e)) if e.is_policy_error() => {
                Err(SparqlError::ServiceError(e))
            }
            Err(e) if silent => {
                warn!("LOAD SILENT <{url}> failed, nothing was loaded: {e}");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn load_inner(
        &mut self,
        url: &str,
        target: &NamedGraph,
        query_settings: &QuerySettings,
        new_triples: Option<&mut Vec<NewTriples>>,
    ) -> Result<(), SparqlError> {
        let settings = self.federation_settings(query_settings);
        if !settings.allow_load {
            return Err(ServiceError::LoadNotAllowed.into());
        }
        settings.check_allowed(url, true)?;
        let web = url.starts_with("http://") || url.starts_with("https://");
        let (bytes, format) = if web {
            let (bytes, media_type) = fetch_for_load(&settings, url)?;
            let format = media_type
                .as_deref()
                .and_then(format_from_media_type)
                .or_else(|| format_from_extension(url))
                .or_else(|| {
                    // Plain text is most likely N-Triples.
                    (media_type.as_deref() == Some("text/plain"))
                        .then_some(ExtendedRdfFormat::Normal(RdfFormat::NTriples))
                });
            let Some(format) = format else {
                return Err(SparqlError::LoadError(format!(
                    "Could not tell the RDF format of <{url}> from its media type {} or its extension",
                    media_type.unwrap_or_else(|| "(none)".to_string())
                )));
            };
            (bytes, format)
        } else {
            let Some(format) = format_from_extension(url) else {
                return Err(SparqlError::LoadError(format!(
                    "Could not tell the RDF format of <{url}> from its extension"
                )));
            };
            let location = RemoteLocation::parse(url, &RemoteOptions::default())
                .map_err(|e| SparqlError::LoadError(e.to_string()))?;
            let bytes = location
                .get()
                .map_err(|e| SparqlError::LoadError(e.to_string()))?;
            (bytes.to_vec(), format)
        };
        self.read_triples_keep_new(
            &bytes,
            format,
            Some(url.to_string()),
            None,
            true,
            false,
            target,
            &HashMap::new(),
            LOAD_TRIPLES_BATCH_SIZE,
            HashMap::new(),
            new_triples,
        )
        .map_err(|e| SparqlError::LoadError(format!("Could not read <{url}>: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_from_extensions_and_media_types() {
        assert!(matches!(
            format_from_extension("http://x.org/a/data.TTL?x=1"),
            Some(ExtendedRdfFormat::Normal(RdfFormat::Turtle))
        ));
        assert!(format_from_extension("http://x.org/a/data").is_none());
        assert!(matches!(
            format_from_media_type("application/n-triples"),
            Some(ExtendedRdfFormat::Normal(RdfFormat::NTriples))
        ));
    }
}
