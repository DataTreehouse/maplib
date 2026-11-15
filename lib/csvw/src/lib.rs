//! CSV on the Web (CSVW): converting tabular data to RDF using CSVW metadata, as specified by
//! <https://www.w3.org/TR/tabular-data-model/>, <https://www.w3.org/TR/tabular-metadata/> and
//! <https://www.w3.org/TR/csv2rdf/>.
//!
//! The conversion works in three steps:
//!
//! 1. The metadata is located, parsed and normalized, the tabular data files are parsed using
//!    the dialect, and the cells are parsed according to the datatypes and formats of the
//!    columns (`metadata`, `parse`, `datatypes`, `annotate`).
//! 2. The annotated tables are added to temporary graphs using the Facade-X representation
//!    (`facade`), with the parsed values of the cells.
//! 3. A SPARQL Update is generated from the metadata (`sparql`), which builds the CSVW output
//!    from the Facade-X representation: the URI templates (aboutUrl, propertyUrl, valueUrl),
//!    datatypes and languages, lists and, in standard mode, the table group, table and row
//!    nodes and the notes and common properties. The update ends by dropping the temporary graphs.
//!
//! The generated update is returned, so that it can be inspected or used as a starting point
//! for a custom mapping.

pub mod annotate;
pub mod datatypes;
pub mod errors;
pub mod facade;
pub mod jsonld;
pub mod loader;
pub mod metadata;
pub mod parse;
pub mod sparql;
mod sparql_util;
pub mod uri_template;
pub mod vocab;

use crate::annotate::{annotate_group, embedded_table_desc, AnnGroup};
use crate::errors::CsvwError;
use crate::facade::{facade_triples, FacadeIris};
use crate::loader::{resolve, Loader};
use crate::metadata::{describes, parse_json, MetadataParser, TableGroupDesc};
use crate::sparql::{generate_update, GenOptions};
use crate::vocab::MAPLIB_CSVW_NS;
use representation::dataset::NamedGraph;
use triplestore::sparql::QuerySettings;
use triplestore::Triplestore;
use uuid::Uuid;

pub type Warnings = Vec<String>;

#[derive(Debug, Clone, Default)]
pub struct CsvwOptions {
    /// Minimal mode: only the triples describing the cells, no table group, table and row nodes.
    pub minimal: bool,
    /// The IRI of user supplied (overriding) metadata.
    pub metadata: Option<String>,
    /// The graph to add the triples to.
    pub graph: NamedGraph,
}

#[derive(Debug, Clone)]
pub struct CsvwResult {
    /// The SPARQL Update that was used to create the RDF from the Facade-X representation.
    pub update: String,
    pub warnings: Vec<String>,
}

fn looks_like_json(bytes: &[u8]) -> bool {
    let s = String::from_utf8_lossy(&bytes[..bytes.len().min(1000)]);
    s.trim_start_matches('\u{feff}')
        .trim_start()
        .starts_with('{')
}

fn parse_metadata(
    iri: &str,
    bytes: &[u8],
    loader: &Loader,
    warnings: &mut Warnings,
) -> Result<TableGroupDesc, CsvwError> {
    let v = parse_json(iri, bytes)?;
    let mut p = MetadataParser { loader, warnings };
    p.parse_top_level(&v, iri)
}

/// The locations to look for metadata for a tabular data file, from the site-wide configuration
/// /.well-known/csvm if the loader finds it, see
/// <https://www.w3.org/TR/tabular-data-model/#site-wide-location-configuration>.
fn metadata_locations(input_iri: &str, loader: &Loader) -> Result<Vec<String>, CsvwError> {
    let no_fragment = input_iri.split('#').next().unwrap();
    let templates = match loader.load(&resolve(input_iri, "/.well-known/csvm")?)? {
        Some(b) => String::from_utf8_lossy(&b)
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect(),
        None => vec![
            "{+url}-metadata.json".to_string(),
            "csv-metadata.json".to_string(),
        ],
    };
    templates
        .iter()
        .map(|t| resolve(input_iri, &t.replace("{+url}", no_fragment)))
        .collect()
}

/// Locates the metadata for a tabular data file or metadata file, and creates the annotated
/// tables.
pub fn annotate(
    input_iri: &str,
    user_metadata: Option<&str>,
    loader: &Loader,
    warnings: &mut Warnings,
) -> Result<AnnGroup, CsvwError> {
    let desc = if let Some(m) = user_metadata {
        let bytes = loader.load_required(m)?;
        parse_metadata(m, &bytes, loader, warnings)?
    } else {
        let bytes = loader.load_required(input_iri)?;
        if looks_like_json(&bytes) {
            parse_metadata(input_iri, &bytes, loader, warnings)?
        } else {
            let candidates = metadata_locations(input_iri, loader)?;
            let mut found = None;
            for c in candidates {
                if let Some(mbytes) = loader.load(&c)? {
                    let mut w = vec![];
                    match parse_metadata(&c, &mbytes, loader, &mut w) {
                        Ok(tg) => {
                            if describes(&tg, input_iri) {
                                warnings.extend(w);
                                found = Some(tg);
                                break;
                            } else {
                                warnings.push(format!(
                                    "ignoring the metadata {c}, which does not describe {input_iri}"
                                ));
                            }
                        }
                        Err(e) => {
                            warnings.push(format!("ignoring the metadata {c}: {e}"));
                        }
                    }
                }
            }
            match found {
                Some(tg) => tg,
                None => TableGroupDesc {
                    id: None,
                    tables: vec![embedded_table_desc(input_iri)],
                    annotations: vec![],
                },
            }
        }
    };
    annotate_group(&desc, loader, warnings)
}

/// Converts tabular data to RDF in the triplestore, see the module documentation.
pub fn csvw_to_rdf(
    triplestore: &mut Triplestore,
    input_iri: &str,
    loader: &Loader,
    options: &CsvwOptions,
) -> Result<CsvwResult, CsvwError> {
    let mut warnings = vec![];
    let group = annotate(
        input_iri,
        options.metadata.as_deref(),
        loader,
        &mut warnings,
    )?;
    let iris = FacadeIris {
        base: format!("{MAPLIB_CSVW_NS}{}", Uuid::new_v4()),
    };
    let graph = match &options.graph {
        NamedGraph::DefaultGraph => None,
        NamedGraph::NamedGraph(nn) => Some(nn.as_str().to_string()),
        NamedGraph::BlankNode(_) => {
            return Err(CsvwError::UpdateError(
                "a blank node graph can not be the target of the conversion".to_string(),
            ))
        }
    };
    let update = generate_update(
        &group,
        &iris,
        &GenOptions {
            minimal: options.minimal,
            graph,
        },
    )?;
    let tta = facade_triples(&group, &iris);
    triplestore.add_triples_vec(tta)?;
    let settings = QuerySettings {
        include_graphs: vec![],
        max_rows: None,
        strict_project: true,
        federation: None,
    };
    let res = triplestore.update(&update, None, &settings, None, None, false);
    if let Err(e) = res {
        // Remove the temporary graphs
        let mut drops = vec![];
        for k in 0..group.tables.len() {
            drops.push(format!("DROP SILENT GRAPH <{}>", iris.table_graph(k)));
        }
        drops.push(format!("DROP SILENT GRAPH <{}>", iris.prefix_graph()));
        let _ = triplestore.update(&drops.join(" ;\n"), None, &settings, None, None, false);
        return Err(CsvwError::UpdateError(format!(
            "{e}\n\nThe update was:\n{update}"
        )));
    }
    for w in &warnings {
        tracing::warn!("CSVW: {w}");
    }
    Ok(CsvwResult { update, warnings })
}

/// Generates the update for an input without running it, the Facade-X graphs are not created.
pub fn csvw_update_string(
    input_iri: &str,
    loader: &Loader,
    options: &CsvwOptions,
) -> Result<CsvwResult, CsvwError> {
    let mut warnings = vec![];
    let group = annotate(
        input_iri,
        options.metadata.as_deref(),
        loader,
        &mut warnings,
    )?;
    let iris = FacadeIris {
        base: format!("{MAPLIB_CSVW_NS}{}", Uuid::new_v4()),
    };
    let graph = match &options.graph {
        NamedGraph::NamedGraph(nn) => Some(nn.as_str().to_string()),
        _ => None,
    };
    let update = generate_update(
        &group,
        &iris,
        &GenOptions {
            minimal: options.minimal,
            graph,
        },
    )?;
    Ok(CsvwResult { update, warnings })
}
