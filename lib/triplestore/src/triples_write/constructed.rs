//! Writes CONSTRUCT and DESCRIBE results with the writers of `Triplestore::write_triples`.

use crate::errors::TriplestoreError;
use crate::sparql::insert::construct_result_as_triples_to_add;
use crate::{StorageOptions, Triplestore};
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::IntoLazy;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::formatting::format_native_columns;
use representation::result::ConstructedTriples;
use representation::solution_mapping::BaseCatState;
use std::collections::HashMap;
use std::io::Write;
use utils::polars::collect_sized;

/// Writes the triples of a CONSTRUCT (or DESCRIBE) result in the format, with the writers of
/// `Triplestore::write_triples` (the parallel N-Triples writer and the pretty Turtle, RDF/XML
/// and JSON-LD writers). The result is put into a temporary in-memory triplestore, which removes
/// duplicate triples, as the result is a graph. In a triple format the triples of all graphs of
/// the result are written as one graph, in a quad format (TriG, N-Quads) each triple is written
/// in its graph, the default graph first.
///
/// The temporary triplestore shares the dictionary `global_cats` of the model that the result
/// comes from when the result only has codes of that dictionary, so that nothing is decoded
/// before it is written. Otherwise the result is decoded and gets a dictionary of its own, so
/// writing a result never adds to the dictionary of the model.
pub fn write_constructed_triples<W: Write>(
    buf: &mut W,
    mut sms: Vec<ConstructedTriples>,
    format: RdfFormat,
    global_cats: &LockedCats,
    prefixes: &HashMap<String, NamedNode>,
) -> Result<(), TriplestoreError> {
    let quads = matches!(format, RdfFormat::TriG | RdfFormat::NQuads);
    if !quads {
        for (_, _, g) in &mut sms {
            *g = NamedGraph::DefaultGraph;
        }
    }
    sms.retain(|(sm, _, _)| sm.mappings.height() > 0);
    let mut ts = Triplestore::new(StorageOptions::memory(), None)?;
    if only_global_codes(&sms, global_cats)? {
        ts.global_cats = global_cats.clone();
    } else {
        for (sm, _, _) in &mut sms {
            let height = sm.mappings.height();
            let lf = format_native_columns(
                sm.mappings.clone().lazy(),
                &mut sm.rdf_node_types,
                global_cats.clone(),
            );
            sm.mappings = collect_sized(lf, height).map_err(TriplestoreError::LazyLoadError)?;
        }
    }
    let to_add = construct_result_as_triples_to_add(sms, &NamedGraph::DefaultGraph);
    if !to_add.is_empty() {
        ts.add_triples_vec(to_add)?;
    }
    if quads {
        ts.write_quads(buf, format, None, prefixes)
    } else {
        ts.write_triples(buf, format, &NamedGraph::DefaultGraph, prefixes)
    }
}

/// Whether the triples can be added to a triplestore that shares the dictionary without adding
/// to it: every column that is stored with codes has codes of the dictionary (not strings or
/// codes of a local dictionary), and the constant predicates and the graph names are in it.
fn only_global_codes(
    sms: &[ConstructedTriples],
    global_cats: &LockedCats,
) -> Result<bool, TriplestoreError> {
    let cats = global_cats.read()?;
    let mut iris = vec![];
    for (sm, predicate, graph) in sms {
        for state in sm.rdf_node_types.values() {
            for (t, s) in &state.map {
                if t.stored_cat() && !matches!(s, BaseCatState::CategoricalNative(None)) {
                    return Ok(false);
                }
            }
        }
        if let Some(p) = predicate {
            iris.push(Some(p.as_str()));
        }
        match graph {
            NamedGraph::DefaultGraph => {}
            NamedGraph::NamedGraph(g) => iris.push(Some(g.as_str())),
            NamedGraph::BlankNode(_) => return Ok(false),
        }
    }
    Ok(cats
        .maybe_encode_iri_slice(&iris)
        .iter()
        .all(Option::is_some))
}
