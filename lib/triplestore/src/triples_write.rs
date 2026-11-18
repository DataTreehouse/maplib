use super::Triplestore;
use crate::errors::TriplestoreError;
use oxrdf::{GraphName, NamedNode};
use oxrdfio::{RdfFormat, RdfSerializer};
use polars::prelude::{by_name, col, IdxSize, LazyFrame};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::runtime::THREAD_POOL;
use representation::cats::maybe_decode_complex_expr;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::{
    date_column_to_strings, datetime_column_to_xsd_strings, global_df_as_triples,
};
use representation::{
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::HashMap;
use std::io::Write;
use utils::polars::collect_sized;

mod fast_ntriples;
mod hdt_write;
mod pretty_turtle;
mod serializers;

const CHUNK_SIZE: usize = 1_024;
/// Rows of a table that are decoded at a time when it is written triple by triple, so that only
/// a part of a large table is held as strings or terms.
const WRITE_SLICE_ROWS: usize = 64 * 1024;

/// The lazy frame in slices of WRITE_SLICE_ROWS rows, in order. The last slice has no end, so
/// no rows are lost if `height` is too low.
fn row_slices(lf: LazyFrame, height: usize) -> impl Iterator<Item = (LazyFrame, usize)> {
    let n = height.div_ceil(WRITE_SLICE_ROWS).max(1);
    (0..n).map(move |i| {
        let offset = i * WRITE_SLICE_ROWS;
        if i + 1 == n {
            (
                lf.clone().slice(offset as i64, IdxSize::MAX),
                height.saturating_sub(offset),
            )
        } else {
            (
                lf.clone().slice(offset as i64, WRITE_SLICE_ROWS as IdxSize),
                WRITE_SLICE_ROWS,
            )
        }
    })
}

impl Triplestore {
    pub fn write_triples<W: Write>(
        &self,
        buf: &mut W,
        format: RdfFormat,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        if matches!(format, RdfFormat::TriG | RdfFormat::NQuads) {
            return self.write_quads(buf, format, Some(std::slice::from_ref(graph)), prefixes);
        }
        if matches!(format, RdfFormat::JsonLd { .. }) {
            // The graph is written as the default graph of the document. Also writes a document
            // without triples.
            return self.write_pretty_jsonld(buf, Some(graph), &[], prefixes);
        }
        if !self.does_graph_exist(graph) {
            return Ok(());
        }
        if RdfFormat::NTriples == format {
            let n_threads = THREAD_POOL.current_num_threads();
            for (predicate, df_map) in self.graph_triples_map.get(graph).unwrap() {
                let predicate_string = predicate.to_string();
                let predicate_bytes = predicate_string.as_bytes();
                for ((subject_type, object_type), tt) in df_map {
                    let types = if object_type.is_lang_string() {
                        HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), subject_type.clone()),
                            (LANG_STRING_VALUE_FIELD.to_string(), object_type.clone()),
                            (LANG_STRING_LANG_FIELD.to_string(), object_type.clone()),
                        ])
                    } else {
                        HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), subject_type.clone()),
                            (OBJECT_COL_NAME.to_string(), object_type.clone()),
                        ])
                    };
                    let slices = tt
                        .get_lazy_frames(&None, &None)?
                        .into_iter()
                        .flat_map(|(lf, height)| row_slices(lf, height));
                    for (mut lf, height) in slices {
                        lf = lf.with_columns([
                            maybe_decode_complex_expr(
                                col(SUBJECT_COL_NAME),
                                subject_type,
                                &subject_type.default_stored_cat_state(),
                                self.global_cats.clone(),
                            )
                            .alias(SUBJECT_COL_NAME),
                            maybe_decode_complex_expr(
                                col(OBJECT_COL_NAME),
                                object_type,
                                &object_type.default_stored_cat_state(),
                                self.global_cats.clone(),
                            )
                            .alias(OBJECT_COL_NAME),
                        ]);
                        if object_type.is_lang_string() {
                            lf = lf
                                .unnest(by_name([OBJECT_COL_NAME], true, false), None)
                                .select([
                                    col(SUBJECT_COL_NAME),
                                    col(LANG_STRING_VALUE_FIELD),
                                    col(LANG_STRING_LANG_FIELD),
                                ]);
                        } else {
                            lf = lf.select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);
                        };
                        let mut df =
                            collect_sized(lf, height).map_err(TriplestoreError::LazyLoadError)?;
                        if !object_type.is_lang_string() {
                            convert_datelike_to_string(
                                &mut df,
                                OBJECT_COL_NAME,
                                object_type.time_zone_offset(),
                            );
                        }

                        fast_ntriples::write_triples_in_df(
                            buf,
                            &df,
                            predicate_bytes,
                            &types,
                            CHUNK_SIZE,
                            n_threads,
                        )
                        .map_err(|e| TriplestoreError::WriteNTriplesError(e.to_string()))?;
                    }
                }
            }
        } else if RdfFormat::Turtle == format {
            self.write_pretty_turtle(buf, graph, prefixes)?;
        } else if RdfFormat::RdfXml == format {
            self.write_pretty_rdfxml(buf, graph, prefixes)?;
        } else {
            let mut writer = RdfSerializer::from_format(format).for_writer(buf);

            for (predicate, df_map) in self.graph_triples_map.get(graph).unwrap() {
                for ((subject_type, object_type), tt) in df_map {
                    let slices = tt
                        .get_lazy_frames(&None, &None)?
                        .into_iter()
                        .flat_map(|(lf, height)| row_slices(lf, height));
                    for (lf, height) in slices {
                        let triples = global_df_as_triples(
                            collect_sized(lf, height).map_err(TriplestoreError::LazyLoadError)?,
                            subject_type.clone(),
                            object_type.clone(),
                            predicate,
                            self.global_cats.clone(),
                        );
                        for t in &triples {
                            writer.serialize_triple(t).map_err(|x| {
                                TriplestoreError::WriteTurtleError(format!(
                                    "Error serializing triple {}: {}",
                                    t, x
                                ))
                            })?;
                        }
                    }
                }
            }
            writer.finish().map_err(|x| {
                TriplestoreError::WriteTurtleError(format!(
                    "Error finishing turtle serialization: {}",
                    x
                ))
            })?;
        }
        Ok(())
    }

    /// Writes quads in a dataset format (TriG or N-Quads).
    /// The given graphs that exist are written, in the given order. If graphs is None, all graphs
    /// are written, the default graph first.
    pub fn write_quads<W: Write>(
        &self,
        buf: &mut W,
        format: RdfFormat,
        graphs: Option<&[NamedGraph]>,
        prefixes: &HashMap<String, NamedNode>,
    ) -> Result<(), TriplestoreError> {
        if !matches!(format, RdfFormat::TriG | RdfFormat::NQuads) {
            return Err(TriplestoreError::UnsupportedWriteFormat(format!(
                "{} as quads",
                format
            )));
        }
        let mut serializer = RdfSerializer::from_format(format);
        if format == RdfFormat::TriG {
            let mut sorted_prefixes: Vec<_> = prefixes.iter().collect();
            sorted_prefixes.sort();
            for (name, iri) in sorted_prefixes {
                serializer = serializer
                    .with_prefix(name, iri.as_str())
                    .map_err(|e| TriplestoreError::InvalidPrefixIRI(e.to_string()))?;
            }
        }
        let graphs: Vec<NamedGraph> = if let Some(graphs) = graphs {
            let mut out: Vec<NamedGraph> = Vec::with_capacity(graphs.len());
            for g in graphs {
                if self.does_graph_exist(g) && !out.contains(g) {
                    out.push(g.clone());
                }
            }
            out
        } else {
            // The transient graph is only written when asked for explicitly.
            let transient = NamedGraph::transient();
            let mut graphs: Vec<_> = self
                .graph_triples_map
                .keys()
                .filter(|g| *g != &transient)
                .cloned()
                .collect();
            graphs.sort_by_key(|g| (!matches!(g, NamedGraph::DefaultGraph), g.to_string()));
            graphs
        };

        let mut writer = serializer.for_writer(buf);
        for graph in &graphs {
            let graph_name = GraphName::from(graph);
            for (predicate, df_map) in self.graph_triples_map.get(graph).unwrap() {
                for ((subject_type, object_type), tt) in df_map {
                    let slices = tt
                        .get_lazy_frames(&None, &None)?
                        .into_iter()
                        .flat_map(|(lf, height)| row_slices(lf, height));
                    for (lf, height) in slices {
                        let triples = global_df_as_triples(
                            collect_sized(lf, height).map_err(TriplestoreError::LazyLoadError)?,
                            subject_type.clone(),
                            object_type.clone(),
                            predicate,
                            self.global_cats.clone(),
                        );
                        for t in triples {
                            let q = t.in_graph(graph_name.clone());
                            writer.serialize_quad(&q).map_err(|x| {
                                TriplestoreError::WriteQuadsError(format!(
                                    "Error serializing quad {}: {}",
                                    q, x
                                ))
                            })?;
                        }
                    }
                }
            }
        }
        writer.finish().map_err(|x| {
            TriplestoreError::WriteQuadsError(format!("Error finishing serialization: {}", x))
        })?;
        Ok(())
    }

    pub fn write_hdt<W: Write>(
        &self,
        buf: &mut W,
        graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        hdt_write::write_hdt(self, buf, graph)
    }

    pub(crate) fn check_graph_exists(&self, graph: &NamedGraph) -> Result<(), TriplestoreError> {
        if !self.graph_triples_map.contains_key(graph) {
            Err(TriplestoreError::GraphDoesNotExist(graph.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn does_graph_exist(&self, graph: &NamedGraph) -> bool {
        self.graph_triples_map.contains_key(graph)
    }
}

pub fn convert_datelike_to_string(df: &mut DataFrame, c: &str, time_zone_offset: Option<i16>) {
    match df.column(c).unwrap().dtype() {
        DataType::Date => {
            df.with_column(date_column_to_strings(df.column(c).unwrap()))
                .unwrap();
        }
        DataType::Datetime(..) => {
            df.with_column(datetime_column_to_xsd_strings(
                df.column(c).unwrap(),
                time_zone_offset,
            ))
            .unwrap();
        }
        _ => {}
    }
}
