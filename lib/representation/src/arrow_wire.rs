//! Query results as Arrow, for the maplib server endpoints and the client of `Model.query(endpoint=)`.
//!
//! Data moves between polars and arrow-rs through the Arrow C data interface without copying.
//!
//! The solution mappings format sends results with maplib's own column representation and the
//! RDF types of each column, so a client gets the same results as from a local query. It is a
//! sequence of frames, each an 8 byte little endian length followed by an Arrow IPC stream:
//! - SELECT: one stream, schema metadata `maplib.kind` = `select`.
//! - ASK: one stream with one row of the boolean column `ask`, `maplib.kind` = `ask`.
//! - CONSTRUCT: one stream per triple pattern of the template, `maplib.kind` = `construct`,
//!   with the constant predicate (if any) in `maplib.predicate`. No patterns give a single
//!   stream without columns and `maplib.empty` = `true`.
//!
//! Each field has the RDF types of the column in `maplib.rdf_types`, a JSON list of
//! `[BaseRDFNodeType, "string" | "nonstring"]`. Dictionary encoded IRIs and strings are decoded
//! to strings before sending, since the dictionaries are not sent, and the columns otherwise
//! keep maplib's layout (multi-type columns are structs with a field per type).

use utils::polars::CollectAuto;
use crate::cats::LockedCats;
use crate::cats::{maybe_decode_complex_expr, maybe_decode_expr};
use crate::result::QueryResultKind;
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use arrow::array::{make_array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use crate::dataset::NamedGraph;
use oxrdf::{BlankNode, NamedNode};
use polars::prelude::{
    as_struct, col, CompatLevel, DataFrame, IntoColumn, IntoLazy, LazyFrame, PlSmallStr, Series,
};
use polars_core::utils::polars_arrow;
use polars_core::utils::polars_arrow::ffi as pl_ffi;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;

pub const SOLUTION_MAPPINGS_MEDIA_TYPE: &str = "application/vnd.maplib.solution-mappings";
const KIND: &str = "maplib.kind";
const PREDICATE: &str = "maplib.predicate";
const EMPTY: &str = "maplib.empty";
const GRAPH: &str = "maplib.graph";
const RDF_TYPES: &str = "maplib.rdf_types";

#[derive(Debug)]
pub struct ArrowWireError(pub String);

impl Display for ArrowWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ArrowWireError {}

fn err(e: impl Display) -> ArrowWireError {
    ArrowWireError(e.to_string())
}

/// Polars uses Arrow types that all Arrow implementations read (large strings, not views).
const COMPAT: CompatLevel = CompatLevel::oldest();

fn export_field(field: &polars_arrow::datatypes::Field) -> FFI_ArrowSchema {
    let s = pl_ffi::export_field_to_c(field);
    // Both are the C ABI ArrowSchema struct.
    unsafe { std::mem::transmute::<pl_ffi::ArrowSchema, FFI_ArrowSchema>(s) }
}

/// The DataFrame as Arrow record batches of at most `batch_rows` rows.
pub fn df_to_batches(
    mut df: DataFrame,
    batch_rows: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowWireError> {
    let mut fields = vec![];
    for c in df.columns() {
        let pl_field = c.field().to_arrow(COMPAT);
        let ffi_schema = export_field(&pl_field);
        fields.push(Field::try_from(&ffi_schema).map_err(err)?);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    // One chunk, so that batch_rows decides the batch sizes rather than how polars chunked
    // the result.
    df.rechunk_mut_par();
    let mut batches = vec![];
    for rb in df.iter_chunks(COMPAT, true) {
        let mut arrays: Vec<ArrayRef> = vec![];
        for (array, field) in rb.columns().iter().zip(rb.schema().iter_values()) {
            let ffi_schema = export_field(field);
            let ffi_array = pl_ffi::export_array_to_c(array.clone());
            let ffi_array =
                unsafe { std::mem::transmute::<pl_ffi::ArrowArray, FFI_ArrowArray>(ffi_array) };
            let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(err)?;
            arrays.push(make_array(data));
        }
        let batch = if arrays.is_empty() {
            RecordBatch::try_new_with_options(
                schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(rb.height())),
            )
        } else {
            RecordBatch::try_new(schema.clone(), arrays)
        }
        .map_err(err)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = batch_rows.max(1).min(batch.num_rows() - offset);
            batches.push(batch.slice(offset, len));
            offset += len;
        }
    }
    Ok((schema, batches))
}

fn array_to_series(name: &str, array: &ArrayRef) -> Result<Series, ArrowWireError> {
    let (ffi_array, ffi_schema) = to_ffi(&array.to_data()).map_err(err)?;
    let ffi_array = unsafe { std::mem::transmute::<FFI_ArrowArray, pl_ffi::ArrowArray>(ffi_array) };
    let ffi_schema =
        unsafe { std::mem::transmute::<FFI_ArrowSchema, pl_ffi::ArrowSchema>(ffi_schema) };
    let pl_field = unsafe { pl_ffi::import_field_from_c(&ffi_schema) }.map_err(err)?;
    let pl_array =
        unsafe { pl_ffi::import_array_from_c(ffi_array, pl_field.dtype.clone()) }.map_err(err)?;
    Series::from_arrow(PlSmallStr::from_str(name), pl_array).map_err(err)
}

/// Record batches as a DataFrame, with the columns of the schema when there are no batches.
pub fn batches_to_df(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<DataFrame, ArrowWireError> {
    let mut out: Option<DataFrame> = None;
    for batch in batches {
        let mut columns = vec![];
        for (array, field) in batch.columns().iter().zip(schema.fields().iter()) {
            columns.push(array_to_series(field.name(), array)?.into_column());
        }
        let df = if columns.is_empty() {
            DataFrame::new(batch.num_rows(), columns)
        } else {
            DataFrame::new_infer_height(columns)
        }
        .map_err(err)?;
        match &mut out {
            None => out = Some(df),
            Some(o) => {
                o.vstack_mut(&df).map_err(err)?;
            }
        }
    }
    match out {
        Some(df) => Ok(df),
        None => {
            let mut columns = vec![];
            for f in schema.fields() {
                let empty = arrow::array::new_empty_array(f.data_type());
                columns.push(array_to_series(f.name(), &empty)?.into_column());
            }
            DataFrame::new(0, columns).map_err(err)
        }
    }
}

/// IPC options, with zstd compressed buffers if `compress`.
pub fn ipc_options(compress: bool) -> Result<IpcWriteOptions, ArrowWireError> {
    let options = IpcWriteOptions::default();
    if compress {
        options
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .map_err(err)
    } else {
        Ok(options)
    }
}

/// An Arrow IPC stream.
pub fn ipc_stream(
    schema: &Schema,
    batches: &[RecordBatch],
    compress: bool,
) -> Result<Vec<u8>, ArrowWireError> {
    let mut w = StreamWriter::try_new_with_options(Vec::new(), schema, ipc_options(compress)?)
        .map_err(err)?;
    for b in batches {
        w.write(b).map_err(err)?;
    }
    w.into_inner().map_err(err)
}

type TypeEntry = (BaseRDFNodeType, String);

fn encode_types(state: &RDFNodeState) -> Result<String, ArrowWireError> {
    let mut entries: Vec<TypeEntry> = vec![];
    for t in state.get_sorted_types() {
        let s = match state.map.get(t).unwrap() {
            BaseCatState::String => "string",
            BaseCatState::NonString => "nonstring",
            BaseCatState::CategoricalNative(_) => {
                return Err(ArrowWireError(
                    "Dictionary encoded columns must be decoded before sending".to_string(),
                ))
            }
        };
        entries.push((t.clone(), s.to_string()));
    }
    serde_json::to_string(&entries).map_err(err)
}

fn decode_types(s: &str) -> Result<RDFNodeState, ArrowWireError> {
    let entries: Vec<TypeEntry> = serde_json::from_str(s).map_err(err)?;
    let mut map = HashMap::new();
    for (t, s) in entries {
        let s = match s.as_str() {
            "string" => BaseCatState::String,
            "nonstring" => BaseCatState::NonString,
            other => return Err(ArrowWireError(format!("Unknown column state {other}"))),
        };
        map.insert(t, s);
    }
    Ok(RDFNodeState { map })
}

fn frame(out: &mut Vec<u8>, stream: Vec<u8>) {
    out.extend_from_slice(&(stream.len() as u64).to_le_bytes());
    out.extend(stream);
}

fn encode_mappings(
    out: &mut Vec<u8>,
    mut sm: EagerSolutionMappings,
    meta: HashMap<String, String>,
    cats: LockedCats,
    compress: bool,
    batch_rows: usize,
) -> Result<(), ArrowWireError> {
    let order: Vec<String> = sm
        .mappings
        .get_column_names()
        .iter()
        .map(|c| c.to_string())
        .collect();
    sm.mappings = decode_cats(sm.mappings.lazy(), &mut sm.rdf_node_types, cats)
        .select(order.iter().map(|c| col(c.as_str())).collect::<Vec<_>>())
        .collect_auto()
        .map_err(err)?;
    let (schema, batches) = df_to_batches(sm.mappings, batch_rows)?;
    let mut fields = vec![];
    for f in schema.fields() {
        let state = sm
            .rdf_node_types
            .get(f.name())
            .ok_or_else(|| ArrowWireError(format!("No RDF type for the column {}", f.name())))?;
        let mut m = HashMap::new();
        m.insert(RDF_TYPES.to_string(), encode_types(state)?);
        fields.push(f.as_ref().clone().with_metadata(m));
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, meta));
    let batches = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    frame(out, ipc_stream(&schema, &batches, compress)?);
    Ok(())
}

/// Decodes dictionary encoded values to strings, keeping maplib's column layout (unlike the
/// native formatting for users), so that the receiver can use the columns as its own.
fn decode_cats(
    mut lf: LazyFrame,
    types: &mut HashMap<String, RDFNodeState>,
    cats: LockedCats,
) -> LazyFrame {
    for (c, state) in types.iter() {
        if !state
            .map
            .values()
            .any(|s| matches!(s, BaseCatState::CategoricalNative(_)))
        {
            continue;
        }
        if state.is_multi() {
            let mut fields = vec![];
            for t in state.get_sorted_types() {
                let s = state.map.get(t).unwrap();
                let names = if t.is_lang_string() {
                    vec![
                        LANG_STRING_VALUE_FIELD.to_string(),
                        LANG_STRING_LANG_FIELD.to_string(),
                    ]
                } else {
                    vec![t.field_col_name()]
                };
                for n in names {
                    fields.push(
                        maybe_decode_expr(col(c).struct_().field_by_name(&n), t, s, cats.clone())
                            .alias(&n),
                    );
                }
            }
            lf = lf.with_column(as_struct(fields).alias(c));
        } else {
            let t = state.get_base_type().unwrap();
            let s = state.get_base_state().unwrap();
            lf = lf.with_column(maybe_decode_complex_expr(col(c), t, s, cats.clone()).alias(c));
        }
    }
    for s in types.values_mut() {
        for v in s.map.values_mut() {
            if matches!(v, BaseCatState::CategoricalNative(..)) {
                *v = BaseCatState::String;
            }
        }
    }
    lf
}

fn kind(k: &str) -> HashMap<String, String> {
    HashMap::from([(KIND.to_string(), k.to_string())])
}

/// A query result in the solution mappings format.
pub fn encode_query_result(
    result: QueryResultKind,
    cats: LockedCats,
    compress: bool,
    batch_rows: usize,
) -> Result<Vec<u8>, ArrowWireError> {
    let mut out = vec![];
    match result {
        QueryResultKind::Select(sm) => {
            encode_mappings(&mut out, sm, kind("select"), cats, compress, batch_rows)?;
        }
        QueryResultKind::Ask(b) => {
            let schema = Arc::new(Schema::new_with_metadata(
                vec![Field::new("ask", ArrowType::Boolean, false)],
                kind("ask"),
            ));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(BooleanArray::from(vec![b])) as ArrayRef],
            )
            .map_err(err)?;
            frame(&mut out, ipc_stream(&schema, &[batch], compress)?);
        }
        QueryResultKind::Construct(sms) => {
            if sms.is_empty() {
                let mut meta = kind("construct");
                meta.insert(EMPTY.to_string(), "true".to_string());
                let schema = Schema::new_with_metadata(Vec::<Field>::new(), meta);
                frame(&mut out, ipc_stream(&schema, &[], compress)?);
            }
            for (sm, predicate, graph) in sms {
                let mut meta = kind("construct");
                if let Some(p) = predicate {
                    meta.insert(PREDICATE.to_string(), p.into_string());
                }
                match graph {
                    NamedGraph::DefaultGraph => {}
                    NamedGraph::NamedGraph(nn) => {
                        meta.insert(GRAPH.to_string(), nn.into_string());
                    }
                    NamedGraph::BlankNode(b) => {
                        meta.insert(GRAPH.to_string(), format!("_:{}", b.as_str()));
                    }
                }
                encode_mappings(&mut out, sm, meta, cats.clone(), compress, batch_rows)?;
            }
        }
    }
    Ok(out)
}

fn read_stream(bytes: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowWireError> {
    let reader =
        arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).map_err(err)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(err)?;
    Ok((schema, batches))
}

fn decode_mappings(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<EagerSolutionMappings, ArrowWireError> {
    let df = batches_to_df(schema, batches)?;
    let mut types = HashMap::new();
    for f in schema.fields() {
        let t = f
            .metadata()
            .get(RDF_TYPES)
            .ok_or_else(|| ArrowWireError(format!("No RDF type for the column {}", f.name())))?;
        types.insert(f.name().to_string(), decode_types(t)?);
    }
    Ok(EagerSolutionMappings::new(df, types))
}

/// Decodes a query result in the solution mappings format.
pub fn decode_query_result(bytes: &[u8]) -> Result<QueryResultKind, ArrowWireError> {
    let mut pos = 0;
    let mut streams = vec![];
    while pos < bytes.len() {
        if bytes.len() - pos < 8 {
            return Err(ArrowWireError("Truncated result".to_string()));
        }
        let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        if bytes.len() - pos < len {
            return Err(ArrowWireError("Truncated result".to_string()));
        }
        streams.push(read_stream(&bytes[pos..pos + len])?);
        pos += len;
    }
    let Some((first, _)) = streams.first() else {
        return Err(ArrowWireError("The result is empty".to_string()));
    };
    let k = first.metadata().get(KIND).cloned().unwrap_or_default();
    match k.as_str() {
        "select" => {
            let (schema, batches) = &streams[0];
            Ok(QueryResultKind::Select(decode_mappings(schema, batches)?))
        }
        "ask" => {
            let (_, batches) = &streams[0];
            let b = batches
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|a| a.value(0))
                })
                .ok_or_else(|| ArrowWireError("Invalid ASK result".to_string()))?;
            Ok(QueryResultKind::Ask(b))
        }
        "construct" => {
            let mut sms = vec![];
            for (schema, batches) in &streams {
                if schema.metadata().get(EMPTY).is_some_and(|v| v == "true") {
                    continue;
                }
                let predicate = schema
                    .metadata()
                    .get(PREDICATE)
                    .map(|p| NamedNode::new(p.as_str()))
                    .transpose()
                    .map_err(err)?;
                let graph = match schema.metadata().get(GRAPH) {
                    None => NamedGraph::DefaultGraph,
                    Some(g) => {
                        if let Some(b) = g.strip_prefix("_:") {
                            NamedGraph::BlankNode(BlankNode::new(b).map_err(err)?)
                        } else {
                            NamedGraph::NamedGraph(NamedNode::new(g.as_str()).map_err(err)?)
                        }
                    }
                };
                sms.push((decode_mappings(schema, batches)?, predicate, graph));
            }
            Ok(QueryResultKind::Construct(sms))
        }
        other => Err(ArrowWireError(format!("Unknown result kind {other}"))),
    }
}
