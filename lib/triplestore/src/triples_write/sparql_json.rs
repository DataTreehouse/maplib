//! SPARQL 1.1 Query Results JSON (<https://www.w3.org/TR/sparql11-results-json/>) for SELECT
//! results, written with the serializers of maplib's N-Triples writer (see `fast_ntriples`): the
//! columns are decoded once, and each chunk of rows is written by a thread of the Polars pool into
//! its own buffer. The general writer (`QueryResultKind::json`) made a term per cell and wrote
//! them one by one, which took about 95% of the time of a query with a large result.

use super::convert_datelike_to_string;
use super::serializers::{serializer_for, write_escaped, Serializer, Style};
use crate::errors::TriplestoreError;
use oxrdf::vocab::{rdf, xsd};
use polars::prelude::{col, IntoLazy};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::Series;
use polars_core::runtime::THREAD_POOL;
use polars_core::utils::polars_arrow::array::Array;
use rayon::prelude::*;
use representation::cats::{maybe_decode_complex_expr, LockedCats};
use representation::multitype::extract_column_from_multitype;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};

/// Rows per chunk that a thread writes at a time.
const MIN_CHUNK_ROWS: usize = 4_096;

/// The values of one variable of one RDF type: a variable with several types has one part per
/// type, and each row has a value in at most one of them.
struct Part {
    variable: usize,
    value: Series,
    lang: Option<Series>,
    rdf_type: BaseRDFNodeType,
    /// Written before the value: `{"type":"...","value":`.
    prefix: Vec<u8>,
    /// Written after the value (and the language tag): `}` or `,"datatype":"..."}`.
    suffix: Vec<u8>,
}

/// The SELECT result as SPARQL JSON, or None if a column has a type that the serializers do not
/// write (the caller then uses the general writer).
pub fn sparql_json(
    sm: &EagerSolutionMappings,
    global_cats: LockedCats,
) -> Result<Option<Vec<u8>>, TriplestoreError> {
    let df = &sm.mappings;
    let height = df.height();
    // The variables in the order of the columns, as the general writer has them.
    let variables: Vec<_> = df
        .get_column_names()
        .into_iter()
        .filter_map(|c| sm.rdf_node_types.get(c.as_str()).map(|t| (c.as_str(), t)))
        .collect();

    let mut out = Vec::with_capacity(64 + height * 64 * variables.len().max(1));
    out.extend_from_slice(b"{\"head\":{\"vars\":[");
    for (i, (v, _)) in variables.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'"');
        write_escaped(v, &mut out);
        out.push(b'"');
    }
    out.extend_from_slice(b"]},\"results\":{\"bindings\":[");
    if height == 0 {
        out.extend_from_slice(b"]}}");
        return Ok(Some(out));
    }

    // Decode the dictionary codes of every part in one go.
    let mut columns = vec![];
    let mut exprs = vec![];
    let mut meta = vec![];
    for (vi, (name, state)) in variables.iter().enumerate() {
        let column = df.column(name).map_err(TriplestoreError::LazyLoadError)?;
        for (k, (t, s)) in state.map.iter().enumerate() {
            if t.is_none() {
                continue;
            }
            let part_name = format!("p{vi}_{k}");
            let mut c = if state.is_multi() {
                extract_column_from_multitype(column, t)
            } else {
                column.clone()
            };
            c.rename(part_name.as_str().into());
            columns.push(c);
            exprs.push(
                maybe_decode_complex_expr(col(&part_name), t, s, global_cats.clone())
                    .alias(&part_name),
            );
            meta.push((vi, t.clone(), part_name));
        }
    }
    let mut decoded = DataFrame::new(height, columns)
        .map_err(TriplestoreError::LazyLoadError)?
        .lazy()
        .with_columns(exprs)
        .collect()
        .map_err(TriplestoreError::LazyLoadError)?;

    let mut parts = vec![];
    for (variable, rdf_type, part_name) in meta {
        let (value, lang) = if rdf_type.is_lang_string() {
            let st = decoded
                .column(&part_name)
                .map_err(TriplestoreError::LazyLoadError)?
                .struct_()
                .map_err(TriplestoreError::LazyLoadError)?
                .clone();
            let field = |f: &str| {
                st.field_by_name(f)
                    .map_err(TriplestoreError::LazyLoadError)
                    .map(|s| s.rechunk())
            };
            (field(LANG_STRING_VALUE_FIELD)?, Some(field(LANG_STRING_LANG_FIELD)?))
        } else {
            if let BaseRDFNodeType::Literal(..) = &rdf_type {
                convert_datelike_to_string(&mut decoded, &part_name, rdf_type.time_zone_offset());
            }
            let s = decoded
                .column(&part_name)
                .map_err(TriplestoreError::LazyLoadError)?
                .as_materialized_series()
                .rechunk();
            (s, None)
        };
        if !writable(value.dtype()) || lang.as_ref().is_some_and(|l| l.dtype() != &DataType::String) {
            return Ok(None);
        }
        let (prefix, suffix) = wrapping(&rdf_type);
        parts.push(Part {
            variable,
            value,
            lang,
            rdf_type,
            prefix,
            suffix,
        });
    }
    let keys: Vec<Vec<u8>> = variables
        .iter()
        .map(|(v, _)| {
            let mut k = vec![b'"'];
            write_escaped(v, &mut k);
            k.extend_from_slice(b"\":");
            k
        })
        .collect();

    let n_threads = THREAD_POOL.current_num_threads().max(1);
    let chunk = MIN_CHUNK_ROWS.max(height.div_ceil(n_threads * 4));
    let n_chunks = height.div_ceil(chunk);
    let buffers: Vec<Vec<u8>> = THREAD_POOL.install(|| {
        (0..n_chunks)
            .into_par_iter()
            .map(|c| {
                let offset = c * chunk;
                let len = chunk.min(height - offset);
                write_rows(&parts, &keys, variables.len(), offset, len)
            })
            .collect::<Result<_, _>>()
    })?;
    for b in &buffers {
        out.extend_from_slice(b);
    }
    // Every row is followed by a comma.
    if out.last() == Some(&b',') {
        out.pop();
    }
    out.extend_from_slice(b"]}}");
    Ok(Some(out))
}

/// Whether the serializers write values of this column type.
fn writable(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::String
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal(..)
    )
}

/// The JSON around a value of the type: its "type" key, and its "datatype" for typed literals
/// (none for xsd:string, and xml:lang instead for language-tagged strings).
fn wrapping(t: &BaseRDFNodeType) -> (Vec<u8>, Vec<u8>) {
    match t {
        BaseRDFNodeType::IRI => (b"{\"type\":\"uri\",\"value\":".to_vec(), b"}".to_vec()),
        BaseRDFNodeType::BlankNode => (b"{\"type\":\"bnode\",\"value\":".to_vec(), b"}".to_vec()),
        BaseRDFNodeType::Literal(dt, _) => {
            let prefix = b"{\"type\":\"literal\",\"value\":".to_vec();
            if dt.as_ref() == xsd::STRING || dt.as_ref() == rdf::LANG_STRING {
                (prefix, b"}".to_vec())
            } else {
                let mut suffix = b",\"datatype\":\"".to_vec();
                write_escaped(dt.as_str(), &mut suffix);
                suffix.extend_from_slice(b"\"}");
                (prefix, suffix)
            }
        }
        BaseRDFNodeType::None => (vec![], vec![]),
    }
}

/// Writes rows `offset..offset + len` of every part, each row followed by a comma.
fn write_rows(
    parts: &[Part],
    keys: &[Vec<u8>],
    n_variables: usize,
    offset: usize,
    len: usize,
) -> Result<Vec<u8>, TriplestoreError> {
    let sliced: Vec<(Series, Option<Series>)> = parts
        .iter()
        .map(|p| {
            (
                p.value.slice(offset as i64, len).rechunk(),
                p.lang.as_ref().map(|l| l.slice(offset as i64, len).rechunk()),
            )
        })
        .collect();
    let arrays: Vec<(&dyn Array, Option<&dyn Array>)> = sliced
        .iter()
        .map(|(v, l)| (&*v.chunks()[0], l.as_ref().map(|l| &*l.chunks()[0])))
        .collect();
    let mut serializers: Vec<(Box<dyn Serializer + Send>, Option<Box<dyn Serializer + Send>>)> =
        Vec::with_capacity(parts.len());
    for ((p, (v, l)), (va, la)) in parts.iter().zip(&sliced).zip(&arrays) {
        let value = serializer_for(*va, v.dtype(), &p.rdf_type, false, Style::Json)
            .map_err(TriplestoreError::LazyLoadError)?;
        let lang = match (l, la) {
            (Some(l), Some(la)) => Some(
                serializer_for(*la, l.dtype(), &p.rdf_type, true, Style::Json)
                    .map_err(TriplestoreError::LazyLoadError)?,
            ),
            _ => None,
        };
        serializers.push((value, lang));
    }
    let mut buf = Vec::with_capacity(len * 48 * n_variables.max(1));
    let mut scratch = vec![];
    let mut written = vec![false; n_variables];
    for row in 0..len {
        buf.push(b'{');
        written.iter_mut().for_each(|w| *w = false);
        let mut first = true;
        for ((p, (value, lang)), (va, _)) in parts.iter().zip(serializers.iter_mut()).zip(&arrays)
        {
            // Every serializer moves on by one row, also where it writes nothing.
            if va.is_valid(row) && !written[p.variable] {
                written[p.variable] = true;
                if !first {
                    buf.push(b',');
                }
                first = false;
                buf.extend_from_slice(&keys[p.variable]);
                buf.extend_from_slice(&p.prefix);
                value.serialize(&mut buf);
                if let Some(lang) = lang {
                    buf.extend_from_slice(b",\"xml:lang\":");
                    lang.serialize(&mut buf);
                }
                buf.extend_from_slice(&p.suffix);
            } else {
                value.serialize(&mut scratch);
                if let Some(lang) = lang {
                    lang.serialize(&mut scratch);
                }
                scratch.clear();
            }
        }
        buf.extend_from_slice(b"},");
    }
    Ok(buf)
}
