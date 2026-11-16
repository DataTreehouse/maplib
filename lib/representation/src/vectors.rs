//! Vector literals, used for embeddings.
//!
//! A vector literal has the datatype [`VECTOR_DATATYPE`] and a lexical form that lists the
//! components in square brackets, separated by commas: `"[0.1, -2.5, 3e-4]"`. Whitespace around
//! the components is ignored. The components are 32 bit floats.
//!
//! Polars columns of fixed size arrays of numbers are mapped to vector literals.

use crate::{BaseRDFNodeType, RDFNodeState};
use oxrdf::NamedNode;
use polars::prelude::{
    Column, DataFrame, DataType, IntoColumn, IntoSeries, StringChunked,
    StringChunkedBuilder,
};
use rayon::prelude::*;
use std::fmt::Write;

pub const VECTOR_DATATYPE: &str = "https://datatreehouse.github.io/maplib/vocab#vector";

pub fn vector_datatype() -> NamedNode {
    NamedNode::new_unchecked(VECTOR_DATATYPE)
}

pub fn vector_base_type() -> BaseRDFNodeType {
    BaseRDFNodeType::Literal(vector_datatype(), None)
}

pub fn is_vector_literal_type(t: &BaseRDFNodeType) -> bool {
    matches!(t, BaseRDFNodeType::Literal(l, _) if l.as_str() == VECTOR_DATATYPE)
}

/// Writes the lexical form of a vector, using the shortest representation of each component
/// that reads back to the same float.
pub fn format_vector_into(v: &[f32], out: &mut String) {
    out.push('[');
    let mut buf = ryu::Buffer::new();
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if x.is_finite() {
            let s = buf.format_finite(*x);
            // ryu writes integral values as "1.0", which is fine, but trims nothing else.
            out.push_str(s);
        } else {
            // Not valid in the lexical space, but keeps the length of the vector.
            write!(out, "{x}").unwrap();
        }
    }
    out.push(']');
}

pub fn format_vector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 11 + 2);
    format_vector_into(v, &mut s);
    s
}

/// Parses the lexical form of a vector literal into `out`, which is cleared first.
pub fn parse_vector_into(s: &str, out: &mut Vec<f32>) -> Result<(), String> {
    out.clear();
    let t = s.trim();
    let inner = t
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(|| format!("a vector literal must be enclosed in square brackets: {s}"))?;
    if inner.trim().is_empty() {
        return Ok(());
    }
    for part in inner.split(',') {
        let part = part.trim();
        let x: f32 = part
            .parse()
            .map_err(|_| format!("invalid vector component {part:?} in {s}"))?;
        out.push(x);
    }
    Ok(())
}

pub fn parse_vector(s: &str) -> Result<Vec<f32>, String> {
    let mut v = vec![];
    parse_vector_into(s, &mut v)?;
    Ok(v)
}

/// Whether a column holds vectors: fixed size arrays of numbers.
pub fn is_vector_column_dtype(dt: &DataType) -> bool {
    if let DataType::Array(inner, _) = dt {
        inner.is_primitive_numeric()
    } else {
        false
    }
}

/// Converts a column of fixed size arrays of numbers into a string column with the lexical forms
/// of vector literals. Null arrays become null.
pub fn vector_column_to_lexical(c: &Column) -> Result<Column, String> {
    let name = c.name().clone();
    let DataType::Array(_, width) = c.dtype() else {
        return Err(format!("column {name} is not an array column"));
    };
    let width = *width;
    let ser = c.as_materialized_series();
    let ser = ser
        .cast(&DataType::Array(Box::new(DataType::Float32), width))
        .map_err(|e| e.to_string())?;
    let arr = ser.array().map_err(|e| e.to_string())?;
    let arr = arr.rechunk();
    let not_null = ser.is_not_null();
    let outer_validity: Vec<bool> = not_null
        .iter()
        .map(|x| x.unwrap_or(false))
        .collect();
    let values = arr.get_inner();
    let values = values.f32().map_err(|e| e.to_string())?;
    let values = values.rechunk();
    // The values are borrowed when they have no nulls, so they are not copied.
    let with_nans: Vec<f32>;
    let slice: &[f32] = if values.null_count() > 0 {
        with_nans = values.iter().map(|x| x.unwrap_or(f32::NAN)).collect();
        &with_nans
    } else {
        values.cont_slice().unwrap()
    };
    // Each chunk of rows is formatted into its own string array, without a String per row.
    const ROWS_PER_CHUNK: usize = 8192;
    let n = arr.len();
    let chunks: Vec<StringChunked> = (0..n.div_ceil(ROWS_PER_CHUNK))
        .into_par_iter()
        .map(|k| {
            let from = k * ROWS_PER_CHUNK;
            let to = n.min(from + ROWS_PER_CHUNK);
            let mut b = StringChunkedBuilder::new(name.clone(), to - from);
            let mut buf = String::with_capacity(width * 12 + 2);
            for i in from..to {
                if outer_validity[i] {
                    buf.clear();
                    format_vector_into(&slice[i * width..(i + 1) * width], &mut buf);
                    b.append_value(&buf);
                } else {
                    b.append_null();
                }
            }
            b.finish()
        })
        .collect();
    let mut chunks = chunks.into_iter();
    let mut out = chunks
        .next()
        .unwrap_or_else(|| StringChunkedBuilder::new(name.clone(), 0).finish());
    for c in chunks {
        out.append_owned(c).map_err(|e| e.to_string())?;
    }
    Ok(out.into_series().into_column())
}

/// Converts the vector columns of a DataFrame into vector literal columns. Returns the names of
/// the converted columns along with the RDF node state they have afterwards.
pub fn vector_columns_to_literals(
    df: &mut DataFrame,
) -> Result<Vec<(String, RDFNodeState)>, String> {
    let mut converted = vec![];
    let names: Vec<_> = df
        .columns()
        .iter()
        .filter(|c| is_vector_column_dtype(c.dtype()))
        .map(|c| c.name().to_string())
        .collect();
    for n in names {
        let c = vector_column_to_lexical(df.column(&n).unwrap())?;
        df.replace(&n, c)
            .map_err(|e| e.to_string())?;
        converted.push((n, vector_base_type().into_default_input_rdf_node_state()));
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let v = vec![0.1f32, -2.5, 3e-4, 1.0, 0.0];
        let s = format_vector(&v);
        assert_eq!(parse_vector(&s).unwrap(), v);
        assert_eq!(parse_vector(" [ 1 , 2.5 ] ").unwrap(), vec![1.0, 2.5]);
        assert_eq!(parse_vector("[]").unwrap(), Vec::<f32>::new());
        assert!(parse_vector("1,2").is_err());
    }
}
