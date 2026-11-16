//! Vector literals, used for embeddings.
//!
//! A vector literal has the datatype [`VECTOR_DATATYPE`] and a lexical form that lists the
//! components in square brackets, separated by commas: `"[0.1, -2.5, 3e-4]"`. Whitespace around
//! the components is ignored. The components are 32 bit floats.
//!
//! Polars columns of fixed size arrays of numbers are mapped to vector literals.
//!
//! The dictionary of vector literals stores canonical lexical forms packed, as the bits of the
//! floats (see [`in_memory::vectors`]), and gives the lexical forms when decoding. Mapped vectors
//! are packed directly, without writing their lexical forms.

use crate::{BaseRDFNodeType, RDFNodeState};
use oxrdf::NamedNode;
use polars::prelude::{
    Column, DataFrame, DataType, IntoColumn, IntoSeries, Series, StringChunked,
    StringChunkedBuilder,
};
use rayon::prelude::*;

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

pub use in_memory::vectors::{
    format_vector_into, is_packed, lexical_to_stored, pack_vector, pack_vector_into,
    parse_vector_into,
    stored_to_lexical, stored_to_vector_into, unpack_vector_into,
};

pub fn format_vector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 11 + 2);
    format_vector_into(v, &mut s);
    s
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

/// Converts a column of fixed size arrays of numbers into a string column with the stored
/// (packed) forms of vector literals, which the dictionary of vector literals takes as they are.
/// Null arrays become null.
pub fn vector_column_to_stored(c: &Column) -> Result<Column, String> {
    vector_column_to_strings(c, true)
}

/// Converts a column of fixed size arrays of numbers into a string column with the lexical forms
/// of vector literals. Null arrays become null.
pub fn vector_column_to_lexical(c: &Column) -> Result<Column, String> {
    vector_column_to_strings(c, false)
}

fn vector_column_to_strings(c: &Column, packed: bool) -> Result<Column, String> {
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
                    let v = &slice[i * width..(i + 1) * width];
                    if packed {
                        pack_vector_into(v, &mut buf);
                    } else {
                        format_vector_into(v, &mut buf);
                    }
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

/// Converts a String series of lexical forms of vector literals into their stored forms (see
/// [`in_memory::vectors`]), in parallel, so that the dictionary of vector literals takes them as
/// they are instead of converting each string when it is looked up and again when it is added.
pub fn lexical_series_to_stored(series: &Series) -> Series {
    let name = series.name().clone();
    let ca = series.str().expect("vector literals are strings").rechunk();
    let n = ca.len();
    const ROWS_PER_CHUNK: usize = 1024;
    let chunks: Vec<StringChunked> = (0..n.div_ceil(ROWS_PER_CHUNK))
        .into_par_iter()
        .map(|k| {
            let from = k * ROWS_PER_CHUNK;
            let to = n.min(from + ROWS_PER_CHUNK);
            let mut b = StringChunkedBuilder::new(name.clone(), to - from);
            for v in ca.slice(from as i64, to - from).iter() {
                match v {
                    Some(v) => b.append_value(lexical_to_stored(v)),
                    None => b.append_null(),
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
        out.append_owned(c).unwrap();
    }
    out.into_series()
}

/// Converts the vector columns of a DataFrame into vector literal columns. Returns the names of
/// the converted columns along with the RDF node state they have afterwards. The columns for
/// which `keep_lexical` is true get the lexical forms (e.g. when they are mapped to another type
/// than vector literals), the others the stored forms, which only the dictionary of vector
/// literals takes.
pub fn vector_columns_to_literals(
    df: &mut DataFrame,
    keep_lexical: impl Fn(&str) -> bool,
) -> Result<Vec<(String, RDFNodeState)>, String> {
    let mut converted = vec![];
    let names: Vec<_> = df
        .columns()
        .iter()
        .filter(|c| is_vector_column_dtype(c.dtype()))
        .map(|c| c.name().to_string())
        .collect();
    for n in names {
        let c = vector_column_to_strings(df.column(&n).unwrap(), !keep_lexical(&n))?;
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

    #[test]
    fn mapped_columns_are_packed() {
        use polars::prelude::{NamedFrom, Series};
        let s = Series::new("v".into(), [0.1f32, -2.0, 3e-4, 1.0, f32::NAN, 0.0])
            .reshape_array(&[
                polars::prelude::ReshapeDimension::Infer,
                polars::prelude::ReshapeDimension::new_dimension(3),
            ])
            .unwrap();
        let c = s.into_column();
        let stored = vector_column_to_stored(&c).unwrap();
        let lexical = vector_column_to_lexical(&c).unwrap();
        for (p, l) in stored
            .str()
            .unwrap()
            .iter()
            .zip(lexical.str().unwrap().iter())
        {
            let (p, l) = (p.unwrap(), l.unwrap());
            assert!(is_packed(p));
            assert_eq!(stored_to_lexical(p), l);
        }
    }
}
