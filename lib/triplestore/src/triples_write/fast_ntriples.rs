// The following code is based on the code below from Polars by Ritchie Vink and other contributors.
// https://github.com/pola-rs/polars/blob/dc94be767d26943be11a40d6171ccc1c41a86c4f/crates/polars-io/src/csv/write/write_impl.rs
// The Polars license can be found in the licensing folder.
use crate::triples_write::serializers::serializer_for;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars_core::prelude::*;
use polars_core::POOL;
use rayon::prelude::*;
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
};
use std::collections::HashMap;
use std::io::Write;

const LINE_TERMINATOR: [u8; 2] = [b'.', b'\n'];
const SEPARATOR: u8 = b' ';
const DATA_TYPE_SEP_CHARS: [u8; 2] = [b'^', b'^'];

pub(crate) fn write_triples_in_df<W: Write>(
    writer: &mut W,
    df: &DataFrame,
    verb: &[u8],
    rdf_node_types: &HashMap<String, BaseRDFNodeType>,
    chunk_size: usize,
    n_threads: usize,
) -> PolarsResult<()> {
    let len = df.height();
    let total_rows_per_pool_iter = n_threads * chunk_size;

    let mut n_rows_finished = 0;
    let use_suffix = df
        .get_columns()
        .iter()
        .map(|x| {
            if x.name() == LANG_STRING_VALUE_FIELD {
                vec![]
            } else if x.name() == OBJECT_COL_NAME {
                if let BaseRDFNodeType::Literal(nn) = rdf_node_types.get(OBJECT_COL_NAME).unwrap() {
                    if needs_explicit_datatype(nn) {
                        let mut suffix: Vec<_> = DATA_TYPE_SEP_CHARS.to_vec();
                        suffix.extend_from_slice(nn.to_string().as_bytes());
                        suffix.push(SEPARATOR);
                        suffix
                    } else {
                        vec![SEPARATOR]
                    }
                } else {
                    vec![SEPARATOR]
                }
            } else {
                vec![SEPARATOR]
            }
        })
        .collect::<Vec<_>>();

    let mut buffers: Vec<_> = (0..n_threads).map(|_| (Vec::new(), Vec::new())).collect();
    while n_rows_finished < len {
        let buf_writer = |thread_no, write_buffer: &mut Vec<_>, serializers_vec: &mut Vec<_>| {
            let thread_offset = thread_no * chunk_size;
            let total_offset = n_rows_finished + thread_offset;
            let mut df = df.slice(total_offset as i64, chunk_size);
            // the `series.iter` needs rechunked series.
            // we don't do this on the whole as this probably needs much less rechunking
            // so will be faster.
            // and allows writing `pl.concat([df] * 100, rechunk=False).write_csv()` as the rechunk
            // would go OOM
            df.as_single_chunk();
            let cols = df.get_columns();

            // SAFETY:
            // the bck thinks the lifetime is bounded to write_buffer_pool, but at the time we return
            // the vectors the buffer pool, the series have already been removed from the buffers
            // in other words, the lifetime does not leave this scope
            let cols = unsafe { std::mem::transmute::<&[Column], &[Column]>(cols) };

            if df.is_empty() {
                return Ok(());
            }

            if serializers_vec.is_empty() {
                *serializers_vec = cols
                    .iter()
                    .map(|col| {
                        let lang_tag = col.name() == LANG_STRING_LANG_FIELD;
                        serializer_for(
                            &*col.as_materialized_series().chunks()[0],
                            col.dtype(),
                            rdf_node_types.get(col.name().as_str()).unwrap(),
                            lang_tag,
                        )
                    })
                    .collect::<Result<_, _>>()?;
            } else {
                for (col_iter, col) in std::iter::zip(serializers_vec.iter_mut(), cols) {
                    col_iter.update_array(&*col.as_materialized_series().chunks()[0]);
                }
            }

            let serializers = serializers_vec.as_mut_slice();

            let len = std::cmp::min(cols[0].len(), chunk_size);

            for _ in 0..len {
                serializers[0].serialize(write_buffer);
                write_buffer.push(SEPARATOR);
                write_buffer.extend_from_slice(verb);
                write_buffer.push(SEPARATOR);
                for (serializer, suffix_use) in serializers[1..].iter_mut().zip(&use_suffix[1..]) {
                    serializer.serialize(write_buffer);
                    write_buffer.extend_from_slice(suffix_use);
                }
                write_buffer.extend_from_slice(&LINE_TERMINATOR);
            }

            Ok(())
        };

        if n_threads > 1 {
            POOL.install(|| {
                buffers
                    .par_iter_mut()
                    .enumerate()
                    .map(|(i, (w, s))| buf_writer(i, w, s))
                    .collect::<PolarsResult<()>>()
            })?;
        } else {
            let (w, s) = &mut buffers[0];
            buf_writer(0, w, s)?;
        }

        for (write_buffer, _) in &mut buffers {
            writer.write_all(write_buffer)?;
            write_buffer.clear();
        }

        n_rows_finished += total_rows_per_pool_iter;
    }
    Ok(())
}

fn needs_explicit_datatype(t: &NamedNode) -> bool {
    t.as_ref() != xsd::STRING && t.as_ref() != rdf::LANG_STRING
}
