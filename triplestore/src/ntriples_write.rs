//Based on writer.rs: https://raw.githubusercontent.com/pola-rs/polars/master/polars/polars-io/src/csv_core/write.rs
//in Pola.rs with license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use super::Triplestore;
use crate::conversion::convert_to_string;
use crate::errors::TriplestoreError;
use oxrdf::NamedNode;
use parquet_io::read_parquet;
use polars::export::rayon::iter::{IntoParallelIterator, ParallelIterator};
use polars::export::rayon::prelude::ParallelExtend;
use polars::prelude::{AnyValue, DataFrame, Series};
use polars::series::SeriesIter;
use polars_core::POOL;
use polars_utils::contention_pool::LowContentionPool;
use representation::{RDFNodeType, TripleType};
use std::io::Write;


/// Utility to write to `&mut Vec<u8>` buffer
struct StringWrap<'a>(pub &'a mut Vec<u8>);

impl<'a> std::fmt::Write for StringWrap<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

impl Triplestore {
    pub fn write_n_triples_all_dfs<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        chunk_size: usize,
    ) -> Result<(), TriplestoreError> {
        self.deduplicate()?;
        let n_threads = POOL.current_num_threads();
        let mut any_value_iter_pool = LowContentionPool::<Vec<_>>::new(n_threads);
        let mut write_buffer_pool = LowContentionPool::<Vec<_>>::new(n_threads);

        for (property, map) in &mut self.df_map {
            for ((_rdf_node_type_k, rdf_node_type_o), tt) in map {
                let dt = if let RDFNodeType::Literal(dt) = rdf_node_type_o {
                    Some(dt.clone())
                } else {
                    None
                };
                let triple_type = rdf_node_type_o.find_triple_type();
                if let Some(dfs) = &mut tt.dfs {
                    for df in dfs {
                        df.as_single_chunk_par();
                        write_ntriples_for_df(
                            df,
                            &property.to_string(),
                            &dt,
                            writer,
                            chunk_size,
                            triple_type.clone(),
                            n_threads,
                            &mut any_value_iter_pool,
                            &mut write_buffer_pool,
                        )?;
                    }
                } else if let Some(paths) = &tt.df_paths {
                    for p in paths {
                        let df = read_parquet(p)
                            .map_err(TriplestoreError::ParquetIOError)?
                            .collect()
                            .unwrap();
                        write_ntriples_for_df(
                            &df,
                            &property.to_string(),
                            &dt,
                            writer,
                            chunk_size,
                            triple_type.clone(),
                            n_threads,
                            &mut any_value_iter_pool,
                            &mut write_buffer_pool,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn write_ntriples_for_df<W: Write + ?Sized>(
    df: &DataFrame,
    verb: &str,
    dt: &Option<NamedNode>,
    writer: &mut W,
    chunk_size: usize,
    triple_type: TripleType,
    n_threads: usize,
    any_value_iter_pool: &mut LowContentionPool<Vec<SeriesIter>>,
    write_buffer_pool: &mut LowContentionPool<Vec<u8>>,
) -> Result<(), TriplestoreError> {
    let dt_str = if triple_type == TripleType::NonStringProperty {
        if let Some(nn) = dt {
            Some(nn.as_str())
        } else {
            panic!("Must have datatype for non string property")
        }
    } else {
        None
    };

    let len = df.height();

    let total_rows_per_pool_iter = n_threads * chunk_size;

    let mut n_rows_finished = 0;

    // holds the buffers that will be written
    let mut result_buf = Vec::with_capacity(n_threads);
    while n_rows_finished < len {
        let par_iter = (0..n_threads).into_par_iter().map(|thread_no| {
            let thread_offset = thread_no * chunk_size;
            let total_offset = n_rows_finished + thread_offset;
            let mut df = df.slice(total_offset as i64, chunk_size);
            //We force all objects to string-representations here
            if let Some(s) = convert_to_string(df.column("object").unwrap()) {
                df.with_column(s).unwrap();
            }

            let cols = df.get_columns();

            // Safety:
            // the bck thinks the lifetime is bounded to write_buffer_pool, but at the time we return
            // the vectors the buffer pool, the series have already been removed from the buffers
            // in other words, the lifetime does not leave this scope
            let cols = unsafe { std::mem::transmute::<&[Series], &[Series]>(cols) };
            let mut write_buffer = write_buffer_pool.get();

            // don't use df.empty, won't work if there are columns.
            if df.height() == 0 {
                return write_buffer;
            }

            let any_value_iters = cols.iter().map(|s| s.iter());
            let mut col_iters = any_value_iter_pool.get();
            col_iters.extend(any_value_iters);

            let mut finished = false;
            // loop rows
            while !finished {
                let mut any_values = vec![];
                for col in &mut col_iters {
                    match col.next() {
                        Some(value) => any_values.push(value),
                        None => {
                            finished = true;
                            break;
                        }
                    }
                }
                if !any_values.is_empty() {
                    match triple_type {
                        TripleType::ObjectProperty => {
                            write_object_property_triple(&mut write_buffer, any_values, verb);
                        }
                        TripleType::StringProperty => {
                            write_string_property_triple(&mut write_buffer, any_values, verb);
                        }
                        TripleType::NonStringProperty => {
                            write_non_string_property_triple(
                                &mut write_buffer,
                                dt_str.unwrap(),
                                any_values,
                                verb,
                            );
                        }
                    }
                }
            }

            // return buffers to the pool
            col_iters.clear();
            any_value_iter_pool.set(col_iters);

            write_buffer
        });
        // rayon will ensure the right order
        result_buf.par_extend(par_iter);

        for mut buf in result_buf.drain(..) {
            let _ = writer
                .write(&buf)
                .map_err(TriplestoreError::WriteNTriplesError);
            buf.clear();
            write_buffer_pool.set(buf);
        }

        n_rows_finished += total_rows_per_pool_iter;
    }
    Ok(())
}

fn write_string_property_triple(f: &mut Vec<u8>, mut any_values: Vec<AnyValue>, v: &str) {
    let lang_opt = if let AnyValue::Utf8(lang) = any_values.pop().unwrap() {
        Some(lang)
    } else {
        None
    };
    let lex = if let AnyValue::Utf8(lex) = any_values.pop().unwrap() {
        lex
    } else {
        panic!()
    };
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_string(f, lex);

    if let Some(lang) = lang_opt {
        writeln!(f, "@{} .", lang).unwrap();
    } else {
        writeln!(f, " .").unwrap();
    }
}

//Assumes that the data has been bulk-converted
fn write_non_string_property_triple(
    f: &mut Vec<u8>,
    dt: &str,
    mut any_values: Vec<AnyValue>,
    v: &str,
) {
    let lex = if let AnyValue::Utf8(lex) = any_values.pop().unwrap() {
        lex
    } else {
        panic!()
    };
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_string(f, lex);
    writeln!(f, "^^<{}> .", dt).unwrap();
}

fn write_object_property_triple(f: &mut Vec<u8>, mut any_values: Vec<AnyValue>, v: &str) {
    let o = if let AnyValue::Utf8(o) = any_values.pop().unwrap() {
        o
    } else {
        panic!()
    };
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_iri_or_blanknode(f, o);
    writeln!(f, " .").unwrap();
}

fn write_iri_or_blanknode(f: &mut Vec<u8>, s: &str) {
    if s.chars().nth(0).unwrap() == '_' {
        write!(f, "{}", s).unwrap();
    } else {
        write_iri(f, s);
    }
}

fn write_iri(f: &mut Vec<u8>, s: &str) {
    let mut chars = s[1..(s.len() - 1)].chars();
    write!(f, "<").unwrap();
    loop {
        if let Some(c) = chars.next() {
            match c {
                '>' => write!(f, "\\>").unwrap(),
                '\\' => write!(f, "\\\\").unwrap(),
                _ => {
                    write!(f, "{}", c).unwrap();
                }
            }
        } else {
            break;
        }
    }
    write!(f, ">").unwrap();
}

fn write_string(f: &mut Vec<u8>, s: &str) {
    write!(f, "\"").unwrap();
    let mut chars = s.chars();
    loop {
        if let Some(c) = chars.next() {
            match c {
                '\n' => {
                    write!(f, "\\n").unwrap();
                }
                '\t' => {
                    write!(f, "\\t").unwrap();
                }
                '\r' => {
                    write!(f, "\\r").unwrap();
                }
                '"' | '\\' => {
                    write!(f, "\\{c}").unwrap();
                }
                _ => {
                    write!(f, "{c}").unwrap();
                }
            }
        } else {
            break;
        }
    }
    write!(f, "\"").unwrap();
}
