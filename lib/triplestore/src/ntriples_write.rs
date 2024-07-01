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
use oxrdf::vocab::xsd;
use parquet_io::scan_parquet;
use polars::export::rayon::iter::{IntoParallelIterator, ParallelIterator};
use polars::export::rayon::prelude::ParallelExtend;
use polars::prelude::{col, AnyValue, DataFrame, IntoLazy, Series};
use polars_core::datatypes::DataType;
use polars_core::series::SeriesIter;
use polars_core::POOL;
use polars_utils::contention_pool::LowContentionPool;
use representation::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::io::Write;

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
            for ((subj_type, obj_type), tt) in map {
                let subj_type = BaseRDFNodeType::from_rdf_node_type(subj_type);
                let obj_type = BaseRDFNodeType::from_rdf_node_type(obj_type);

                if let Some(dfs) = &mut tt.dfs {
                    for df in dfs {
                        df.as_single_chunk_par();
                        write_ntriples_for_df(
                            df,
                            property.as_str(),
                            writer,
                            chunk_size,
                            &subj_type,
                            &obj_type,
                            n_threads,
                            &mut any_value_iter_pool,
                            &mut write_buffer_pool,
                        )?;
                    }
                } else if let Some(paths) = &tt.df_paths {
                    for p in paths {
                        let df = scan_parquet(p)
                            .map_err(TriplestoreError::ParquetIOError)?
                            .collect()
                            .unwrap();
                        write_ntriples_for_df(
                            &df,
                            property.as_str(),
                            writer,
                            chunk_size,
                            &subj_type,
                            &obj_type,
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
    writer: &mut W,
    chunk_size: usize,
    subj_type: &BaseRDFNodeType,
    obj_type: &BaseRDFNodeType,
    n_threads: usize,
    any_value_iter_pool: &mut LowContentionPool<Vec<SeriesIter>>,
    write_buffer_pool: &mut LowContentionPool<Vec<u8>>,
) -> Result<(), TriplestoreError> {
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

            let s = convert_to_string(df.column(SUBJECT_COL_NAME).unwrap());
            df.with_column(s).unwrap();
            if obj_type.is_lang_string() {
                df = df
                    .lazy()
                    .with_columns([
                        col(OBJECT_COL_NAME)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .cast(DataType::String)
                            .alias(LANG_STRING_VALUE_FIELD),
                        col(OBJECT_COL_NAME)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .cast(DataType::String)
                            .alias(LANG_STRING_LANG_FIELD),
                    ])
                    .select([
                        col(SUBJECT_COL_NAME),
                        col(LANG_STRING_VALUE_FIELD),
                        col(LANG_STRING_LANG_FIELD),
                    ])
                    .collect()
                    .unwrap();
            } else {
                let o = convert_to_string(df.column(OBJECT_COL_NAME).unwrap());
                df.with_column(o).unwrap();
            }

            let subject_blank = matches!(subj_type, BaseRDFNodeType::BlankNode);
            let is_plain_string = if let BaseRDFNodeType::Literal(l) = obj_type {
                l.as_ref() == xsd::STRING
            } else {
                false
            };
            let is_lang_string = obj_type.is_lang_string();

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
                    match obj_type {
                        BaseRDFNodeType::IRI => {
                            write_object_property_triple(
                                &mut write_buffer,
                                any_values,
                                verb,
                                subject_blank,
                                false,
                            );
                        }
                        BaseRDFNodeType::BlankNode => {
                            write_object_property_triple(
                                &mut write_buffer,
                                any_values,
                                verb,
                                subject_blank,
                                true,
                            );
                        }
                        BaseRDFNodeType::Literal(nn) => {
                            if is_plain_string {
                                write_string_property_triple(
                                    &mut write_buffer,
                                    any_values,
                                    verb,
                                    subject_blank,
                                );
                            } else if is_lang_string {
                                write_lang_string_property_triple(
                                    &mut write_buffer,
                                    any_values,
                                    verb,
                                    subject_blank,
                                );
                            } else {
                                write_non_string_property_triple(
                                    &mut write_buffer,
                                    nn.as_str(),
                                    any_values,
                                    verb,
                                    subject_blank,
                                );
                            }
                        }
                        BaseRDFNodeType::None => panic!("Should never happen"),
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

fn write_lang_string_property_triple(
    f: &mut Vec<u8>,
    mut any_values: Vec<AnyValue>,
    v: &str,
    subject_blank: bool,
) {
    let lang = if let AnyValue::String(lang) = any_values.pop().unwrap() {
        lang
    } else {
        panic!()
    };
    let lex = if let AnyValue::String(lex) = any_values.pop().unwrap() {
        lex
    } else {
        panic!()
    };
    let s = if let AnyValue::String(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s, subject_blank);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_lang_string(f, lex, lang);
    writeln!(f, " .").unwrap();
}

fn write_string_property_triple(
    f: &mut Vec<u8>,
    mut any_values: Vec<AnyValue>,
    v: &str,
    subject_blank: bool,
) {
    let lex = if let AnyValue::String(lex) = any_values.pop().unwrap() {
        lex
    } else {
        panic!()
    };
    let s = if let AnyValue::String(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s, subject_blank);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_string(f, lex);
    writeln!(f, " .").unwrap();
}

//Assumes that the data has been bulk-converted
fn write_non_string_property_triple(
    f: &mut Vec<u8>,
    dt: &str,
    mut any_values: Vec<AnyValue>,
    v: &str,
    subject_blank: bool,
) {
    let lex = if let AnyValue::String(lex) = any_values.pop().unwrap() {
        lex
    } else {
        panic!()
    };
    let s = if let AnyValue::String(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s, subject_blank);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_string(f, lex);
    writeln!(f, "^^<{}> .", dt).unwrap();
}

fn write_object_property_triple(
    f: &mut Vec<u8>,
    mut any_values: Vec<AnyValue>,
    v: &str,
    subject_blank: bool,
    object_blank: bool,
) {
    let o = if let AnyValue::String(o) = any_values.pop().unwrap() {
        o
    } else {
        panic!()
    };
    let s = if let AnyValue::String(s) = any_values.pop().unwrap() {
        s
    } else {
        panic!()
    };
    write_iri_or_blanknode(f, s, subject_blank);
    write!(f, " ").unwrap();
    write_iri(f, v);
    write!(f, " ").unwrap();
    write_iri_or_blanknode(f, o, object_blank);
    writeln!(f, " .").unwrap();
}

fn write_iri_or_blanknode(f: &mut Vec<u8>, s: &str, blank: bool) {
    if blank {
        write!(f, "_:{}", s).unwrap();
    } else {
        write_iri(f, s);
    }
}

fn write_iri(f: &mut Vec<u8>, s: &str) {
    write!(f, "<{}>", s).unwrap();
}

fn write_lang_string(f: &mut Vec<u8>, lex: &str, lang: &str) {
    write_string(f, lex);
    write!(f, "@{}", lang).unwrap();
}

fn write_string(f: &mut Vec<u8>, s: &str) {
    write!(f, "\"").unwrap();
    let mut chars = s.chars();
    loop {
        if let Some(c) = chars.next() {
            write_escaped_char(c, f);
        } else {
            break;
        }
    }
    write!(f, "\"").unwrap();
}

fn write_escaped_char(c: char, f: &mut Vec<u8>) {
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
}
