use super::Triplestore;
use crate::errors::TriplestoreError;
use oxrdfio::{RdfFormat, RdfSerializer};
use polars::prelude::col;
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::POOL;
use representation::polars_to_rdf::{
    date_column_to_strings, datetime_column_to_strings, df_as_triples,
};
use representation::{
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::HashMap;
use std::io::Write;

mod fast_ntriples;
mod serializers;

const CHUNK_SIZE: usize = 1_024;

impl Triplestore {
    pub fn write_triples<W: Write>(
        &mut self,
        buf: &mut W,
        format: RdfFormat,
    ) -> Result<(), TriplestoreError> {
        self.index_unindexed()?;
        if RdfFormat::NTriples == format {
            let n_threads = POOL.current_num_threads();
            for (verb, df_map) in &self.triples_map {
                let verb_string = verb.to_string();
                let verb_bytes = verb_string.as_bytes();
                for ((subject_type, object_type), tt) in df_map {
                    if object_type.is_lang_string() {
                        let types = HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), subject_type.clone()),
                            (LANG_STRING_VALUE_FIELD.to_string(), object_type.clone()),
                            (LANG_STRING_LANG_FIELD.to_string(), object_type.clone()),
                        ]);
                        let lfs = tt.get_lazy_frames(&None, &None)?;
                        for (lf, _) in lfs {
                            let df = lf
                                .unnest([OBJECT_COL_NAME])
                                .select([
                                    col(SUBJECT_COL_NAME),
                                    col(LANG_STRING_VALUE_FIELD),
                                    col(LANG_STRING_LANG_FIELD),
                                ])
                                .collect()
                                .unwrap();
                            fast_ntriples::write_triples_in_df(
                                buf, &df, verb_bytes, &types, CHUNK_SIZE, n_threads,
                            )
                            .unwrap();
                        }
                    } else {
                        let types = HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), subject_type.clone()),
                            (OBJECT_COL_NAME.to_string(), object_type.clone()),
                        ]);
                        for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                            let mut df = lf
                                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                                .collect()
                                .unwrap();
                            convert_datelike_to_string(&mut df, OBJECT_COL_NAME);
                            fast_ntriples::write_triples_in_df(
                                buf, &df, verb_bytes, &types, CHUNK_SIZE, n_threads,
                            )
                            .unwrap();
                        }
                    }
                }
            }
        } else {
            let mut writer = RdfSerializer::from_format(format).for_writer(buf);

            for (verb, df_map) in &self.triples_map {
                for ((subject_type, object_type), tt) in df_map {
                    for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                        let triples = df_as_triples(
                            lf.collect().unwrap(),
                            &subject_type.as_rdf_node_type(),
                            &object_type.as_rdf_node_type(),
                            verb,
                        );
                        for t in &triples {
                            writer.serialize_triple(t).unwrap();
                        }
                    }
                }
            }
            writer.finish().unwrap();
        }
        Ok(())
    }
}

pub fn convert_datelike_to_string(df: &mut DataFrame, c: &str) {
    match df.column(c).unwrap().dtype() {
        DataType::Date => {
            df.with_column(date_column_to_strings(df.column(c).unwrap()))
                .unwrap();
        }
        DataType::Datetime(_, tz_opt) => {
            df.with_column(datetime_column_to_strings(df.column(c).unwrap(), tz_opt))
                .unwrap();
        }
        _ => {}
    }
}
