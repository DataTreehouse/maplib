use super::Triplestore;
use crate::errors::TriplestoreError;
use oxrdfio::{RdfFormat, RdfSerializer};
use polars::prelude::{by_name, col, IntoLazy};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::IntoColumn;
use polars_core::POOL;
use representation::polars_to_rdf::{
    date_column_to_strings, datetime_column_to_strings, global_df_as_triples,
};
use representation::{
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::HashMap;
use std::io::Write;
use log::warn;

mod fast_ntriples;
mod serializers;

const CHUNK_SIZE: usize = 1_024;

impl Triplestore {
    pub fn write_triples<W: Write>(
        &mut self,
        buf: &mut W,
        format: RdfFormat,
    ) -> Result<(), TriplestoreError> {
        if RdfFormat::NTriples == format {
            let n_threads = POOL.current_num_threads();
            for (predicate, df_map) in &self.triples_map {
                let predicate_string = predicate.to_string();
                let predicate_bytes = predicate_string.as_bytes();
                for ((subject_type, object_type), tt) in df_map {
                    let base_subject_type = subject_type.as_base_rdf_node_type();
                    let base_object_type = object_type.as_base_rdf_node_type();
                    if base_object_type.is_lang_string() {
                        let types = HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), base_subject_type.clone()),
                            (
                                LANG_STRING_VALUE_FIELD.to_string(),
                                base_object_type.clone(),
                            ),
                            (LANG_STRING_LANG_FIELD.to_string(), base_object_type.clone()),
                        ]);
                        let lfs = tt.get_lazy_frames(&None, &None)?;
                        for (lf, _) in lfs {
                            let mut df = lf
                                .unnest(by_name([OBJECT_COL_NAME], true))
                                .select([
                                    col(SUBJECT_COL_NAME),
                                    col(LANG_STRING_VALUE_FIELD),
                                    col(LANG_STRING_LANG_FIELD),
                                ])
                                .collect()
                                .unwrap();
                            // Debug to catch error
                            let nulls_df = df.clone().lazy().filter(col(LANG_STRING_VALUE_FIELD).is_null().or(col(LANG_STRING_LANG_FIELD).is_null())).collect().unwrap();
                            if nulls_df.height() > 0 {
                                warn!("Triplestore had null lang strings {}", nulls_df);
                            }
                            df = df.lazy().drop_nulls(None).collect().unwrap();

                            if let Some(prefix) = subject_type.as_cat_type() {
                                let ser = self
                                    .cats
                                    .decode_of_type(
                                        &df.column(SUBJECT_COL_NAME)
                                            .unwrap()
                                            .as_materialized_series_maintain_scalar(),
                                        &prefix,
                                    )
                                    .unwrap();
                                df.with_column(ser.into_column()).unwrap();
                            }
                            fast_ntriples::write_triples_in_df(
                                buf,
                                &df,
                                predicate_bytes,
                                &types,
                                CHUNK_SIZE,
                                n_threads,
                            )
                            .unwrap();
                        }
                    } else {
                        let types = HashMap::from([
                            (SUBJECT_COL_NAME.to_string(), base_subject_type.clone()),
                            (OBJECT_COL_NAME.to_string(), base_object_type.clone()),
                        ]);
                        for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                            let mut df = lf
                                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)])
                                .collect()
                                .unwrap();
                            convert_datelike_to_string(&mut df, OBJECT_COL_NAME);
                            if let Some(prefix) = subject_type.as_cat_type() {
                                let ser = self
                                    .cats
                                    .decode_of_type(
                                        &df.column(SUBJECT_COL_NAME)
                                            .unwrap()
                                            .as_materialized_series_maintain_scalar(),
                                        &prefix,
                                    )
                                    .unwrap();
                                df.with_column(ser.into_column()).unwrap();
                            }
                            if let Some(prefix) = object_type.as_cat_type() {
                                let ser = self
                                    .cats
                                    .decode_of_type(
                                        &df.column(OBJECT_COL_NAME)
                                            .unwrap()
                                            .as_materialized_series_maintain_scalar(),
                                        &prefix,
                                    )
                                    .unwrap();
                                df.with_column(ser.into_column()).unwrap();
                            }
                            fast_ntriples::write_triples_in_df(
                                buf,
                                &df,
                                predicate_bytes,
                                &types,
                                CHUNK_SIZE,
                                n_threads,
                            )
                            .unwrap();
                        }
                    }
                }
            }
        } else {
            let mut writer = RdfSerializer::from_format(format).for_writer(buf);

            for (predicate, df_map) in &self.triples_map {
                for ((subject_type, object_type), tt) in df_map {
                    for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                        let triples = global_df_as_triples(
                            lf.collect().unwrap(),
                            subject_type.as_base_rdf_node_type(),
                            object_type.as_base_rdf_node_type(),
                            predicate,
                            self.cats.clone(),
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
