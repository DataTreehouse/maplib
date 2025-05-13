use super::Triplestore;
use crate::errors::TriplestoreError;
use oxrdf::vocab::xsd;
use oxrdfio::{RdfFormat, RdfSerializer};
use polars::prelude::{col, lit};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::POOL;
use representation::polars_to_rdf::{
    date_column_to_strings, datetime_column_to_strings, df_as_triples,
};
use representation::{
    BaseRDFNodeType, IRI_PREFIX_FIELD, IRI_SUFFIX_FIELD, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use std::collections::HashMap;
use std::io::Write;

mod fast_ntriples;
mod serializers;

const CHUNK_SIZE: usize = 1_024;
const SUBJECT_COL_IRI_PREFIX: &str = "subject-IRI-prefix";
const SUBJECT_COL_IRI_SUFFIX: &str = "subject-IRI-suffix";
const OBJECT_COL_IRI_PREFIX: &str = "object-IRI-prefix";
const OBJECT_COL_IRI_SUFFIX: &str = "object-IRI-suffix";

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
                    let mut types = HashMap::new();
                    let mut select = vec![];
                    if let BaseRDFNodeType::IRI(nn) = subject_type {
                        types.insert(SUBJECT_COL_IRI_PREFIX.to_string(), BaseRDFNodeType::IRI(None));
                        types.insert(SUBJECT_COL_IRI_SUFFIX.to_string(), BaseRDFNodeType::IRI(None));
                        if let Some(nn) = nn {
                            select.push(
                                lit(nn.as_str())
                                    .alias(SUBJECT_COL_IRI_PREFIX),
                            );
                        } else {
                            select.push(
                                col(SUBJECT_COL_NAME)
                                    .struct_()
                                    .field_by_name(IRI_PREFIX_FIELD)
                                    .alias(SUBJECT_COL_IRI_PREFIX),
                            );
                        }
                        select.push(
                            col(SUBJECT_COL_NAME)
                                .struct_()
                                .field_by_name(IRI_SUFFIX_FIELD)
                                .alias(SUBJECT_COL_IRI_SUFFIX),
                        );
                    } else {
                        types.insert(SUBJECT_COL_NAME.to_string(), subject_type.clone());
                        select.push(col(SUBJECT_COL_NAME));
                    }

                    if object_type.is_lang_string() {
                        types.insert(LANG_STRING_VALUE_FIELD.to_string(), object_type.clone());
                        types.insert(LANG_STRING_LANG_FIELD.to_string(), object_type.clone());
                        select.push(
                            col(OBJECT_COL_NAME)
                                .struct_()
                                .field_by_name(LANG_STRING_VALUE_FIELD)
                                .alias(LANG_STRING_VALUE_FIELD),
                        );
                        select.push(
                            col(OBJECT_COL_NAME)
                                .struct_()
                                .field_by_name(LANG_STRING_LANG_FIELD)
                                .alias(LANG_STRING_LANG_FIELD),
                        );
                    } else if let BaseRDFNodeType::IRI(nn) = object_type {
                        types.insert(OBJECT_COL_IRI_PREFIX.to_string(), BaseRDFNodeType::IRI(None));
                        types.insert(OBJECT_COL_IRI_SUFFIX.to_string(), BaseRDFNodeType::IRI(None));
                        if let Some(nn) = nn{
                            select.push(
                                lit(nn.as_str())
                                    .alias(OBJECT_COL_IRI_PREFIX),
                            );
                        } else {
                            select.push(
                                col(OBJECT_COL_NAME)
                                    .struct_()
                                    .field_by_name(IRI_PREFIX_FIELD)
                                    .alias(OBJECT_COL_IRI_PREFIX),
                            );
                        }
                        select.push(
                            col(OBJECT_COL_NAME)
                                .struct_()
                                .field_by_name(IRI_SUFFIX_FIELD)
                                .alias(OBJECT_COL_IRI_SUFFIX),
                        );
                    } else {
                        types.insert(OBJECT_COL_NAME.to_string(), object_type.clone());
                        select.push(col(OBJECT_COL_NAME));
                    }

                    let lfs = tt.get_lazy_frames(&None, &None)?;
                    for (lf, _) in lfs {
                        let mut df = lf.select(select.clone()).collect().unwrap();
                        if let BaseRDFNodeType::Literal(l) = object_type {
                            let l_ref = l.as_ref();
                            if matches!(l_ref, xsd::DATE | xsd::DATE_TIME | xsd::DATE_TIME_STAMP) {
                                convert_datelike_to_string(&mut df, OBJECT_COL_NAME);
                            }
                        }
                        fast_ntriples::write_triples_in_df(
                            buf, &df, verb_bytes, &types, CHUNK_SIZE, n_threads,
                        )
                        .unwrap();
                    }
                }
            }
        } else {
            let mut writer = RdfSerializer::from_format(format).for_writer(buf);

            for (verb, df_map) in &self.triples_map {
                for ((subject_type, object_type), tt) in df_map {
                    for (lf, _) in tt.get_lazy_frames(&None, &None)? {
                        let df = lf.collect().unwrap();
                        let triples = df_as_triples(
                            df,
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
