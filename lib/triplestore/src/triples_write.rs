use super::Triplestore;
use crate::errors::TriplestoreError;
use representation::polars_to_rdf::df_as_triples;
use std::io::Write;
use oxrdfio::{RdfFormat, RdfSerializer};

impl Triplestore {
    pub fn write_triples(&self, buf:&mut dyn Write, format:RdfFormat) -> Result<(), TriplestoreError> {
        let mut writer = RdfSerializer::from_format(format).for_writer(buf);

        for (verb, df_map) in &self.df_map {
            for ((subject_type, object_type), tt) in df_map {
                for lf in tt.get_lazy_frames()? {
                    let triples =
                        df_as_triples(lf.collect().unwrap(), subject_type, object_type, verb);
                    for t in &triples {
                        writer.serialize_triple(t).unwrap();
                    }
                }
            }
        }
        writer.finish().unwrap();
        Ok(())
    }
}
