use super::Triplestore;
use crate::errors::TriplestoreError;
use oxttl::NTriplesSerializer;
use rayon::prelude::IntoParallelIterator;
use representation::polars_to_rdf::df_as_triples;
use std::io::Write;

impl Triplestore {
    pub fn write_ntriples(&self, buf: &mut dyn Write) -> Result<(), TriplestoreError> {
        let mut writer = NTriplesSerializer::new().for_writer(buf);

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
        Ok(())
    }
}
