use super::Triplestore;
use crate::errors::TriplestoreError;
use file_io::{property_to_filename, write_parquet};
use log::debug;
use polars::prelude::ParquetCompression;
use representation::BaseRDFNodeType;
use std::path::Path;
use std::time::Instant;

impl Triplestore {
    pub fn write_native_parquet(&mut self, path: &Path) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        self.index_unindexed()?;
        if !path.exists() {
            return Err(TriplestoreError::PathDoesNotExist(
                path.to_str().unwrap().to_string(),
            ));
        }
        let path_buf = path.to_path_buf();

        for (property, tts) in &mut self.triples_map {
            for ((_rdf_node_type_s, rdf_node_type_o), tt) in tts {
                let filename;
                if let BaseRDFNodeType::Literal(literal_type) = rdf_node_type_o {
                    filename = format!(
                        "{}_{}",
                        property_to_filename(property.as_str()),
                        property_to_filename(literal_type.as_str())
                    );
                } else {
                    filename = format!(
                        "{}_object_property",
                        property_to_filename(property.as_str()),
                    )
                }
                let file_path = path_buf.clone();

                for (i, (lf, _)) in tt.get_lazy_frames(&None, &None)?.into_iter().enumerate() {
                    let filename = format!("{filename}_part_{i}.parquet");
                    let mut file_path = file_path.clone();
                    file_path.push(filename);
                    write_parquet(
                        &mut lf.collect().unwrap(),
                        file_path.as_path(),
                        ParquetCompression::default(),
                    )
                    .map_err(TriplestoreError::FileIOError)?
                }
            }
        }
        debug!(
            "Writing native parquet took {} seconds",
            now.elapsed().as_secs_f64()
        );
        Ok(())
    }
}
