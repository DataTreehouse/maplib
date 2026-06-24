use super::{IndexingOptions, Triplestore};
use crate::errors::TriplestoreError;
use crate::storage::Triples;
use file_io::{scan_parquet, write_parquet, FileIOError};
use oxrdf::NamedNode;
use polars::prelude::{IntoLazy, LazyFrame, ParquetCompression, PolarsError};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::serialization::CatSerializationError;
use representation::cats::{Cats, LockedCats};
use representation::dataset::NamedGraph;
use representation::BaseRDFNodeType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum TripleSerializationError {
    #[error("failed to save data: {0}")]
    IOError(#[from] std::io::Error),
    #[error("failed to serialize data with serde: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Triplestore error: {0}")]
    TriplestoreError(#[from] TriplestoreError),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("File IO error: {0}")]
    FileIOError(#[from] FileIOError),
    #[error("Cats serialization error: {0}")]
    CatSerializationError(#[from] CatSerializationError),
}

const TRIPLES_PATH: &str = "triples";
const SO_TRIPLES_FILE: &str = "so_triples.parquet";
const SO_SPARSE_INDEX_FILE: &str = "so_index.parquet";
const OS_TRIPLES_FILE: &str = "os_triples.parquet";
const OS_SPARSE_INDEX_FILE: &str = "os_index.parquet";
const METADATA_FILE: &str = "metadata.json";

// Todo:
// - Serialize triple tables:
//      - sort
//      - sparse index
// - Serialize triple tables metadata

#[derive(Serialize, Deserialize)]
struct TriplestoreMetadata {
    triples_uuid_map: HashMap<String, (NamedGraph, NamedNode, BaseRDFNodeType, BaseRDFNodeType)>,
    indexing: HashMap<NamedGraph, IndexingOptions>,
}

impl Triplestore {
    pub fn serialize_triples(&mut self, path: &Path) -> Result<(), TripleSerializationError> {
        self.garbage_collect()?;
        self.compact()?;

        let mut to_serialize = vec![];
        let triples_path = create_path(path, TRIPLES_PATH);

        let mut triples_uuid_map = HashMap::new();

        for (graph, map) in &self.graph_triples_map {
            for (pred, triples_map) in map {
                for ((subject_type, object_type), triples) in triples_map {
                    let value = (
                        graph.clone(),
                        pred.clone(),
                        subject_type.clone(),
                        object_type.clone(),
                    );
                    let key = Uuid::new_v4().to_string();
                    let this_triples_folder = create_path(&triples_path, &key);
                    create_dir_all(&this_triples_folder)?;
                    triples_uuid_map.insert(key, value);

                    let pb_subjects = create_path(&this_triples_folder, SO_TRIPLES_FILE);
                    let pb_subjects_index = create_path(&this_triples_folder, SO_SPARSE_INDEX_FILE);
                    let mut subject_object_sort = triples.get_all_triples_lazy_frames(true)?;
                    let mut subject_object_sparse_index =
                        triples.get_sparse_indices_as_data_frames(true);
                    assert_eq!(
                        subject_object_sort.len(),
                        1,
                        "Should always be compacted before serialization"
                    );
                    to_serialize.push((pb_subjects, subject_object_sort.pop().unwrap()));
                    to_serialize.push((
                        pb_subjects_index,
                        subject_object_sparse_index.pop().unwrap().lazy(),
                    ));
                    let mut object_subject_sort = triples.get_all_triples_lazy_frames(false)?;
                    assert!(
                        object_subject_sort.len() < 2,
                        "Should always be compacted before serialization"
                    );
                    if !object_subject_sort.is_empty() {
                        let pb_objects = create_path(&this_triples_folder, OS_TRIPLES_FILE);
                        to_serialize.push((pb_objects, object_subject_sort.pop().unwrap()));

                        let pb_objects_index =
                            create_path(&this_triples_folder, OS_SPARSE_INDEX_FILE);
                        let mut object_subject_sparse_index =
                            triples.get_sparse_indices_as_data_frames(false);
                        to_serialize.push((
                            pb_objects_index,
                            object_subject_sparse_index.pop().unwrap().lazy(),
                        ));
                    }
                }
            }
        }

        let triplestore_metadata = TriplestoreMetadata {
            triples_uuid_map,
            indexing: self.indexing.clone(),
        };

        //Serializing metadata
        let triplestore_metadata_path = create_path(&triples_path, METADATA_FILE);
        let triplestore_metadata_string = serde_json::to_string_pretty(&triplestore_metadata)?;

        let mut f = File::create(triplestore_metadata_path)?;
        write!(f, "{}", triplestore_metadata_string)?;

        let res: Result<(), TripleSerializationError> = to_serialize
            .into_par_iter()
            .map(|(pb, triples)| {
                write_triples_parquet(triples, &pb)?;
                Ok(())
            })
            .collect();
        let _ = res?;

        self.global_cats
            .read()
            .expect("Should always be readable")
            .serialize_cats(path)?;

        Ok(())
    }

    pub fn deserialize_triples(
        path: &Path,
        storage_folder: Option<String>,
    ) -> Result<Triplestore, TripleSerializationError> {
        let mut triplestore = Triplestore::new(storage_folder, None)?;
        let triples_path = create_path(path, TRIPLES_PATH);
        let metadata_file_path = create_path(&triples_path, METADATA_FILE);
        assert!(metadata_file_path.exists());
        let metadata_bytes = std::fs::read(&metadata_file_path)?;
        let triplestore_metadata: TriplestoreMetadata =
            serde_json::from_slice(&metadata_bytes).unwrap();

        let start_deserialize_triples = Instant::now();
        let triples_vec: Result<Vec<_>, FileIOError> = triplestore_metadata
            .triples_uuid_map
            .into_par_iter()
            .map(|(uuid, (graph, predicate, subject_type, object_type))| {
                let this_triple_path = create_path(&triples_path, &uuid);
                let subject_object_file = create_path(&this_triple_path, SO_TRIPLES_FILE);
                let object_subject_file = create_path(&this_triple_path, OS_TRIPLES_FILE);
                let subject_object_index_file =
                    create_path(&this_triple_path, SO_SPARSE_INDEX_FILE);
                let object_subject_index_file =
                    create_path(&this_triple_path, OS_SPARSE_INDEX_FILE);

                let subject_object_df = scan_parquet(&subject_object_file)?.collect().unwrap();
                let subject_object_index_df =
                    scan_parquet(&subject_object_index_file)?.collect().unwrap();
                let (object_subject_df, object_subject_index_df) = if object_subject_file.exists() {
                    let object_subject_df = scan_parquet(&object_subject_file)?.collect().unwrap();
                    let object_subject_index_df =
                        scan_parquet(&object_subject_index_file)?.collect().unwrap();
                    (Some(object_subject_df), Some(object_subject_index_df))
                } else {
                    (None, None)
                };
                let triples = Triples::in_memory_from_serialization(
                    subject_object_df,
                    subject_object_index_df,
                    object_subject_df,
                    object_subject_index_df,
                    subject_type.clone(),
                    object_type.clone(),
                );
                Ok((graph, predicate, subject_type, object_type, triples))
            })
            .collect();
        for (graph, pred, subject_type, object_type, triples) in triples_vec? {
            if !triplestore.graph_triples_map.contains_key(&graph) {
                triplestore
                    .graph_triples_map
                    .insert(graph.clone(), HashMap::new());
            }
            let pred_map = triplestore.graph_triples_map.get_mut(&graph).unwrap();
            if !pred_map.contains_key(&pred) {
                pred_map.insert(pred.clone(), HashMap::new());
            }
            let triples_map = pred_map.get_mut(&pred).unwrap();
            triples_map.insert((subject_type, object_type), triples);
        }
        debug!(
            "Deserialized triples in {} seconds",
            start_deserialize_triples.elapsed().as_secs_f32()
        );
        let start_deserialize_cats = Instant::now();
        let cats = Cats::deserialize_cats_to_in_memory(path);
        debug!(
            "Deserialized cats in {} seconds",
            start_deserialize_cats.elapsed().as_secs_f32()
        );
        triplestore.global_cats = LockedCats::new(cats);
        triplestore.indexing = triplestore_metadata.indexing;
        Ok(triplestore)
    }
}

fn write_triples_parquet(lf: LazyFrame, path: &Path) -> Result<(), TripleSerializationError> {
    write_parquet(&mut lf.collect()?, path, ParquetCompression::Snappy)?;
    Ok(())
}

fn create_path(path: &Path, s: &str) -> PathBuf {
    let mut new_path = path.to_owned();
    new_path.push(s);
    new_path
}
