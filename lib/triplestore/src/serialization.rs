use super::{
    maybe_create_fts_index_map, maybe_create_geo_index, prepare_storage, IndexingOptions,
    StorageOptions, Triplestore,
};
use crate::errors::TriplestoreError;
use crate::storage::{Triples, DISK_ROW_GROUP_SIZE};
use file_io::{scan_parquet, write_parquet, FileIOError};
use oxrdf::NamedNode;
use polars::prelude::{IntoLazy, LazyFrame, ParquetCompression, PolarsError};
use query_processing::udf::UdfRegistry;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::serialization::{CatSerializationError, CATS_FOLDER};
use representation::cats::{Cats, LockedCats};
use representation::dataset::NamedGraph;
use representation::BaseRDFNodeType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
/// Triple tables use the row group size of disk storage, as disk storage deserializes by copying
/// the files and reads whole row groups for lookups through the sparse indices. Sparse indices
/// are small and use the default.
const TRIPLES_ROW_GROUP_SIZE: Option<usize> = Some(DISK_ROW_GROUP_SIZE);

// Todo:
// - Serialize triple tables:
//      - sort
//      - sparse index
// - Serialize triple tables metadata

#[derive(Serialize, Deserialize)]
struct TriplestoreMetadata {
    triples_uuid_map: HashMap<String, (NamedGraph, NamedNode, BaseRDFNodeType, BaseRDFNodeType)>,
    indexing: IndexingOptions,
}

impl Triplestore {
    /// Serializes the triplestore to the folder `path`. The triple tables are parquet files, and
    /// so are the cat maps unless `raw_db` is set, which writes them as DuckDB databases.
    pub fn serialize_triples(
        &mut self,
        path: &Path,
        raw_db: bool,
    ) -> Result<(), TripleSerializationError> {
        self.garbage_collect()?;
        self.compact()?;
        if path.exists() {
            for f in std::fs::read_dir(path)? {
                let f = f?;
                let p = f.path();
                if p.is_dir()
                    && matches!(f.file_name().to_str().unwrap(), TRIPLES_PATH | CATS_FOLDER)
                {
                    std::fs::remove_dir_all(f.path())?
                }
            }
        }

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
                    to_serialize.push((
                        pb_subjects,
                        subject_object_sort.pop().unwrap(),
                        TRIPLES_ROW_GROUP_SIZE,
                    ));
                    to_serialize.push((
                        pb_subjects_index,
                        subject_object_sparse_index.pop().unwrap().lazy(),
                        None,
                    ));
                    let mut object_subject_sort = triples.get_all_triples_lazy_frames(false)?;
                    assert!(
                        object_subject_sort.len() < 2,
                        "Should always be compacted before serialization"
                    );
                    if !object_subject_sort.is_empty() {
                        let pb_objects = create_path(&this_triples_folder, OS_TRIPLES_FILE);
                        to_serialize.push((
                            pb_objects,
                            object_subject_sort.pop().unwrap(),
                            TRIPLES_ROW_GROUP_SIZE,
                        ));

                        let pb_objects_index =
                            create_path(&this_triples_folder, OS_SPARSE_INDEX_FILE);
                        let mut object_subject_sparse_index =
                            triples.get_sparse_indices_as_data_frames(false);
                        to_serialize.push((
                            pb_objects_index,
                            object_subject_sparse_index.pop().unwrap().lazy(),
                            None,
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
            .map(|(pb, triples, row_group_size)| {
                write_triples_parquet(triples, &pb, row_group_size)?;
                Ok(())
            })
            .collect();
        res?;

        self.global_cats
            .read()
            .expect("Should always be readable")
            .serialize_cats(path, raw_db)?;

        Ok(())
    }

    pub fn deserialize_triples(
        path: &Path,
        storage: StorageOptions,
    ) -> Result<Triplestore, TripleSerializationError> {
        let (storage_pathbuf, triples_folder) = prepare_storage(&storage)?;

        let triples_path = create_path(path, TRIPLES_PATH);
        let metadata_file_path = create_path(&triples_path, METADATA_FILE);
        assert!(metadata_file_path.exists());
        let metadata_bytes = std::fs::read(&metadata_file_path)?;
        let triplestore_metadata: TriplestoreMetadata =
            serde_json::from_slice(&metadata_bytes).unwrap();

        // The cats do not depend on the triples, so they are deserialized at the same time. With
        // hybrid and disk storage, building the on-disk cat maps (and their indices) is usually
        // the slower of the two.
        let storage_folder = storage_pathbuf.as_deref();
        let (triples_vec, cats) = rayon::join(
            || {
                let start_deserialize_triples = Instant::now();
                let triples_vec: Result<Vec<_>, TripleSerializationError> = triplestore_metadata
                    .triples_uuid_map
                    .into_par_iter()
                    .map(|(uuid, (graph, predicate, subject_type, object_type))| {
                        let this_triple_path = create_path(&triples_path, &uuid);
                        let subject_object_file = create_path(&this_triple_path, SO_TRIPLES_FILE);
                        let object_subject_file = create_path(&this_triple_path, OS_TRIPLES_FILE);
                        let subject_object_sparse_index_file =
                            create_path(&this_triple_path, SO_SPARSE_INDEX_FILE);
                        let object_subject_sparse_index_file =
                            create_path(&this_triple_path, OS_SPARSE_INDEX_FILE);

                        let subject_object_index_df =
                            scan_parquet(&subject_object_sparse_index_file)?
                                .collect()
                                .unwrap();
                        let (object_subject_file, object_subject_index_df) =
                            if object_subject_file.exists() {
                                let object_subject_index_df =
                                    scan_parquet(&object_subject_sparse_index_file)?
                                        .collect()
                                        .unwrap();
                                (Some(object_subject_file), Some(object_subject_index_df))
                            } else {
                                (None, None)
                            };
                        let triples = Triples::from_serialization(
                            &subject_object_file,
                            subject_object_index_df,
                            object_subject_file.as_deref(),
                            object_subject_index_df,
                            subject_type.clone(),
                            object_type.clone(),
                            triples_folder.as_deref(),
                        )?;
                        Ok((graph, predicate, subject_type, object_type, triples))
                    })
                    .collect();
                debug!(
                    "Deserialized triples in {} seconds",
                    start_deserialize_triples.elapsed().as_secs_f32()
                );
                triples_vec
            },
            || {
                let start_deserialize_cats = Instant::now();
                let cats = Cats::deserialize_cats(path, storage_folder);
                debug!(
                    "Deserialized cats in {} seconds",
                    start_deserialize_cats.elapsed().as_secs_f32()
                );
                cats
            },
        );
        let cats = cats?;
        let mut graph_triples_map = HashMap::new();
        for (graph, pred, subject_type, object_type, triples) in triples_vec? {
            if !graph_triples_map.contains_key(&graph) {
                graph_triples_map.insert(graph.clone(), HashMap::new());
            }
            let pred_map = graph_triples_map.get_mut(&graph).unwrap();
            if !pred_map.contains_key(&pred) {
                pred_map.insert(pred.clone(), HashMap::new());
            }
            let triples_map = pred_map.get_mut(&pred).unwrap();
            triples_map.insert((subject_type, object_type), triples);
        }

        let fts_index = maybe_create_fts_index_map(&triplestore_metadata.indexing)?;
        let geo_index =
            maybe_create_geo_index(&triplestore_metadata.indexing, storage_pathbuf.as_deref())?;
        let mut triplestore = Triplestore {
            graph_triples_map,
            storage_folder: storage_pathbuf,
            storage_mode: storage.mode,
            triples_folder,
            parser_call: 0,
            indexing: triplestore_metadata.indexing,
            fts_index,
            geo_index,
            global_cats: LockedCats::new(cats),
            n_triples_deleted: 0,
            udf_registry: Arc::new(UdfRegistry::new()),
        };
        // The geo index is not serialized, it is rebuilt from the triples.
        triplestore.rebuild_geo_index()?;
        Ok(triplestore)
    }
}

fn write_triples_parquet(
    lf: LazyFrame,
    path: &Path,
    row_group_size: Option<usize>,
) -> Result<(), TripleSerializationError> {
    write_parquet(
        &mut lf.collect()?,
        path,
        ParquetCompression::Snappy,
        row_group_size,
    )?;
    Ok(())
}

fn create_path(path: &Path, s: &str) -> PathBuf {
    let mut new_path = path.to_owned();
    new_path.push(s);
    new_path
}
