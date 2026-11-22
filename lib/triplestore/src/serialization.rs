use super::{
    maybe_create_fts_index_map, maybe_create_geo_index, prepare_storage, IndexingOptions,
    StorageOptions, Triplestore,
};
use crate::graph_map::GraphTriplesMap;
use crate::query_cache::QueryCache;
use crate::errors::TriplestoreError;
use crate::storage::{segment_filter_bits, PredicateTriples, Triples, DISK_ROW_GROUP_SIZE};
use external_sparql::federation::FederationSettings;
use file_io::remote::{RemoteLocation, RemoteOptions, TempFile};
use file_io::FileIOError;
use oxrdf::NamedNode;
use polars::prelude::{IntoLazy, ParquetCompression, PolarsError};
use query_processing::udf::UdfRegistry;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::bundle::{
    BundleWriter, RemoteFolderWriter, SerializationSource, SerializationTarget,
};
use fts::{FtsIndex, FtsOptions, FtsSerializedState};
use representation::cats::serialization::{
    CatMapsOnDisk, CatSerializationError, DeserializeDbHook, SerializeDbHook, CATS_FOLDER,
};
use representation::cats::{CatType, Cats, LockedCats};
use representation::dataset::NamedGraph;
use representation::constants::normalize_legacy_chrontext_named_node;
use representation::BaseRDFNodeType;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
/// Bloom filters of the subject and object codes (see `utils::bloom`). Serializations without
/// them are read with no filters, which lets every lookup through.
const SO_FILTER_FILE: &str = "so_filter.bin";
const OS_FILTER_FILE: &str = "os_filter.bin";
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
    /// The number of triples of each table, so that tables in object storage need not be
    /// downloaded to be opened. Missing in serializations of earlier versions.
    #[serde(default)]
    heights: HashMap<String, usize>,
    /// The full text search indexes, when they are serialized (with `raw_db`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fts: Option<FtsMetadata>,
}

/// The full text search indexes of a serialization with `raw_db`. Their tables are in the raw
/// databases of the cat maps that they refer to, and deserializing uses them there (or reads
/// them into memory) instead of indexing the strings again. Other serializations have no full
/// text search indexes, and one is made with `create_index` once they are deserialized.
#[derive(Serialize, Deserialize)]
struct FtsMetadata {
    predicates: Option<Vec<NamedNode>>,
    local_names: bool,
    iris: bool,
    /// The id of the index of each graph, and its predicates in the order of the ids that its
    /// tables refer to them by.
    indexes: Vec<(NamedGraph, String, Vec<NamedNode>)>,
}

impl Triplestore {
    /// Serializes the triplestore to `path`: with `single_file` a single file bundle (see
    /// [`representation::bundle`]), otherwise a folder. The triple tables are parquet files, and so are the cat
    /// maps unless `raw_db` is set, which writes them as DuckDB databases.
    pub fn serialize_triples(
        &mut self,
        path: &Path,
        raw_db: bool,
        single_file: bool,
    ) -> Result<(), TripleSerializationError> {
        if !single_file {
            return self.serialize_triples_folder(path, raw_db);
        }
        if path.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!(
                    "{} is a folder, serialize it with single_file=False or choose a file path",
                    path.display()
                ),
            )
            .into());
        }
        let file_name = path.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{} is not a file path", path.display()),
            )
        })?;
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        // The bundle is written next to the target, so that renaming it into place stays on the
        // same file system and never leaves a partial file at `path`. DuckDB databases are made
        // in a scratch folder there before they are added.
        let id = Uuid::new_v4();
        let file_name = file_name.to_string_lossy();
        let scratch_dir = parent.join(format!(".{file_name}.{id}.tmp"));
        let tmp_file = parent.join(format!(".{file_name}.{id}.partial"));
        let res = BundleWriter::create(&tmp_file, scratch_dir.clone())
            .map_err(TripleSerializationError::from)
            .and_then(|w| {
                let target = SerializationTarget::Bundle(w);
                self.serialize_triples_to(&target, raw_db)?;
                Ok(target.finish()?)
            })
            .and_then(|_| Ok(std::fs::rename(&tmp_file, path)?));
        if scratch_dir.exists() {
            let _ = std::fs::remove_dir_all(&scratch_dir);
        }
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp_file);
        }
        res
    }

    fn serialize_triples_folder(
        &mut self,
        path: &Path,
        raw_db: bool,
    ) -> Result<(), TripleSerializationError> {
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
        self.serialize_triples_to(&SerializationTarget::Folder(path.to_owned()), raw_db)
    }

    fn serialize_triples_to(
        &mut self,
        target: &SerializationTarget,
        raw_db: bool,
    ) -> Result<(), TripleSerializationError> {
        self.garbage_collect()?;
        self.compact()?;

        let mut to_serialize = vec![];
        let mut filters = vec![];
        let mut triples_uuid_map = HashMap::new();
        let mut heights = HashMap::new();

        for (graph, map) in self.graph_triples_map.iter() {
            for (pred, triples_map) in map {
                for ((subject_type, object_type), triples) in triples_map {
                    let value = (
                        graph.clone(),
                        pred.clone(),
                        subject_type.clone(),
                        object_type.clone(),
                    );
                    let key = Uuid::new_v4().to_string();
                    let rel = |name: &str| format!("{TRIPLES_PATH}/{key}/{name}");
                    let pb_subjects = rel(SO_TRIPLES_FILE);
                    let pb_subjects_index = rel(SO_SPARSE_INDEX_FILE);
                    let pb_objects = rel(OS_TRIPLES_FILE);
                    let pb_objects_index = rel(OS_SPARSE_INDEX_FILE);
                    for (name, by_object) in [(SO_FILTER_FILE, false), (OS_FILTER_FILE, true)] {
                        if let Some(bytes) = triples.code_filter_bytes(by_object) {
                            filters.push((rel(name), bytes));
                        }
                    }
                    heights.insert(key.clone(), triples.len());
                    triples_uuid_map.insert(key, value);

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
                        to_serialize.push((
                            pb_objects,
                            object_subject_sort.pop().unwrap(),
                            TRIPLES_ROW_GROUP_SIZE,
                        ));

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

        // Compaction has rebuilt the full text search indexes, so they have exactly the triples
        // that are serialized, with the cats that are serialized.
        let fts_indexes: Vec<(&NamedGraph, &FtsIndex)> = if raw_db {
            self.fts_index.iter().collect()
        } else {
            vec![]
        };
        let fts = (!fts_indexes.is_empty()).then(|| FtsMetadata {
            predicates: self.indexing.fts_predicates.clone(),
            local_names: self.indexing.fts_local_names,
            iris: self.indexing.fts_iris,
            indexes: fts_indexes
                .iter()
                .map(|(graph, index)| {
                    let state = index.serialized_state();
                    ((*graph).clone(), state.id, state.predicates)
                })
                .collect(),
        });
        let fts_hook = |t: &CatType, cats: &Cats, db: &mut CatMapsOnDisk| -> Result<(), String> {
            for (_, index) in &fts_indexes {
                index.serialize_into(t, cats, db).map_err(|e| e.to_string())?;
            }
            Ok(())
        };
        let fts_hook: SerializeDbHook = &fts_hook;

        let triplestore_metadata = TriplestoreMetadata {
            triples_uuid_map,
            indexing: self.indexing.clone(),
            heights,
            fts,
        };

        let res: Result<(), TripleSerializationError> = to_serialize
            .into_par_iter()
            .map(|(rel, triples, row_group_size)| {
                target.write_parquet(
                    &rel,
                    &mut triples.collect()?,
                    ParquetCompression::Snappy,
                    row_group_size,
                )?;
                Ok(())
            })
            .collect();
        res?;
        for (rel, bytes) in filters {
            target.write_bytes(&rel, &bytes)?;
        }

        self.global_cats
            .read()
            .expect("Should always be readable")
            .serialize_cats(
                target,
                raw_db,
                triplestore_metadata.fts.is_some().then_some(fts_hook),
            )?;

        // The metadata is written last, so that a folder with metadata has all its files.
        let triplestore_metadata_string = serde_json::to_string_pretty(&triplestore_metadata)?;
        target.write_bytes(
            &format!("{TRIPLES_PATH}/{METADATA_FILE}"),
            triplestore_metadata_string.as_bytes(),
        )?;

        Ok(())
    }

    /// Serializes the triplestore to object storage at `location`. With `single_file`, the
    /// bundle is written to a temporary file in `scratch_parent` (by default the system
    /// temporary folder) and uploaded; the object appears whole when the upload completes.
    /// Otherwise a serialization folder is written to a new generation and published, see
    /// [`RemoteFolderWriter`]. Deserializing a folder into disk storage only downloads the
    /// triple tables when they are read.
    pub fn serialize_triples_remote(
        &mut self,
        location: &RemoteLocation,
        raw_db: bool,
        single_file: bool,
        scratch_parent: Option<&Path>,
    ) -> Result<(), TripleSerializationError> {
        let scratch_parent = scratch_parent
            .map(|p| p.to_owned())
            .or_else(|| self.storage_folder.clone());
        if single_file {
            let name = location.file_name().unwrap_or("model.maplib").to_string();
            let tmp = TempFile::new(scratch_parent.as_deref(), &name)?;
            self.serialize_triples(tmp.path(), raw_db, true)?;
            location
                .upload_from(tmp.path())
                .map_err(std::io::Error::from)?;
            return Ok(());
        }
        let writer = RemoteFolderWriter::create(location.clone(), scratch_parent.as_deref())?;
        let target = SerializationTarget::Remote(writer);
        match self.serialize_triples_to(&target, raw_db) {
            Ok(()) => Ok(target.finish()?),
            Err(e) => {
                target.abort();
                Err(e)
            }
        }
    }

    /// Deserializes a triplestore serialized to object storage at `url`, a single file bundle
    /// or a folder, see [`SerializationSource::open_remote`].
    pub fn deserialize_triples_remote(
        url: &str,
        options: &RemoteOptions,
        storage: StorageOptions,
    ) -> Result<Triplestore, TripleSerializationError> {
        let (storage_pathbuf, triples_folder) = prepare_storage(&storage)?;
        // A bundle is downloaded next to the storage, where there is room for it.
        let source = SerializationSource::open_remote(url, options, storage_pathbuf.as_deref())?;
        Self::deserialize_triples_from(source, storage, storage_pathbuf, triples_folder)
    }

    /// Deserializes a triplestore serialized to `path`, which is either a single file bundle or a
    /// folder.
    pub fn deserialize_triples(
        path: &Path,
        storage: StorageOptions,
    ) -> Result<Triplestore, TripleSerializationError> {
        let source = SerializationSource::open(path)?;
        let (storage_pathbuf, triples_folder) = prepare_storage(&storage)?;
        Self::deserialize_triples_from(source, storage, storage_pathbuf, triples_folder)
    }

    fn deserialize_triples_from(
        source: SerializationSource,
        storage: StorageOptions,
        storage_pathbuf: Option<PathBuf>,
        triples_folder: Option<PathBuf>,
    ) -> Result<Triplestore, TripleSerializationError> {
        let metadata_bytes = source
            .file(&format!("{TRIPLES_PATH}/{METADATA_FILE}"))
            .read()?;
        let triplestore_metadata: TriplestoreMetadata = serde_json::from_slice(&metadata_bytes)?;
        let heights = &triplestore_metadata.heights;

        // Serialized full text search indexes are loaded from the raw databases of the cat maps
        // as these are deserialized.
        let mut fts_indexes = vec![];
        if let Some(fts) = &triplestore_metadata.fts {
            let options = FtsOptions {
                predicates: fts.predicates.clone(),
                local_names: fts.local_names,
                iris: fts.iris,
            };
            for (graph, id, predicates) in &fts.indexes {
                let state = FtsSerializedState {
                    id: id.clone(),
                    predicates: predicates.clone(),
                };
                let index = FtsIndex::from_serialized(options.clone(), state)
                    .map_err(TriplestoreError::FtsError)?;
                fts_indexes.push((graph.clone(), index));
            }
        }
        let fts_hook = |t: &CatType, db: &CatMapsOnDisk, in_place: bool| -> Result<(), String> {
            for (_, index) in &fts_indexes {
                index
                    .load_serialized(t, db, in_place)
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        };
        let fts_hook: DeserializeDbHook = &fts_hook;
        let fts_hook = (!fts_indexes.is_empty()).then_some(fts_hook);

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
                        let file =
                            |name: &str| source.file(&format!("{TRIPLES_PATH}/{uuid}/{name}"));
                        let subject_object_file = file(SO_TRIPLES_FILE);
                        let object_subject_file = file(OS_TRIPLES_FILE);

                        let subject_object_index_df = file(SO_SPARSE_INDEX_FILE).read_parquet()?;
                        let (object_subject_file, object_subject_index_df) =
                            if object_subject_file.exists() {
                                let object_subject_index_df =
                                    file(OS_SPARSE_INDEX_FILE).read_parquet()?;
                                (Some(object_subject_file), Some(object_subject_index_df))
                            } else {
                                (None, None)
                            };
                        // The filters are small and read now, so that lookups of absent codes in
                        // tables in object storage need not download the tables.
                        let read_filter = |name: &str| -> Result<_, TripleSerializationError> {
                            let f = file(name);
                            if segment_filter_bits() == 0 || !f.exists() {
                                return Ok(None);
                            }
                            Ok(utils::bloom::SplitBlockBloom::from_bytes(&f.read()?))
                        };
                        let subject_filter = read_filter(SO_FILTER_FILE)?;
                        let object_filter = if object_subject_file.is_some() {
                            read_filter(OS_FILTER_FILE)?
                        } else {
                            None
                        };
                        let triples = Triples::from_serialization(
                            subject_object_file,
                            subject_object_index_df,
                            object_subject_file,
                            object_subject_index_df,
                            subject_type.clone(),
                            object_type.clone(),
                            triples_folder.as_deref(),
                            heights.get(&uuid).copied(),
                            subject_filter,
                            object_filter,
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
                let cats = Cats::deserialize_cats(&source, storage_folder, fts_hook);
                debug!(
                    "Deserialized cats in {} seconds",
                    start_deserialize_cats.elapsed().as_secs_f32()
                );
                cats
            },
        );
        let mut cats = cats?;
        let mut graph_triples_map: HashMap<NamedGraph, PredicateTriples> = HashMap::new();
        // Predicates renamed below (from the chrontext namespace) may never have been encoded.
        let mut renamed_predicates = HashSet::new();
        for (graph, pred, subject_type, object_type, triples) in triples_vec? {
            // Stored predicates in the former chrontext namespace are read as the predicates of
            // the maplib vocabulary, unless a stored table already has that predicate.
            let pred = {
                let new_pred = normalize_legacy_chrontext_named_node(pred.clone());
                let taken = graph_triples_map
                    .get(&graph)
                    .and_then(|m| m.get(&new_pred))
                    .is_some_and(|m| m.contains_key(&(subject_type.clone(), object_type.clone())));
                if taken {
                    pred
                } else {
                    if new_pred != pred {
                        renamed_predicates.insert(new_pred.as_str().to_string());
                    }
                    new_pred
                }
            };
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

        cats.encode_iris(renamed_predicates);
        let mut indexing = triplestore_metadata.indexing;
        let fts_index = if let Some(fts) = triplestore_metadata.fts {
            // The options of full text search are otherwise not serialized.
            indexing.fts = true;
            indexing.fts_predicates = fts.predicates;
            indexing.fts_local_names = fts.local_names;
            indexing.fts_iris = fts.iris;
            fts_indexes.into_iter().collect()
        } else {
            maybe_create_fts_index_map(&indexing)?
        };
        let geo_index = maybe_create_geo_index(&indexing, storage_pathbuf.as_deref())?;
        let query_cache = QueryCache::new();
        let mut triplestore = Triplestore {
            graph_triples_map: GraphTriplesMap::new(graph_triples_map, query_cache.clone()),
            query_cache,
            storage_folder: storage_pathbuf,
            storage_mode: storage.mode,
            triples_folder,
            parser_call: 0,
            indexing,
            fts_index,
            geo_index,
            vector_index: HashMap::new(),
            global_cats: LockedCats::new(cats),
            n_triples_deleted: 0,
            distinct_count_cache: Default::default(),
            path_graph_cache: Default::default(),
            stats_cache: Default::default(),
            udf_registry: Arc::new(UdfRegistry::new()),
            federation: Arc::new(FederationSettings::default()),
        };
        // The geo index is not serialized, it is rebuilt from the triples.
        triplestore.rebuild_geo_index()?;
        triplestore.rebuild_vector_indexes()?;
        Ok(triplestore)
    }
}
