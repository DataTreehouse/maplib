use super::{CatEncs, CatType, Cats};
use crate::cats::maps::CatMaps;
use arrow::array::{AsArray, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use disk::CatMapsOnDisk;
use in_memory::CatMapsInMemory;
use oxrdf::NamedNode;
use polars::prelude::{
    Column, DataFrame, IntoColumn, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter,
    PlSmallStr, PolarsError, SerReader, Series,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum CatSerializationError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error("Malformed cat map file {0}: {1}")]
    MalformedCatMapFile(String, String),
}

const CATS_METADATA_FILE: &str = "metadata.json";
pub const CATS_FOLDER: &str = "cats";
const CAT_MAP_FILE_EXTENSION: &str = "parquet";
const STR_COLUMN: &str = "str";
const CAT_COLUMN: &str = "cat";

fn create_path(path: &Path, s: &str) -> PathBuf {
    let mut pb = path.to_owned();
    pb.push(s);
    pb
}

fn cat_map_parquet_path(cat_path: &Path, id: &str) -> PathBuf {
    create_path(cat_path, &format!("{id}.{CAT_MAP_FILE_EXTENSION}"))
}

#[derive(Serialize, Deserialize)]
pub struct CatsMetadata {
    iri_counter: u32,
    blank_counter: u32,
    literal_counter_map: Vec<(NamedNode, u32)>,
    uuid_cat_type_map: HashMap<String, CatMapOnDiskMetadata>,
}

impl CatsMetadata {
    pub fn new(cats: &Cats, uuid_cat_type_map: HashMap<String, CatMapOnDiskMetadata>) -> Self {
        Self {
            iri_counter: cats.iri_counter,
            blank_counter: cats.blank_counter,
            literal_counter_map: cats
                .literal_counter_map
                .iter()
                .map(|(x, y)| (x.clone(), y.clone()))
                .collect(),
            uuid_cat_type_map,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct CatMapOnDiskMetadata {
    /// Name of the table in the legacy DuckDB format, empty for parquet cat maps.
    name: String,
    cat_type: CatType,
}

impl CatMapOnDiskMetadata {
    pub fn new(name: String, cat_type: CatType) -> Self {
        Self { name, cat_type }
    }
}

impl Cats {
    pub fn deserialize_cats(
        path: &Path,
        storage_folder: Option<&Path>,
    ) -> Result<Self, CatSerializationError> {
        let cat_path = create_path(path, CATS_FOLDER);
        let metadata_file_path = create_path(&cat_path, CATS_METADATA_FILE);

        let cats_metadata: CatsMetadata =
            serde_json::from_slice(&std::fs::read(metadata_file_path)?)?;

        let map: Result<HashMap<CatType, CatEncs>, CatSerializationError> = cats_metadata
            .uuid_cat_type_map
            .par_iter()
            .map(|(id, t)| {
                let is_iri = t.cat_type.as_base_rdf_node_type().is_iri();
                let parquet_path = cat_map_parquet_path(&cat_path, id);
                let maps = if parquet_path.exists() {
                    let df = read_cat_map_parquet(&parquet_path)?;
                    if let Some(storage_folder) = storage_folder {
                        let dest_path = create_path(storage_folder, id);
                        let rb = data_frame_to_record_batch(&df, &parquet_path)?;
                        CatMaps::OnDisk(CatMapsOnDisk::from_in_memory(&dest_path, rb))
                    } else {
                        CatMaps::InMemory(data_frame_to_in_memory(&df, is_iri, &parquet_path)?)
                    }
                } else {
                    // Raw DuckDB database (`raw_db`, and the format of earlier versions).
                    let src_path = create_path(&cat_path, id);
                    if let Some(storage_folder) = storage_folder {
                        let dest_path = create_path(storage_folder, id);
                        CatMaps::OnDisk(CatMapsOnDisk::new(
                            &src_path,
                            t.name.clone(),
                            Some(&dest_path),
                        ))
                    } else {
                        let cmod = CatMapsOnDisk::new(&src_path, t.name.clone(), None);
                        CatMaps::InMemory(cmod.to_memory(is_iri))
                    }
                };
                Ok((t.cat_type.clone(), CatEncs { maps }))
            })
            .collect();
        let cats = Cats::from_map(map?, storage_folder);
        Ok(cats)
    }

    /// Serializes the cat maps as parquet files, or with `raw_db` as DuckDB databases (the format
    /// of earlier versions, requires maplib Enterprise Edition). Deserializing raw DuckDB cat maps
    /// into hybrid or disk storage uses the databases in place, which is much faster than building
    /// them from parquet files.
    pub fn serialize_cats(&self, path: &Path, raw_db: bool) -> Result<(), CatSerializationError> {
        let cat_path = create_path(path, CATS_FOLDER);
        create_dir_all(&cat_path)?;

        let map: Result<HashMap<String, CatMapOnDiskMetadata>, CatSerializationError> = self
            .cat_map
            .par_iter()
            .map(|(t, e)| {
                let path_uuid = Uuid::new_v4().to_string();
                // Rows are in string order, which parquet deserialization relies on.
                let rb = match &e.maps {
                    CatMaps::InMemory(im) => im.to_record_batch(),
                    CatMaps::OnDisk(od) => od
                        .to_memory(t.as_base_rdf_node_type().is_iri())
                        .to_record_batch(),
                };
                let name = if raw_db {
                    let db_path = create_path(&cat_path, &path_uuid);
                    CatMapsOnDisk::from_in_memory(&db_path, rb)
                        .get_name()
                        .to_string()
                } else {
                    let mut df = record_batch_to_data_frame(&rb)?;
                    drop(rb);
                    write_cat_map_parquet(&mut df, &cat_map_parquet_path(&cat_path, &path_uuid))?;
                    String::new()
                };
                Ok((path_uuid, CatMapOnDiskMetadata::new(name, t.clone())))
            })
            .collect();
        let cats_metadata = CatsMetadata::new(&self, map?);
        let cats_string = serde_json::to_string_pretty(&cats_metadata)?;
        let metadata_file_path = create_path(&cat_path, CATS_METADATA_FILE);
        let mut f = File::create(metadata_file_path)?;
        write!(f, "{}", cats_string)?;

        Ok(())
    }
}

fn write_cat_map_parquet(df: &mut DataFrame, path: &Path) -> Result<(), CatSerializationError> {
    let file = BufWriter::new(File::create(path)?);
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .set_parallel(true)
        .finish(df)?;
    Ok(())
}

fn read_cat_map_parquet(path: &Path) -> Result<DataFrame, CatSerializationError> {
    let file = File::open(path)?;
    Ok(ParquetReader::new(file).finish()?)
}

fn record_batch_to_data_frame(rb: &RecordBatch) -> Result<DataFrame, CatSerializationError> {
    let strs = rb.column(0).as_string::<i32>();
    let cats: &UInt32Array = rb.column(1).as_primitive();
    let str_ser = Series::new(
        PlSmallStr::from_static(STR_COLUMN),
        strs.iter().map(|s| s.unwrap()).collect::<Vec<_>>(),
    );
    let cat_ser = Series::new(PlSmallStr::from_static(CAT_COLUMN), cats.values().as_ref());
    let columns: Vec<Column> = vec![str_ser.into_column(), cat_ser.into_column()];
    Ok(DataFrame::new(rb.num_rows(), columns)?)
}

fn malformed(path: &Path, e: impl ToString) -> CatSerializationError {
    CatSerializationError::MalformedCatMapFile(path.to_string_lossy().to_string(), e.to_string())
}

fn data_frame_to_in_memory(
    df: &DataFrame,
    is_iri: bool,
    path: &Path,
) -> Result<CatMapsInMemory, CatSerializationError> {
    let strs = df
        .column(STR_COLUMN)?
        .str()
        .map_err(|e| malformed(path, e))?;
    let cats = df
        .column(CAT_COLUMN)?
        .u32()
        .map_err(|e| malformed(path, e))?;
    let ss: Vec<(Cow<str>, u32)> = strs
        .iter()
        .map(|s| s.expect("cat map strings are not null"))
        .zip(cats.into_no_null_iter())
        .map(|(s, u)| (Cow::Borrowed(s), u))
        .collect();
    Ok(CatMapsInMemory::from_ordered_vec(ss, is_iri))
}

fn data_frame_to_record_batch(
    df: &DataFrame,
    path: &Path,
) -> Result<RecordBatch, CatSerializationError> {
    let strs = df
        .column(STR_COLUMN)?
        .str()
        .map_err(|e| malformed(path, e))?;
    let cats = df
        .column(CAT_COLUMN)?
        .u32()
        .map_err(|e| malformed(path, e))?;
    let strs = StringArray::from_iter(strs.iter());
    let cats = UInt32Array::from_iter_values(cats.into_no_null_iter());
    let schema = Schema::new(vec![
        Field::new(STR_COLUMN, DataType::Utf8, true),
        Field::new(CAT_COLUMN, DataType::UInt32, true),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(cats)])
        .map_err(|e| malformed(path, e))
}
