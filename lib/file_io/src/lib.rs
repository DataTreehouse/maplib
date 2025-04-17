use polars::prelude::{
    DataFrame, LazyFrame, ParallelStrategy, ParquetCompression, ParquetWriter, PolarsError,
    ScanArgsParquet,
};
use std::fs::{create_dir, File};
use std::path::Path;

use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

pub fn create_folder_if_not_exists(path: &Path) -> Result<(), FileIOError> {
    if !path.exists() {
        create_dir(path).map_err(FileIOError::FileCreateIOError)?;
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum FileIOError {
    FileCreateIOError(io::Error),
    WriteParquetError(PolarsError),
    ReadParquetError(PolarsError),
}

impl Display for FileIOError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileIOError::FileCreateIOError(e) => {
                write!(f, "Creating file for writing resulted in an error: {}", e)
            }
            FileIOError::WriteParquetError(e) => {
                write!(f, "Writing to parquet file produced an error {:?}", e)
            }
            FileIOError::ReadParquetError(p) => {
                write!(f, "Reading parquet file resulted in an error: {:?}", p)
            }
        }
    }
}

pub fn property_to_filename(property_name: &str) -> String {
    property_name
        .chars()
        .filter(|x| x.is_alphanumeric())
        .collect()
}

pub fn write_parquet(
    df: &mut DataFrame,
    file_path: &Path,
    compression: ParquetCompression,
) -> Result<(), FileIOError> {
    let file = File::create(file_path).map_err(FileIOError::FileCreateIOError)?;
    let mut writer = ParquetWriter::new(file);
    writer = writer.with_row_group_size(Some(1_000));
    writer = writer.with_compression(compression);
    writer.finish(df).map_err(FileIOError::WriteParquetError)?;
    Ok(())
}

pub fn scan_parquet(file_path: &Path) -> Result<LazyFrame, FileIOError> {
    LazyFrame::scan_parquet(
        file_path,
        ScanArgsParquet {
            n_rows: None,
            cache: false,
            parallel: ParallelStrategy::Auto,
            rechunk: false,
            low_memory: false,
            ..Default::default()
        },
    )
    .map_err(FileIOError::ReadParquetError)
}
