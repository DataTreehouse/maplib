use polars::prelude::{
    DataFrame, LazyFrame, ParallelStrategy, ParquetWriter, PolarsError, ScanArgsParquet,
};
use std::fs::File;
use std::path::Path;

use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParquetIOError {
    FileCreateIOError(io::Error),
    WriteParquetError(PolarsError),
    ReadParquetError(PolarsError),
}

impl Display for ParquetIOError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetIOError::FileCreateIOError(e) => {
                write!(f, "Creating file for writing resulted in an error: {}", e)
            }
            ParquetIOError::WriteParquetError(e) => {
                write!(f, "Writing to parquet file produced an error {:?}", e)
            }
            ParquetIOError::ReadParquetError(p) => {
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

pub fn write_parquet(df: &mut DataFrame, file_path: &Path) -> Result<(), ParquetIOError> {
    let file = File::create(file_path).map_err(ParquetIOError::FileCreateIOError)?;
    let mut writer = ParquetWriter::new(file);
    writer = writer.with_row_group_size(Some(1_000));
    writer
        .finish(df)
        .map_err(ParquetIOError::WriteParquetError)?;
    Ok(())
}

pub fn scan_parquet(file_path: &String) -> Result<LazyFrame, ParquetIOError> {
    LazyFrame::scan_parquet(
        Path::new(file_path),
        ScanArgsParquet {
            n_rows: None,
            cache: false,
            parallel: ParallelStrategy::Auto,
            rechunk: false,
            low_memory: false,
            ..Default::default()
        },
    )
    .map_err(ParquetIOError::ReadParquetError)
}
