use polars::prelude::{
    DataFrame, LazyFrame, ParallelStrategy, ParquetWriter, PolarsError, ScanArgsParquet,
};
use std::cmp::min;
use std::fs::File;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

const PARQUET_DF_SIZE: usize = 50_000_000;

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
            rechunk: true,
            low_memory: false,
            ..Default::default()
        },
    )
    .map_err(ParquetIOError::ReadParquetError)
}

pub fn split_write_tmp_df(
    caching_folder: &str,
    df: DataFrame,
    predicate: &str,
) -> Result<Vec<String>, ParquetIOError> {
    let n_of_size = (df.estimated_size() / PARQUET_DF_SIZE) + 1;
    let chunk_size = df.height() / n_of_size;
    let mut offset = 0i64;
    let mut paths = vec![];
    loop {
        let to_row = min(df.height(), offset as usize + chunk_size);
        let mut df_slice = df.slice_par(offset, to_row);
        let file_name = format!("tmp_{}_{}.parquet", predicate, Uuid::new_v4());
        let path_buf: PathBuf = [caching_folder, &file_name].iter().collect();
        let path = path_buf.as_path();
        write_parquet(&mut df_slice, path)?;
        paths.push(path.to_str().unwrap().to_string());
        offset += chunk_size as i64;
        if offset >= df.height() as i64 {
            break;
        }
    }
    Ok(paths)
}
