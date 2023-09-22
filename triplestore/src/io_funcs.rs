use crate::errors::TriplestoreError;
use std::fs::{create_dir, read_dir, remove_file};
use std::path::Path;

//Based on: https://stackoverflow.com/posts/65573340/revisions
pub(crate) fn delete_tmp_parquets_in_caching_folder(
    caching_folder: &Path,
) -> Result<(), TriplestoreError> {
    let contents = read_dir(caching_folder).map_err(TriplestoreError::ReadCachingDirectoryError)?;
    for f in contents {
        let entry = f.map_err(TriplestoreError::ReadCachingDirectoryEntryError)?;
        let fname = entry.file_name().to_str().unwrap().to_string();
        if fname.starts_with("tmp_") && fname.ends_with(".parquet") {
            remove_file(entry.path()).map_err(TriplestoreError::RemoveParquetFileError)?;
        }
    }
    Ok(())
}

pub(crate) fn create_folder_if_not_exists(path: &Path) -> Result<(), TriplestoreError> {
    if !path.exists() {
        create_dir(path).map_err(TriplestoreError::FolderCreateIOError)?;
    }
    Ok(())
}
