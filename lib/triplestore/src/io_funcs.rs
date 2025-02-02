use crate::errors::TriplestoreError;
use std::fs::create_dir;
use std::path::Path;

pub(crate) fn create_folder_if_not_exists(path: &Path) -> Result<(), TriplestoreError> {
    if !path.exists() {
        create_dir(path).map_err(TriplestoreError::FolderCreateIOError)?;
    }
    Ok(())
}
