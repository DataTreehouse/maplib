use super::{CatEncs, CatType, Cats};
use crate::cats::maps::CatMaps;
use disk::CatMapsOnDisk;
use oxrdf::NamedNode;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum CatSerializationError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
}

const CATS_METADATA_FILE: &str = "metadata.json";
pub const CATS_FOLDER: &str = "cats";

fn create_path(path: &Path, s: &str) -> PathBuf {
    let mut pb = path.to_owned();
    pb.push(s);
    pb
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
            serde_json::from_slice(&std::fs::read(metadata_file_path).unwrap()).unwrap();

        let map = if let Some(storage_folder) = storage_folder {
            let r: Result<Vec<_>, CatSerializationError> = cats_metadata
                .uuid_cat_type_map
                .par_iter()
                .map(|(id, ..)| {
                    // Remove the source base path to maintain directory structure
                    let src_path = create_path(&cat_path, id);
                    let dest_path = create_path(storage_folder, id);

                    // Perform the copy
                    fs::copy(&src_path, &dest_path)
                        .map_err(|x| CatSerializationError::IOError(x))?;
                    Ok(())
                })
                .collect();
            r?;
            let map: HashMap<CatType, CatEncs> = cats_metadata
                .uuid_cat_type_map
                .par_iter()
                .map(|(id, t)| {
                    let cat_enc_path = create_path(storage_folder, id);
                    let cmod = CatMapsOnDisk::new(&cat_enc_path, t.name.clone());
                    let cm = CatMaps::OnDisk(cmod);
                    let ce = CatEncs { maps: cm };
                    (t.cat_type.clone(), ce)
                })
                .collect();
            map
        } else {
            let map: HashMap<CatType, CatEncs> = cats_metadata
                .uuid_cat_type_map
                .par_iter()
                .map(|(id, t)| {
                    let cat_enc_path = create_path(&cat_path, id);
                    let cmod = CatMapsOnDisk::new(&cat_enc_path, t.name.clone());
                    let cmim = cmod.to_memory(t.cat_type.as_base_rdf_node_type().is_iri());
                    let cm = CatMaps::InMemory(cmim);
                    let ce = CatEncs { maps: cm };
                    (t.cat_type.clone(), ce)
                })
                .collect();
            map
        };
        let cats = Cats::from_map(map, storage_folder);
        Ok(cats)
    }

    pub fn serialize_cats(&self, path: &Path) -> Result<(), CatSerializationError> {
        let cat_path = create_path(path, CATS_FOLDER);
        create_dir_all(&cat_path)?;

        let map: HashMap<String, CatMapOnDiskMetadata> = self
            .cat_map
            .par_iter()
            .map(|(t, e)| {
                let path_uuid = Uuid::new_v4().to_string();
                let mut this_cat_path = cat_path.clone();
                this_cat_path.push(path_uuid.clone());
                let disk_encs = if let CatMaps::InMemory(im) = &e.maps {
                    CatMapsOnDisk::from_in_memory(&this_cat_path, im.to_record_batch())
                } else {
                    todo!()
                };
                (
                    path_uuid,
                    CatMapOnDiskMetadata::new(disk_encs.get_name().to_string(), t.clone()),
                )
            })
            .collect();
        let cats_metadata = CatsMetadata::new(&self, map);
        let cats_string = serde_json::to_string_pretty(&cats_metadata)?;
        let metadata_file_path = create_path(&cat_path, CATS_METADATA_FILE);
        let mut f = File::create(metadata_file_path)?;
        write!(f, "{}", cats_string)?;

        Ok(())
    }
}
