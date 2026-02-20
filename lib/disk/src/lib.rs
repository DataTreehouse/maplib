use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::path::Path;
use duckdb::arrow::array::RecordBatch;
use nohash_hasher::NoHashHasher;

#[derive(Clone, Debug)]
pub struct CatMapsOnDisk {}

impl CatMapsOnDisk {
    pub fn inner_join_re_enc_on_disk(&self, _other: &CatMapsOnDisk) -> Vec<(u32, u32)> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn inner_join_re_enc_in_memory(&self, _rb: RecordBatch) -> Vec<(u32, u32)> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn merge_other_in_memory(
        &mut self,
        _rb: RecordBatch,
        _c: &mut u32,
    ) -> HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn contains_u32(&self, _u: &u32) -> bool {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn is_empty(&self) -> bool {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn maybe_encode_strs(&self, _s: &[Option<&str>]) -> Vec<Option<u32>> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn encode_all_new_non_duplicated_strings(
        &mut self,
        _maybe_new_strings: Vec<String>,
        _c: &mut u32,
    ) {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn counter(&self) -> u32 {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn decode_batch(&self, _us: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn maybe_decode(&self, _u: &u32) -> Option<Cow<'_, str>> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn rank_map(&self, _us: HashSet<u32>) -> HashMap<u32, u32> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn image_disk(&self, _path: &Path, _us: &HashSet<u32>) -> Option<CatMapsOnDisk> {
        unimplemented!("Contact Data Treehouse to try!")
    }
    pub fn image_memory(&self, _us: &HashSet<u32>) -> Option<HashMap<String, u32>> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn new_empty(_path: &Path) -> CatMapsOnDisk {
        unimplemented!("Contact Data Treehouse to try!")
    }
}
