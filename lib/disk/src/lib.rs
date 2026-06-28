use arrow::array::RecordBatch;
use in_memory::CatMapsInMemory;
use nohash_hasher::NoHashHasher;
use range_set_blaze::RangeSetBlaze;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct CatMapsOnDisk {}

impl CatMapsOnDisk {
    pub fn new(_: &Path, _: String) -> Self {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn get_name(&self) -> &str {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn from_in_memory(_: &Path, _: RecordBatch) -> Self {
        unimplemented!("Contact Data Treehouse to try!")
    }
    pub fn garbage_collect_cats(&self, _: RangeSetBlaze<u32>) {
        unimplemented!("Contact Data Treehouse to try!")
    }
    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        unimplemented!("Contact Data Treehouse to try!")
    }
    pub fn add_encs_to_cache(&mut self, _us: &HashSet<u32>, _is_iri: bool) {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn inner_join_re_enc_on_disk(&self, _other: &CatMapsOnDisk) -> Vec<(u32, u32)> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn inner_join_re_enc_in_memory(&self, _rb: RecordBatch) -> Vec<(u32, u32)> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn merge_other_in_memory(
        &mut self,
        _: RecordBatch,
        _: &mut u32,
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

    pub fn encode_all_new_non_duplicated_strings(&mut self, _: Vec<String>, _: &mut u32) {
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

    pub fn local_rank_map(&self, _us: &HashSet<u32>) -> HashMap<u32, u32> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn global_rank_map(&self, _us: &HashSet<u32>) -> HashMap<u32, u32> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn image_disk(&self, _path: &Path, _us: &HashSet<u32>) -> Option<CatMapsOnDisk> {
        unimplemented!("Contact Data Treehouse to try!")
    }
    pub fn image_memory(&self, _us: &HashSet<u32>, _is_iri: bool) -> Option<CatMapsInMemory> {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn new_empty(_path: &Path) -> CatMapsOnDisk {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn compact(&self) {
        unimplemented!("Contact Data Treehouse to try!")
    }

    pub fn to_memory(&self, _: bool) -> CatMapsInMemory {
        unimplemented!("Contact Data Treehouse to try!")
    }
}
