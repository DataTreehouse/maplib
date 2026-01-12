use std::borrow::Cow;
use crate::cats::maps::in_memory::CatMapsInMemory;
use crate::cats::CatReEnc;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct CatMapsOnDisk {
    map: (),
    rev_map: (),
}

impl CatMapsOnDisk {
    pub fn new_remap(disk: &CatMapsOnDisk, c: &mut u32, path: &Path) -> (CatMapsOnDisk, CatReEnc) {
        todo!()
    }

    // This one is used in an image function, wonder if it is better if the image function is batched
    // This means moving parts of the existing function into mem and reimplementing that for disk
    pub fn contains_u32(&self, p0: &u32) -> bool {
        todo!()
    }

    // This one is only used when encoding predicates, perhaps also something that should be done in batch as above with contains_u32
    // Perhaps calling code can be rewritten using batched method?
    pub fn contains_str(&self, p0: &str) -> bool {
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        todo!()
    }

    pub fn new_singular(s: &str, u: u32, p: &Path) -> CatMapsInMemory {
        todo!()
    }

    pub fn encode_new_string(&self, s: String, u: u32) {
        todo!()
    }
    pub fn maybe_encode_str(&self, s: &str) -> Option<&u32> {
        todo!()
    }
    pub fn new_empty(path: &Path) -> CatMapsOnDisk {
        todo!()
    }

    pub fn counter(&self) -> u32 {
        todo!()
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<str>> {
        todo!()
    }

    pub fn decode_batch(&self, us: &[Option<u32>]) -> Vec<Option<Cow<str>>> {
        todo!()
    }

    pub fn image(&self, s: &HashSet<u32>) -> Option<CatMapsOnDisk> {
        todo!()
    }

    pub fn merge(&mut self, other: &CatMapsOnDisk, c: &mut u32) -> CatReEnc {
        todo!()
    }

    pub fn inner_join_re_enc(&self, p0: &CatMapsOnDisk) -> Vec<(u32, u32)> {
        todo!()
    }
}
