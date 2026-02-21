pub mod in_memory;

use crate::cats::maps::in_memory::CatMapsInMemory;
use crate::cats::CatReEnc;
use crate::BaseRDFNodeType;
use disk::CatMapsOnDisk;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CatMaps {
    InMemory(CatMapsInMemory),
    OnDisk(CatMapsOnDisk),
}

impl CatMaps {
    pub fn inner_join_re_enc(&self, other: &Self) -> Vec<(u32, u32)> {
        match self {
            CatMaps::InMemory(m) => match other {
                CatMaps::InMemory(other_m) => m.inner_join_re_enc(other_m),
                CatMaps::OnDisk(_) => {
                    todo!()
                }
            },
            CatMaps::OnDisk(d) => match other {
                CatMaps::InMemory(m) => d.inner_join_re_enc_in_memory(m.to_record_batch()),
                CatMaps::OnDisk(d) => d.inner_join_re_enc_on_disk(d),
            },
        }
    }
    pub fn merge(&mut self, other: &Self, c: &mut u32) -> CatReEnc {
        match self {
            CatMaps::InMemory(m) => match other {
                CatMaps::InMemory(other) => m.merge(other, c),
                CatMaps::OnDisk(..) => {
                    unreachable!()
                }
            },
            CatMaps::OnDisk(d) => match other {
                CatMaps::InMemory(m) => {
                    let cat_map = d.merge_other_in_memory(m.to_record_batch(), c);
                    let cat_reenc = CatReEnc {
                        cat_map: Arc::new(cat_map),
                    };
                    cat_reenc
                }
                CatMaps::OnDisk(..) => {
                    unreachable!()
                }
            },
        }
    }
    pub fn contains_u32(&self, u: &u32) -> bool {
        match self {
            CatMaps::InMemory(m) => m.contains_u32(u),
            CatMaps::OnDisk(d) => d.contains_u32(u),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            CatMaps::InMemory(m) => m.is_empty(),
            CatMaps::OnDisk(d) => d.is_empty(),
        }
    }
    pub fn maybe_encode_strs(&self, s: &[Option<&str>]) -> Vec<Option<u32>> {
        match self {
            CatMaps::InMemory(m) => m.maybe_encode_strs(s),
            CatMaps::OnDisk(d) => d.maybe_encode_strs(s),
        }
    }

    //
    pub fn encode_all_new_non_duplicated_strings(
        &mut self,
        maybe_new_strings: Vec<String>,
        c: &mut u32,
    ) {
        match self {
            CatMaps::InMemory(m) => {
                m.encode_all_new_non_duplicated_strings(maybe_new_strings, c);
            }
            CatMaps::OnDisk(d) => {
                d.encode_all_new_non_duplicated_strings(maybe_new_strings, c);
            }
        }
    }
    pub fn encode_new_in_memory_string(&mut self, s: String, u: u32) {
        match self {
            CatMaps::InMemory(m) => {
                m.encode_new_in_memory_string(s, u);
            }
            CatMaps::OnDisk(..) => {
                unreachable!("Should never happen")
            }
        }
    }
    pub fn maybe_encode_in_memory_str(&self, s: &str) -> Option<u32> {
        match self {
            CatMaps::InMemory(m) => m.maybe_encode_in_memory_str(s),
            CatMaps::OnDisk(..) => {
                unreachable!("Should never happen")
            }
        }
    }
    pub fn counter(&self) -> u32 {
        match self {
            CatMaps::InMemory(m) => m.counter(),
            CatMaps::OnDisk(d) => d.counter(),
        }
    }
    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        match self {
            CatMaps::InMemory(m) => m.decode_batch(v),
            CatMaps::OnDisk(d) => d.decode_batch(v),
        }
    }
    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<'_, str>> {
        match self {
            CatMaps::InMemory(m) => m.maybe_decode(u),
            CatMaps::OnDisk(d) => d.maybe_decode(u),
        }
    }
    pub fn image(
        &self,
        path: Option<&Path>,
        s: &HashSet<u32>,
        dt: &BaseRDFNodeType,
    ) -> Option<Self> {
        match self {
            CatMaps::InMemory(m) => m.image(s),
            CatMaps::OnDisk(d) => {
                if let Some(path) = path {
                    d.image_disk(path, s).map(CatMaps::OnDisk)
                } else {
                    let im = d.image_memory(s);
                    if let Some(im) = im {
                        let mut inmem = CatMapsInMemory::new_empty(dt);
                        for (s, u) in im {
                            inmem.encode_new_in_memory_string(s, u)
                        }
                        Some(CatMaps::InMemory(inmem))
                    } else {
                        None
                    }
                }
            }
        }
    }
    pub fn new_empty(path: Option<&Path>, bt: &BaseRDFNodeType) -> Self {
        match path {
            None => CatMaps::InMemory(CatMapsInMemory::new_empty(bt)),
            Some(p) => CatMaps::OnDisk(CatMapsOnDisk::new_empty(p)),
        }
    }

    pub fn rank_map(&self, us: HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMaps::InMemory(m) => m.rank_map(us),
            CatMaps::OnDisk(d) => d.rank_map(us),
        }
    }
}
