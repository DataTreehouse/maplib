pub mod in_memory;
pub mod on_disk;

use crate::cats::maps::in_memory::CatMapsInMemory;
use crate::cats::maps::on_disk::CatMapsOnDisk;
use crate::cats::CatReEnc;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CatMaps {
    InMemory(CatMapsInMemory),
    OnDisk(CatMapsOnDisk),
}

impl CatMaps {
    pub(crate) fn new_remap(maps: &CatMaps, path: Option<&Path>) -> (CatMaps, CatReEnc) {
        if let Some(path) = path {
            todo!()
        } else {
            if let CatMaps::InMemory(mem) = maps {
                let (mem, re_enc) = CatMapsInMemory::new_remap(mem);
                (CatMaps::InMemory(mem), re_enc)
            } else {
                unreachable!("Should never happen")
            }
        }
    }

    pub(crate) fn inner_join_re_enc(&self, other: &CatMaps) -> Vec<(u32, u32)> {
        match self {
            CatMaps::InMemory(mem) => {
                if let CatMaps::InMemory(other_mem) = other {
                    mem.inner_join_re_enc(other_mem)
                } else {
                    unreachable!("Should never happen")
                }
            }
            CatMaps::OnDisk(_) => {
                todo!()
            }
        }
    }

    pub fn merge(&mut self, other: &CatMaps) -> CatReEnc {
        match self {
            CatMaps::InMemory(mem) => {
                if let CatMaps::InMemory(other_mem) = other {
                    mem.merge(other_mem)
                } else {
                    unreachable!("Should never happen")
                }
            }
            CatMaps::OnDisk(_) => {
                todo!()
            }
        }
    }

    pub fn contains_str(&self, s: &str) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.contains_str(s),
            CatMaps::OnDisk(disk) => {
                todo!()
            }
        }
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.contains_u32(u),
            CatMaps::OnDisk(disk) => {
                todo!()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.is_empty(),
            CatMaps::OnDisk(_) => {
                todo!()
            }
        }
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        match self {
            CatMaps::InMemory(mem) => mem.encode_new_string(s, u),
            CatMaps::OnDisk(disk) => {
                todo!()
            }
        }
    }

    pub fn maybe_encode_string(&self, s: &str) -> Option<&u32> {
        match self {
            CatMaps::InMemory(mem) => mem.maybe_encode_string(s),
            CatMaps::OnDisk(_) => {
                todo!()
            }
        }
    }

    pub fn new_singular(value: &str, u: u32, path: Option<&Path>) -> CatMaps {
        if let Some(path) = path {
            todo!()
        } else {
            CatMaps::InMemory(CatMapsInMemory::new_singular(value, u))
        }
    }

    pub fn new_empty(path: Option<&Path>) -> CatMaps {
        if let Some(path) = path {
            todo!()
        } else {
            CatMaps::InMemory(CatMapsInMemory::new_empty())
        }
    }

    fn batch_decode(&self, keys: &[u32]) -> Vec<Option<&str>> {
        todo!()
    }
    fn batch_encode(&self, kvs: &[(u32, Arc<String>)]) -> u32 {
        todo!()
    }

    pub fn encode(&self, key: &Arc<String>) -> Option<&u32> {
        match self {
            CatMaps::InMemory(mem) => mem.get(key),
            CatMaps::OnDisk(disk) => todo!(),
        }
    }

    pub fn counter(&self) -> u32 {
        match self {
            CatMaps::InMemory(m) => m.counter(),
            CatMaps::OnDisk(d) => todo!(),
        }
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<&str>> {
        match self {
            CatMaps::InMemory(mem) => mem.decode_batch(v),
            CatMaps::OnDisk(disk) => disk.decode_batch(v),
        }
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<&str> {
        match self {
            CatMaps::InMemory(mem) => mem.maybe_decode(u),
            CatMaps::OnDisk(disk) => disk.maybe_decode_string(u),
        }
    }

    pub fn image(&self, s: &HashSet<u32>) -> Option<Self> {
        match self {
            CatMaps::InMemory(m) => {
                if let Some(m) = m.image(s) {
                    Some(CatMaps::InMemory(m))
                } else {
                    None
                }
            }
            CatMaps::OnDisk(d) => {
                if let Some(m) = d.image(s) {
                    Some(CatMaps::OnDisk(m))
                } else {
                    None
                }
            }
        }
    }
}
