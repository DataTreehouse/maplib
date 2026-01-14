pub mod in_memory;
pub mod on_disk;

use crate::cats::maps::in_memory::CatMapsInMemory;
use crate::cats::maps::on_disk::CatMapsOnDisk;
use crate::cats::CatReEnc;
use crate::BaseRDFNodeType;
use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum CatMaps {
    InMemory(CatMapsInMemory),
    OnDisk(CatMapsOnDisk),
}

impl CatMaps {
    pub(crate) fn new_remap(
        maps: &CatMaps,
        path: Option<&Path>,
        c: &mut u32,
    ) -> (CatMaps, CatReEnc) {
        if let Some(path) = path {
            if let CatMaps::OnDisk(disk) = maps {
                let (disk, re_enc) = CatMapsOnDisk::new_remap(disk, c, path);
                (CatMaps::OnDisk(disk), re_enc)
            } else {
                unreachable!("Should never happen")
            }
        } else {
            if let CatMaps::InMemory(mem) = maps {
                let (mem, re_enc) = CatMapsInMemory::new_remap(mem, c);
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
            CatMaps::OnDisk(disk) => {
                if let CatMaps::OnDisk(other_disk) = other {
                    disk.inner_join_re_enc(other_disk)
                } else {
                    unreachable!("Should never happen")
                }
            }
        }
    }

    pub fn merge(&mut self, other: &CatMaps, c: &mut u32) -> CatReEnc {
        match self {
            CatMaps::InMemory(mem) => {
                if let CatMaps::InMemory(other_mem) = other {
                    mem.merge(other_mem, c)
                } else {
                    unreachable!("Should never happen")
                }
            }
            CatMaps::OnDisk(disk) => {
                if let CatMaps::OnDisk(other_disk) = other {
                    disk.merge(other_disk, c)
                } else {
                    unreachable!("Should never happen")
                }
            }
        }
    }

    pub fn contains_str(&self, s: &str) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.contains_str(s),
            CatMaps::OnDisk(disk) => disk.contains_str(s),
        }
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.contains_u32(u),
            CatMaps::OnDisk(disk) => disk.contains_u32(u),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            CatMaps::InMemory(mem) => mem.is_empty(),
            CatMaps::OnDisk(disk) => disk.is_empty(),
        }
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        match self {
            CatMaps::InMemory(mem) => mem.encode_new_string(s, u),
            CatMaps::OnDisk(disk) => disk.encode_new_string(s, u),
        }
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<&u32> {
        match self {
            CatMaps::InMemory(mem) => mem.maybe_encode_str(s),
            CatMaps::OnDisk(disk) => disk.maybe_encode_str(s),
        }
    }

    pub fn new_singular(value: &str, u: u32, path: Option<&Path>, bt: &BaseRDFNodeType) -> CatMaps {
        if let Some(path) = path {
            CatMaps::InMemory(CatMapsOnDisk::new_singular(value, u, path))
        } else {
            CatMaps::InMemory(CatMapsInMemory::new_singular(value, u, bt))
        }
    }

    pub fn new_empty(path: Option<&Path>, bt: &BaseRDFNodeType) -> CatMaps {
        if let Some(path) = path {
            CatMaps::OnDisk(CatMapsOnDisk::new_empty(path))
        } else {
            CatMaps::InMemory(CatMapsInMemory::new_empty(bt))
        }
    }

    pub fn counter(&self) -> u32 {
        match self {
            CatMaps::InMemory(m) => m.counter(),
            CatMaps::OnDisk(d) => d.counter(),
        }
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<str>>> {
        match self {
            CatMaps::InMemory(mem) => mem.decode_batch(v),
            CatMaps::OnDisk(disk) => disk.decode_batch(v),
        }
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<str>> {
        match self {
            CatMaps::InMemory(mem) => mem.maybe_decode(u),
            CatMaps::OnDisk(disk) => disk.maybe_decode(u),
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
