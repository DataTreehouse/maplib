use crate::cats::CatReEnc;
use crate::BaseRDFNodeType;
use disk::CatMapsOnDisk;
use in_memory::{CatMapsInMemory, RankMap};
use in_memory::vectors::stored_to_vector_into;
use range_set_blaze::RangeSetBlaze;
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CatMaps {
    InMemory(Box<CatMapsInMemory>),
    OnDisk(Box<CatMapsOnDisk>),
}

impl CatMaps {
    pub fn in_memory(maps: CatMapsInMemory) -> CatMaps {
        CatMaps::InMemory(Box::new(maps))
    }

    pub fn on_disk(maps: CatMapsOnDisk) -> CatMaps {
        CatMaps::OnDisk(Box::new(maps))
    }

    pub fn get_spaced_strings(&self, stride: usize) -> Vec<(String, String)> {
        match self {
            CatMaps::InMemory(cat_maps) => cat_maps.get_spaced_strings(stride),
            CatMaps::OnDisk(cat_maps) => cat_maps.get_spaced_strings(stride),
        }
    }

    /// Whether some encoded string may start with `prefix`. On disk this is not checked, so
    /// the answer there is always true.
    pub fn may_have_string_with_prefix(&self, prefix: &str) -> bool {
        self.has_string_with_prefix(prefix).unwrap_or(true)
    }

    /// Whether some encoded string starts with `prefix`, None if this is not known (on disk).
    pub fn has_string_with_prefix(&self, prefix: &str) -> Option<bool> {
        match self {
            CatMaps::InMemory(cat_maps) => Some(cat_maps.has_string_with_prefix(prefix)),
            CatMaps::OnDisk(_) => None,
        }
    }

    pub fn compact(&mut self) {
        match self {
            CatMaps::InMemory(cat_maps) => {
                cat_maps.compact();
            }
            CatMaps::OnDisk(cat_maps) => {
                cat_maps.compact();
            }
        }
    }

    pub fn inner_join_re_enc(&self, other: &Self) -> Vec<(u32, u32)> {
        match self {
            CatMaps::InMemory(m) => match other {
                CatMaps::InMemory(other_m) => m.inner_join_re_enc(other_m),
                // The disk side gives (disk code, in-memory code), so the pairs are swapped.
                CatMaps::OnDisk(other_d) => other_d
                    .inner_join_re_enc_in_memory(m.to_record_batch())
                    .into_iter()
                    .map(|(d, m)| (m, d))
                    .collect(),
            },
            CatMaps::OnDisk(d) => match other {
                CatMaps::InMemory(m) => d.inner_join_re_enc_in_memory(m.to_record_batch()),
                CatMaps::OnDisk(other_d) => d.inner_join_re_enc_on_disk(other_d),
            },
        }
    }
    pub fn merge(&mut self, other: &Self, c: &mut u32) -> CatReEnc {
        let map = match self {
            CatMaps::InMemory(m) => match other {
                CatMaps::InMemory(other) => m.merge(other, c),
                CatMaps::OnDisk(..) => {
                    unreachable!()
                }
            },
            CatMaps::OnDisk(d) => match other {
                CatMaps::InMemory(m) => d.merge_other_in_memory(m.to_record_batch(), c),
                CatMaps::OnDisk(..) => {
                    unreachable!()
                }
            },
        };
        CatReEnc {
            cat_map: Arc::new(map),
        }
    }
    /// The in-memory dictionary, if the dictionary is in memory.
    /// Orders the strings of two codes without decoding them; None on disk or for a code that
    /// is not here.
    #[inline]
    pub fn cmp_codes(&self, a: u32, b: u32) -> Option<std::cmp::Ordering> {
        match self {
            CatMaps::InMemory(m) => m.cmp_codes(a, b),
            CatMaps::OnDisk(_) => None,
        }
    }

    pub fn as_in_memory(&self) -> Option<&CatMapsInMemory> {
        match self {
            CatMaps::InMemory(m) => Some(m.as_ref()),
            CatMaps::OnDisk(_) => None,
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

    pub fn garbage_collect_cats(&mut self, range_set: RangeSetBlaze<u32>) {
        match self {
            CatMaps::InMemory(m) => {
                m.garbage_collect_cats(range_set);
            }
            CatMaps::OnDisk(d) => {
                d.garbage_collect_cats(range_set);
            }
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
    pub fn encode_new_in_memory_string(&mut self, s: Cow<'_, str>, u: u32) {
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
    /// The vectors of the vector literals with the codes `v`: None for codes that are not here,
    /// an error for literals that are not valid vectors. Vectors stored packed are read without
    /// writing their lexical forms (see [`in_memory::vectors`]).
    pub fn decode_vectors_batch(&self, v: &[Option<u32>]) -> Vec<Option<Result<Vec<f32>, String>>> {
        match self {
            CatMaps::InMemory(m) => v
                .par_iter()
                .with_min_len(256)
                .map(|u| {
                    let (_, q) = m.decode_parts((*u)?)?;
                    let mut out = Vec::new();
                    Some(stored_to_vector_into(q, &mut out).map(|_| out))
                })
                .collect(),
            CatMaps::OnDisk(d) => d
                .decode_batch(v)
                .into_par_iter()
                .with_min_len(256)
                .map(|s| {
                    let s = s?;
                    let mut out = Vec::new();
                    Some(stored_to_vector_into(&s, &mut out).map(|_| out))
                })
                .collect(),
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
            CatMaps::InMemory(m) => m.image(s).map(CatMaps::in_memory),
            CatMaps::OnDisk(d) => {
                if let Some(path) = path {
                    d.image_disk(path, s).map(CatMaps::on_disk)
                } else {
                    d.image_memory(s, dt.is_iri()).map(CatMaps::in_memory)
                }
            }
        }
    }
    pub fn new_empty(path: Option<&Path>, bt: &BaseRDFNodeType) -> Self {
        match path {
            None => CatMaps::in_memory(CatMapsInMemory::new_empty_kind(
                bt.is_iri(),
                crate::vectors::is_vector_literal_type(bt),
            )),
            Some(p) => CatMaps::on_disk(CatMapsOnDisk::new_empty(p)),
        }
    }

    pub fn global_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMaps::InMemory(m) => m.global_rank_map(us),
            CatMaps::OnDisk(d) => d.global_rank_map(us),
        }
    }

    pub fn local_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMaps::InMemory(m) => m.global_rank_map(us),
            CatMaps::OnDisk(d) => d.local_rank_map(us),
        }
    }

    /// Ranks that order `us` by string value, valid only for comparing values within `us`.
    /// Cheap for in-memory maps: O(|us| log |us|) rather than a walk over the whole map.
    pub fn sort_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMaps::InMemory(m) => m.local_rank_map(us),
            CatMaps::OnDisk(d) => d.local_rank_map(us),
        }
    }

    /// Positions of the codes `us` (sorted, distinct) in the string order of the whole map.
    pub fn global_ranks(&self, us: &[u32]) -> RankMap {
        match self {
            CatMaps::InMemory(m) => m.global_ranks(),
            CatMaps::OnDisk(d) => {
                RankMap::from_hash_map(d.global_rank_map(&us.iter().copied().collect()))
            }
        }
    }

    /// The positions of all codes in the string order of the whole map, when they are cheap to
    /// have: in-memory maps cache them. `None` for maps on disk, which look up ranks per call.
    pub fn all_global_ranks(&self) -> Option<RankMap> {
        match self {
            CatMaps::InMemory(m) => Some(m.global_ranks()),
            CatMaps::OnDisk(_) => None,
        }
    }

    /// Ranks that order `us` (sorted, distinct) by string value, valid only for comparing values
    /// within `us`.
    pub fn sort_ranks(&self, us: &[u32]) -> RankMap {
        match self {
            CatMaps::InMemory(m) => m.sort_ranks(us),
            CatMaps::OnDisk(d) => {
                RankMap::from_hash_map(d.local_rank_map(&us.iter().copied().collect()))
            }
        }
    }

    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        match self {
            CatMaps::InMemory(in_memory) => in_memory.range_set(),
            CatMaps::OnDisk(on_disk) => on_disk.range_set(),
        }
    }
}
