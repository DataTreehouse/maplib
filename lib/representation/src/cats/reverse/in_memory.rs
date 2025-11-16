use nohash_hasher::NoHashHasher;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReverseCatInMemory {
    rev_map: HashMap<u32, Arc<String>, BuildHasherDefault<NoHashHasher<u32>>>,
}

impl ReverseCatInMemory {
    pub(crate) fn image(&self, s: &HashSet<u32>) -> Option<ReverseCatInMemory> {
        let new_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = s
            .par_iter()
            .map(|x| {
                if let Some(s) = self.rev_map.get(x) {
                    Some((s.clone(), x))
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .map(|(x, y)| (*y, x))
            .collect();
        if new_map.is_empty() {
            None
        } else {
            Some(Self { rev_map: new_map })
        }
    }

    pub fn from<T: Iterator<Item = (u32, Arc<String>)>>(iter: T) -> Self {
        let rev_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = iter.collect();
        ReverseCatInMemory { rev_map }
    }
}
