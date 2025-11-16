use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use nohash_hasher::NoHashHasher;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use crate::cats::forwards::ForwardsCat;
use crate::cats::reverse::ReverseCat;

#[derive(Debug, Clone)]
pub struct ForwardsCatInMemory {
    map: HashMap<u32, Arc<String>, BuildHasherDefault<NoHashHasher<u32>>>
}

impl ForwardsCatInMemory {
    pub fn image(&self, rev_img: &ReverseCat) -> ForwardsCatInMemory {
        let fw = Self::from(rev_img.iter().map(|(x, y)| (y,x)));
        fw
    }
}