use std::collections::{HashMap};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use nohash_hasher::NoHashHasher;
use crate::cats::reverse::ReverseCat;

#[derive(Debug, Clone)]
pub struct ForwardsCatInMemory {
    map: HashMap<Arc<String>,u32, BuildHasherDefault<NoHashHasher<u32>>>
}

impl ForwardsCatInMemory {
    pub fn image(&self, rev_img: &ReverseCat) -> ForwardsCatInMemory {
        let fw = Self::from(rev_img.iter().map(|(x, y)| (y,x)));
        fw
    }

    pub fn iter(&self) -> ForwardsCatInMemoryIterator {
        ForwardsCatInMemoryIterator {
            fwc: self.map.iter()
        }
    }

    pub fn get(&self, key: &Arc<String>) -> Option<&u32> {
        self.map.get(key)
    }
}

pub struct ForwardsCatInMemoryIterator<'a> {
    fwc: std::collections::hash_map::Iter<'a, Arc<String>, u32>
}

impl ForwardsCatInMemoryIterator<'_> {
    pub fn next(&mut self) -> Option<(Arc<String>, u32)> {
        match self.fwc.next() {
            None => {None}
            Some((s,u)) => {Some((s.clone(), *u))}
        }
    }
}
