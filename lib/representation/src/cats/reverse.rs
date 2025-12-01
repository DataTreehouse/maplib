mod in_memory;
mod on_disk;

use std::collections::HashSet;
use std::sync::Arc;
use crate::cats::reverse::in_memory::{ReverseCatInMemory, ReverseCatInMemoryIterator};
use crate::cats::reverse::on_disk::{ReverseCatOnDisk, ReverseCatOnDiskIterator};

#[derive(Debug, Clone)]
pub enum ReverseCat {
    ReverseCatInMemory(ReverseCatInMemory),
    ReverseCatOnDisk(ReverseCatOnDisk),
}

impl ReverseCat {
    pub(crate) fn counter(&self) -> u32 {
        match self {
            ReverseCat::ReverseCatInMemory(m) => {
                m.counter()
            }
            ReverseCat::ReverseCatOnDisk(d) => {
                d.counter()
            }
        }
    }
    
    pub fn get(&self, key:&u32) -> Option<&Arc<String>> {
        match self {
            ReverseCat::ReverseCatInMemory(mem) => {
                mem.get(key)
            }
            ReverseCat::ReverseCatOnDisk(disk) => {
                disk.get(key)
            }
        }
    }
}

impl ReverseCat {
    pub fn from<T:Iterator<Item=(u32, Arc<String>)>>(iter:T, on_disk:bool) -> Self {
        if on_disk {
            ReverseCat::ReverseCatOnDisk(ReverseCatOnDisk::from(iter))
        } else {
            ReverseCat::ReverseCatInMemory(ReverseCatInMemory::from(iter))
        }
    }
    
    pub fn image(&self, s: &HashSet<u32>) -> Option<Self> {
        match self {
            ReverseCat::ReverseCatInMemory(m) => {
                m.image(s).map(|x|ReverseCat::ReverseCatInMemory(x))
            }
            ReverseCat::ReverseCatOnDisk(d) => {
                d.image(s).map(|x|ReverseCat::ReverseCatOnDisk(x))
            }
        }
    }
    
    pub fn iter(&self) -> ReverseCatIterator {
        match self {
            ReverseCat::ReverseCatInMemory(mem) => {ReverseCatIterator::InMemory(mem.iter())}
            ReverseCat::ReverseCatOnDisk(disk) => {ReverseCatIterator::OnDisk(disk.iter())}
        }
    }
}

enum ReverseCatIterator<'a> {
    InMemory(ReverseCatInMemoryIterator<'a>),
    OnDisk(ReverseCatOnDiskIterator<'a>),
}

impl Iterator for ReverseCatIterator<'_> {
    type Item = (u32, Arc<String>);


    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReverseCatIterator::InMemory(inmem) => {
                inmem.next()
            }
            ReverseCatIterator::OnDisk(ondisk) => {
                ondisk.next()
            }
        }
    }
}