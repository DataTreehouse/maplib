mod in_memory;
mod on_disk;

use std::sync::Arc;
use crate::cats::forwards::in_memory::{ForwardsCatInMemory, ForwardsCatInMemoryIterator};
use crate::cats::forwards::on_disk::{ForwardsCatOnDisk, ForwardsCatOnDiskIterator};
use crate::cats::reverse::ReverseCat;

#[derive(Debug, Clone)]
pub enum ForwardsCat {
    ForwardCatInMemory(ForwardsCatInMemory),
    ForwardCatOnDisk(ForwardsCatOnDisk),
}

impl ForwardsCat {
    pub fn image(&self, rev_img: &ReverseCat) -> Self {
        match self {
            ForwardsCat::ForwardCatInMemory(m) => {
                ForwardsCat::ForwardCatInMemory(m.image(rev_img))
            }
            ForwardsCat::ForwardCatOnDisk(d) => {
                ForwardsCat::ForwardCatOnDisk(d.image(rev_img))
            }
        }
    }
    fn lookup(&self, key:&u32) -> Option<&str> {
        todo!()
    }
    fn batch_lookup(&self, keys: &[u32]) -> Vec<Option<&str>> {
        todo!()
    }
    fn batch_insert(&self, kvs: &[(u32, Arc<String>)]) -> u32 {
        todo!()
    }

    pub(crate) fn get(&self, key:&Arc<String>) -> Option<&u32> {
        match self {
            ForwardsCat::ForwardCatInMemory(mem) => {
                mem.get(key)
            }
            ForwardsCat::ForwardCatOnDisk(disk) => {
                disk.get(key)
            }
        }
    }

    pub fn iter(&self) -> ForwardsCatIterator {
        match self {
            ForwardsCat::ForwardCatInMemory(mem) => { ForwardsCatIterator::InMemory(mem.iter())}
            ForwardsCat::ForwardCatOnDisk(disk) => { ForwardsCatIterator::OnDisk(disk.iter())}
        }
    }
}

enum ForwardsCatIterator<'a> {
    InMemory(ForwardsCatInMemoryIterator<'a>),
    OnDisk(ForwardsCatOnDiskIterator<'a>),
}

impl Iterator for ForwardsCatIterator<'_> {
    type Item = (Arc<String>, u32);


    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ForwardsCatIterator::InMemory(inmem) => {
                inmem.next()
            }
            ForwardsCatIterator::OnDisk(ondisk) => {
                ondisk.next()
            }
        }
    }
}