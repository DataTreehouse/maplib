mod in_memory;
mod on_disk;

use std::collections::HashSet;
use std::sync::Arc;
use crate::cats::forwards::in_memory::ForwardsCatInMemory;
use crate::cats::forwards::on_disk::ForwardsCatOnDisk;
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
            ForwardsCat::ForwardCatOnDisk(d) => {}
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
}