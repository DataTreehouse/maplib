mod in_memory;
mod on_disk;

use std::collections::HashSet;
use std::sync::Arc;
use crate::cats::reverse::in_memory::ReverseCatInMemory;
use crate::cats::reverse::on_disk::ReverseCatOnDisk;

#[derive(Debug, Clone)]
pub enum ReverseCat {
    ReverseCatInMemory(ReverseCatInMemory),
    ReverseCatOnDisk(ReverseCatOnDisk),
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
}