use std::sync::Arc;
use crate::cats::reverse::ReverseCat;

#[derive(Debug, Clone)]
pub struct ForwardsCatOnDisk {
    map: ()
}

impl ForwardsCatOnDisk {
    pub(crate) fn image(&self, rev: &ReverseCat) -> ForwardsCatOnDisk {
        todo!()
    }
}

impl ForwardsCatOnDisk {
    pub fn get(&self, key: &Arc<String>) -> Option<&u32> {
        self.map.get(key)
    }

    pub fn iter(&self) -> ForwardsCatOnDiskIterator {
        ForwardsCatOnDiskIterator{
            fwc: &()
        }
    }
}

pub struct ForwardsCatOnDiskIterator<'a> {
    fwc: &'a ()
}

impl ForwardsCatOnDiskIterator<'_> {
    pub fn next(&mut self) -> Option<(Arc<String>, u32)> {
        match self.fwc.next() {
            None => {None}
            Some((s,u)) => {Some((s.clone(), *u))}
        }
    }
}