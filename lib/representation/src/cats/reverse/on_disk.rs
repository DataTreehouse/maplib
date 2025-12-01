use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReverseCatOnDisk {
    
}

impl ReverseCatOnDisk {
    pub(crate) fn counter(&self) -> u32 {
        todo!()
    }
}

impl ReverseCatOnDisk {
    pub(crate) fn image(&self, us: &HashSet<u32>) -> Option<ReverseCatOnDisk> {
        todo!()
    }
}

impl ReverseCatOnDisk {
    pub fn from<T:Iterator<Item=(u32, Arc<String>)>>(iter:T) -> Self {
        todo!()
    }

    pub fn iter(&self) -> ReverseCatOnDiskIterator {
        ReverseCatOnDiskIterator{
            rwc:&()
        }
    }

    pub fn get(&self, key: &u32) -> Option<&Arc<String>> {
        todo!()
    }
}

pub struct ReverseCatOnDiskIterator<'a> {
    rwc: &'a ()
}

impl ReverseCatOnDiskIterator<'_> {
    pub fn next(&mut self) -> Option<(u32, Arc<String>)> {
        match self.rwc.next() {
            None => {None}
            Some((u,s)) => {Some((*u, s.clone()))}
        }
    }
}