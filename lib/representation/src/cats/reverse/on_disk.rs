use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReverseCatOnDisk {
    
}

impl ReverseCatOnDisk {
    pub(crate) fn image(&self, p0: &HashSet<u32>) -> Option<ReverseCatOnDisk> {
        todo!()
    }
}

impl ReverseCatOnDisk {
    pub fn from<T:Iterator<Item=(u32, Arc<String>)>>(iter:T) -> Self {
        todo!()
    }
}