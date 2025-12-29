use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct CatMapsOnDisk {
    map: (),
    rev_map: (),
}

impl CatMapsOnDisk {
    pub(crate) fn decode_batch(&self, us: &[Option<u32>]) -> Vec<Option<&str>> {
        todo!()
    }
}

impl CatMapsOnDisk {
    pub(crate) fn image(&self, s: &HashSet<u32>) -> Option<CatMapsOnDisk> {
        todo!()
    }
}

impl CatMapsOnDisk {
    pub(crate) fn maybe_decode_string(&self, p0: &u32) -> Option<&str> {
        todo!()
    }
}
