use crate::cats::CatReEnc;
use nohash_hasher::NoHashHasher;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CatMapsInMemory {
    map: BTreeMap<Arc<String>, u32>,
    rev_map: HashMap<u32, Arc<String>, BuildHasherDefault<NoHashHasher<u32>>>,
}

impl CatMapsInMemory {
    pub fn new_remap(maps: &CatMapsInMemory, c: &mut u32) -> (CatMapsInMemory, CatReEnc) {
        let mut remap = vec![];
        let mut new_maps = CatMapsInMemory::new_empty();
        for (s, v) in maps.map.iter() {
            remap.push((*v, *c));
            new_maps.encode_new_arc_string(s.clone(), *c);
            *c += 1;
        }
        let remap: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> =
            remap.into_iter().collect();
        (
            new_maps,
            CatReEnc {
                cat_map: Arc::new(remap),
            },
        )
    }

    fn encode_new_arc_string(&mut self, s: Arc<String>, u: u32) {
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub(crate) fn contains_str(&self, s: &str) -> bool {
        let arcs = Arc::new(s.to_string());
        self.map.contains_key(&arcs)
    }

    pub(crate) fn contains_u32(&self, u: &u32) -> bool {
        self.rev_map.contains_key(u)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn height(&self) -> u32 {
        self.map.len() as u32
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        let s = Arc::new(s.clone());
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn maybe_encode_string(&self, s: &str) -> Option<&u32> {
        let s = Arc::new(s.to_string());
        self.map.get(&s)
    }

    pub fn new_singular(value: &str, u: u32) -> Self {
        let mut sing = CatMapsInMemory::new_empty();
        let s = Arc::new(value.to_string());
        sing.map.insert(s.clone(), u);
        sing.rev_map.insert(u, s);
        sing
    }

    pub fn new_empty() -> CatMapsInMemory {
        CatMapsInMemory {
            map: Default::default(),
            rev_map: Default::default(),
        }
    }

    pub fn get(&self, key: &Arc<String>) -> Option<&u32> {
        self.map.get(key)
    }

    pub fn counter(&self) -> u32 {
        self.rev_map.keys().max().unwrap().clone() + 1
    }

    pub fn get_rev(&self, key: &u32) -> Option<&Arc<String>> {
        self.rev_map.get(key)
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<&str>> {
        let decoded_vec_iter = v
            .into_par_iter()
            .map(|x| x.map(|x| self.rev_map.get(&x).unwrap().as_str()));
        decoded_vec_iter.collect()
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<&str> {
        self.rev_map.get(u).map(|x| x.as_str())
    }

    pub fn image(&self, s: &HashSet<u32>) -> Option<CatMapsInMemory> {
        let new_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = s
            .par_iter()
            .map(|x| {
                if let Some(s) = self.rev_map.get(x) {
                    Some((s.clone(), x))
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .map(|(x, y)| (*y, x))
            .collect();
        if new_map.is_empty() {
            None
        } else {
            let map = new_map.iter().map(|(x, y)| (y.clone(), *x)).collect();
            Some(Self {
                map,
                rev_map: new_map,
            })
        }
    }

    pub fn inner_join_re_enc(&self, other: &CatMapsInMemory) -> Vec<(u32, u32)> {
        let renc: Vec<_> = self
            .map
            .iter()
            .map(|(x, l)| {
                if let Some(r) = other.maybe_encode_string(x.as_str()) {
                    if l != r {
                        Some((*l, *r))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        renc
    }

    pub fn merge(&mut self, other: &CatMapsInMemory, c: &mut u32) -> CatReEnc {
        let (remap, insert): (Vec<_>, Vec<_>) = other
            .map
            .iter()
            .map(|(s, u)| {
                if let Some(e) = self.map.get(s) {
                    (Some((*u, *e)), None)
                } else {
                    (None, Some((s.clone(), u)))
                }
            })
            .unzip();
        let mut numbered_insert = Vec::new();
        let mut new_remap = Vec::new();
        for k in insert {
            if let Some((s, u)) = k {
                numbered_insert.push((s, *c));
                new_remap.push((*u, *c));
                *c += 1;
            }
        }
        for (s, u) in numbered_insert {
            self.encode_new_arc_string(s.clone(), u);
        }
        let remap: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = remap
            .into_iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .chain(new_remap.into_iter())
            .collect();
        let reenc = CatReEnc {
            cat_map: Arc::new(remap),
        };
        reenc
    }
}
