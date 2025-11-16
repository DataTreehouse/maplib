mod decode;
mod encode;
mod globalize;
mod image;
mod re_encode;
mod split;
pub mod forwards;
pub mod reverse;

pub use decode::*;
pub use encode::*;
pub use globalize::*;
pub use image::*;
pub use re_encode::*;
pub use split::*;
use std::cmp;

use crate::BaseRDFNodeType;
use nohash_hasher::NoHashHasher;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef};
use polars::prelude::DataFrame;
use std::collections::{HashMap};
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;
use crate::cats::forwards::ForwardsCat;
use crate::cats::reverse::ReverseCat;

const SUBJECT_PREFIX_COL_NAME: &str = "subject_prefix";
const OBJECT_PREFIX_COL_NAME: &str = "object_prefix";

pub const OBJECT_RANK_COL_NAME: &str = "object_rank";
pub const SUBJECT_RANK_COL_NAME: &str = "subject_rank";

pub fn literal_is_cat(nn: NamedNodeRef) -> bool {
    nn == xsd::STRING
}

#[derive(Debug)]
pub struct CatTriples {
    pub encoded_triples: Vec<EncodedTriples>,
    pub predicate: NamedNode,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub local_cats: Vec<LockedCats>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, PartialOrd, Ord)]
pub enum CatType {
    Prefix(NamedNode),
    Blank,
    Literal(NamedNode),
}

impl CatType {
    pub fn as_base_rdf_node_type(&self) -> BaseRDFNodeType {
        match self {
            CatType::Prefix(_) => BaseRDFNodeType::IRI,
            CatType::Blank => BaseRDFNodeType::BlankNode,
            CatType::Literal(nn) => BaseRDFNodeType::Literal(nn.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodedTriples {
    pub df: DataFrame,
    pub subject: Option<CatType>,
    pub subject_local_cat_uuid: Option<String>,
    pub object: Option<CatType>,
    pub object_local_cat_uuid: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CatEncs {
    // We use a BTree map to keep strings sorted
    pub forward: ForwardsCat,
    pub reverse: Option<ReverseCat>,
}

#[derive(Debug, Clone)]
pub struct LockedCats {
    inner: Arc<RwLock<Cats>>,
}

impl LockedCats {
    pub fn read(
        &self,
    ) -> Result<
        std::sync::RwLockReadGuard<'_, Cats>,
        std::sync::PoisonError<std::sync::RwLockReadGuard<'_, Cats>>,
    > {
        let r = self.inner.read();
        r
    }

    pub fn write(
        &self,
    ) -> Result<
        std::sync::RwLockWriteGuard<'_, Cats>,
        std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, Cats>>,
    > {
        self.inner.write()
    }

    pub fn new(cats: Cats) -> Self {
        Arc::new(RwLock::new(cats)).into()
    }

    pub fn new_empty() -> Self {
        Self::new(Cats::new_empty(None))
    }

    pub fn deref(&self) -> Arc<RwLock<Cats>> {
        // panic!("I got derefed!");
        self.inner.clone()
    }

    /// Aquires a lock on self, clones the inner Cats and returns it as a new LockedCats.
    /// Panics if lock is poisoned
    pub fn deep_clone(&self) -> Self {
        let cats = {
            let lock = self.read().unwrap();
            lock.deref().clone()
        };
        Self::new(cats)
    }

    pub fn get_cloned_cats(&self) -> Cats {
        let guard = self.read().unwrap();
        guard.deref().clone()
    }

    pub fn ptr_eq(this: &LockedCats, other: &LockedCats) -> bool {
        Arc::ptr_eq(&this.deref(), &other.deref())
    }
}

impl From<Arc<RwLock<Cats>>> for LockedCats {
    fn from(cats: Arc<RwLock<Cats>>) -> Self {
        Self { inner: cats }
    }
}

#[derive(Debug, Clone)]
pub struct Cats {
    pub cat_map: HashMap<CatType, CatEncs>,
    iri_counter: u32,
    blank_counter: u32,
    literal_counter_map: HashMap<NamedNode, u32>,
    pub uuid: String,
    pub rev_iri_suffix_map: ForwardsCat,
    belongs_prefix_map: HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>>,
    prefix_map: HashMap<u32, NamedNode>,
    prefix_rev_map: HashMap<NamedNode, u32>,
}

impl Cats {
    pub fn new_singular_literal(l: &str, dt: NamedNode, u: u32) -> (u32, Cats) {
        let t = CatType::Literal(dt);
        let catenc = CatEncs::new_singular(l, u, false);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }

    pub fn new_singular_blank(s: &str, u: u32) -> (u32, Cats) {
        let t = CatType::Blank;
        let catenc = CatEncs::new_singular(s, u, false);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }

    pub fn new_singular_iri(s: &str, u: u32) -> (u32, Cats) {
        let (pre, suf) = rdf_split_iri_str(s);
        let t = CatType::Prefix(NamedNode::new_unchecked(pre));
        let catenc = CatEncs::new_singular(suf, u, true);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }
}

impl Cats {
    pub fn get_rev_map(
        &self,
        bt: &BaseRDFNodeType,
    ) -> &HashMap<u32, Arc<String>, BuildHasherDefault<NoHashHasher<u32>>> {
        if bt.is_iri() {
            &self.rev_iri_suffix_map
        } else {
            let ct = if let BaseRDFNodeType::Literal(l) = bt {
                CatType::Literal(l.clone())
            } else if bt.is_blank_node() {
                CatType::Blank
            } else {
                panic!();
            };
            self.cat_map.get(&ct).unwrap().reverse.as_ref().unwrap()
        }
    }

    pub(crate) fn from_map(cat_map: HashMap<CatType, CatEncs>) -> Self {
        let mut cats = Cats {
            cat_map,
            iri_counter: 0,
            blank_counter: 0,
            literal_counter_map: Default::default(),
            uuid: Uuid::new_v4().to_string(),
            rev_iri_suffix_map: HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default()),
            belongs_prefix_map: HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default()),
            prefix_map: Default::default(),
            prefix_rev_map: Default::default(),
        };

        let mut i = 0u32;
        for (cat_type, cat_enc) in cats.cat_map.iter() {
            if let CatType::Prefix(nn) = cat_type {
                cats.prefix_map.insert(i, nn.clone());
                cats.prefix_rev_map.insert(nn.clone(), i);
                cats.rev_iri_suffix_map
                    .extend(cat_enc.forward.iter().map(|(x, y)| (*y, x.clone())));
                cats.belongs_prefix_map
                    .extend(cat_enc.forward.values().map(|x| (*x, i)));
                i += 1;
            }
        }
        cats.iri_counter = cats.calc_new_iri_counter();
        cats.blank_counter = cats.calc_new_blank_counter();
        cats.literal_counter_map = cats.calc_new_literal_counter();
        cats
    }

    pub fn new_empty(counts_from: Option<&Cats>) -> Cats {
        let (blank_counter, iri_counter, literal_counter_map) =
            if let Some(counts_from) = counts_from {
                (
                    counts_from.blank_counter,
                    counts_from.iri_counter,
                    counts_from.literal_counter_map.clone(),
                )
            } else {
                (0, 0, Default::default())
            };

        Cats {
            cat_map: HashMap::new(),
            blank_counter,
            iri_counter,
            literal_counter_map,
            uuid: uuid::Uuid::new_v4().to_string(),
            rev_iri_suffix_map: HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default()),
            belongs_prefix_map: HashMap::with_capacity_and_hasher(2, BuildHasherDefault::default()),
            prefix_map: Default::default(),
            prefix_rev_map: Default::default(),
        }
    }

    fn calc_new_blank_counter(&self) -> u32 {
        let mut counter = 0;
        if let Some(enc) = self.cat_map.get(&CatType::Blank) {
            counter = cmp::max(
                enc.reverse.as_ref().unwrap().keys().max().unwrap() + 1,
                counter,
            );
        }
        counter
    }

    fn calc_new_iri_counter(&self) -> u32 {
        let counter = self.rev_iri_suffix_map.keys().max().map(|x| x + 1);
        counter.unwrap_or(0)
    }

    fn calc_new_literal_counter(&self) -> HashMap<NamedNode, u32> {
        let mut map = HashMap::new();
        for (p, cat) in &self.cat_map {
            if let CatType::Literal(nn) = p {
                let counter = cat.reverse.as_ref().unwrap().keys().max().unwrap() + 1;
                map.insert(nn.clone(), counter);
            }
        }
        map
    }

    fn get_counter(&self, c: &CatType) -> u32 {
        match c {
            CatType::Prefix(..) => self.get_iri_counter(),
            CatType::Blank => self.get_blank_counter(),
            CatType::Literal(nn) => self.get_literal_counter(nn),
        }
    }

    fn set_counter(&mut self, u: u32, c: &CatType) {
        match c {
            CatType::Prefix(..) => {
                self.iri_counter = u;
            }
            CatType::Blank => {
                self.blank_counter = u;
            }
            CatType::Literal(nn) => {
                self.literal_counter_map.insert(nn.clone(), u);
            }
        }
    }

    pub fn get_iri_counter(&self) -> u32 {
        self.iri_counter
    }
    pub fn get_blank_counter(&self) -> u32 {
        self.blank_counter
    }
    pub fn get_literal_counter(&self, nn: &NamedNode) -> u32 {
        self.literal_counter_map.get(nn).map(|x| *x).unwrap_or(0)
    }
}
