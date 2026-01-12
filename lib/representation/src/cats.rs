mod decode;
mod encode;
mod globalize;
mod image;
pub mod maps;
mod re_encode;

pub use decode::*;
pub use encode::*;
pub use globalize::*;
pub use image::*;
pub use re_encode::*;
use std::cmp;

use crate::cats::maps::CatMaps;
use crate::dataset::NamedGraph;
use crate::BaseRDFNodeType;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef};
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

pub const OBJECT_RANK_COL_NAME: &str = "object_rank";
pub const SUBJECT_RANK_COL_NAME: &str = "subject_rank";

pub fn literal_is_cat(nn: NamedNodeRef) -> bool {
    nn == xsd::STRING
}

#[derive(Debug)]
pub struct CatTriples {
    pub encoded_triples: EncodedTriples,
    pub predicate: NamedNode,
    pub graph: NamedGraph,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub local_cats: Vec<LockedCats>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, PartialOrd, Ord)]
pub enum CatType {
    IRI,
    Blank,
    Literal(NamedNode),
}

impl CatType {
    pub fn as_base_rdf_node_type(&self) -> BaseRDFNodeType {
        match self {
            CatType::IRI => BaseRDFNodeType::IRI,
            CatType::Blank => BaseRDFNodeType::BlankNode,
            CatType::Literal(nn) => BaseRDFNodeType::Literal(nn.clone()),
        }
    }

    pub fn from_base_rdf_node_type(bt: &BaseRDFNodeType) -> Self {
        match bt {
            BaseRDFNodeType::IRI => CatType::IRI,
            BaseRDFNodeType::BlankNode => CatType::Blank,
            BaseRDFNodeType::Literal(l) => CatType::Literal(l.clone()),
            BaseRDFNodeType::None => {
                unimplemented!("Should not be called for None")
            }
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
    pub maps: CatMaps,
}

impl CatEncs {
    pub(crate) fn new_remap(
        other: &CatEncs,
        path: Option<&Path>,
        c: &mut u32,
    ) -> (CatEncs, CatReEnc) {
        let (maps, remap) = CatMaps::new_remap(&other.maps, path, c);
        let encs = CatEncs { maps };
        (encs, remap)
    }
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
}

impl Cats {
    pub fn new_singular_literal(
        l: &str,
        dt: NamedNode,
        u: u32,
        path: Option<&Path>,
    ) -> (u32, Cats) {
        let dt = BaseRDFNodeType::Literal(dt);
        let catenc = CatEncs::new_singular(l, u, path, &dt);
        let t = if let BaseRDFNodeType::Literal(dt) = dt {
            CatType::Literal(dt)
        } else {
            unreachable!("Should never happen")
        };
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }

    pub fn new_singular_blank(s: &str, u: u32, path: Option<&Path>) -> (u32, Cats) {
        let t = CatType::Blank;
        let catenc = CatEncs::new_singular(s, u, path, &BaseRDFNodeType::BlankNode);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }

    pub fn new_singular_iri(s: &str, u: u32, path: Option<&Path>) -> (u32, Cats) {
        let t = CatType::IRI;
        let catenc = CatEncs::new_singular(s, u, path, &BaseRDFNodeType::IRI);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }
}

impl Cats {
    pub fn get_cat_encs(&self, bt: &BaseRDFNodeType) -> &CatEncs {
        let ct = CatType::from_base_rdf_node_type(bt);
        self.cat_map.get(&ct).unwrap()
    }

    pub(crate) fn from_map(cat_map: HashMap<CatType, CatEncs>) -> Self {
        let mut cats = Cats {
            cat_map,
            iri_counter: 0,
            blank_counter: 0,
            literal_counter_map: Default::default(),
            uuid: Uuid::new_v4().to_string(),
        };

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
            uuid: Uuid::new_v4().to_string(),
        }
    }

    fn calc_new_blank_counter(&self) -> u32 {
        let mut counter = 0;
        if let Some(enc) = self.cat_map.get(&CatType::Blank) {
            counter = cmp::max(enc.counter() + 1, counter);
        }
        counter
    }

    fn calc_new_iri_counter(&self) -> u32 {
        let mut counter = 0;
        if let Some(enc) = self.cat_map.get(&CatType::IRI) {
            counter = cmp::max(enc.counter() + 1, counter);
        }
        counter
    }

    fn calc_new_literal_counter(&self) -> HashMap<NamedNode, u32> {
        let mut map = HashMap::new();
        for (p, cat) in &self.cat_map {
            if let CatType::Literal(nn) = p {
                let counter = cat.counter() + 1;
                map.insert(nn.clone(), counter);
            }
        }
        map
    }

    fn get_counter(&self, t: &CatType) -> u32 {
        match t {
            CatType::IRI => self.get_iri_counter(),
            CatType::Blank => self.get_blank_counter(),
            CatType::Literal(nn) => self.get_literal_counter(nn),
        }
    }

    fn set_counter(&mut self, u: u32, c: &CatType) {
        match c {
            CatType::IRI => {
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
