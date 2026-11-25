mod bulk_encode;
mod decode;
mod encode;
mod globalize;
mod image;
pub mod maps;
mod re_encode;
pub mod serialization;

pub use decode::*;
pub use encode::*;
pub use globalize::*;
pub use image::*;
pub use in_memory::{distinct_codes, RankMap};
use polars::prelude::{Column, IntoColumn};
pub use re_encode::*;
use std::borrow::Cow;
use std::cmp;

use crate::cats::maps::CatMaps;
use crate::dataset::NamedGraph;
use crate::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use oxrdf::vocab::rdf;
use oxrdf::NamedNode;
use polars::prelude::DataFrame;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

pub const OBJECT_RANK_COL_NAME: &str = "object_rank";
pub const SUBJECT_RANK_COL_NAME: &str = "subject_rank";

#[derive(Debug)]
pub struct CatTriples {
    pub encoded_triples: EncodedTriples,
    pub predicate: NamedNode,
    pub graph: NamedGraph,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub local_cats: Vec<LockedCats>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, PartialOrd, Ord, Serialize, Deserialize)]
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
            CatType::Literal(nn) => BaseRDFNodeType::Literal(nn.clone(), None),
        }
    }

    pub fn from_base_rdf_node_type(bt: &BaseRDFNodeType) -> Self {
        match bt {
            BaseRDFNodeType::IRI => CatType::IRI,
            BaseRDFNodeType::BlankNode => CatType::Blank,
            BaseRDFNodeType::Literal(l, _) => CatType::Literal(l.clone()),
            BaseRDFNodeType::None => {
                unimplemented!("Should not be called for None")
            }
            // Triple term ids are not codes of a string dictionary.
            BaseRDFNodeType::TripleTerm => crate::rdf12::unreachable_triple_term(),
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

    pub fn new_empty(path: Option<&Path>) -> Self {
        Self::new(Cats::new_empty(None, path))
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
    path: Option<PathBuf>,
}

impl Cats {
    pub fn get_spaced_strings(&self, stride: usize, t: &BaseRDFNodeType) -> Vec<(String, String)> {
        if let Some(cat) = self.cat_map.get(&CatType::from_base_rdf_node_type(t)) {
            cat.maps.get_spaced_strings(stride)
        } else {
            vec![]
        }
    }

    /// Whether some string of the given type may start with `prefix` (see
    /// CatMaps::may_have_string_with_prefix).
    pub fn may_have_string_with_prefix(&self, t: &BaseRDFNodeType, prefix: &str) -> bool {
        if let Some(cat) = self.cat_map.get(&CatType::from_base_rdf_node_type(t)) {
            cat.maps.may_have_string_with_prefix(prefix)
        } else {
            false
        }
    }

    /// Whether some string of the given type starts with `prefix`, None if this is not known
    /// (see CatMaps::has_string_with_prefix).
    pub fn has_string_with_prefix(&self, t: &BaseRDFNodeType, prefix: &str) -> Option<bool> {
        if let Some(cat) = self.cat_map.get(&CatType::from_base_rdf_node_type(t)) {
            cat.maps.has_string_with_prefix(prefix)
        } else {
            Some(false)
        }
    }

    pub fn compact(&mut self) {
        let _: Vec<_> = self
            .cat_map
            .iter_mut()
            .map(|(_, e)| e.maps.compact())
            .collect();
        self.recompute_counters();
    }

    pub fn new_local_singular_literal(l: &str, dt: NamedNode, u: u32) -> (u32, Cats) {
        let dt = BaseRDFNodeType::Literal(dt, None);
        let catenc = CatEncs::new_local_singular(l, u, &dt);
        let t = if let BaseRDFNodeType::Literal(dt, _) = dt {
            CatType::Literal(dt)
        } else {
            unreachable!("Should never happen")
        };
        (u, Cats::from_map(HashMap::from([(t, catenc)]), None))
    }

    pub fn new_local_lang_string(val: &str, lang: &str, u: u32) -> (u32, u32, Cats) {
        let dt = BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None);
        let mut catenc = CatEncs::new_local_singular(val, u, &dt);
        let lang_u = u + 1;
        catenc
            .maps
            .encode_new_in_memory_string(Cow::Borrowed(lang), lang_u);
        let t = if let BaseRDFNodeType::Literal(dt, _) = dt {
            CatType::Literal(dt)
        } else {
            unreachable!("Should never happen")
        };
        (
            u,
            lang_u,
            Cats::from_map(HashMap::from([(t, catenc)]), None),
        )
    }

    pub fn new_local_singular_blank(s: &str, u: u32) -> (u32, Cats) {
        let t = CatType::Blank;
        let catenc = CatEncs::new_local_singular(s, u, &BaseRDFNodeType::BlankNode);
        (u, Cats::from_map(HashMap::from([(t, catenc)]), None))
    }

    pub fn new_local_singular_iri(s: &str, u: u32) -> (u32, Cats) {
        let t = CatType::IRI;
        let catenc = CatEncs::new_local_singular(s, u, &BaseRDFNodeType::IRI);
        (u, Cats::from_map(HashMap::from([(t, catenc)]), None))
    }
}

impl Cats {
    pub fn get_cat_encs(&self, bt: &BaseRDFNodeType) -> &CatEncs {
        let ct = CatType::from_base_rdf_node_type(bt);
        self.cat_map.get(&ct).unwrap()
    }

    pub(crate) fn from_map(cat_map: HashMap<CatType, CatEncs>, path: Option<&Path>) -> Self {
        let path_buf = path.map(new_cat_path);
        let mut cats = Cats {
            cat_map,
            iri_counter: 0,
            blank_counter: 0,
            literal_counter_map: Default::default(),
            uuid: Uuid::new_v4().to_string(),
            path: path_buf,
        };
        cats.recompute_counters();
        cats
    }

    fn recompute_counters(&mut self) {
        self.iri_counter = self.calc_new_iri_counter();
        self.blank_counter = self.calc_new_blank_counter();
        self.literal_counter_map = self.calc_new_literal_counter();
    }

    pub fn new_empty(counts_from: Option<&Cats>, path: Option<&Path>) -> Cats {
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
        let path_buf = path.map(new_cat_path);
        Cats {
            cat_map: HashMap::new(),
            blank_counter,
            iri_counter,
            literal_counter_map,
            uuid: Uuid::new_v4().to_string(),
            path: path_buf,
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
        self.literal_counter_map.get(nn).copied().unwrap_or(0)
    }

    pub fn local_rank_maps(
        &self,
        dfs: Vec<&DataFrame>,
        subject_type: &BaseRDFNodeType,
        object_type: &BaseRDFNodeType,
    ) -> HashMap<BaseRDFNodeType, RankMap> {
        let mut src_u_map: HashMap<BaseRDFNodeType, Vec<u32>> = HashMap::new();
        for df in dfs {
            if subject_type.stored_cat() {
                let v = src_u_map.entry(subject_type.clone()).or_default();
                extend_codes(v, df.column(SUBJECT_COL_NAME).unwrap());
            }
            if object_type.stored_cat() {
                let v = src_u_map.entry(object_type.clone()).or_default();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                if object_type.is_lang_string() {
                    let st = o.struct_().unwrap();
                    extend_codes(
                        v,
                        &st.field_by_name(LANG_STRING_VALUE_FIELD)
                            .unwrap()
                            .into_column(),
                    );
                    extend_codes(
                        v,
                        &st.field_by_name(LANG_STRING_LANG_FIELD)
                            .unwrap()
                            .into_column(),
                    );
                } else {
                    extend_codes(v, o);
                }
            }
        }
        src_u_map
            .into_par_iter()
            .map(|(k, v)| {
                let rm = self.sort_ranks(&distinct_codes(v), &k);
                (k, rm)
            })
            .collect()
    }

    /// Positions of the codes `us` (sorted, distinct) in the string order of the whole map.
    pub fn global_ranks(&self, us: &[u32], dt: &BaseRDFNodeType) -> RankMap {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.global_ranks(us)
        } else {
            RankMap::empty()
        }
    }

    /// The positions of all codes of `dt` in the string order of the whole map, the same as
    /// [`Cats::global_ranks`] gives for any codes, so that values can be ranked batch by batch.
    /// `None` when that would be expensive (maps on disk).
    pub fn all_global_ranks(&self, dt: &BaseRDFNodeType) -> Option<RankMap> {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.all_global_ranks()
        } else {
            Some(RankMap::empty())
        }
    }

    /// Ranks that order `us` (sorted, distinct) by string value, valid only for comparing values
    /// within `us` (e.g. to sort a batch of triples).
    pub fn sort_ranks(&self, us: &[u32], dt: &BaseRDFNodeType) -> RankMap {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.sort_ranks(us)
        } else {
            RankMap::empty()
        }
    }

    pub fn rank_map(&self, us: &HashSet<u32>, dt: &BaseRDFNodeType) -> HashMap<u32, u32> {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.global_rank_map(us)
        } else {
            HashMap::new()
        }
    }

    /// Ranks that order `us` by string value, valid only for comparing values within `us`
    /// (e.g. to sort a batch of triples). Do not merge these with ranks from another call.
    pub fn sort_rank_map(&self, us: &HashSet<u32>, dt: &BaseRDFNodeType) -> HashMap<u32, u32> {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.sort_rank_map(us)
        } else {
            HashMap::new()
        }
    }

    pub fn local_rank_map(&self, us: &HashSet<u32>, dt: &BaseRDFNodeType) -> HashMap<u32, u32> {
        if let Some(cm) = self.cat_map.get(&CatType::from_base_rdf_node_type(dt)) {
            cm.maps.local_rank_map(us)
        } else {
            HashMap::new()
        }
    }
}

fn new_cat_path(path: &Path) -> PathBuf {
    path.join(uuid::Uuid::new_v4().to_string())
}

/// Appends the non-null codes of a UInt32 column.
pub fn extend_codes(v: &mut Vec<u32>, c: &Column) {
    let ch = c.u32().unwrap();
    v.reserve(ch.len() - ch.null_count());
    if ch.null_count() == 0 {
        for arr in ch.downcast_iter() {
            v.extend_from_slice(arr.values().as_slice());
        }
    } else {
        v.extend(ch.iter().flatten());
    }
}
