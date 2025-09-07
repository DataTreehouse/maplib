mod decode;
mod encode;
mod globalize;
mod image;
mod re_encode;
mod split;

pub use decode::*;
pub use encode::*;
pub use globalize::*;
pub use image::*;
pub use re_encode::*;
pub use split::*;

use crate::BaseRDFNodeType;
use nohash_hasher::NoHashHasher;
use oxrdf::NamedNode;
use polars::prelude::DataFrame;
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use uuid::Uuid;

const SUBJECT_PREFIX_COL_NAME: &str = "subject_prefix";
const OBJECT_PREFIX_COL_NAME: &str = "object_prefix";

pub const OBJECT_RANK_COL_NAME: &str = "object_rank";
pub const SUBJECT_RANK_COL_NAME: &str = "subject_rank";

pub struct CatTriples {
    pub encoded_triples: Vec<EncodedTriples>,
    pub predicate: NamedNode,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub local_cats: Vec<Arc<Cats>>,
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
    pub map: BTreeMap<String, u32>,
    pub rev_map: HashMap<u32, String, BuildHasherDefault<NoHashHasher<u32>>>,
}

#[derive(Debug, Clone)]
pub struct Cats {
    pub cat_map: HashMap<CatType, CatEncs>,
    iri_height: u32,
    blank_height: u32,
    literal_height_map: HashMap<NamedNode, u32>,
    pub uuid: String,
}

impl Cats {
    pub fn new_singular_literal(l: &str, dt: NamedNode, u: u32) -> (u32, Cats) {
        let catenc = CatEncs::new_singular(l, u);
        let t = CatType::Literal(dt);
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }

    pub(crate) fn new_singular_blank(s: &str, u: u32) -> (u32, Cats) {
        let catenc = CatEncs::new_singular(s, u);
        (u, Cats::from_map(HashMap::from([(CatType::Blank, catenc)])))
    }

    pub fn new_singular_iri(s: &str, u: u32) -> (u32, Cats) {
        let (pre, suf) = rdf_split_iri_str(s);
        let catenc = CatEncs::new_singular(suf, u);
        let t = CatType::Prefix(NamedNode::new_unchecked(pre));
        (u, Cats::from_map(HashMap::from([(t, catenc)])))
    }
}

impl Cats {
    pub(crate) fn from_map(cat_map: HashMap<CatType, CatEncs>) -> Self {
        let mut cats = Cats {
            cat_map,
            iri_height: 0,
            blank_height: 0,
            literal_height_map: Default::default(),
            uuid: Uuid::new_v4().to_string(),
        };
        cats.iri_height = cats.calc_new_iri_height();
        cats.blank_height = cats.calc_new_blank_height();
        cats.literal_height_map = cats.calc_new_literal_height();
        cats
    }

    pub fn new_empty() -> Cats {
        Cats {
            cat_map: HashMap::new(),
            blank_height: 0,
            iri_height: 0,
            literal_height_map: Default::default(),
            uuid: uuid::Uuid::new_v4().to_string(),
        }
    }

    fn calc_new_blank_height(&self) -> u32 {
        let mut height = 0;
        if let Some(enc) = self.cat_map.get(&CatType::Blank) {
            height += enc.height();
        }
        height
    }

    fn calc_new_iri_height(&self) -> u32 {
        let mut height = 0;
        for (p, cat) in &self.cat_map {
            if matches!(p, CatType::Prefix(_)) {
                height += cat.height();
            }
        }
        height
    }

    fn get_height(&self, c: &CatType) -> u32 {
        match c {
            CatType::Prefix(..) => self.get_iri_height(),
            CatType::Blank => self.get_blank_height(),
            CatType::Literal(nn) => self.get_literal_height(nn),
        }
    }

    fn set_height(&mut self, u: u32, c: &CatType) {
        match c {
            CatType::Prefix(..) => {
                self.iri_height = u;
            }
            CatType::Blank => {
                self.blank_height = u;
            }
            CatType::Literal(nn) => {
                self.literal_height_map.insert(nn.clone(), u);
            }
        }
    }

    pub fn get_iri_height(&self) -> u32 {
        self.iri_height
    }
    pub fn get_blank_height(&self) -> u32 {
        self.blank_height
    }
    pub fn get_literal_height(&self, nn: &NamedNode) -> u32 {
        self.literal_height_map.get(nn).map(|x| *x).unwrap_or(0)
    }

    fn calc_new_literal_height(&self) -> HashMap<NamedNode, u32> {
        let mut map = HashMap::new();
        for (p, cat) in &self.cat_map {
            if let CatType::Literal(nn) = p {
                map.insert(nn.clone(), cat.height());
            }
        }
        map
    }
}
