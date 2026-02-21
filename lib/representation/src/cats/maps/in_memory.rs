use crate::cats::maps::CatMaps;
use crate::cats::CatReEnc;
use crate::BaseRDFNodeType;
use arrow::array::{Array, RecordBatch, StringArray, StringViewArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use nohash_hasher::NoHashHasher;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::BuildHasherDefault;
use std::sync::Arc;

#[derive(Debug, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub struct PrefixCompressedString {
    prefix: Arc<String>,
    suffix: Arc<String>,
}

impl Display for PrefixCompressedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.prefix, self.suffix)
    }
}

impl PrefixCompressedString {
    pub fn new(iri: &str, prefix_map: &mut HashMap<String, Arc<String>>) -> Self {
        let (pre, suf) = split_iri(iri);
        let arc_pre = if let Some(arc_pre) = prefix_map.get(pre) {
            arc_pre.clone()
        } else {
            let pre_string = pre.to_string();
            let arc_pre = Arc::new(pre_string.clone());
            prefix_map.insert(pre_string, arc_pre.clone());
            arc_pre
        };
        PrefixCompressedString {
            prefix: arc_pre,
            suffix: Arc::new(suf.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CatMapsInMemory {
    Compressed(PrefixCompressedCatMapsInMemory),
    Uncompressed(UncompressedCatMapsInMemory),
}

impl CatMapsInMemory {
    pub fn inner_join_re_enc(&self, other: &CatMapsInMemory) -> Vec<(u32, u32)> {
        match self {
            CatMapsInMemory::Compressed(m) => {
                if let CatMapsInMemory::Compressed(other) = other {
                    m.inner_join_re_enc(other)
                } else {
                    unreachable!("Should never happen")
                }
            }
            CatMapsInMemory::Uncompressed(m) => {
                if let CatMapsInMemory::Uncompressed(other) = other {
                    m.inner_join_re_enc(other)
                } else {
                    unreachable!("Should never happen")
                }
            }
        }
    }

    pub fn merge(&mut self, other: &CatMapsInMemory, c: &mut u32) -> CatReEnc {
        match self {
            CatMapsInMemory::Compressed(m) => {
                if let CatMapsInMemory::Compressed(other) = other {
                    m.merge(other, c)
                } else {
                    unreachable!("Should never happen")
                }
            }
            CatMapsInMemory::Uncompressed(m) => {
                if let CatMapsInMemory::Uncompressed(other) = other {
                    m.merge(other, c)
                } else {
                    unreachable!("Should never happen")
                }
            }
        }
    }

    pub fn rank_map(&self, us: HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMapsInMemory::Compressed(c) => c.rank_map(us),
            CatMapsInMemory::Uncompressed(u) => u.rank_map(us),
        }
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
        match self {
            CatMapsInMemory::Compressed(m) => m.contains_u32(u),
            CatMapsInMemory::Uncompressed(m) => m.contains_u32(u),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            CatMapsInMemory::Compressed(m) => m.is_empty(),
            CatMapsInMemory::Uncompressed(m) => m.is_empty(),
        }
    }

    pub fn maybe_encode_strs(&self, s: &[Option<&str>]) -> Vec<Option<u32>> {
        s.into_iter()
            .map(|x| {
                if let Some(x) = x {
                    self.maybe_encode_in_memory_str(x)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn encode_all_new_non_duplicated_strings(
        &mut self,
        maybe_new_strings: Vec<String>,
        c: &mut u32,
    ) {
        let maybes: Vec<_> = maybe_new_strings.iter().map(|x| Some(x.as_str())).collect();
        let maybe_encoded = self.maybe_encode_strs(&maybes);
        for (s, u) in maybe_new_strings.into_iter().zip(maybe_encoded) {
            if u.is_none() {
                self.encode_new_in_memory_string(s, *c);
                *c += 1;
            }
        }
    }

    pub fn encode_new_in_memory_string(&mut self, s: String, u: u32) {
        match self {
            CatMapsInMemory::Compressed(m) => {
                m.encode_new_string(s, u);
            }
            CatMapsInMemory::Uncompressed(m) => {
                m.encode_new_string(s, u);
            }
        }
    }

    pub fn maybe_encode_in_memory_str(&self, s: &str) -> Option<u32> {
        match self {
            CatMapsInMemory::Compressed(m) => m.maybe_encode_str(s),
            CatMapsInMemory::Uncompressed(m) => m.maybe_encode_str(s),
        }
    }

    pub fn counter(&self) -> u32 {
        match self {
            CatMapsInMemory::Compressed(m) => m.counter(),
            CatMapsInMemory::Uncompressed(m) => m.counter(),
        }
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        match self {
            CatMapsInMemory::Compressed(m) => m.decode_batch(v),
            CatMapsInMemory::Uncompressed(m) => m.decode_batch(v),
        }
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<'_, str>> {
        match self {
            CatMapsInMemory::Compressed(m) => m.maybe_decode(u),
            CatMapsInMemory::Uncompressed(m) => m.maybe_decode(u),
        }
    }

    pub fn image(&self, s: &HashSet<u32>) -> Option<CatMaps> {
        let i = match self {
            CatMapsInMemory::Compressed(m) => m.image(s).map(CatMapsInMemory::Compressed),
            CatMapsInMemory::Uncompressed(m) => m.image(None, s).map(CatMapsInMemory::Uncompressed),
        };
        i.map(CatMaps::InMemory)
    }

    pub fn new_empty(bt: &BaseRDFNodeType) -> CatMapsInMemory {
        if bt.is_iri() {
            CatMapsInMemory::Compressed(PrefixCompressedCatMapsInMemory::new_empty())
        } else {
            CatMapsInMemory::Uncompressed(UncompressedCatMapsInMemory::new_empty())
        }
    }
}

impl CatMapsInMemory {
    pub fn new_singular(value: &str, u: u32, bt: &BaseRDFNodeType) -> Self {
        if bt.is_iri() {
            CatMapsInMemory::Compressed(PrefixCompressedCatMapsInMemory::new_singular(value, u))
        } else {
            CatMapsInMemory::Uncompressed(UncompressedCatMapsInMemory::new_singular(value, u))
        }
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        match self {
            CatMapsInMemory::Compressed(m) => m.to_record_batch(),
            CatMapsInMemory::Uncompressed(m) => m.to_record_batch(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrefixCompressedCatMapsInMemory {
    map: BTreeMap<PrefixCompressedString, u32>,
    rev_map: HashMap<u32, PrefixCompressedString, BuildHasherDefault<NoHashHasher<u32>>>,
    prefix_map: HashMap<String, Arc<String>>,
}

impl PrefixCompressedCatMapsInMemory {
    pub fn new_empty() -> PrefixCompressedCatMapsInMemory {
        PrefixCompressedCatMapsInMemory {
            map: Default::default(),
            rev_map: Default::default(),
            prefix_map: Default::default(),
        }
    }

    pub fn encode_new_prefix_compressed_string(&mut self, s: PrefixCompressedString, u: u32) {
        self.encode_or_add_new_prefix(&s.prefix);
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn contains_str(&self, s: &str) -> bool {
        let (pre, suf) = split_iri(s);
        if let Some(pre) = self.prefix_map.get(pre) {
            let compr = PrefixCompressedString {
                prefix: pre.clone(),
                suffix: Arc::new(suf.to_string()),
            };
            self.map.contains_key(&compr)
        } else {
            false
        }
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
        self.rev_map.contains_key(u)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn height(&self) -> u32 {
        self.map.len() as u32
    }

    pub fn encode_new_string(&mut self, s: String, u: u32) {
        self.encode_new_str(&s, u)
    }

    fn encode_new_str(&mut self, s: &str, u: u32) {
        let (pre, suf) = split_iri(&s);
        let arc_pre = self.encode_or_add_new_prefix_str(pre);
        let compr = PrefixCompressedString {
            prefix: arc_pre,
            suffix: Arc::new(suf.to_string()),
        };
        self.map.insert(compr.clone(), u);
        self.rev_map.insert(u, compr);
    }

    fn encode_or_add_new_prefix(&mut self, pre: &Arc<String>) -> Arc<String> {
        if let Some(arc_pre) = self.prefix_map.get(pre.as_str()) {
            arc_pre.clone()
        } else {
            self.prefix_map.insert(pre.to_string(), pre.clone());
            pre.clone()
        }
    }

    fn encode_or_add_new_prefix_str(&mut self, pre: &str) -> Arc<String> {
        if let Some(arc_pre) = self.prefix_map.get(pre.as_str()) {
            arc_pre.clone()
        } else {
            let pre_string = pre.to_string();
            let arc_pre = Arc::new(pre_string.clone());
            self.prefix_map.insert(pre_string, arc_pre.clone());
            arc_pre
        }
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<u32> {
        let (pre, suf) = split_iri(s);
        if let Some(pre) = self.prefix_map.get(pre) {
            let compr = PrefixCompressedString {
                prefix: pre.clone(),
                suffix: Arc::new(suf.to_string()),
            };
            self.map.get(&compr).map(|u| *u)
        } else {
            None
        }
    }

    pub fn maybe_encode_prefix_compressed(&self, p: &PrefixCompressedString) -> Option<&u32> {
        self.map.get(p)
    }

    pub fn new_singular(value: &str, u: u32) -> Self {
        let mut sing = PrefixCompressedCatMapsInMemory::new_empty();
        sing.encode_new_str(value, u);
        sing
    }

    pub fn counter(&self) -> u32 {
        self.rev_map.keys().max().unwrap().clone() + 1
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        let decoded_vec_iter = v
            .into_par_iter()
            .map(|x| x.map(|x| Cow::Owned(self.rev_map.get(&x).unwrap().to_string())));
        decoded_vec_iter.collect()
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<'_, str>> {
        self.rev_map.get(u).map(|x| Cow::Owned(x.to_string()))
    }

    pub fn image(&self, s: &HashSet<u32>) -> Option<PrefixCompressedCatMapsInMemory> {
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
            let mut prefix_map = HashMap::new();
            for p in new_map.values() {
                if !prefix_map.contains_key(p.prefix.as_str()) {
                    prefix_map.insert(p.prefix.to_string(), p.prefix.clone());
                }
            }
            Some(Self {
                map,
                rev_map: new_map,
                prefix_map,
            })
        }
    }

    pub fn inner_join_re_enc(&self, other: &PrefixCompressedCatMapsInMemory) -> Vec<(u32, u32)> {
        let renc: Vec<_> = self
            .map
            .iter()
            .map(|(x, l)| {
                if let Some(r) = other.maybe_encode_prefix_compressed(x) {
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

    pub fn merge(&mut self, other: &PrefixCompressedCatMapsInMemory, c: &mut u32) -> CatReEnc {
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
            self.encode_new_prefix_compressed_string(s.clone(), u);
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

    pub fn to_record_batch(&self) -> RecordBatch {
        let mut us = Vec::new();
        let mut strs = Vec::new();
        for (k, u) in self.map.range(..) {
            let s = format!(
                "{}{}",
                self.prefix_map.get(k.prefix.as_str()).unwrap(),
                &k.suffix
            );
            strs.push(s);
            us.push(*u);
        }
        let strs = StringArray::from_iter(strs.iter().map(|x| Some(x.as_str())));
        let us = UInt32Array::from_iter(us);

        let schema = Schema::new(vec![
            Field::new("str", strs.data_type().clone(), true),
            Field::new("cat", us.data_type().clone(), true),
        ]);
        let rb =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(us)]).unwrap();
        rb
    }

    pub fn rank_map(&self, us: HashSet<u32>) -> HashMap<u32, u32> {
        let mut ranked = HashMap::new();
        let mut rank = 0;
        for (_, v) in self.map.range(..) {
            if us.contains(v) {
                ranked.insert(*v, rank);
                rank += 1;
            }
        }
        ranked
    }
}

#[derive(Debug, Clone)]
pub struct UncompressedCatMapsInMemory {
    map: BTreeMap<Arc<String>, u32>,
    rev_map: HashMap<u32, Arc<String>, BuildHasherDefault<NoHashHasher<u32>>>,
}

impl UncompressedCatMapsInMemory {
    pub fn new_empty() -> UncompressedCatMapsInMemory {
        UncompressedCatMapsInMemory {
            map: Default::default(),
            rev_map: Default::default(),
        }
    }

    pub fn encode_new_arc_string(&mut self, s: Arc<String>, u: u32) {
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn contains_str(&self, s: &str) -> bool {
        let arcs = Arc::new(s.to_string());
        self.map.contains_key(&arcs)
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
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

    pub fn maybe_encode_str(&self, s: &str) -> Option<u32> {
        let s = Arc::new(s.to_string());
        self.map.get(&s).map(|u| *u)
    }

    pub fn new_singular(value: &str, u: u32) -> Self {
        let mut sing = UncompressedCatMapsInMemory::new_empty();
        let s = Arc::new(value.to_string());
        sing.map.insert(s.clone(), u);
        sing.rev_map.insert(u, s);
        sing
    }

    pub fn counter(&self) -> u32 {
        self.rev_map.keys().max().unwrap().clone() + 1
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        let decoded_vec_iter = v
            .into_par_iter()
            .map(|x| x.map(|x| Cow::Borrowed(self.rev_map.get(&x).unwrap().as_str())));
        decoded_vec_iter.collect()
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<'_, str>> {
        self.rev_map.get(u).map(|x| Cow::Borrowed(x.as_str()))
    }

    pub fn image(
        &self,
        _: Option<&std::path::Path>,
        s: &HashSet<u32>,
    ) -> Option<UncompressedCatMapsInMemory> {
        let new_rev_map: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = s
            .par_iter() // go over each u32 in s
            .map(|x| {
                // Get the reverse lookup string
                if let Some(s) = self.rev_map.get(x) {
                    Some((s.clone(), x))
                } else {
                    None
                }
            })
            .filter(|x| x.is_some()) // Only keep the ones which were present
            .map(|x| x.unwrap())
            .map(|(x, y)| (*y, x)) // reverse the tuple to construct a rev_map
            .collect();
        if new_rev_map.is_empty() {
            None
        } else {
            let map = new_rev_map.iter().map(|(x, y)| (y.clone(), *x)).collect(); // Flip it back again
            Some(Self {
                map,
                rev_map: new_rev_map,
            })
        }
    }

    pub fn inner_join_re_enc(&self, other: &UncompressedCatMapsInMemory) -> Vec<(u32, u32)> {
        let renc: Vec<_> = self
            .map
            .iter()
            .map(|(x, l)| {
                if let Some(r) = other.maybe_encode_str(x.as_str()) {
                    if *l != r {
                        Some((*l, r))
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

    pub fn merge(&mut self, other: &UncompressedCatMapsInMemory, c: &mut u32) -> CatReEnc {
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
            self.encode_new_arc_string(s, u);
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

    pub fn to_record_batch(&self) -> RecordBatch {
        let mut us = Vec::new();
        let mut strs = Vec::new();
        for (k, u) in self.map.range::<Arc<String>, _>(..) {
            strs.push(k.as_str());
            us.push(*u);
        }

        let strs = StringArray::from_iter(strs.iter().map(|x| Some(*x)));
        let us = UInt32Array::from_iter(us);
        let schema = Schema::new(vec![
            Field::new("str", strs.data_type().clone(), true),
            Field::new("cat", us.data_type().clone(), true),
        ]);
        let rb =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(us)]).unwrap();
        rb
    }

    pub fn rank_map(&self, us: HashSet<u32>) -> HashMap<u32, u32> {
        let mut ranked = HashMap::new();
        let mut rank = 0;
        for (_, v) in self.map.range::<Arc<String>, _>(..) {
            if us.contains(v) {
                ranked.insert(*v, rank);
                rank += 1;
            }
        }
        ranked
    }
}

impl CatMapsInMemory {
    pub fn height(&self) -> u32 {
        match self {
            CatMapsInMemory::Compressed(m) => m.height(),
            CatMapsInMemory::Uncompressed(m) => m.height(),
        }
    }
}

pub fn split_iri(iri: &str) -> (&str, &str) {
    // Apache 2 / MIT The Rust Project Contributors
    #[inline]
    fn rsplit_once_inclusive_l<P: std::str::pattern::Pattern>(
        this: &str,
        delimiter: P,
    ) -> Option<(&'_ str, &'_ str)>
    where
        for<'a> P::Searcher<'a>: std::str::pattern::ReverseSearcher<'a>,
    {
        let (_, end) = std::str::pattern::ReverseSearcher::next_match_back(
            &mut delimiter.into_searcher(this),
        )?;
        // SAFETY: `Searcher` is known to return valid indices.
        unsafe { Some((this.get_unchecked(..end), this.get_unchecked(end..))) }
    }

    const DELIMITERS: &[char] = &['/', '#', ':'];

    let (prefix, suffix) = match rsplit_once_inclusive_l(iri, DELIMITERS) {
        Some(pair) => pair,
        None => ("", iri),
    };
    (prefix, suffix)
}
