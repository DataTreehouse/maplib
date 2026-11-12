#![feature(pattern)]
#![feature(str_as_str)]

use arrow::array::{Array, RecordBatch, StringArray, StringBuilder, UInt32Array};
use arrow::datatypes::{Field, Schema};
use nohash_hasher::NoHashHasher;
use range_set_blaze::RangeSetBlaze;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter, Write};
use std::hash::BuildHasherDefault;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PrefixCompressedString {
    // Arc<str> keeps the bytes inline with the refcount: one allocation instead of two.
    prefix: Arc<str>,
    suffix: Arc<str>,
}

impl PartialEq for PrefixCompressedString {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.suffix == other.suffix
    }
}

impl Eq for PrefixCompressedString {}

impl PartialOrd for PrefixCompressedString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrefixCompressedString {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_split_strs(self.parts(), other.parts())
    }
}

/// Orders two strings that are each given as (prefix, suffix), as if the parts were concatenated.
#[inline]
fn cmp_split_strs((a_pre, a_suf): (&str, &str), (b_pre, b_suf): (&str, &str)) -> Ordering {
    // Prefixes are interned, so identical prefixes usually share the same allocation.
    if a_pre.as_ptr() == b_pre.as_ptr() && a_pre.len() == b_pre.len() {
        return a_suf.as_bytes().cmp(b_suf.as_bytes());
    }
    let (a_pre, a_suf, b_pre, b_suf) = (
        a_pre.as_bytes(),
        a_suf.as_bytes(),
        b_pre.as_bytes(),
        b_suf.as_bytes(),
    );
    let (mut a, mut a_on_suffix) = (a_pre, false);
    let (mut b, mut b_on_suffix) = (b_pre, false);
    loop {
        if a.is_empty() {
            if a_on_suffix {
                break;
            }
            a = a_suf;
            a_on_suffix = true;
            continue;
        }
        if b.is_empty() {
            if b_on_suffix {
                break;
            }
            b = b_suf;
            b_on_suffix = true;
            continue;
        }
        let n = a.len().min(b.len());
        match a[..n].cmp(&b[..n]) {
            Ordering::Equal => {}
            o => return o,
        }
        a = &a[n..];
        b = &b[n..];
    }
    (a_pre.len() + a_suf.len()).cmp(&(b_pre.len() + b_suf.len()))
}

/// Lets the prefix compressed map be probed with borrowed parts, without allocating a key.
trait PrefixCompressedKey {
    fn parts(&self) -> (&str, &str);
}

impl PrefixCompressedKey for PrefixCompressedString {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        (&self.prefix, &self.suffix)
    }
}

struct PrefixCompressedRef<'a> {
    prefix: &'a str,
    suffix: &'a str,
}

impl PrefixCompressedKey for PrefixCompressedRef<'_> {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        (self.prefix, self.suffix)
    }
}

impl<'a> Borrow<dyn PrefixCompressedKey + 'a> for PrefixCompressedString {
    fn borrow(&self) -> &(dyn PrefixCompressedKey + 'a) {
        self
    }
}

impl PartialEq for dyn PrefixCompressedKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        cmp_split_strs(self.parts(), other.parts()) == Ordering::Equal
    }
}

impl Eq for dyn PrefixCompressedKey + '_ {}

impl PartialOrd for dyn PrefixCompressedKey + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn PrefixCompressedKey + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_split_strs(self.parts(), other.parts())
    }
}

impl Display for PrefixCompressedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.prefix, self.suffix)
    }
}

impl PrefixCompressedString {
    pub fn new(iri: &str, prefix_map: &mut HashSet<Arc<str>>) -> Self {
        let (pre, suf) = split_iri(iri);
        PrefixCompressedString {
            prefix: intern_prefix(prefix_map, pre),
            suffix: Arc::from(suf),
        }
    }
}

/// Returns the shared allocation for a prefix, adding it to the set if it is new.
fn intern_prefix(prefix_map: &mut HashSet<Arc<str>>, pre: &str) -> Arc<str> {
    if let Some(arc_pre) = prefix_map.get(pre) {
        arc_pre.clone()
    } else {
        let arc_pre: Arc<str> = Arc::from(pre);
        prefix_map.insert(arc_pre.clone());
        arc_pre
    }
}

#[derive(Debug, Clone)]
pub enum CatMapsInMemory {
    Compressed(PrefixCompressedCatMapsInMemory),
    Uncompressed(UncompressedCatMapsInMemory),
}

impl CatMapsInMemory {
    pub fn get_spaced_strings(&self, stride: usize) -> Vec<(String, String)> {
        match self {
            CatMapsInMemory::Compressed(c) => c.get_spaced_strings(stride),
            CatMapsInMemory::Uncompressed(u) => u.get_spaced_strings(stride),
        }
    }

    pub fn compact(&mut self) {
        match self {
            CatMapsInMemory::Compressed(c) => c.compact(),
            CatMapsInMemory::Uncompressed(u) => u.compact(),
        }
    }

    pub fn garbage_collect_cats(&mut self, p0: RangeSetBlaze<u32>) {
        match self {
            CatMapsInMemory::Compressed(c) => {
                c.garbage_collect_cats(p0);
            }
            CatMapsInMemory::Uncompressed(u) => {
                u.garbage_collect_cats(p0);
            }
        }
    }
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

    pub fn merge(
        &mut self,
        other: &CatMapsInMemory,
        c: &mut u32,
    ) -> HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>> {
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

    pub fn global_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        match self {
            CatMapsInMemory::Compressed(c) => c.global_rank_map(us),
            CatMapsInMemory::Uncompressed(u) => u.global_rank_map(us),
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
        s.par_iter()
            .with_min_len(4096)
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
                self.encode_new_in_memory_string(Cow::Owned(s), *c);
                *c += 1;
            }
        }
    }

    pub fn encode_new_in_memory_string(&mut self, s: Cow<'_, str>, u: u32) {
        match self {
            CatMapsInMemory::Compressed(m) => {
                m.encode_new_string(&*s, u);
            }
            CatMapsInMemory::Uncompressed(m) => {
                m.encode_new_string(s.into_owned(), u);
            }
        }
    }

    pub fn from_ordered_vec(ss: Vec<(Cow<'_, str>, u32)>, is_iri: bool) -> Self {
        if is_iri {
            CatMapsInMemory::Compressed(PrefixCompressedCatMapsInMemory::from_ordered_vec(ss))
        } else {
            CatMapsInMemory::Uncompressed(UncompressedCatMapsInMemory::from_ordered_vec(ss))
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

    pub fn image(&self, s: &HashSet<u32>) -> Option<CatMapsInMemory> {
        let i = match self {
            CatMapsInMemory::Compressed(m) => m.image(s).map(CatMapsInMemory::Compressed),
            CatMapsInMemory::Uncompressed(m) => m.image(None, s).map(CatMapsInMemory::Uncompressed),
        };
        i
    }

    pub fn new_empty(is_iri: bool) -> CatMapsInMemory {
        if is_iri {
            CatMapsInMemory::Compressed(PrefixCompressedCatMapsInMemory::new_empty())
        } else {
            CatMapsInMemory::Uncompressed(UncompressedCatMapsInMemory::new_empty())
        }
    }

    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        match self {
            CatMapsInMemory::Compressed(c) => c.range_set(),
            CatMapsInMemory::Uncompressed(u) => u.range_set(),
        }
    }
}

impl CatMapsInMemory {
    pub fn new_singular(value: &str, u: u32, is_iri: bool) -> Self {
        if is_iri {
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
    prefix_map: HashSet<Arc<str>>,
}

impl PrefixCompressedCatMapsInMemory {
    pub fn get_spaced_strings(&self, stride: usize) -> Vec<(String, String)> {
        let mut spaced = vec![];
        let mut i = 0;
        let mut start_string = None;
        for (k, _) in self.map.iter() {
            if start_string.is_none() {
                start_string = Some(k.clone());
            }
            if i != 0 && i % stride == 0 || i == self.map.len() - 1 {
                spaced.push((
                    start_string.take().unwrap().to_string(),
                    k.clone().to_string(),
                ));
            }
            i = i + 1;
        }
        spaced
    }

    fn from_ordered_vec(ss: Vec<(Cow<str>, u32)>) -> PrefixCompressedCatMapsInMemory {
        let split_vec = ss
            .par_iter()
            .map(|(s, u)| {
                let (pre, suf) = split_iri(s.as_str());
                (pre, Arc::<str>::from(suf), *u)
            })
            .collect::<Vec<_>>();
        let mut prefix_map: HashSet<Arc<str>> = HashSet::new();
        let mut prefix_compressed_vec = Vec::with_capacity(split_vec.len());
        for (pre, suf, u) in split_vec {
            let pre_arc = intern_prefix(&mut prefix_map, pre);
            let pc_string = PrefixCompressedString {
                prefix: pre_arc,
                suffix: suf,
            };
            prefix_compressed_vec.push((pc_string, u))
        }
        drop(ss);
        let map = BTreeMap::from_iter(prefix_compressed_vec.into_iter());
        let mut rev_map =
            HashMap::with_capacity_and_hasher(map.len(), BuildHasherDefault::default());
        rev_map.extend(map.iter().map(|(x, y)| (*y, x.clone())));
        PrefixCompressedCatMapsInMemory {
            map,
            rev_map,
            prefix_map,
        }
    }
}

impl PrefixCompressedCatMapsInMemory {
    pub(crate) fn compact(&mut self) {
        for (i, (_, v)) in self
            .map
            .range_mut::<PrefixCompressedString, _>(..)
            .enumerate()
        {
            *v = i as u32;
        }
        self.rev_map = self.map.iter().map(|(x, y)| (*y, x.clone())).collect();
    }

    pub(crate) fn garbage_collect_cats(&mut self, p0: RangeSetBlaze<u32>) {
        let mut to_delete = Vec::new();
        for r in p0 {
            to_delete.push(self.rev_map.remove(&r).unwrap());
        }
        for r in to_delete {
            self.map.remove(&r).unwrap();
        }
    }

    pub fn new_empty() -> PrefixCompressedCatMapsInMemory {
        PrefixCompressedCatMapsInMemory {
            map: Default::default(),
            rev_map: Default::default(),
            prefix_map: Default::default(),
        }
    }

    pub fn encode_new_prefix_compressed_string(&mut self, mut s: PrefixCompressedString, u: u32) {
        // Share our own prefix allocation rather than keeping the other map's copy alive
        s.prefix = intern_prefix(&mut self.prefix_map, &s.prefix);
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn contains_str(&self, s: &str) -> bool {
        self.maybe_encode_str(s).is_some()
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

    pub fn encode_new_string(&mut self, s: &str, u: u32) {
        self.encode_new_str(s, u)
    }

    fn encode_new_str(&mut self, s: &str, u: u32) {
        let (pre, suf) = split_iri(s);
        let compr = PrefixCompressedString {
            prefix: intern_prefix(&mut self.prefix_map, pre),
            suffix: Arc::from(suf),
        };
        self.map.insert(compr.clone(), u);
        self.rev_map.insert(u, compr);
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<u32> {
        let (pre, suf) = split_iri(s);
        // Use the interned prefix so that comparisons can take the pointer equality fast path
        let pre = self.prefix_map.get(pre)?;
        let key = PrefixCompressedRef {
            prefix: pre,
            suffix: suf,
        };
        self.map
            .get(&key as &dyn PrefixCompressedKey)
            .copied()
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
        // We must tolerate missing values here, as values may be in global instead of local
        let decoded_vec_iter = v.into_par_iter().map(|u| {
            if let Some(u) = u {
                if let Some(r) = self.rev_map.get(&u) {
                    Some(Cow::Owned(r.to_string()))
                } else {
                    None
                }
            } else {
                None
            }
        });
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
            let prefix_map: HashSet<Arc<str>> =
                new_map.values().map(|p| p.prefix.clone()).collect();
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

    pub fn merge(
        &mut self,
        other: &PrefixCompressedCatMapsInMemory,
        c: &mut u32,
    ) -> HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>> {
        let (remap, insert): (Vec<_>, Vec<_>) = other
            .map
            .par_iter()
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
            self.encode_new_prefix_compressed_string(s, u);
        }
        let remap: HashMap<_, _, BuildHasherDefault<NoHashHasher<u32>>> = remap
            .into_iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .chain(new_remap.into_iter())
            .collect();
        remap
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        let data_len: usize = self
            .map
            .keys()
            .map(|k| k.prefix.len() + k.suffix.len())
            .sum();
        let mut strs = StringBuilder::with_capacity(self.map.len(), data_len);
        let mut us = Vec::with_capacity(self.map.len());
        for (k, u) in self.map.iter() {
            // Write the parts straight into the arrow buffer instead of allocating a String per row
            strs.write_str(&k.prefix).unwrap();
            strs.write_str(&k.suffix).unwrap();
            strs.append_value("");
            us.push(*u);
        }
        let strs = strs.finish();
        let us = UInt32Array::from(us);

        let schema = Schema::new(vec![
            Field::new("str", strs.data_type().clone(), true),
            Field::new("cat", us.data_type().clone(), true),
        ]);
        let rb =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(us)]).unwrap();
        rb
    }

    pub fn global_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        let mut ranked = HashMap::new();
        for (i, (_, v)) in self.map.iter().enumerate() {
            if us.contains(v) {
                ranked.insert(*v, i as u32);
            }
        }
        ranked
    }

    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        RangeSetBlaze::from_iter(self.rev_map.keys())
    }
}

#[derive(Debug, Clone)]
pub struct UncompressedCatMapsInMemory {
    map: BTreeMap<Arc<str>, u32>,
    rev_map: HashMap<u32, Arc<str>, BuildHasherDefault<NoHashHasher<u32>>>,
}

impl UncompressedCatMapsInMemory {
    pub fn get_spaced_strings(&self, stride: usize) -> Vec<(String, String)> {
        let mut spaced = vec![];
        let mut i = 0;
        let mut start_string = None;
        for (k, _) in self.map.range::<Arc<str>, _>(..) {
            if start_string.is_none() {
                start_string = Some(k.clone());
            }
            if i != 0 && i % stride == 0 || i == self.map.len() - 1 {
                spaced.push((
                    start_string.take().unwrap().to_string(),
                    k.clone().to_string(),
                ));
            }
            i = i + 1;
        }
        spaced
    }

    fn from_ordered_vec(ss: Vec<(Cow<str>, u32)>) -> UncompressedCatMapsInMemory {
        let ss_arced = ss
            .into_par_iter()
            .map(|(s, u)| (Arc::<str>::from(s.into_owned()), u))
            .collect::<Vec<_>>();
        let map = BTreeMap::from_iter(ss_arced.into_iter());
        let mut rev_map =
            HashMap::with_capacity_and_hasher(map.len(), BuildHasherDefault::default());
        rev_map.extend(map.iter().map(|(s, u)| (*u, s.clone())));
        UncompressedCatMapsInMemory { map, rev_map }
    }
}

impl UncompressedCatMapsInMemory {
    pub(crate) fn compact(&mut self) {
        for (i, (_, v)) in self.map.range_mut::<Arc<str>, _>(..).enumerate() {
            *v = i as u32;
        }
        self.rev_map = self.map.iter().map(|(x, y)| (*y, x.clone())).collect();
    }

    pub fn new_empty() -> UncompressedCatMapsInMemory {
        UncompressedCatMapsInMemory {
            map: Default::default(),
            rev_map: Default::default(),
        }
    }

    pub fn encode_new_arc_string(&mut self, s: Arc<str>, u: u32) {
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn contains_str(&self, s: &str) -> bool {
        self.map.contains_key(s)
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
        let s: Arc<str> = Arc::from(s);
        self.map.insert(s.clone(), u);
        self.rev_map.insert(u, s);
    }

    pub fn maybe_encode_str(&self, s: &str) -> Option<u32> {
        self.map.get(s).copied()
    }

    pub fn new_singular(value: &str, u: u32) -> Self {
        let mut sing = UncompressedCatMapsInMemory::new_empty();
        let s: Arc<str> = Arc::from(value);
        sing.map.insert(s.clone(), u);
        sing.rev_map.insert(u, s);
        sing
    }

    pub fn counter(&self) -> u32 {
        self.rev_map.keys().max().unwrap().clone() + 1
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        // We must tolerate missing values here, as values may be in global instead of local
        let decoded_vec_iter = v.into_par_iter().map(|u| {
            if let Some(u) = u {
                if let Some(r) = self.rev_map.get(&u) {
                    Some(Cow::Borrowed(r.as_str()))
                } else {
                    None
                }
            } else {
                None
            }
        });
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

    pub fn merge(
        &mut self,
        other: &UncompressedCatMapsInMemory,
        c: &mut u32,
    ) -> HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>> {
        let (remap, insert): (Vec<_>, Vec<_>) = other
            .map
            .par_iter()
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
        remap
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        let strs = StringArray::from_iter_values(self.map.keys().map(|k| &**k));
        let us = UInt32Array::from_iter_values(self.map.values().copied());
        let schema = Schema::new(vec![
            Field::new("str", strs.data_type().clone(), true),
            Field::new("cat", us.data_type().clone(), true),
        ]);
        let rb =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(us)]).unwrap();
        rb
    }

    pub fn global_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        let mut ranked = HashMap::new();
        for (i, (_, v)) in self.map.range::<Arc<str>, _>(..).enumerate() {
            if us.contains(v) {
                ranked.insert(*v, i as u32);
            }
        }
        ranked
    }

    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        RangeSetBlaze::from_iter(self.rev_map.keys())
    }

    pub fn garbage_collect_cats(&mut self, range_set: RangeSetBlaze<u32>) {
        // Får inn et range set med U32-er. Disse skal fjernes fra catmapet, og så brukes til å fjerne strenger fra revmapet
        let mut to_delete = Vec::new();
        for r in range_set {
            to_delete.push(self.rev_map.remove(&r).unwrap());
        }
        for t in to_delete {
            self.map.remove(&t);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_compare_matches_concatenated_compare() {
        let strs = [
            "", "a", "ab", "abc", "abd", "b", "ba", "http://ex.org/a", "http://ex.org/ab",
            "http://ex.org#a", "http://ex.org/", "http://ex.org", "æøå", "æøb",
        ];
        for a in strs {
            for b in strs {
                for i in 0..=a.len() {
                    if !a.is_char_boundary(i) {
                        continue;
                    }
                    for j in 0..=b.len() {
                        if !b.is_char_boundary(j) {
                            continue;
                        }
                        let got = cmp_split_strs((&a[..i], &a[i..]), (&b[..j], &b[j..]));
                        assert_eq!(got, a.cmp(b), "{a:?} split at {i} vs {b:?} split at {j}");
                    }
                }
            }
        }
    }

    #[test]
    fn prefix_compressed_lookup_without_allocation() {
        let iris = [
            "http://ex.org/ns#b",
            "http://ex.org/ns#a",
            "http://other.org/x/y",
            "urn:a",
        ];
        let mut m = PrefixCompressedCatMapsInMemory::new_empty();
        for (u, s) in iris.iter().enumerate() {
            m.encode_new_string(s, u as u32);
        }
        for (u, s) in iris.iter().enumerate() {
            assert_eq!(m.maybe_encode_str(s), Some(u as u32));
            assert!(m.contains_str(s));
        }
        assert_eq!(m.maybe_encode_str("http://ex.org/ns#c"), None);
        assert_eq!(m.maybe_encode_str("http://nope.org/a"), None);
        let ordered: Vec<_> = m.map.keys().map(|k| k.to_string()).collect();
        let mut sorted: Vec<_> = iris.iter().map(|s| s.to_string()).collect();
        sorted.sort();
        assert_eq!(ordered, sorted);

        let rb = m.to_record_batch();
        let got: Vec<_> = rb
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|x| x.unwrap().to_string())
            .collect();
        assert_eq!(got, sorted);

        let mut u = UncompressedCatMapsInMemory::new_empty();
        u.encode_new_string("hello".to_string(), 7);
        assert_eq!(u.maybe_encode_str("hello"), Some(7));
        assert_eq!(u.maybe_encode_str("hell"), None);
    }
}
