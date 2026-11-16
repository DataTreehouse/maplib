#![feature(pattern)]

//! In-memory dictionaries ("cat maps") from strings to u32 codes.
//!
//! The strings are stored back to back in a chunked byte arena, and each entry (a "slot") has
//! its offset, length, prefix and code in flat vectors. Strings are found through a hash table of
//! slots, codes through a dense array of slots (or a hash map when the codes are spread out).
//! This costs about 30 bytes per entry on top of the string, where a `BTreeMap` with a reverse
//! `HashMap` of shared strings costs about 100. IRIs are stored prefix compressed: the part up to
//! the last '/', '#' or ':' is interned and only the rest is stored per entry.
//!
//! String order (for ranks, compaction and ordered output) is not maintained on every insert. It
//! is computed when needed and cached; when strings have only been added since, the cache is
//! updated by sorting the new strings and merging them with the cached order.

use arrow::array::{Array, RecordBatch, StringBuilder, UInt32Array};
use arrow::datatypes::{Field, Schema};
use hashbrown::HashTable;
use nohash_hasher::NoHashHasher;
use range_set_blaze::RangeSetBlaze;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use rayon::slice::ParallelSliceMut;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter, Write};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::{Arc, RwLock};

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

/// Marks a slot whose entry was removed.
const DEAD: u32 = u32::MAX;
/// Marks a code without a slot in the dense code index.
const NO_SLOT: u32 = u32::MAX;
/// Largest arena chunk that is allocated up front; longer strings get a chunk of their own.
const MAX_CHUNK: usize = 1 << 22;
const MIN_CHUNK: usize = 64;

#[inline]
fn hash_parts(prefix_id: u32, suffix: &[u8]) -> u64 {
    let mut h = foldhash::fast::FixedState::with_seed(0x6d61_706c_6962).build_hasher();
    h.write_u32(prefix_id);
    h.write(suffix);
    h.finish()
}

/// The strings of a cat map, by slot.
#[derive(Clone, Default)]
struct Strings {
    compressed: bool,
    /// Byte arena. A string never spans chunks, so chunks are never reallocated once full.
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
    /// Per slot: (chunk index << 32) | offset in chunk.
    starts: Vec<u64>,
    /// Per slot: length of the (suffix) string, DEAD if the entry was removed.
    lens: Vec<u32>,
    /// Per slot (only when compressed): index into `prefixes`.
    prefix_ids: Vec<u32>,
    /// Per slot: the code.
    codes: Vec<u32>,
    prefixes: Vec<Box<str>>,
    prefix_index: hashbrown::HashMap<Box<str>, u32>,
}

impl Strings {
    fn new(compressed: bool) -> Self {
        Strings {
            compressed,
            ..Default::default()
        }
    }

    #[inline]
    fn n_slots(&self) -> usize {
        self.codes.len()
    }

    #[inline]
    fn is_live(&self, slot: u32) -> bool {
        self.lens[slot as usize] != DEAD
    }

    #[inline]
    fn prefix_id(&self, slot: u32) -> u32 {
        if self.compressed {
            self.prefix_ids[slot as usize]
        } else {
            0
        }
    }

    #[inline]
    fn suffix_bytes(&self, slot: u32) -> &[u8] {
        let s = self.starts[slot as usize];
        let chunk = &self.chunks[(s >> 32) as usize];
        let off = (s & 0xffff_ffff) as usize;
        &chunk[off..off + self.lens[slot as usize] as usize]
    }

    #[inline]
    fn parts(&self, slot: u32) -> (&str, &str) {
        let pre = if self.compressed {
            &*self.prefixes[self.prefix_ids[slot as usize] as usize]
        } else {
            ""
        };
        // Safety: the bytes were copied from a str.
        let suf = unsafe { std::str::from_utf8_unchecked(self.suffix_bytes(slot)) };
        (pre, suf)
    }

    fn to_string(&self, slot: u32) -> String {
        let (pre, suf) = self.parts(slot);
        let mut s = String::with_capacity(pre.len() + suf.len());
        s.push_str(pre);
        s.push_str(suf);
        s
    }

    #[inline]
    fn hash_slot(&self, slot: u32) -> u64 {
        hash_parts(self.prefix_id(slot), self.suffix_bytes(slot))
    }

    #[inline]
    fn cmp_slots(&self, a: u32, b: u32) -> Ordering {
        cmp_split_strs(self.parts(a), self.parts(b))
    }

    /// The prefix id and suffix that `s` is stored as, if its prefix is known.
    #[inline]
    fn key<'a>(&self, s: &'a str) -> Option<(u32, &'a str)> {
        if self.compressed {
            let (pre, suf) = split_iri(s);
            Some((*self.prefix_index.get(pre)?, suf))
        } else {
            Some((0, s))
        }
    }

    fn intern_prefix(&mut self, pre: &str) -> u32 {
        if let Some(id) = self.prefix_index.get(pre) {
            *id
        } else {
            let id = self.prefixes.len() as u32;
            self.prefixes.push(pre.into());
            self.prefix_index.insert(pre.into(), id);
            id
        }
    }

    fn push_bytes(&mut self, b: &[u8]) -> u64 {
        assert!(
            b.len() < DEAD as usize,
            "String too long for the dictionary"
        );
        let fits = self
            .chunks
            .last()
            .is_some_and(|c| c.len() + b.len() <= c.capacity());
        if !fits {
            let cap = self.total_bytes.clamp(MIN_CHUNK, MAX_CHUNK).max(b.len());
            self.chunks.push(Vec::with_capacity(cap));
        }
        let chunk_idx = self.chunks.len() - 1;
        let chunk = self.chunks.last_mut().unwrap();
        let off = chunk.len();
        chunk.extend_from_slice(b);
        self.total_bytes += b.len();
        ((chunk_idx as u64) << 32) | off as u64
    }

    /// Appends a new slot for the string given as (prefix, suffix), split anywhere, and returns
    /// the slot.
    fn push_parts(&mut self, pre: &str, suf: &str, code: u32) -> u32 {
        if self.compressed {
            let joined;
            let s = if pre.is_empty() {
                suf
            } else {
                joined = [pre, suf].concat();
                &joined
            };
            let (pre, suf) = split_iri(s);
            let id = self.intern_prefix(pre);
            self.prefix_ids.push(id);
            self.push_suffix(suf, code)
        } else if pre.is_empty() {
            self.push_suffix(suf, code)
        } else {
            self.push_suffix(&[pre, suf].concat(), code)
        }
    }

    fn push_suffix(&mut self, suf: &str, code: u32) -> u32 {
        let slot = self.codes.len() as u32;
        assert!(slot != DEAD, "Too many entries for the dictionary");
        let start = self.push_bytes(suf.as_bytes());
        self.starts.push(start);
        self.lens.push(suf.len() as u32);
        self.codes.push(code);
        slot
    }

    fn reserve(&mut self, n: usize) {
        self.starts.reserve(n);
        self.lens.reserve(n);
        self.codes.reserve(n);
        if self.compressed {
            self.prefix_ids.reserve(n);
        }
    }

    fn live_slots(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.n_slots() as u32).filter(|s| self.is_live(*s))
    }
}

/// From codes to slots.
#[derive(Clone)]
enum CodeIndex {
    /// `slots[code - base]`, NO_SLOT for codes without a slot.
    Dense {
        base: u32,
        slots: Vec<u32>,
    },
    Sparse(hashbrown::HashMap<u32, u32>),
}

impl Default for CodeIndex {
    fn default() -> Self {
        CodeIndex::Dense {
            base: 0,
            slots: vec![],
        }
    }
}

impl CodeIndex {
    #[inline]
    fn get(&self, code: u32) -> Option<u32> {
        match self {
            CodeIndex::Dense { base, slots } => {
                match slots.get(code.wrapping_sub(*base) as usize) {
                    Some(s) if *s != NO_SLOT => Some(*s),
                    _ => None,
                }
            }
            CodeIndex::Sparse(m) => m.get(&code).copied(),
        }
    }

    /// Adds a code, where `n_codes` is the number of codes after adding it.
    fn insert(&mut self, code: u32, slot: u32, n_codes: usize) {
        if let CodeIndex::Dense { base, slots } = self {
            if slots.is_empty() {
                *base = code;
            }
            let end = *base as usize + slots.len();
            let (lo, hi) = if code < *base {
                (code as usize, end)
            } else {
                (*base as usize, (code as usize + 1).max(end))
            };
            if hi - lo <= n_codes.saturating_mul(4).saturating_add(4096) {
                if code < *base {
                    let shift = (*base - code) as usize;
                    slots.splice(0..0, std::iter::repeat_n(NO_SLOT, shift));
                    *base = code;
                }
                let i = (code - *base) as usize;
                if i >= slots.len() {
                    slots.resize(i + 1, NO_SLOT);
                }
                slots[i] = slot;
                return;
            }
            // Too spread out for an array.
            let mut m = hashbrown::HashMap::with_capacity(n_codes);
            for (i, s) in slots.iter().enumerate() {
                if *s != NO_SLOT {
                    m.insert(*base + i as u32, *s);
                }
            }
            *self = CodeIndex::Sparse(m);
        }
        if let CodeIndex::Sparse(m) = self {
            m.insert(code, slot);
        }
    }

    fn remove(&mut self, code: u32) -> Option<u32> {
        match self {
            CodeIndex::Dense { base, slots } => {
                let s = slots.get_mut(code.wrapping_sub(*base) as usize)?;
                if *s == NO_SLOT {
                    None
                } else {
                    Some(std::mem::replace(s, NO_SLOT))
                }
            }
            CodeIndex::Sparse(m) => m.remove(&code),
        }
    }
}

#[derive(Default)]
struct SortCache {
    /// Live slots in string order, covering the slots below the given number.
    sorted: Option<(Arc<Vec<u32>>, usize)>,
    /// Positions of the codes in string order.
    ranks: Option<RankMap>,
}

/// Cached string order of a cat map. Clones start without a cache.
#[derive(Default)]
struct RankCache(RwLock<SortCache>);

impl Clone for RankCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl RankCache {
    /// New slots were added: the order is extended when it is next needed.
    fn on_append(&mut self) {
        self.0.get_mut().unwrap().ranks = None;
    }

    /// Entries were removed or renumbered.
    fn invalidate(&mut self) {
        *self.0.get_mut().unwrap() = SortCache::default();
    }
}

/// A dictionary from strings to u32 codes, see the module documentation.
#[derive(Clone)]
pub struct CatMapsInMemory {
    strings: Strings,
    /// Live slots, by the hash of their (prefix id, suffix).
    table: HashTable<u32>,
    codes: CodeIndex,
    n_live: usize,
    rank_cache: RankCache,
}

impl Debug for CatMapsInMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatMapsInMemory")
            .field("compressed", &self.strings.compressed)
            .field("len", &self.n_live)
            .field("bytes", &self.strings.total_bytes)
            .finish()
    }
}

impl CatMapsInMemory {
    pub fn new_empty(is_iri: bool) -> CatMapsInMemory {
        CatMapsInMemory {
            strings: Strings::new(is_iri),
            table: HashTable::new(),
            codes: CodeIndex::default(),
            n_live: 0,
            rank_cache: RankCache::default(),
        }
    }

    pub fn new_singular(value: &str, u: u32, is_iri: bool) -> Self {
        let mut m = Self::new_empty(is_iri);
        m.encode_new_in_memory_string(Cow::Borrowed(value), u);
        m
    }

    pub fn from_ordered_vec(ss: Vec<(Cow<'_, str>, u32)>, is_iri: bool) -> Self {
        let mut m = Self::new_empty(is_iri);
        m.insert_many(ss.iter().map(|(s, u)| (("", s.as_ref()), *u)), ss.len());
        m
    }

    pub fn height(&self) -> u32 {
        self.n_live as u32
    }

    pub fn is_empty(&self) -> bool {
        self.n_live == 0
    }

    pub fn contains_u32(&self, u: &u32) -> bool {
        self.codes.get(*u).is_some()
    }

    /// One more than the largest code, or 0 if the map is empty. Deletes can
    /// leave a cat that is present but empty, e.g. after DROP GRAPH.
    pub fn counter(&self) -> u32 {
        self.strings
            .live_slots()
            .map(|s| self.strings.codes[s as usize])
            .max()
            .map_or(0, |m| m + 1)
    }

    fn find(&self, prefix_id: u32, suf: &str) -> Option<u32> {
        let h = hash_parts(prefix_id, suf.as_bytes());
        self.table
            .find(h, |&slot| {
                self.strings.prefix_id(slot) == prefix_id
                    && self.strings.suffix_bytes(slot) == suf.as_bytes()
            })
            .copied()
    }

    /// The slot of the string given as (prefix, suffix), split anywhere.
    fn find_parts(&self, pre: &str, suf: &str) -> Option<u32> {
        if pre.is_empty() {
            let (id, suf) = self.strings.key(suf)?;
            self.find(id, suf)
        } else {
            let s = [pre, suf].concat();
            let (id, suf) = self.strings.key(&s)?;
            self.find(id, suf)
        }
    }

    pub fn maybe_encode_in_memory_str(&self, s: &str) -> Option<u32> {
        let (id, suf) = self.strings.key(s)?;
        self.find(id, suf)
            .map(|slot| self.strings.codes[slot as usize])
    }

    pub fn maybe_encode_strs(&self, s: &[Option<&str>]) -> Vec<Option<u32>> {
        s.into_par_iter()
            .with_min_len(4096)
            .map(|x| x.and_then(|x| self.maybe_encode_in_memory_str(x)))
            .collect()
    }

    /// Adds a string that is not in the map with the code `u`.
    pub fn encode_new_in_memory_string(&mut self, s: Cow<'_, str>, u: u32) {
        self.rank_cache.on_append();
        let slot = self.strings.push_parts("", &s, u);
        let strings = &self.strings;
        let h = strings.hash_slot(slot);
        self.table.insert_unique(h, slot, |&x| strings.hash_slot(x));
        self.n_live += 1;
        self.codes.insert(u, slot, self.n_live);
    }

    /// Adds strings that are not in the map, given as ((prefix, suffix), code) with the string
    /// split anywhere. The hashes are computed in parallel.
    fn insert_many<'a>(
        &mut self,
        items: impl Iterator<Item = ((&'a str, &'a str), u32)>,
        n: usize,
    ) {
        if n == 0 {
            return;
        }
        self.rank_cache.on_append();
        let first = self.strings.n_slots() as u32;
        self.strings.reserve(n);
        for ((pre, suf), u) in items {
            self.strings.push_parts(pre, suf, u);
        }
        let last = self.strings.n_slots() as u32;
        let strings = &self.strings;
        let hashes: Vec<u64> = (first..last)
            .into_par_iter()
            .map(|s| strings.hash_slot(s))
            .collect();
        self.table
            .reserve((last - first) as usize, |&x| strings.hash_slot(x));
        for (slot, h) in (first..last).zip(hashes) {
            self.table.insert_unique(h, slot, |&x| strings.hash_slot(x));
        }
        for slot in first..last {
            self.n_live += 1;
            self.codes
                .insert(strings.codes[slot as usize], slot, self.n_live);
        }
    }

    /// Adds strings that are not in the map, which must be distinct, with the codes
    /// `first_code`, `first_code + 1`, ...
    pub fn insert_new_strs(&mut self, strs: &[&str], first_code: u32) {
        self.insert_many(
            strs.iter()
                .enumerate()
                .map(|(i, s)| (("", *s), first_code + i as u32)),
            strs.len(),
        );
    }

    /// The string of a code as (prefix, suffix), to be concatenated.
    #[inline]
    pub fn decode_parts(&self, u: u32) -> Option<(&str, &str)> {
        Some(self.strings.parts(self.codes.get(u)?))
    }

    /// Whether some encoded string starts with `prefix`: a binary search in the cached string
    /// order, where the first string not less than `prefix` starts with it if any string does.
    pub fn has_string_with_prefix(&self, prefix: &str) -> bool {
        let sorted = self.sorted_slots();
        let strings = &self.strings;
        let i = sorted.partition_point(|slot| {
            cmp_split_strs(strings.parts(*slot), ("", prefix)) == Ordering::Less
        });
        let Some(slot) = sorted.get(i) else {
            return false;
        };
        let (pre, suf) = strings.parts(*slot);
        if pre.len() >= prefix.len() {
            pre.starts_with(prefix)
        } else {
            prefix.starts_with(pre) && suf.starts_with(&prefix[pre.len()..])
        }
    }

    pub fn encode_all_new_non_duplicated_strings(
        &mut self,
        maybe_new_strings: Vec<String>,
        c: &mut u32,
    ) {
        for s in maybe_new_strings {
            if self.maybe_encode_in_memory_str(&s).is_none() {
                self.encode_new_in_memory_string(Cow::Owned(s), *c);
                *c += 1;
            }
        }
    }

    pub fn maybe_decode(&self, u: &u32) -> Option<Cow<'_, str>> {
        let slot = self.codes.get(*u)?;
        Some(self.decode_slot(slot))
    }

    #[inline]
    fn decode_slot(&self, slot: u32) -> Cow<'_, str> {
        let (pre, suf) = self.strings.parts(slot);
        if pre.is_empty() {
            Cow::Borrowed(suf)
        } else {
            Cow::Owned(self.strings.to_string(slot))
        }
    }

    pub fn decode_batch(&self, v: &[Option<u32>]) -> Vec<Option<Cow<'_, str>>> {
        // We must tolerate missing values here, as values may be in global instead of local
        v.into_par_iter()
            .with_min_len(1024)
            .map(|u| {
                u.and_then(|u| self.codes.get(u))
                    .map(|slot| self.decode_slot(slot))
            })
            .collect()
    }

    /// The live slots in string order.
    fn sorted_slots(&self) -> Arc<Vec<u32>> {
        let n = self.strings.n_slots();
        if let Some((sorted, covered)) = &self.rank_cache.0.read().unwrap().sorted {
            if *covered == n {
                return sorted.clone();
            }
        }
        let mut cache = self.rank_cache.0.write().unwrap();
        let (old, covered) = match &cache.sorted {
            Some((sorted, covered)) if *covered == n => return sorted.clone(),
            Some((sorted, covered)) => (sorted.clone(), *covered),
            None => (Arc::new(vec![]), 0),
        };
        let strings = &self.strings;
        let mut new: Vec<u32> = (covered as u32..n as u32)
            .into_par_iter()
            .filter(|s| strings.is_live(*s))
            .collect();
        new.par_sort_unstable_by(|a, b| strings.cmp_slots(*a, *b));
        let sorted = if old.is_empty() {
            new
        } else {
            let mut merged = Vec::with_capacity(old.len() + new.len());
            let (mut i, mut j) = (0, 0);
            while i < old.len() && j < new.len() {
                if strings.cmp_slots(old[i], new[j]) != Ordering::Greater {
                    merged.push(old[i]);
                    i += 1;
                } else {
                    merged.push(new[j]);
                    j += 1;
                }
            }
            merged.extend_from_slice(&old[i..]);
            merged.extend_from_slice(&new[j..]);
            merged
        };
        let sorted = Arc::new(sorted);
        cache.sorted = Some((sorted.clone(), n));
        sorted
    }

    /// The position of every code in the string order of the whole map, cached until the map
    /// changes.
    pub fn global_ranks(&self) -> RankMap {
        if let Some(r) = &self.rank_cache.0.read().unwrap().ranks {
            return r.clone();
        }
        let sorted = self.sorted_slots();
        let strings = &self.strings;
        let pairs: Vec<(u32, u32)> = sorted
            .par_iter()
            .enumerate()
            .map(|(i, slot)| (strings.codes[*slot as usize], i as u32))
            .collect();
        let ranks = RankMap::from_pairs(pairs);
        self.rank_cache.0.write().unwrap().ranks = Some(ranks.clone());
        ranks
    }

    /// Ranks of `us` (sorted, distinct codes) in string order, dense from 0. Only the relative
    /// order is meaningful.
    pub fn sort_ranks(&self, us: &[u32]) -> RankMap {
        let strings = &self.strings;
        // Few codes (e.g. the triples of one rule execution) are sorted on this thread, since
        // waking the thread pool costs more than the sort.
        let slots = if us.len() <= SEQUENTIAL_SORT_RANKS {
            let mut slots: Vec<u32> = us.iter().filter_map(|u| self.codes.get(*u)).collect();
            slots.sort_unstable_by(|a, b| strings.cmp_slots(*a, *b));
            slots
        } else {
            let mut slots: Vec<u32> = us
                .into_par_iter()
                .filter_map(|u| self.codes.get(*u))
                .collect();
            slots.par_sort_unstable_by(|a, b| strings.cmp_slots(*a, *b));
            slots
        };
        RankMap::from_pairs(
            slots
                .into_iter()
                .enumerate()
                .map(|(i, s)| (strings.codes[s as usize], i as u32))
                .collect(),
        )
    }

    pub fn global_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        let ranks = self.global_ranks();
        us.iter()
            .filter_map(|u| ranks.get(*u).map(|r| (*u, r)))
            .collect()
    }

    /// Ranks of `us` in string order, dense from 0. Only the relative order is meaningful.
    pub fn local_rank_map(&self, us: &HashSet<u32>) -> HashMap<u32, u32> {
        let us = distinct_codes(us.iter().copied().collect());
        let ranks = self.sort_ranks(&us);
        us.iter()
            .filter_map(|u| ranks.get(*u).map(|r| (*u, r)))
            .collect()
    }

    pub fn get_spaced_strings(&self, stride: usize) -> Vec<(String, String)> {
        let sorted = self.sorted_slots();
        let mut spaced = vec![];
        let mut start_string = None;
        for (i, slot) in sorted.iter().enumerate() {
            if start_string.is_none() {
                start_string = Some(*slot);
            }
            if i != 0 && i % stride == 0 || i == sorted.len() - 1 {
                spaced.push((
                    self.strings.to_string(start_string.take().unwrap()),
                    self.strings.to_string(*slot),
                ));
            }
        }
        spaced
    }

    /// Renumbers the codes in string order (0, 1, ...), and drops removed entries.
    pub fn compact(&mut self) {
        let sorted = self.sorted_slots();
        let mut new = Self::new_empty(self.strings.compressed);
        new.insert_many(
            sorted
                .iter()
                .enumerate()
                .map(|(i, slot)| (self.strings.parts(*slot), i as u32)),
            sorted.len(),
        );
        // The slots of the new map are in string order.
        let n = new.strings.n_slots();
        *new.rank_cache.0.get_mut().unwrap() = SortCache {
            sorted: Some((Arc::new((0..n as u32).collect()), n)),
            ranks: None,
        };
        *self = new;
    }

    pub fn garbage_collect_cats(&mut self, p0: RangeSetBlaze<u32>) {
        self.rank_cache.invalidate();
        for u in p0 {
            if let Some(slot) = self.codes.remove(u) {
                let h = self.strings.hash_slot(slot);
                if let Ok(e) = self.table.find_entry(h, |&x| x == slot) {
                    e.remove();
                }
                self.strings.lens[slot as usize] = DEAD;
                self.n_live -= 1;
            }
        }
    }

    pub fn range_set(&self) -> RangeSetBlaze<u32> {
        RangeSetBlaze::from_iter(
            self.strings
                .live_slots()
                .map(|s| self.strings.codes[s as usize]),
        )
    }

    /// Pairs (code here, code in other) of the strings in both maps whose codes differ.
    pub fn inner_join_re_enc(&self, other: &CatMapsInMemory) -> Vec<(u32, u32)> {
        let strings = &self.strings;
        (0..strings.n_slots() as u32)
            .into_par_iter()
            .filter(|s| strings.is_live(*s))
            .filter_map(|slot| {
                let (pre, suf) = strings.parts(slot);
                let r = other.strings.codes[other.find_parts(pre, suf)? as usize];
                let l = strings.codes[slot as usize];
                (l != r).then_some((l, r))
            })
            .collect()
    }

    /// Adds the strings of `other` that are not here with new codes from `c`, and returns the
    /// re-encoding from the codes of `other` to codes here.
    pub fn merge(
        &mut self,
        other: &CatMapsInMemory,
        c: &mut u32,
    ) -> HashMap<u32, u32, BuildHasherDefault<NoHashHasher<u32>>> {
        let o = &other.strings;
        let found: Vec<(u32, Option<u32>)> = (0..o.n_slots() as u32)
            .into_par_iter()
            .filter(|s| o.is_live(*s))
            .map(|slot| {
                let (pre, suf) = o.parts(slot);
                (slot, self.find_parts(pre, suf))
            })
            .collect();
        let mut remap =
            HashMap::with_capacity_and_hasher(found.len(), BuildHasherDefault::default());
        let mut new_slots = vec![];
        for (slot, here) in found {
            let ou = o.codes[slot as usize];
            if let Some(here) = here {
                remap.insert(ou, self.strings.codes[here as usize]);
            } else {
                remap.insert(ou, *c);
                new_slots.push((slot, *c));
                *c += 1;
            }
        }
        let n = new_slots.len();
        self.insert_many(new_slots.into_iter().map(|(s, u)| (o.parts(s), u)), n);
        remap
    }

    /// The entries with the codes in `s`.
    pub fn image(&self, s: &HashSet<u32>) -> Option<CatMapsInMemory> {
        let mut us: Vec<u32> = s.iter().copied().collect();
        us.sort_unstable();
        let slots: Vec<(u32, u32)> = us
            .into_iter()
            .filter_map(|u| self.codes.get(u).map(|s| (s, u)))
            .collect();
        if slots.is_empty() {
            return None;
        }
        let mut m = Self::new_empty(self.strings.compressed);
        let n = slots.len();
        m.insert_many(
            slots.into_iter().map(|(s, u)| (self.strings.parts(s), u)),
            n,
        );
        Some(m)
    }

    /// The entries in string order, as a record batch with the columns "str" and "cat".
    pub fn to_record_batch(&self) -> RecordBatch {
        let sorted = self.sorted_slots();
        let data_len: usize = sorted
            .iter()
            .map(|s| {
                let (p, q) = self.strings.parts(*s);
                p.len() + q.len()
            })
            .sum();
        let mut strs = StringBuilder::with_capacity(sorted.len(), data_len);
        let mut us = Vec::with_capacity(sorted.len());
        for slot in sorted.iter() {
            let (pre, suf) = self.strings.parts(*slot);
            strs.write_str(pre).unwrap();
            strs.write_str(suf).unwrap();
            strs.append_value("");
            us.push(self.strings.codes[*slot as usize]);
        }
        let strs = strs.finish();
        let us = UInt32Array::from(us);
        let schema = Schema::new(vec![
            Field::new("str", strs.data_type().clone(), true),
            Field::new("cat", us.data_type().clone(), true),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strs), Arc::new(us)]).unwrap()
    }
}

/// The distinct strings of `strs`, in an order that depends only on the strings. The strings
/// are partitioned by hash and the partitions deduplicated in parallel.
pub fn distinct_strs<'a>(strs: &[&'a str]) -> Vec<&'a str> {
    const BUCKET_BITS: u32 = 8;
    const N_BUCKETS: usize = 1 << BUCKET_BITS;
    let state = foldhash::fast::FixedState::with_seed(0x6469_7374);
    // One byte per string for its bucket, then the string indices grouped by bucket (counting
    // sort), so that only 5 bytes per string are needed besides the strings themselves.
    let bucket_of: Vec<u8> = strs
        .par_iter()
        .with_min_len(4096)
        .map(|s| (state.hash_one(s) >> (64 - BUCKET_BITS)) as u8)
        .collect();
    let mut starts = [0usize; N_BUCKETS + 1];
    for b in &bucket_of {
        starts[*b as usize + 1] += 1;
    }
    for i in 0..N_BUCKETS {
        starts[i + 1] += starts[i];
    }
    let mut next = starts;
    let mut grouped = vec![0u32; strs.len()];
    for (i, b) in bucket_of.iter().enumerate() {
        grouped[next[*b as usize]] = i as u32;
        next[*b as usize] += 1;
    }
    drop(bucket_of);
    let deduped: Vec<Vec<&'a str>> = (0..N_BUCKETS)
        .into_par_iter()
        .map(|b| {
            let idxs = &grouped[starts[b]..starts[b + 1]];
            let mut seen: HashTable<u32> = HashTable::with_capacity(idxs.len());
            let mut out = vec![];
            for i in idxs {
                let s = strs[*i as usize];
                let h = state.hash_one(s);
                match seen.entry(
                    h,
                    |j| strs[*j as usize] == s,
                    |j| state.hash_one(strs[*j as usize]),
                ) {
                    hashbrown::hash_table::Entry::Occupied(_) => {}
                    hashbrown::hash_table::Entry::Vacant(v) => {
                        v.insert(*i);
                        out.push(s);
                    }
                }
            }
            out
        })
        .collect();
    deduped.into_iter().flatten().collect()
}

/// Ranks of dictionary codes. Built from dense code ranges as an array indexed by the code, so
/// that looking up a rank is an array access rather than a hash map lookup.
#[derive(Debug, Clone)]
pub enum RankMap {
    /// `ranks[u - base]` is the rank of code `u`, `u32::MAX` if `u` has no rank.
    Dense { base: u32, ranks: Arc<Vec<u32>> },
    /// Codes too spread out for an array: sorted codes and their ranks.
    Sorted {
        codes: Arc<Vec<u32>>,
        ranks: Arc<Vec<u32>>,
    },
}

const NO_RANK: u32 = u32::MAX;

/// [`CatMapsInMemory::sort_ranks`] sorts at most this many codes without the thread pool.
const SEQUENTIAL_SORT_RANKS: usize = 4096;

/// An array over a code range is used when it is at most this many times the number of codes
/// (plus a constant), otherwise a sorted list.
fn dense_ok(range: usize, n: usize) -> bool {
    range <= n.saturating_mul(16).saturating_add(1 << 16)
}

impl RankMap {
    pub fn empty() -> Self {
        RankMap::Sorted {
            codes: Arc::new(vec![]),
            ranks: Arc::new(vec![]),
        }
    }

    #[inline]
    pub fn get(&self, u: u32) -> Option<u32> {
        match self {
            RankMap::Dense { base, ranks } => match ranks.get(u.wrapping_sub(*base) as usize) {
                Some(r) if *r != NO_RANK => Some(*r),
                _ => None,
            },
            RankMap::Sorted { codes, ranks } => codes.binary_search(&u).ok().map(|i| ranks[i]),
        }
    }

    /// From (code, rank) pairs with distinct codes.
    pub fn from_pairs(mut pairs: Vec<(u32, u32)>) -> Self {
        if pairs.is_empty() {
            return Self::empty();
        }
        let (mut min, mut max) = (u32::MAX, 0u32);
        for (u, _) in &pairs {
            min = min.min(*u);
            max = max.max(*u);
        }
        let range = (max - min) as usize + 1;
        if dense_ok(range, pairs.len()) {
            let mut ranks = vec![NO_RANK; range];
            for (u, r) in pairs {
                ranks[(u - min) as usize] = r;
            }
            RankMap::Dense {
                base: min,
                ranks: Arc::new(ranks),
            }
        } else {
            pairs.par_sort_unstable_by_key(|x| x.0);
            let (codes, ranks): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
            RankMap::Sorted {
                codes: Arc::new(codes),
                ranks: Arc::new(ranks),
            }
        }
    }

    pub fn from_hash_map(m: HashMap<u32, u32>) -> Self {
        Self::from_pairs(m.into_iter().collect())
    }
}

/// The distinct codes of `us`, sorted.
pub fn distinct_codes(mut us: Vec<u32>) -> Vec<u32> {
    if us.is_empty() {
        return us;
    }
    let (mut min, mut max) = (u32::MAX, 0u32);
    for u in &us {
        min = min.min(*u);
        max = max.max(*u);
    }
    let range = (max - min) as usize + 1;
    if dense_ok(range, us.len()) {
        let mut bits = vec![0u64; range.div_ceil(64)];
        for u in &us {
            let i = (u - min) as usize;
            bits[i >> 6] |= 1 << (i & 63);
        }
        us.clear();
        for (w, word) in bits.into_iter().enumerate() {
            let mut word = word;
            while word != 0 {
                let b = word.trailing_zeros() as usize;
                us.push(min + ((w << 6) + b) as u32);
                word &= word - 1;
            }
        }
        us
    } else {
        us.par_sort_unstable();
        us.dedup();
        us
    }
}

#[cfg(test)]
mod tests;
