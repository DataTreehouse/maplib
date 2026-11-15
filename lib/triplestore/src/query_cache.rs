//! A cache of query results, invalidated per (named graph, predicate).
//!
//! Every write to the triplestore records the (graph, predicate) pairs it changed in the
//! [`VersionTable`], each with a stamp from a counter that every write advances. A cached result
//! remembers the stamp the model had when it was computed and its read set: the pairs the query
//! can read, found from its algebra. The result is served while no pair of the read set was
//! written after that stamp, so writes to other graphs or other predicates keep it.
//!
//! The stamp of a result must be read while the model's read lock is held, since no write runs
//! then. Lookups take no model lock, so a cached result is served while a write runs elsewhere.

mod plan;

pub use plan::{cache_plan, CachePlan, PlanContext};

use oxrdf::NamedNode;
use representation::dataset::NamedGraph;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

/// When one write changes more predicates than this in one graph, the whole graph counts as
/// changed. This keeps the version table small after a large write.
const PREDICATES_PER_GRAPH_LIMIT: usize = 1024;

/// What one write changed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Touched {
    /// The triples with this predicate in this graph.
    Cell(NamedGraph, NamedNode),
    /// Any triples of this graph, e.g. when the graph is truncated.
    Graph(NamedGraph),
    /// Anything, e.g. when indexing options change.
    All,
    /// The model's prefixes, used when queries are parsed.
    Prefixes,
}

/// What a query can read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReadItem {
    /// The triples with this predicate in this graph.
    Cell(NamedGraph, NamedNode),
    /// The triples with any predicate in this graph.
    Graph(NamedGraph),
    /// The triples with this predicate in any graph.
    Predicate(NamedNode),
    /// Anything.
    All,
    /// The model's prefixes.
    Prefixes,
}

#[derive(Default)]
struct Table {
    cell: HashMap<(NamedGraph, NamedNode), u64>,
    /// The last write to any predicate of the graph.
    graph_any: HashMap<NamedGraph, u64>,
    /// The last write to the whole graph.
    graph_all: HashMap<NamedGraph, u64>,
    /// The last write to the predicate in any graph.
    predicate_any: HashMap<NamedNode, u64>,
    max_graph_all: u64,
    all: u64,
    prefixes: u64,
    /// Stamps of removed entries are at most this.
    floor: u64,
}

fn stamp_of<K: std::hash::Hash + Eq>(m: &HashMap<K, u64>, k: &K, floor: u64) -> u64 {
    m.get(k).copied().unwrap_or(floor)
}

impl Table {
    fn record(&mut self, s: u64, t: &Touched) {
        match t {
            Touched::Cell(g, p) => {
                self.cell.insert((g.clone(), p.clone()), s);
                self.graph_any.insert(g.clone(), s);
                self.predicate_any.insert(p.clone(), s);
            }
            Touched::Graph(g) => {
                self.graph_all.insert(g.clone(), s);
                self.graph_any.insert(g.clone(), s);
                self.max_graph_all = s;
            }
            Touched::All => self.all = s,
            Touched::Prefixes => self.prefixes = s,
        }
    }

    /// Whether nothing that `reads` covers was written after `stamp`.
    fn unchanged_since(&self, stamp: u64, reads: &[ReadItem], clock: u64) -> bool {
        if self.all > stamp {
            return false;
        }
        let f = self.floor;
        reads.iter().all(|r| match r {
            ReadItem::Cell(g, p) => {
                stamp_of(&self.cell, &(g.clone(), p.clone()), f) <= stamp
                    && stamp_of(&self.graph_all, g, f) <= stamp
            }
            ReadItem::Graph(g) => stamp_of(&self.graph_any, g, f) <= stamp,
            ReadItem::Predicate(p) => {
                stamp_of(&self.predicate_any, p, f) <= stamp && self.max_graph_all <= stamp
            }
            ReadItem::All => clock <= stamp,
            ReadItem::Prefixes => self.prefixes <= stamp,
        })
    }

    /// Forgets the stamps that are at most `oldest`, the stamp of the oldest cached result:
    /// they can not invalidate anything.
    fn prune(&mut self, oldest: u64) {
        if oldest <= self.floor {
            return;
        }
        self.cell.retain(|_, s| *s > oldest);
        self.graph_any.retain(|_, s| *s > oldest);
        self.graph_all.retain(|_, s| *s > oldest);
        self.predicate_any.retain(|_, s| *s > oldest);
        self.floor = oldest;
    }

    fn len(&self) -> usize {
        self.cell.len() + self.graph_any.len() + self.graph_all.len() + self.predicate_any.len()
    }
}

/// The stamps of the writes to each (graph, predicate) pair of a model.
#[derive(Default)]
pub struct VersionTable {
    clock: AtomicU64,
    table: Mutex<Table>,
}

impl VersionTable {
    /// The stamp of the last write. Read it while holding the model's read lock.
    pub fn stamp(&self) -> u64 {
        self.clock.load(Ordering::SeqCst)
    }

    fn lock(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Records a change. Writers call it while holding the model's write lock.
    pub fn record(&self, touched: Touched) {
        self.record_all(std::iter::once(touched));
    }

    /// Records the changes of one step of a write under one stamp.
    pub fn record_all(&self, touched: impl IntoIterator<Item = Touched>) {
        let touched: Vec<Touched> = touched.into_iter().collect();
        if touched.is_empty() {
            return;
        }
        let mut per_graph: HashMap<&NamedGraph, usize> = HashMap::new();
        for t in &touched {
            if let Touched::Cell(g, _) = t {
                *per_graph.entry(g).or_default() += 1;
            }
        }
        let mut table = self.lock();
        let s = self.clock.load(Ordering::SeqCst) + 1;
        for t in &touched {
            match t {
                Touched::Cell(g, _) if per_graph[g] > PREDICATES_PER_GRAPH_LIMIT => {
                    table.record(s, &Touched::Graph(g.clone()))
                }
                t => table.record(s, t),
            }
        }
        self.clock.store(s, Ordering::SeqCst);
    }

    /// The number of stamps kept, for statistics.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Settings of the query result cache.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryCacheOptions {
    pub enabled: bool,
    /// The largest number of results kept.
    pub max_entries: usize,
    /// The largest total size of the results kept, in bytes (estimated).
    pub max_bytes: usize,
    /// Results larger than this are returned but not kept.
    pub max_entry_bytes: usize,
    /// Results older than this are not served. None keeps them until a write changes what
    /// they read, or they are evicted.
    pub ttl: Option<Duration>,
    /// Queries that ran faster than this are not kept.
    pub min_run_time: Duration,
    /// Also keep results of queries over virtualized databases. The cache does not see changes
    /// in those databases, so set a ttl with this.
    pub cache_virtualized: bool,
}

impl Default for QueryCacheOptions {
    fn default() -> Self {
        QueryCacheOptions {
            enabled: false,
            max_entries: 1024,
            max_bytes: 256 << 20,
            max_entry_bytes: 16 << 20,
            ttl: None,
            min_run_time: Duration::ZERO,
            cache_virtualized: false,
        }
    }
}

/// Counts of what the cache did since the model was created (or the counts were reset).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryCacheStats {
    pub enabled: bool,
    pub entries: usize,
    pub bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub bypassed: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub expirations: u64,
    pub stamp: u64,
    pub version_cells: usize,
}

struct Entry {
    stamp: u64,
    reads: Vec<ReadItem>,
    value: Arc<dyn Any + Send + Sync>,
    bytes: usize,
    created: Instant,
    used: u64,
}

#[derive(Default)]
struct Entries {
    map: HashMap<String, Entry>,
    bytes: usize,
    tick: u64,
}

impl Entries {
    fn remove(&mut self, key: &str) {
        if let Some(e) = self.map.remove(key) {
            self.bytes -= e.bytes;
        }
    }

    fn evict_least_recently_used(&mut self) -> bool {
        let key = self
            .map
            .iter()
            .min_by_key(|(_, e)| e.used)
            .map(|(k, _)| k.clone());
        match key {
            Some(k) => {
                self.remove(&k);
                true
            }
            None => false,
        }
    }

    fn oldest_stamp(&self) -> Option<u64> {
        self.map.values().map(|e| e.stamp).min()
    }
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    bypassed: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    expirations: AtomicU64,
}

/// What a lookup found.
pub enum Lookup {
    Hit(Arc<dyn Any + Send + Sync>),
    /// Not cached, or no longer valid.
    Miss,
    /// A query that is never cached, e.g. one calling NOW().
    Bypass,
    /// The cache is disabled.
    Disabled,
}

/// The query result cache of one model, shared by `Model.query` and `Model.serve`.
#[derive(Default)]
pub struct QueryCache {
    pub versions: VersionTable,
    options: RwLock<QueryCacheOptions>,
    entries: Mutex<Entries>,
    /// Keys of queries that can not be cached, so that they are not looked up again.
    uncacheable: Mutex<HashSet<String>>,
    counters: Counters,
}

/// The most keys of queries that can not be cached that are remembered.
const MAX_UNCACHEABLE: usize = 10_000;

impl QueryCache {
    pub fn new() -> Arc<QueryCache> {
        Arc::new(QueryCache::default())
    }

    fn entries(&self) -> MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn options(&self) -> QueryCacheOptions {
        self.options.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn enabled(&self) -> bool {
        self.options.read().unwrap_or_else(|e| e.into_inner()).enabled
    }

    /// Changes the settings. Results over the new limits are evicted, and all of them when the
    /// cache is disabled.
    pub fn set_options(&self, options: QueryCacheOptions) {
        let enabled = options.enabled;
        *self.options.write().unwrap_or_else(|e| e.into_inner()) = options;
        self.uncacheable().clear();
        if enabled {
            self.shrink();
        } else {
            self.clear();
        }
    }

    fn uncacheable(&self) -> MutexGuard<'_, HashSet<String>> {
        self.uncacheable.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Remembers that the query of the key can not be cached (it is volatile, calls a user
    /// defined function or reads virtualized data), and counts it as bypassed.
    pub fn note_uncacheable(&self, key: String) {
        let mut u = self.uncacheable();
        if u.len() >= MAX_UNCACHEABLE {
            u.clear();
        }
        u.insert(key);
        drop(u);
        self.note_bypass();
    }

    /// Removes every result.
    pub fn clear(&self) {
        self.uncacheable().clear();
        let mut entries = self.entries();
        entries.map.clear();
        entries.bytes = 0;
        let stamp = self.versions.stamp();
        self.versions.lock().prune(stamp);
    }

    fn shrink(&self) {
        let options = self.options();
        let mut entries = self.entries();
        let mut evicted = 0;
        while entries.map.len() > options.max_entries || entries.bytes > options.max_bytes {
            if !entries.evict_least_recently_used() {
                break;
            }
            evicted += 1;
        }
        self.counters
            .evictions
            .fetch_add(evicted, Ordering::Relaxed);
    }

    /// The result kept for the key, if nothing it read has been written since it was computed.
    pub fn get(&self, key: &str) -> Lookup {
        let options = self.options();
        if !options.enabled {
            return Lookup::Disabled;
        }
        if self.uncacheable().contains(key) {
            self.counters.bypassed.fetch_add(1, Ordering::Relaxed);
            return Lookup::Bypass;
        }
        let mut entries = self.entries();
        let Some(e) = entries.map.get(key) else {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Lookup::Miss;
        };
        if options.ttl.is_some_and(|ttl| e.created.elapsed() > ttl) {
            entries.remove(key);
            self.counters.expirations.fetch_add(1, Ordering::Relaxed);
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Lookup::Miss;
        }
        let valid = {
            let table = self.versions.lock();
            table.unchanged_since(e.stamp, &e.reads, self.versions.stamp())
        };
        if !valid {
            entries.remove(key);
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Lookup::Miss;
        }
        entries.tick += 1;
        let tick = entries.tick;
        let e = entries.map.get_mut(key).unwrap();
        e.used = tick;
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
        Lookup::Hit(e.value.clone())
    }

    /// Counts a query that was not looked up because it can not be cached.
    pub fn note_bypass(&self) {
        if self.enabled() {
            self.counters.bypassed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Whether a result of this run time and size would be kept.
    pub fn worth_keeping(&self, run_time: Duration, bytes: usize) -> bool {
        let options = self.options();
        options.enabled && run_time >= options.min_run_time && bytes <= options.max_entry_bytes
    }

    /// Keeps a result computed at `stamp` (read under the model's read lock) that read `reads`.
    /// A result that a write has already made stale is not kept. Returns whether it was kept.
    pub fn insert(
        &self,
        key: String,
        stamp: u64,
        reads: Vec<ReadItem>,
        value: Arc<dyn Any + Send + Sync>,
        bytes: usize,
    ) -> bool {
        let options = self.options();
        if !options.enabled || bytes > options.max_entry_bytes || bytes > options.max_bytes {
            return false;
        }
        let mut entries = self.entries();
        {
            let table = self.versions.lock();
            if !table.unchanged_since(stamp, &reads, self.versions.stamp()) {
                return false;
            }
        }
        entries.remove(&key);
        let mut evicted = 0;
        while !entries.map.is_empty()
            && (entries.map.len() + 1 > options.max_entries
                || entries.bytes + bytes > options.max_bytes)
        {
            entries.evict_least_recently_used();
            evicted += 1;
        }
        if options.max_entries == 0 {
            return false;
        }
        entries.tick += 1;
        let used = entries.tick;
        entries.bytes += bytes;
        entries.map.insert(
            key,
            Entry {
                stamp,
                reads,
                value,
                bytes,
                created: Instant::now(),
                used,
            },
        );
        self.counters
            .evictions
            .fetch_add(evicted, Ordering::Relaxed);
        self.counters.inserts.fetch_add(1, Ordering::Relaxed);
        if let Some(oldest) = entries.oldest_stamp() {
            let mut table = self.versions.lock();
            if table.len() > 4 * entries.map.len() + 1024 {
                table.prune(oldest);
            }
        }
        true
    }

    pub fn stats(&self) -> QueryCacheStats {
        let entries = self.entries();
        let c = &self.counters;
        QueryCacheStats {
            enabled: self.enabled(),
            entries: entries.map.len(),
            bytes: entries.bytes,
            hits: c.hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            bypassed: c.bypassed.load(Ordering::Relaxed),
            inserts: c.inserts.load(Ordering::Relaxed),
            evictions: c.evictions.load(Ordering::Relaxed),
            invalidations: c.invalidations.load(Ordering::Relaxed),
            expirations: c.expirations.load(Ordering::Relaxed),
            stamp: self.versions.stamp(),
            version_cells: self.versions.len(),
        }
    }
}

/// Builds a cache key from its parts. The parts are separated by a character that does not
/// occur in IRIs or in the other parts.
#[derive(Default)]
pub struct KeyBuilder(String);

impl KeyBuilder {
    pub fn new(kind: &str) -> KeyBuilder {
        KeyBuilder(kind.to_string())
    }

    pub fn part(mut self, name: &str, value: impl std::fmt::Display) -> KeyBuilder {
        use std::fmt::Write;
        let _ = write!(self.0, "\u{1f}{name}\u{1e}{value}");
        self
    }

    pub fn finish(self) -> String {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nn(s: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("http://example.org/{s}"))
    }

    fn g(s: &str) -> NamedGraph {
        NamedGraph::NamedGraph(nn(s))
    }

    fn enabled() -> Arc<QueryCache> {
        let c = QueryCache::new();
        c.set_options(QueryCacheOptions {
            enabled: true,
            ..Default::default()
        });
        c
    }

    fn put(c: &QueryCache, key: &str, reads: Vec<ReadItem>) {
        let stamp = c.versions.stamp();
        assert!(c.insert(key.to_string(), stamp, reads, Arc::new(1u8), 1));
    }

    fn hit(c: &QueryCache, key: &str) -> bool {
        matches!(c.get(key), Lookup::Hit(_))
    }

    #[test]
    fn a_write_invalidates_only_what_it_touched() {
        let c = enabled();
        put(&c, "a", vec![ReadItem::Cell(g("1"), nn("p"))]);
        put(&c, "b", vec![ReadItem::Cell(g("1"), nn("q"))]);
        put(&c, "c", vec![ReadItem::Cell(g("2"), nn("p"))]);
        put(&c, "d", vec![ReadItem::Graph(g("1"))]);
        put(&c, "e", vec![ReadItem::Predicate(nn("p"))]);
        put(&c, "f", vec![ReadItem::Predicate(nn("q"))]);
        put(&c, "g", vec![ReadItem::All]);
        c.versions.record(Touched::Cell(g("1"), nn("p")));
        assert!(!hit(&c, "a"));
        assert!(hit(&c, "b"));
        assert!(hit(&c, "c"));
        assert!(!hit(&c, "d"));
        assert!(!hit(&c, "e"));
        assert!(hit(&c, "f"));
        assert!(!hit(&c, "g"));
    }

    #[test]
    fn a_whole_graph_write_invalidates_its_cells_and_any_graph_predicates() {
        let c = enabled();
        put(&c, "a", vec![ReadItem::Cell(g("1"), nn("p"))]);
        put(&c, "b", vec![ReadItem::Cell(g("2"), nn("p"))]);
        put(&c, "c", vec![ReadItem::Predicate(nn("q"))]);
        c.versions.record(Touched::Graph(g("1")));
        assert!(!hit(&c, "a"));
        assert!(hit(&c, "b"));
        assert!(!hit(&c, "c"));
    }

    #[test]
    fn everything_and_prefixes() {
        let c = enabled();
        put(&c, "a", vec![ReadItem::Cell(g("1"), nn("p"))]);
        put(&c, "b", vec![ReadItem::Prefixes]);
        c.versions.record(Touched::Prefixes);
        assert!(hit(&c, "a"));
        assert!(!hit(&c, "b"));
        c.versions.record(Touched::All);
        assert!(!hit(&c, "a"));
    }

    #[test]
    fn a_result_computed_before_a_write_is_not_kept() {
        let c = enabled();
        let stamp = c.versions.stamp();
        c.versions.record(Touched::Cell(g("1"), nn("p")));
        let reads = vec![ReadItem::Cell(g("1"), nn("p"))];
        assert!(!c.insert("a".into(), stamp, reads, Arc::new(1u8), 1));
        let reads = vec![ReadItem::Cell(g("1"), nn("q"))];
        assert!(c.insert("b".into(), stamp, reads, Arc::new(1u8), 1));
        assert!(hit(&c, "b"));
    }

    #[test]
    fn many_predicates_in_one_write_count_as_the_whole_graph() {
        let c = enabled();
        put(&c, "a", vec![ReadItem::Cell(g("1"), nn("other"))]);
        c.versions.record_all(
            (0..PREDICATES_PER_GRAPH_LIMIT + 1).map(|i| Touched::Cell(g("1"), nn(&i.to_string()))),
        );
        assert!(!hit(&c, "a"));
        assert!(c.versions.len() < 10);
    }

    #[test]
    fn limits_evict_the_least_recently_used() {
        let c = QueryCache::new();
        c.set_options(QueryCacheOptions {
            enabled: true,
            max_entries: 2,
            max_bytes: 100,
            max_entry_bytes: 60,
            ..Default::default()
        });
        let r = || vec![ReadItem::All];
        assert!(c.insert("a".into(), 0, r(), Arc::new(1u8), 10));
        assert!(c.insert("b".into(), 0, r(), Arc::new(1u8), 10));
        assert!(hit(&c, "a"));
        assert!(c.insert("c".into(), 0, r(), Arc::new(1u8), 10));
        assert!(hit(&c, "a"));
        assert!(!hit(&c, "b"));
        assert!(!c.insert("big".into(), 0, r(), Arc::new(1u8), 61));
        assert!(c.insert("d".into(), 0, r(), Arc::new(1u8), 60));
        assert!(c.insert("e".into(), 0, r(), Arc::new(1u8), 50));
        assert!(c.stats().bytes <= 100);
        assert!(c.stats().entries <= 2);
    }

    #[test]
    fn ttl_and_disabling() {
        let c = QueryCache::new();
        c.set_options(QueryCacheOptions {
            enabled: true,
            ttl: Some(Duration::from_millis(20)),
            ..Default::default()
        });
        put(&c, "a", vec![ReadItem::All]);
        assert!(hit(&c, "a"));
        std::thread::sleep(Duration::from_millis(30));
        assert!(!hit(&c, "a"));
        put(&c, "b", vec![ReadItem::All]);
        c.set_options(QueryCacheOptions::default());
        assert!(matches!(c.get("b"), Lookup::Disabled));
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn pruning_keeps_validity() {
        let c = enabled();
        for i in 0..3000 {
            c.versions.record(Touched::Cell(g("1"), nn(&i.to_string())));
        }
        put(&c, "a", vec![ReadItem::Cell(g("1"), nn("5"))]);
        put(&c, "b", vec![ReadItem::Cell(g("1"), nn("x"))]);
        assert!(c.versions.len() < 100, "pruned to {}", c.versions.len());
        assert!(hit(&c, "a"));
        c.versions.record(Touched::Cell(g("1"), nn("5")));
        assert!(!hit(&c, "a"));
        assert!(hit(&c, "b"));
    }
}
