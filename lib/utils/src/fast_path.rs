//! Counts of the fast paths that queries and updates take, for maplib's tests: a test of a fast
//! path checks that its case took that path instead of the general one, which gives the same
//! results. Counting is off until a test turns it on, and then costs one relaxed atomic add per
//! decision (not per row).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);

macro_rules! fast_paths {
    ($($name:ident => $label:literal,)*) => {
        /// A branch of the query engine that tests check is taken.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(usize)]
        pub enum FastPath {
            $($name,)*
        }

        const LABELS: &[&str] = &[$($label,)*];
    };
}

fast_paths! {
    // Property paths: walk on dictionary codes from at most 64 starts, or keying the tables.
    PathCodeWalk => "path.code_walk",
    PathCodeWalkAborted => "path.code_walk_aborted",
    PathKeyedBound => "path.keyed_bound",
    PathKeyedUnbound => "path.keyed_unbound",
    PathEveryNodeStep => "path.every_node_step",
    // COUNT(DISTINCT ?s) / COUNT(DISTINCT ?o) over dictionary codes.
    CountDistinctBitset => "count_distinct.bitset",
    CountDistinctBitsetParallel => "count_distinct.bitset_parallel",
    CountDistinctSorted => "count_distinct.sorted",
    // Whole-table distinct counts: the exact count of a table's single segment, or the cache
    // keyed by the generations of the tables counted.
    CountDistinctSegmentExact => "count_distinct.segment_exact",
    CountDistinctCached => "count_distinct.cached",
    // COUNT(*) over joins, EXISTS, OPTIONAL, MINUS and UNION of constant-predicate patterns,
    // counted on dictionary codes without building the joined rows.
    CountJoinsOnCodes => "count.joins_on_codes",
    // FILTER(?r = 1) over RANK.
    RankOne => "rank_one.applied",
    RankOneWindow => "rank_one.window",
    // The write buffer and small updates.
    InsertBuffered => "write_buffer.insert_buffered",
    InsertSegment => "write_buffer.insert_segment",
    BufferFlush => "write_buffer.flush",
    SegmentCompaction => "segments.compaction",
    UpdateEmptyWhereAsData => "update.empty_where_as_data",
    DeleteByCodePairs => "delete.code_pairs",
    DeleteByJoin => "delete.join",
    // Per-segment Bloom filters.
    SegmentFilterSkip => "segment_filter.skip",
    TableFilterSkip => "segment_filter.table_skip",
    // Codes of lookup targets found for the filters: at most 128 (1,024 with remote segments).
    FilterTargetCodes => "segment_filter.target_codes",
    FilterTooManyTargets => "segment_filter.too_many_targets",
    // Sparse index lookups: seek per target when targets x log2(keys) <= keys, else walk.
    LookupSeek => "lookup.seek",
    LookupWalk => "lookup.walk",
    // The Bloom filter in front of the on-disk dictionary (maplib Enterprise Edition).
    DiskDictionaryBloomAbsent => "disk_dictionary_bloom.absent",
    // Table statistics: the order of a BGP's triple patterns, its join estimates, and where a
    // table's statistics come from.
    BgpOrderStatistics => "bgp_order.statistics",
    BgpOrderWritten => "bgp_order.written",
    BgpOrderNoStatistics => "bgp_order.no_statistics",
    BgpEstimateTypeDisjoint => "bgp_estimate.type_disjoint",
    TableStatsDeep => "table_stats.deep",
    TableStatsMerged => "table_stats.merged",
    TableStatsAssumed => "table_stats.assumed",
    TableStatsExactDistinct => "table_stats.exact_distinct",
    // SELECT results as SPARQL JSON: one chunk (at most 4,096 rows) on the calling thread.
    JsonOneChunk => "sparql_json.one_chunk",
    JsonChunks => "sparql_json.chunks",
    // Basic graph patterns on codes.
    BgpSmallTableInPlan => "bgp.small_table_in_plan",
    BgpValuesPushed => "bgp.values_pushed",
    BgpProvenEmpty => "bgp.proven_empty",
    JoinEstimateBoundedPerKey => "join.estimate_bounded_per_key",
    OptionalLeftKeys => "optional.left_keys",
    OptionalNoKeys => "optional.no_keys",
    ExistsKeysCollected => "exists.keys_collected",
    ExistsKeysLazy => "exists.keys_lazy",
    MinusAntiJoin => "minus.anti_join",
}

/// Counts a hit of `path`, when counting is on.
#[inline]
pub fn hit(path: FastPath) {
    if ENABLED.load(Ordering::Relaxed) {
        COUNTS[path as usize].fetch_add(1, Ordering::Relaxed);
    }
}

/// Counts a hit of `path` when `taken`.
#[inline]
pub fn hit_when(taken: bool, path: FastPath) {
    if taken {
        hit(path)
    }
}

/// Counts a hit of `yes` when `taken`, else of `no`.
#[inline]
pub fn hit_if(taken: bool, yes: FastPath, no: FastPath) {
    hit(if taken { yes } else { no })
}

/// Counts a hit of `path` and gives None, for `Option::or_else`.
#[inline]
pub fn hit_none<T>(path: FastPath) -> Option<T> {
    hit(path);
    None
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static COUNTS: [AtomicU64; LABELS.len()] = [ZERO; LABELS.len()];

/// Turns counting on and sets every count to zero.
pub fn reset() {
    ENABLED.store(true, Ordering::Relaxed);
    for c in &COUNTS {
        c.store(0, Ordering::Relaxed);
    }
}

/// Every fast path's label and count since the last [`reset`].
pub fn counts() -> Vec<(&'static str, u64)> {
    LABELS
        .iter()
        .zip(COUNTS.iter())
        .map(|(l, c)| (*l, c.load(Ordering::Relaxed)))
        .collect()
}
