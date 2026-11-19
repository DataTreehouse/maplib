//! Sketches of a column of dictionary codes, for estimating the size of a join between two
//! tables on any pair of their columns without keeping the columns' histograms.
//!
//! A sketch keeps the column's number of rows and distinct codes, its [TOP_KEYS] most common
//! codes with their exact counts, and a HyperLogLog of its codes ([HLL_P] bits of register
//! index, 4 KiB). Two sketches of columns of the same key type give:
//! - the codes the columns share, `|A| + |B| - |A ∪ B|`, the union from merging the two
//!   HyperLogLogs;
//! - the join size, `Σ cA(k)·cB(k)`: exact over the most common codes of either column whose
//!   count in the other column is known (from its list of most common codes, or from its counts
//!   of the codes that are common in other columns, which [Triplestore::analyze] adds); for a
//!   common code whose count in the other column is not known, its count times the average count
//!   of the other column's less common codes, weighted by the chance that the other column has
//!   it; and for the less common codes of both, the number they share times both average counts.
//!
//! [Triplestore::analyze]: crate::Triplestore::analyze
//!
//! Codes are only comparable within one key type (IRIs, blank nodes, ...), as each type has its
//! own dictionary.

use representation::BaseRDFNodeType;
use std::collections::HashMap;

/// The number of most common codes kept per column.
pub const TOP_KEYS: usize = 64;
/// The fewest common codes of a column whose presence in another must be known for their share
/// to estimate the codes the columns share (see [ColumnSketch::join_size]).
const MIN_CHECKED: f64 = 8.0;
/// The number of bits of a hash that index the HyperLogLog registers.
pub const HLL_P: u32 = 12;
const HLL_M: usize = 1 << HLL_P;

#[derive(Debug, Clone)]
pub struct ColumnSketch {
    /// The type of the column's values: only sketches of the same type are compared.
    pub key_type: BaseRDFNodeType,
    pub rows: usize,
    pub distinct: usize,
    /// The most common codes with their counts, most common first; only codes with more than
    /// one row.
    pub top: Vec<(u32, u64)>,
    /// The counts in this column of the codes that are among the most common ones of other
    /// columns of the same type (only those it has: the others count 0), set by
    /// [Triplestore::analyze]: with them, the
    /// join of a code common in one column with a column where it is rare is exact.
    ///
    /// [Triplestore::analyze]: crate::Triplestore::analyze
    pub cross: Option<HashMap<u32, u64>>,
    hll: Box<[u8]>,
}

/// A 64-bit mix of a code (SplitMix64), so that HyperLogLog sees uniform hashes.
fn mix(x: u32) -> u64 {
    let mut z = (x as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hll_add(regs: &mut [u8], code: u32) {
    let h = mix(code);
    let idx = (h >> (64 - HLL_P)) as usize;
    let w = h << HLL_P;
    let rho = (w.leading_zeros() + 1).min(64 - HLL_P + 1) as u8;
    if regs[idx] < rho {
        regs[idx] = rho;
    }
}

/// The HyperLogLog estimate of the number of distinct codes, with linear counting for small
/// numbers.
fn hll_count(regs: impl Iterator<Item = u8>) -> f64 {
    let m = HLL_M as f64;
    let mut sum = 0.0;
    let mut zeros = 0usize;
    for r in regs {
        sum += 2f64.powi(-(r as i32));
        if r == 0 {
            zeros += 1;
        }
    }
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let e = alpha * m * m / sum;
    if e <= 2.5 * m && zeros > 0 {
        m * (m / zeros as f64).ln()
    } else {
        e
    }
}

impl ColumnSketch {
    /// The sketch of a column of codes. Sorts them, so it takes ownership.
    pub fn from_codes(key_type: BaseRDFNodeType, codes: Vec<u32>) -> ColumnSketch {
        Self::from_codes_sorted(key_type, codes).0
    }

    /// The sketch of a column of codes, and the codes sorted.
    pub fn from_codes_sorted(
        key_type: BaseRDFNodeType,
        mut codes: Vec<u32>,
    ) -> (ColumnSketch, Vec<u32>) {
        let rows = codes.len();
        if codes.len() > 1 << 16 {
            use rayon::slice::ParallelSliceMut;
            codes.par_sort_unstable();
        } else {
            codes.sort_unstable();
        }
        let mut hll = vec![0u8; HLL_M].into_boxed_slice();
        let mut distinct = 0usize;
        // The most common codes, kept as a min-heap of (count, code).
        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(u64, u32)>> =
            std::collections::BinaryHeap::with_capacity(TOP_KEYS + 1);
        let mut i = 0;
        while i < codes.len() {
            let c = codes[i];
            let mut j = i + 1;
            while j < codes.len() && codes[j] == c {
                j += 1;
            }
            let n = (j - i) as u64;
            distinct += 1;
            hll_add(&mut hll, c);
            if n > 1 {
                heap.push(std::cmp::Reverse((n, c)));
                if heap.len() > TOP_KEYS {
                    heap.pop();
                }
            }
            i = j;
        }
        let mut top: Vec<(u32, u64)> = heap
            .into_iter()
            .map(|std::cmp::Reverse((n, c))| (c, n))
            .collect();
        top.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        (
            ColumnSketch {
                key_type,
                rows,
                distinct,
                top,
                cross: None,
                hll,
            },
            codes,
        )
    }

    /// The sketch with the counts of `keys` (the most common codes of other columns) in this
    /// column, from its sorted codes.
    pub fn with_cross(&self, sorted_codes: &[u32], keys: &[u32]) -> ColumnSketch {
        let mut cross = HashMap::new();
        for k in keys {
            let lo = sorted_codes.partition_point(|c| c < k);
            let hi = sorted_codes.partition_point(|c| c <= k);
            // Only the codes the column has: the others count 0.
            if hi > lo {
                cross.insert(*k, (hi - lo) as u64);
            }
        }
        let mut s = self.clone();
        s.cross = Some(cross);
        s
    }

    /// The count of a code in this column, when it is known: from the most common codes, or
    /// from the counts of other columns' common codes.
    fn known_count(&self, top: &HashMap<u32, u64>, k: u32) -> Option<u64> {
        if let Some(n) = top.get(&k) {
            return Some(*n);
        }
        self.cross.as_ref().map(|c| c.get(&k).copied().unwrap_or(0))
    }

    /// The most rows that share one code.
    pub fn max_rows_per_key(&self) -> usize {
        match self.top.first() {
            Some((_, n)) => *n as usize,
            None => self.rows.min(1),
        }
    }

    /// The estimated number of distinct codes in either column.
    pub fn union_distinct(&self, other: &ColumnSketch) -> f64 {
        let merged = self.hll.iter().zip(other.hll.iter()).map(|(a, b)| *a.max(b));
        hll_count(merged)
    }

    /// The estimated number of rows of a join of the two columns on equal codes.
    pub fn join_size(&self, other: &ColumnSketch) -> f64 {
        if self.key_type != other.key_type || self.rows == 0 || other.rows == 0 {
            return 0.0;
        }
        let a_top: HashMap<u32, u64> = self.top.iter().cloned().collect();
        let b_top: HashMap<u32, u64> = other.top.iter().cloned().collect();
        // The average count of the codes not in the list of most common ones.
        let light = |s: &ColumnSketch| -> (f64, f64) {
            let heavy_rows: u64 = s.top.iter().map(|(_, n)| n).sum();
            let rows = s.rows.saturating_sub(heavy_rows as usize) as f64;
            let distinct = s.distinct.saturating_sub(s.top.len()) as f64;
            (rows, distinct)
        };
        let (a_light_rows, a_light_distinct) = light(self);
        let (b_light_rows, b_light_distinct) = light(other);
        let a_avg = if a_light_distinct > 0.0 { a_light_rows / a_light_distinct } else { 0.0 };
        let b_avg = if b_light_distinct > 0.0 { b_light_rows / b_light_distinct } else { 0.0 };

        // Every code common in either column: exact where its count in the other column is
        // known. Those of each column also show what share of its codes the other column has.
        let mut heavy: Vec<(u32, bool)> = self.top.iter().map(|(c, _)| (*c, true)).collect();
        for (c, _) in &other.top {
            if !a_top.contains_key(c) {
                heavy.push((*c, false));
            }
        }
        let mut size = 0.0;
        let mut known_shared = 0.0;
        // Per column: its common codes whose count in the other column is known, and how many
        // of those the other column has.
        let (mut a_checked, mut a_present, mut b_checked, mut b_present) = (0.0, 0.0, 0.0, 0.0);
        let mut a_unknown = vec![];
        let mut b_unknown = vec![];
        for (k, of_a) in &heavy {
            match (self.known_count(&a_top, *k), other.known_count(&b_top, *k)) {
                (Some(x), Some(y)) => {
                    size += (x as f64) * (y as f64);
                    let both = x > 0 && y > 0;
                    if both {
                        known_shared += 1.0;
                    }
                    if *of_a {
                        a_checked += 1.0;
                        a_present += f64::from(u8::from(both));
                    } else {
                        b_checked += 1.0;
                        b_present += f64::from(u8::from(both));
                    }
                }
                (Some(x), None) => b_unknown.push(x as f64),
                (None, Some(y)) => a_unknown.push(y as f64),
                (None, None) => {}
            }
        }
        let shared = self.shared_estimate(other, known_shared, (a_checked, a_present), (b_checked, b_present));
        if shared <= 0.0 {
            return size;
        }
        // The chance that a code of one column whose count in the other is unknown is in it.
        let shared_rest = (shared - known_shared).max(0.0);
        let p_in_b = (shared_rest / (self.distinct as f64 - known_shared).max(1.0)).min(1.0);
        let p_in_a = (shared_rest / (other.distinct as f64 - known_shared).max(1.0)).min(1.0);
        let mut expected_shared = 0.0;
        for x in &b_unknown {
            size += x * b_avg * p_in_b;
            expected_shared += p_in_b;
        }
        for y in &a_unknown {
            size += y * a_avg * p_in_a;
            expected_shared += p_in_a;
        }
        let light_shared = (shared_rest - expected_shared).max(0.0);
        size += light_shared * a_avg * b_avg;
        size.clamp(0.0, self.rows as f64 * other.rows as f64)
    }

    /// The estimated number of codes the two columns share. From the HyperLogLogs, unless their
    /// error (about 1.6% of the union) is large next to the smaller column: then from the share
    /// of the smaller column's common codes that the other column is known to have (their
    /// counts in the other column are known after [Triplestore::analyze]), when at least
    /// [MIN_CHECKED] of them are. Never fewer than the codes known to be in both.
    ///
    /// [Triplestore::analyze]: crate::Triplestore::analyze
    fn shared_estimate(
        &self,
        other: &ColumnSketch,
        known_shared: f64,
        (a_checked, a_present): (f64, f64),
        (b_checked, b_present): (f64, f64),
    ) -> f64 {
        let (a, b) = (self.distinct as f64, other.distinct as f64);
        let union = self.union_distinct(other);
        let hll_error = 1.04 / (HLL_M as f64).sqrt() * union;
        let (small, checked, present) = if a <= b {
            (a, a_checked, a_present)
        } else {
            (b, b_checked, b_present)
        };
        let shared = if hll_error > 0.2 * small && checked >= MIN_CHECKED {
            present / checked * small
        } else {
            a + b - union
        };
        shared.clamp(known_shared, a.min(b).max(known_shared))
    }

    /// The memory the sketch holds, in bytes.
    pub fn memory(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.hll.len()
            + self.top.len() * 16
            + self.cross.as_ref().map_or(0, |c| c.len() * 24)
    }
}

/// The sketches of one segment of a table, made when the segment is created (by adding,
/// compacting or reading triples), so that a table's statistics are always at hand: they are
/// merged from its segments' sketches ([ColumnSketch::merge]) without reading any triples.
#[derive(Debug, Clone)]
pub struct SegmentSketch {
    /// The rows of the segment, less those deleted since the sketch was made.
    pub rows: usize,
    /// The rows deleted from the segment since the sketch was made: the counts of the column
    /// sketches still include them.
    pub deleted: usize,
    /// Sketches of the subject and object columns, where they hold dictionary codes.
    pub subject: Option<ColumnSketch>,
    pub object: Option<ColumnSketch>,
    /// The distinct values of a column without a sketch (e.g. numbers), counted exactly.
    pub subject_distinct: usize,
    pub object_distinct: usize,
}

impl SegmentSketch {
    /// The sketch after deleting `n` rows from the segment, or None when so many have been
    /// deleted (more than a fifth of the rows sketched) that it should be made again.
    pub fn after_delete(&self, n: usize) -> Option<SegmentSketch> {
        let deleted = self.deleted + n;
        let sketched = self.rows + self.deleted;
        if deleted * 5 > sketched {
            return None;
        }
        let mut s = self.clone();
        s.rows = self.rows.saturating_sub(n);
        s.deleted = deleted;
        Some(s)
    }
}

impl ColumnSketch {
    /// The sketch of the rows of several columns of one key type together: rows add up, the
    /// HyperLogLogs merge exactly, distinct codes are estimated from the merged HyperLogLog (exact
    /// for one sketch), and the most common codes are those with the most rows summed over the
    /// sketches' lists (a code common overall but in no list is missed, and a code's rows in the
    /// sketches that do not list it are not counted).
    pub fn merge(sketches: &[&ColumnSketch]) -> Option<ColumnSketch> {
        let first = *sketches.first()?;
        if sketches.len() == 1 {
            let mut s = first.clone();
            s.cross = None;
            return Some(s);
        }
        let mut hll = first.hll.clone();
        let mut rows = 0usize;
        let mut max_distinct = 0usize;
        let mut sum_distinct = 0usize;
        let mut counts: HashMap<u32, u64> = HashMap::new();
        for s in sketches {
            if s.key_type != first.key_type {
                return None;
            }
            for (r, x) in hll.iter_mut().zip(s.hll.iter()) {
                *r = (*r).max(*x);
            }
            rows += s.rows;
            max_distinct = max_distinct.max(s.distinct);
            sum_distinct += s.distinct;
            for (c, n) in &s.top {
                *counts.entry(*c).or_default() += n;
            }
        }
        let distinct = (hll_count(hll.iter().cloned()).round() as usize)
            .clamp(max_distinct, sum_distinct.min(rows));
        let mut top: Vec<(u32, u64)> = counts.into_iter().collect();
        top.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        top.truncate(TOP_KEYS);
        Some(ColumnSketch {
            key_type: first.key_type.clone(),
            rows,
            distinct,
            top,
            cross: None,
            hll,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch(codes: Vec<u32>) -> ColumnSketch {
        ColumnSketch::from_codes(BaseRDFNodeType::IRI, codes)
    }

    fn exact(a: &[u32], b: &[u32]) -> f64 {
        let mut ca: HashMap<u32, u64> = HashMap::new();
        for x in a {
            *ca.entry(*x).or_default() += 1;
        }
        b.iter().map(|x| *ca.get(x).unwrap_or(&0) as f64).sum()
    }

    #[test]
    fn counts_and_top_keys() {
        let mut codes = vec![];
        for i in 0..1000u32 {
            codes.push(i);
        }
        for _ in 0..500 {
            codes.push(7);
        }
        let s = sketch(codes);
        assert_eq!((s.rows, s.distinct), (1500, 1000));
        assert_eq!(s.top[0], (7, 501));
        assert_eq!(s.max_rows_per_key(), 501);
        let d = hll_count(s.hll.iter().cloned());
        assert!((d - 1000.0).abs() < 50.0, "{d}");
    }

    #[test]
    fn key_joins_within_a_few_percent() {
        // Keys 0..20000 against 10000..40000: 10000 shared, one row each.
        let a: Vec<u32> = (0..20_000).collect();
        let b: Vec<u32> = (10_000..40_000).collect();
        let est = sketch(a.clone()).join_size(&sketch(b.clone()));
        let ex = exact(&a, &b);
        assert!((est - ex).abs() / ex < 0.15, "{est} vs {ex}");
    }

    #[test]
    fn skewed_joins_within_two_times() {
        // A: 90% of rows on 3 classes; B: every subject once (like ?s a ?c . ?c :p ?x).
        let mut a = vec![];
        for i in 0..30_000u32 {
            a.push(if i % 10 < 9 { i % 3 } else { 100 + i });
        }
        let b: Vec<u32> = (0..5_000).chain(50_000..60_000).collect();
        let ex = exact(&a, &b);
        // With the counts of each other's common codes, as analyze adds them.
        let (sa, ca) = ColumnSketch::from_codes_sorted(BaseRDFNodeType::IRI, a.clone());
        let (sb, cb) = ColumnSketch::from_codes_sorted(BaseRDFNodeType::IRI, b.clone());
        let keys: Vec<u32> = sa.top.iter().chain(sb.top.iter()).map(|(c, _)| *c).collect();
        let (sa, sb) = (sa.with_cross(&ca, &keys), sb.with_cross(&cb, &keys));
        let est = sa.join_size(&sb);
        assert!(est / ex < 1.25 && ex / est < 1.25, "{est} vs {ex}");
        assert!((sb.join_size(&sa) - est).abs() < 1e-6 * est);
        // Without them, a code common in A and once in B gets the average chance of being in B:
        // an underestimate, but never more than the true size.
        let est = sketch(a.clone()).join_size(&sketch(b.clone()));
        assert!(est <= ex, "{est} vs {ex}");
    }

    #[test]
    fn common_on_both_sides() {
        // ?x a ?c . ?y a ?c: classes common in both columns, a quadratic join.
        let a: Vec<u32> = (0..10_000).map(|i| i % 5).collect();
        let est = sketch(a.clone()).join_size(&sketch(a.clone()));
        let ex = exact(&a, &a);
        assert!((est - ex).abs() / ex < 0.01, "{est} vs {ex}");
    }

    #[test]
    fn disjoint_and_other_types_give_nothing() {
        let a = sketch((0..5_000).collect());
        let b = sketch((100_000..105_000).collect());
        assert!(a.join_size(&b) < 0.05 * 5_000.0);
        let c = ColumnSketch::from_codes(BaseRDFNodeType::BlankNode, (0..5_000).collect());
        assert_eq!(a.join_size(&c), 0.0);
    }

    #[test]
    fn merged_segments() {
        let a: Vec<u32> = (0..10_000).map(|i| i % 3000).collect();
        let b: Vec<u32> = (2_000..8_000).chain((0..2_000).map(|_| 5)).collect();
        let (sa, sb) = (sketch(a.clone()), sketch(b.clone()));
        let m = ColumnSketch::merge(&[&sa, &sb]).unwrap();
        let mut all = a.clone();
        all.extend(&b);
        let whole = sketch(all);
        assert_eq!(m.rows, whole.rows);
        assert!((m.distinct as f64 - whole.distinct as f64).abs() / (whole.distinct as f64) < 0.05);
        // Code 5 has 2,004 rows; a's list may miss its 4 there.
        assert_eq!(m.top[0].0, 5);
        assert!((2_000..=2_004).contains(&m.top[0].1), "{:?}", m.top[0]);
    }

    #[test]
    fn few_codes_against_many() {
        // Regression (RINF 0076, era:validity objects against rdf:type subjects, 207 against
        // 361k distinct): the overlap clamped to 0 and the join was estimated empty although
        // the counts of the common codes were known. 200 codes (17,000 rows on the most common)
        // all among 360,000 codes: the HyperLogLog
        // error is far larger than 200, so the share comes from the common codes.
        let mut a = vec![];
        for k in 0..200u32 {
            for _ in 0..(if k < 10 { 1_000 } else { 30 }) {
                a.push(k * 1_000);
            }
        }
        let b: Vec<u32> = (0..360_000).map(|i| i * 3 + (i % 2)).collect();
        let b: Vec<u32> = b.into_iter().chain((0..200u32).map(|k| k * 1_000)).collect();
        let (sa, ca) = ColumnSketch::from_codes_sorted(BaseRDFNodeType::IRI, a.clone());
        let (sb, cb) = ColumnSketch::from_codes_sorted(BaseRDFNodeType::IRI, b.clone());
        let keys: Vec<u32> = sa.top.iter().chain(sb.top.iter()).map(|(c, _)| *c).collect();
        let (sa, sb) = (sa.with_cross(&ca, &keys), sb.with_cross(&cb, &keys));
        let ex = exact(&a, &b);
        let est = sa.join_size(&sb);
        assert!(est / ex < 1.5 && ex / est < 1.5, "{est} vs {ex}");
        // And none of them: the estimate is near zero, not a share of the HyperLogLog error.
        let c: Vec<u32> = (0..360_000).map(|i| 10_000_000 + i).collect();
        let (sc, cc) = ColumnSketch::from_codes_sorted(BaseRDFNodeType::IRI, c);
        let sc = sc.with_cross(&cc, &keys);
        assert!(sa.join_size(&sc) < 0.05 * ex, "{}", sa.join_size(&sc));
    }
}
