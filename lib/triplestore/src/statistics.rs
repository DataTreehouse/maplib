//! Statistics of the stored triple tables, for estimating how many rows the triple patterns of a
//! query match and joins between them give.
//!
//! Each table (graph, predicate, subject type, object type) gets its number of triples, its
//! number of distinct subjects and objects, the most triples per subject and per object, its most
//! common objects, and sketches of its columns of dictionary codes for estimating joins
//! ([sketch]).
//!
//! They are kept up to date without reading the triples again: every segment of a table is
//! sketched when it is created (by adding, compacting or reading triples), deletes adjust the
//! sketches of the segments they change, and a table's statistics are merged from its segments'
//! sketches ([merged_table_stats]) whenever the table has changed. [Triplestore::analyze] with
//! `deep` reads every table again for exact counts, also of tables in disk storage mode, and adds
//! to each column the counts of the codes that are most common in other columns, for exact joins
//! of codes that are common on one side only. Those statistics are kept, scaled, until the table
//! has grown or shrunk by more than [STALE_FRACTION]; after that the merged ones are used again.

pub mod sketch;

use crate::errors::TriplestoreError;
use crate::storage::Triples;
use crate::Triplestore;
use oxrdf::{NamedNode, Term};
use polars::prelude::{col, concat, len, DataType, IntoLazy, LazyFrame, SortMultipleOptions, UnionArgs};
use rayon::prelude::*;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::column_as_terms;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sketch::ColumnSketch;
use utils::fast_path::{self, FastPath};
use utils::polars::collect_in_memory;

/// The statistics of a deep [Triplestore::analyze] are kept until the number of triples of a
/// table has changed by more than this fraction.
pub const STALE_FRACTION: f64 = 0.2;

/// The number of most common objects kept per table.
pub const MOST_COMMON_OBJECTS: usize = 16;

/// The statistics of one table of triples.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// The number of triples when the statistics were computed.
    pub rows: usize,
    pub distinct_subjects: usize,
    pub distinct_objects: usize,
    /// The most triples that share one subject, and one object: upper bounds on how many
    /// triples a join on the subject or object finds per key.
    pub max_subject_rows: usize,
    pub max_object_rows: usize,
    /// The most common objects and how many triples have them, most common first. Only kept
    /// when some object is shared by more than one triple, and only for objects that can be
    /// turned into terms.
    pub most_common_objects: Vec<(Term, usize)>,
    /// Sketches of the subject and object columns, for estimating joins with other tables
    /// ([ColumnSketch::join_size]); only for columns of dictionary codes.
    pub subject_sketch: Option<Arc<ColumnSketch>>,
    pub object_sketch: Option<Arc<ColumnSketch>>,
    /// Whether the counts come from the stored triples (read, or from the segments' sketches),
    /// or are only assumed from the number of triples (every subject and object distinct).
    pub analyzed: bool,
    /// Whether the counts are exact: read from the triples, or from the sketch of a table's only
    /// segment with nothing deleted since. Merged and scaled counts are estimates.
    pub exact: bool,
    /// The generation of the table the counts were read from ([Triples::generation]), 0 when
    /// they were assumed. Scaled statistics keep it, so they are exact only when it is the
    /// table's current generation.
    pub generation: u64,
}

impl TableStats {
    /// Assumed statistics of a table that has not been read: every subject and object distinct.
    pub fn assumed(rows: usize) -> TableStats {
        TableStats {
            rows,
            distinct_subjects: rows,
            distinct_objects: rows,
            // Unknown: any key may have all the triples.
            max_subject_rows: rows,
            max_object_rows: rows,
            most_common_objects: vec![],
            subject_sketch: None,
            object_sketch: None,
            analyzed: false,
            exact: false,
            generation: 0,
        }
    }

    /// Whether the counts are exact for a table of this generation: read from it, not assumed
    /// or scaled. Only then are the maximums per key upper bounds.
    pub fn exact_for(&self, generation: u64) -> bool {
        self.exact && self.generation == generation
    }

    /// The statistics scaled to a table that now has `rows` triples.
    pub fn scaled(&self, rows: usize) -> TableStats {
        if rows == self.rows || self.rows == 0 {
            let mut s = self.clone();
            s.rows = rows;
            return s;
        }
        let f = rows as f64 / self.rows as f64;
        let scale = |x: usize| ((x as f64 * f).round() as usize).clamp(1.min(rows), rows);
        TableStats {
            rows,
            distinct_subjects: scale(self.distinct_subjects),
            distinct_objects: scale(self.distinct_objects),
            max_subject_rows: scale(self.max_subject_rows),
            max_object_rows: scale(self.max_object_rows),
            most_common_objects: self
                .most_common_objects
                .iter()
                .map(|(t, c)| (t.clone(), scale(*c)))
                .collect(),
            subject_sketch: self.subject_sketch.clone(),
            object_sketch: self.object_sketch.clone(),
            analyzed: self.analyzed,
            exact: false,
            generation: self.generation,
        }
    }

    /// The estimated number of triples with the object of dictionary code `code`, for a column
    /// of codes: its count among the object sketch's most common codes, or else the average count
    /// of the other objects, at most that of the least common listed one, as in
    /// [Self::object_frequency].
    pub fn object_frequency_code(&self, code: u32) -> f64 {
        let top: &[(u32, u64)] = self
            .object_sketch
            .as_ref()
            .map(|s| s.top.as_slice())
            .unwrap_or(&[]);
        if let Some((_, c)) = top.iter().find(|(k, _)| *k == code) {
            return *c as f64;
        }
        let common: u64 = top.iter().map(|(_, c)| c).sum();
        let rest_rows = (self.rows as u64).saturating_sub(common) as f64;
        let rest_distinct = (self.distinct_objects.saturating_sub(top.len())).max(1) as f64;
        let cap = top.last().map(|(_, c)| *c as f64).unwrap_or(f64::INFINITY);
        (rest_rows / rest_distinct).min(cap).max(1.0)
    }

    /// The estimated number of triples with the object `o`.
    pub fn object_frequency(&self, o: &Term) -> f64 {
        if let Some((_, c)) = self.most_common_objects.iter().find(|(t, _)| t == o) {
            return *c as f64;
        }
        let common: usize = self.most_common_objects.iter().map(|(_, c)| c).sum();
        let rest_rows = self.rows.saturating_sub(common) as f64;
        let rest_distinct =
            (self.distinct_objects.saturating_sub(self.most_common_objects.len())).max(1) as f64;
        // An object that is not among the most common ones has at most as many triples as the
        // least common of them.
        let cap = self
            .most_common_objects
            .last()
            .map(|(_, c)| *c as f64)
            .unwrap_or(f64::INFINITY);
        (rest_rows / rest_distinct).min(cap).max(1.0)
    }
}

/// The key of a table of triples.
pub type TableKey = (NamedGraph, NamedNode, BaseRDFNodeType, BaseRDFNodeType);

/// The statistics of the tables: those of the last deep [Triplestore::analyze], and those merged
/// from the segments' sketches, each with the generation of the table they were made for.
#[derive(Default)]
pub struct StatsCache {
    deep: Mutex<HashMap<TableKey, Arc<TableStats>>>,
    merged: Mutex<HashMap<TableKey, Arc<TableStats>>>,
}

impl Clone for StatsCache {
    fn clone(&self) -> Self {
        let lock = |m: &Mutex<HashMap<TableKey, Arc<TableStats>>>| {
            m.lock().unwrap_or_else(|e| e.into_inner()).clone()
        };
        StatsCache {
            deep: Mutex::new(lock(&self.deep)),
            merged: Mutex::new(lock(&self.merged)),
        }
    }
}

fn get(m: &Mutex<HashMap<TableKey, Arc<TableStats>>>, key: &TableKey) -> Option<Arc<TableStats>> {
    m.lock().unwrap_or_else(|e| e.into_inner()).get(key).cloned()
}

fn put(m: &Mutex<HashMap<TableKey, Arc<TableStats>>>, key: TableKey, stats: Arc<TableStats>) {
    m.lock().unwrap_or_else(|e| e.into_inner()).insert(key, stats);
}

impl StatsCache {
    /// The memory held by the sketches of the statistics of deep analyses, in bytes (those of
    /// merged statistics are held by the segments, see [Triplestore::sketch_memory]).
    fn deep_sketch_memory(&self) -> usize {
        self.deep
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|s| {
                s.subject_sketch.as_ref().map_or(0, |k| k.memory())
                    + s.object_sketch.as_ref().map_or(0, |k| k.memory())
            })
            .sum()
    }

    /// Forgets the statistics of every table, e.g. after the triples were replaced.
    pub fn clear(&self) {
        self.deep.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.merged.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

fn is_stale(stats: &TableStats, rows: usize) -> bool {
    let d = (rows as f64 - stats.rows as f64).abs();
    d > STALE_FRACTION * (stats.rows.max(1) as f64)
}

/// The subject and object columns of a table, collected; None when it has no triples.
fn table_columns(t: &Triples) -> Result<Option<polars::prelude::DataFrame>, TriplestoreError> {
    if t.height() == 0 {
        return Ok(None);
    }
    let Some(lfs) = t.get_lazy_frame_slices()? else {
        return Ok(None);
    };
    let lf: LazyFrame = if lfs.len() == 1 {
        lfs.into_iter().next().unwrap()
    } else {
        concat(lfs, UnionArgs { parallel: true, rechunk: false, to_supertypes: false, ..Default::default() })
            .map_err(TriplestoreError::LazyLoadError)?
    };
    Ok(Some(
        collect_in_memory(lf.select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]))
            .map_err(TriplestoreError::LazyLoadError)?,
    ))
}

/// The sorted codes of a table's subject and object columns, where they hold codes.
type SortedCodes = (Option<Vec<u32>>, Option<Vec<u32>>);

/// Reads the statistics of a table from its stored columns, and returns the sorted codes of
/// its sketched columns too.
fn stats_and_codes(
    t: &Triples,
    df: &polars::prelude::DataFrame,
    global_cats: LockedCats,
) -> Result<(TableStats, SortedCodes), TriplestoreError> {
    let rows = t.height();
    let s = df.column(SUBJECT_COL_NAME).unwrap();
    let o = df.column(OBJECT_COL_NAME).unwrap();
    let (subject_sketch, subject_codes) = column_sketch(s, &t.subject_type).unzip();
    let (object_sketch, object_codes) = column_sketch(o, &t.object_type).unzip();
    // From the sketch where there is one; a column type without n_unique is counted as all
    // distinct.
    let distinct = |c: &polars::prelude::Column, sk: &Option<Arc<ColumnSketch>>| match sk {
        Some(sk) => sk.distinct.clamp(1, rows),
        None => c.n_unique().unwrap_or(rows).clamp(1, rows),
    };
    let distinct_subjects = distinct(s, &subject_sketch);
    let distinct_objects = distinct(o, &object_sketch);
    let max_rows_per_key = |c: &str, distinct: usize, sk: &Option<Arc<ColumnSketch>>| -> usize {
        if distinct == rows {
            return 1;
        }
        if let Some(sk) = sk {
            return sk.max_rows_per_key();
        }
        collect_in_memory(
            df.select([c])
                .unwrap()
                .lazy()
                .group_by([col(c)])
                .agg([len().alias("n")])
                .select([col("n").max()]),
        )
        .ok()
        .and_then(|d| d.column("n").ok()?.cast(&DataType::UInt64).ok()?.u64().ok()?.get(0))
        .map(|n| n as usize)
        .unwrap_or(rows)
    };
    let max_subject_rows = max_rows_per_key(SUBJECT_COL_NAME, distinct_subjects, &subject_sketch);
    let max_object_rows = max_rows_per_key(OBJECT_COL_NAME, distinct_objects, &object_sketch);
    let mut most_common_objects = vec![];
    // Only scalar columns: language-tagged strings are stored as structs.
    let scalar = !matches!(o.dtype(), DataType::Struct(_) | DataType::List(_) | DataType::Array(..));
    // Triple terms are not compared with the constants of queries here yet.
    if distinct_objects < rows && scalar && !t.object_type.is_triple_term() {
        let counts = collect_in_memory(
            df.select([OBJECT_COL_NAME])
                .unwrap()
                .lazy()
                .group_by([col(OBJECT_COL_NAME)])
                .agg([len().alias("n")])
                .sort(
                    ["n"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .limit(MOST_COMMON_OBJECTS as u32),
        )
        .map_err(TriplestoreError::LazyLoadError)?;
        let values = counts.column(OBJECT_COL_NAME).unwrap();
        let ns = counts.column("n").unwrap().cast(&DataType::UInt64).unwrap();
        let ns = ns.u64().unwrap();
        let state = t.object_type.clone().into_default_stored_rdf_node_state();
        let terms = column_as_terms(values, &state, global_cats, false);
        for (term, n) in terms.into_iter().zip(ns.iter()) {
            if let (Some(term), Some(n)) = (term, n) {
                // An object of one triple is no more common than any other.
                if n > 1 {
                    most_common_objects.push((term, n as usize));
                }
            }
        }
    }
    Ok((
        TableStats {
            rows,
            distinct_subjects,
            distinct_objects,
            max_subject_rows,
            max_object_rows,
            most_common_objects,
            subject_sketch,
            object_sketch,
            analyzed: true,
            exact: true,
            generation: t.generation(),
        },
        (subject_codes, object_codes),
    ))
}

/// The sketch of a stored column and its codes sorted, when it holds dictionary codes.
fn column_sketch(
    c: &polars::prelude::Column,
    key_type: &BaseRDFNodeType,
) -> Option<(Arc<ColumnSketch>, Vec<u32>)> {
    if c.dtype() != &DataType::UInt32 {
        return None;
    }
    let ca = c.as_materialized_series().u32().ok()?;
    let codes: Vec<u32> = ca.iter().flatten().collect();
    let (sk, sorted) = ColumnSketch::from_codes_sorted(key_type.clone(), codes);
    Some((Arc::new(sk), sorted))
}

/// The sketches of a segment's subject and object columns, from its triples (`df`, with the
/// subject and object columns).
pub(crate) fn sketch_segment(
    df: &polars::prelude::DataFrame,
    subject_type: &BaseRDFNodeType,
    object_type: &BaseRDFNodeType,
) -> sketch::SegmentSketch {
    let rows = df.height();
    let one = |c: &str, t: &BaseRDFNodeType| -> (Option<ColumnSketch>, usize) {
        let Ok(c) = df.column(c) else {
            return (None, rows);
        };
        match column_sketch(c, t) {
            Some((sk, _)) => {
                let d = sk.distinct;
                (Some(Arc::unwrap_or_clone(sk)), d)
            }
            None => (None, c.n_unique().unwrap_or(rows).min(rows)),
        }
    };
    let (subject, subject_distinct) = one(SUBJECT_COL_NAME, subject_type);
    let (object, object_distinct) = one(OBJECT_COL_NAME, object_type);
    sketch::SegmentSketch {
        rows,
        deleted: 0,
        subject,
        object,
        subject_distinct,
        object_distinct,
    }
}

/// The statistics of a table merged from its segments' sketches.
pub(crate) fn merged_table_stats(
    t: &Triples,
    sketches: &[Arc<sketch::SegmentSketch>],
    global_cats: LockedCats,
) -> TableStats {
    let rows = t.height();
    if rows == 0 || sketches.is_empty() {
        return TableStats::assumed(rows);
    }
    let merge = |f: &dyn Fn(&sketch::SegmentSketch) -> Option<&ColumnSketch>| {
        let cols: Option<Vec<&ColumnSketch>> = sketches.iter().map(|s| f(s)).collect();
        cols.and_then(|c| ColumnSketch::merge(&c)).map(Arc::new)
    };
    let subject_sketch = merge(&|s| s.subject.as_ref());
    let object_sketch = merge(&|s| s.object.as_ref());
    // Without a sketch, distinct values add up over the segments: an upper bound.
    let distinct = |sk: &Option<Arc<ColumnSketch>>, plain: &dyn Fn(&sketch::SegmentSketch) -> usize| {
        match sk {
            Some(sk) => sk.distinct,
            None => sketches.iter().map(|s| plain(s)).sum(),
        }
        .clamp(1, rows)
    };
    // The table's own exact count where it has one (one segment and an empty write buffer).
    let distinct_subjects = t
        .exact_distinct(false)
        .inspect(|_| fast_path::hit(FastPath::TableStatsExactDistinct))
        .map(|d| d.clamp(1, rows))
        .unwrap_or_else(|| distinct(&subject_sketch, &|s| s.subject_distinct));
    let distinct_objects = t
        .exact_distinct(true)
        .map(|d| d.clamp(1, rows))
        .unwrap_or_else(|| distinct(&object_sketch, &|s| s.object_distinct));
    let max_rows = |sk: &Option<Arc<ColumnSketch>>, d: usize| match sk {
        _ if d == rows => 1,
        Some(sk) => sk.max_rows_per_key().max(1),
        None => rows,
    };
    let max_subject_rows = max_rows(&subject_sketch, distinct_subjects);
    let max_object_rows = max_rows(&object_sketch, distinct_objects);
    let mut most_common_objects = vec![];
    // Triple terms are not compared with the constants of queries here yet.
    let object_sketch_terms = object_sketch
        .as_ref()
        .filter(|_| !t.object_type.is_triple_term());
    if let Some(sk) = object_sketch_terms {
        let top: Vec<&(u32, u64)> = sk.top.iter().take(MOST_COMMON_OBJECTS).collect();
        if !top.is_empty() {
            let codes = polars::prelude::Column::new(
                OBJECT_COL_NAME.into(),
                top.iter().map(|(c, _)| *c).collect::<Vec<u32>>(),
            );
            let state = t.object_type.clone().into_default_stored_rdf_node_state();
            let terms = column_as_terms(&codes, &state, global_cats, false);
            for (term, (_, n)) in terms.into_iter().zip(top) {
                if let Some(term) = term {
                    most_common_objects.push((term, *n as usize));
                }
            }
        }
    }
    let exact = sketches.len() == 1 && sketches[0].deleted == 0 && sketches[0].rows == rows;
    TableStats {
        rows,
        distinct_subjects,
        distinct_objects,
        max_subject_rows,
        max_object_rows,
        most_common_objects,
        subject_sketch,
        object_sketch,
        analyzed: true,
        exact,
        generation: t.generation(),
    }
}

/// Up to this many triples in all, [Triplestore::analyze] keeps the sorted codes of the first
/// pass (4 bytes per code) for the second; above it, it reads and sorts them again.
const KEEP_CODES_MAX_ROWS: usize = 50_000_000;

/// One row of [Triplestore::analyze]: the statistics of a table.
pub struct AnalyzedTable {
    pub graph: NamedGraph,
    pub predicate: NamedNode,
    pub subject_type: BaseRDFNodeType,
    pub object_type: BaseRDFNodeType,
    pub stats: Arc<TableStats>,
}

impl Triplestore {
    /// The statistics of a table: those of the last deep [Triplestore::analyze] while the table
    /// has not changed much since, otherwise those merged from its segments' sketches. With
    /// `read`, segments read from storage into memory that have no sketch yet are sketched;
    /// without, and for tables in disk storage mode until a deep analysis, a table without
    /// sketches gets assumed statistics.
    pub(crate) fn table_stats(
        &self,
        graph: &NamedGraph,
        predicate: &NamedNode,
        types: &(BaseRDFNodeType, BaseRDFNodeType),
        t: &Triples,
        read: bool,
    ) -> Arc<TableStats> {
        let rows = t.height();
        let key: TableKey = (graph.clone(), predicate.clone(), types.0.clone(), types.1.clone());
        if let Some(s) = get(&self.stats_cache.deep, &key) {
            if !is_stale(&s, rows) {
                fast_path::hit(FastPath::TableStatsDeep);
                return if s.generation == t.generation() {
                    s
                } else {
                    Arc::new(s.scaled(rows))
                };
            }
        }
        if let Some(s) = get(&self.stats_cache.merged, &key) {
            if s.generation == t.generation() {
                fast_path::hit(FastPath::TableStatsMerged);
                return s;
            }
        }
        let Some(sketches) = t.segment_sketches(read && t.in_memory()) else {
            fast_path::hit(FastPath::TableStatsAssumed);
            return Arc::new(TableStats::assumed(rows));
        };
        fast_path::hit(FastPath::TableStatsMerged);
        let stats = Arc::new(merged_table_stats(t, &sketches, self.global_cats.clone()));
        put(&self.stats_cache.merged, key, stats.clone());
        stats
    }

    /// The statistics of every table, sorted by graph, predicate and types. Without `deep`,
    /// those kept up to date from the segments' sketches. With `deep`, every table is read
    /// again for exact counts (see [Triplestore::analyze_deep]), and they are kept for planning.
    pub fn analyze(&self, deep: bool) -> Result<Vec<AnalyzedTable>, TriplestoreError> {
        if deep {
            return self.analyze_deep();
        }
        let mut out = vec![];
        for (g, pm) in self.graph_triples_map.iter() {
            for (p, tm) in pm {
                for (types, t) in tm {
                    out.push(AnalyzedTable {
                        graph: g.clone(),
                        predicate: p.clone(),
                        subject_type: types.0.clone(),
                        object_type: types.1.clone(),
                        stats: self.table_stats(g, p, types, t, true),
                    });
                }
            }
        }
        sort_analyzed(&mut out);
        Ok(out)
    }

    /// Reads the statistics of every table again, also of those in disk storage mode, and keeps
    /// them for planning queries. Returns them, sorted by graph, predicate and types.
    ///
    /// Two passes: the first sketches every column; the second adds to each sketch the counts in
    /// its column of the codes that are most common in other columns of the same type
    /// ([ColumnSketch::with_cross]), so that joins of a common code with a column where it is
    /// rare are estimated exactly.
    pub fn analyze_deep(&self) -> Result<Vec<AnalyzedTable>, TriplestoreError> {
        let mut tables = vec![];
        let mut total_rows = 0usize;
        for (g, pm) in self.graph_triples_map.iter() {
            for (p, tm) in pm {
                for (types, t) in tm {
                    total_rows = total_rows.saturating_add(t.height());
                    tables.push((g, p, types, t));
                }
            }
        }
        let keep_codes = total_rows <= KEEP_CODES_MAX_ROWS;
        let global_cats = &self.global_cats;
        type First = (TableStats, SortedCodes);
        let first: Vec<First> = tables
            .par_iter()
            .map(|(_, _, _, t)| -> Result<First, TriplestoreError> {
                Ok(match table_columns(t)? {
                    Some(df) => {
                        let (st, codes) = stats_and_codes(t, &df, global_cats.clone())?;
                        (st, if keep_codes { codes } else { (None, None) })
                    }
                    None => (TableStats::assumed(t.height()), (None, None)),
                })
            })
            .collect::<Result<_, _>>()?;
        // The codes that are most common in some column, per key type, sorted.
        let mut common: HashMap<BaseRDFNodeType, Vec<u32>> = HashMap::new();
        for (st, _) in &first {
            for sk in [&st.subject_sketch, &st.object_sketch].into_iter().flatten() {
                common
                    .entry(sk.key_type.clone())
                    .or_default()
                    .extend(sk.top.iter().map(|(c, _)| *c));
            }
        }
        for v in common.values_mut() {
            v.sort_unstable();
            v.dedup();
        }
        let cache = &self.stats_cache;
        let common = &common;
        let computed: Vec<Result<AnalyzedTable, TriplestoreError>> = tables
            .into_par_iter()
            .zip(first.into_par_iter())
            .map(|((g, p, types, t), (mut st, (sc, oc)))| {
                let (sc, oc) = if keep_codes || (st.subject_sketch.is_none() && st.object_sketch.is_none()) {
                    (sc, oc)
                } else {
                    match table_columns(t)? {
                        Some(df) => (
                            sorted_codes(&df, SUBJECT_COL_NAME).map(Arc::unwrap_or_clone),
                            sorted_codes(&df, OBJECT_COL_NAME).map(Arc::unwrap_or_clone),
                        ),
                        None => (None, None),
                    }
                };
                let cross = |sk: &Option<Arc<ColumnSketch>>, codes: &Option<Vec<u32>>| match (sk, codes) {
                    (Some(sk), Some(codes)) => {
                        let keys = common.get(&sk.key_type).map(|v| v.as_slice()).unwrap_or(&[]);
                        Some(Arc::new(sk.with_cross(codes, keys)))
                    }
                    (sk, _) => sk.clone(),
                };
                st.subject_sketch = cross(&st.subject_sketch, &sc);
                st.object_sketch = cross(&st.object_sketch, &oc);
                let stats = Arc::new(st);
                let key: TableKey = (g.clone(), p.clone(), types.0.clone(), types.1.clone());
                put(&cache.deep, key, stats.clone());
                Ok(AnalyzedTable {
                    graph: g.clone(),
                    predicate: p.clone(),
                    subject_type: types.0.clone(),
                    object_type: types.1.clone(),
                    stats,
                })
            })
            .collect();
        let mut out = computed.into_iter().collect::<Result<Vec<_>, _>>()?;
        sort_analyzed(&mut out);
        Ok(out)
    }
}

fn sort_analyzed(out: &mut [AnalyzedTable]) {
    out.sort_by(|a, b| {
        (a.graph.to_string(), a.predicate.as_str(), a.subject_type.to_string(), a.object_type.to_string())
            .cmp(&(b.graph.to_string(), b.predicate.as_str(), b.subject_type.to_string(), b.object_type.to_string()))
    });
}

/// A pair of columns of two tables of one graph whose values can be equal, with the estimated
/// and (optionally) exact size of their join, from [Triplestore::join_size_estimates].
pub struct JoinSizeEstimate {
    pub graph: NamedGraph,
    pub left_predicate: NamedNode,
    pub left_position: &'static str,
    pub right_predicate: NamedNode,
    pub right_position: &'static str,
    pub key_type: BaseRDFNodeType,
    pub estimate: f64,
    pub exact: Option<u64>,
}

/// The exact size of a join of two sorted columns of codes.
fn exact_join_size(a: &[u32], b: &[u32]) -> u64 {
    let (mut i, mut j, mut n) = (0, 0, 0u64);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] > b[j] {
            j += 1;
        } else {
            let c = a[i];
            let i0 = i;
            while i < a.len() && a[i] == c {
                i += 1;
            }
            let j0 = j;
            while j < b.len() && b[j] == c {
                j += 1;
            }
            n += ((i - i0) as u64) * ((j - j0) as u64);
        }
    }
    n
}

/// A column of a table for [Triplestore::join_size_estimates]: its predicate, position, sketch
/// and, for exact sizes, its sorted codes.
type PairColumn<'a> = (&'a NamedNode, &'static str, Arc<ColumnSketch>, Option<Arc<Vec<u32>>>);

fn sorted_codes(df: &polars::prelude::DataFrame, c: &str) -> Option<Arc<Vec<u32>>> {
    let ca = df.column(c).ok()?.as_materialized_series().u32().ok()?.clone();
    let mut v: Vec<u32> = ca.iter().flatten().collect();
    v.sort_unstable();
    Some(Arc::new(v))
}

impl Triplestore {
    /// The estimated size of the join of every pair of columns (subject with subject, object
    /// with subject, object with object) of two tables of the same graph whose key types match,
    /// from the tables' sketches, and with `exact` also the exact size. For measuring the
    /// sketches: the exact sizes read every pair of columns.
    pub fn join_size_estimates(
        &self,
        exact: bool,
    ) -> Result<Vec<JoinSizeEstimate>, TriplestoreError> {
        self.analyze(true)?;
        let mut out = vec![];
        for (g, pm) in self.graph_triples_map.iter() {
            let mut cols: Vec<PairColumn> = vec![];
            for (p, tm) in pm {
                for (types, t) in tm {
                    let st = self.table_stats(g, p, types, t, true);
                    let (sc, oc) = if exact {
                        match table_columns(t)? {
                            Some(df) => (
                                sorted_codes(&df, SUBJECT_COL_NAME),
                                sorted_codes(&df, OBJECT_COL_NAME),
                            ),
                            None => (None, None),
                        }
                    } else {
                        (None, None)
                    };
                    if let Some(sk) = &st.subject_sketch {
                        cols.push((p, "subject", sk.clone(), sc));
                    }
                    if let Some(sk) = &st.object_sketch {
                        cols.push((p, "object", sk.clone(), oc));
                    }
                }
            }
            let n = cols.len();
            let mut pairs: Vec<(usize, usize)> = vec![];
            for i in 0..n {
                for j in 0..n {
                    let (a, b) = (&cols[i], &cols[j]);
                    if a.2.key_type != b.2.key_type {
                        continue;
                    }
                    // Subject-subject and object-object pairs once, object-subject both ways.
                    let keep = if a.1 == b.1 { i <= j } else { a.1 == "object" };
                    if keep {
                        pairs.push((i, j));
                    }
                }
            }
            let rows: Vec<JoinSizeEstimate> = pairs
                .into_par_iter()
                .map(|(i, j)| {
                    let (a, b) = (&cols[i], &cols[j]);
                    let exact = match (&a.3, &b.3) {
                        (Some(x), Some(y)) => Some(exact_join_size(x, y)),
                        _ => None,
                    };
                    JoinSizeEstimate {
                        graph: g.clone(),
                        left_predicate: a.0.clone(),
                        left_position: a.1,
                        right_predicate: b.0.clone(),
                        right_position: b.1,
                        key_type: a.2.key_type.clone(),
                        estimate: a.2.join_size(&b.2),
                        exact,
                    }
                })
                .collect();
            out.extend(rows);
        }
        Ok(out)
    }

    /// The memory held by the sketches of every table's statistics, in bytes.
    pub fn sketch_memory(&self) -> usize {
        let mut n = self.stats_cache.deep_sketch_memory();
        for pm in self.graph_triples_map.values() {
            for tm in pm.values() {
                for t in tm.values() {
                    for sk in t.segment_sketches(false).unwrap_or_default() {
                        n += sk.subject.as_ref().map_or(0, |k| k.memory())
                            + sk.object.as_ref().map_or(0, |k| k.memory());
                    }
                }
            }
        }
        n
    }
}
