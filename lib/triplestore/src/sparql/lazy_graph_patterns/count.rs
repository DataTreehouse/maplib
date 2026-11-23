//! Counts over a single triple pattern answered straight from the stored triple tables, without
//! building solution mappings: e.g. `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }` or
//! `SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`.
//!
//! The tables of a graph are deduplicated and disjoint, and every table holds one subject type
//! and one object type, so:
//! - the number of solutions is the sum of the heights of the tables the pattern can match,
//! - a filter on the kind of the subject or object (isIRI, isBLANK, isLITERAL) selects whole
//!   tables,
//! - the distinct subjects (objects) are the distinct stored values per subject (object) type,
//!   counted over the stored columns, which are still dictionary codes for IRIs, blank nodes and
//!   strings.
//!
//! With `GROUP BY ?p` on the predicate variable, the same counts are given per predicate: one row
//! for each predicate with a table the pattern and filter select, e.g.
//! `SELECT ?p (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p`.
//!
//! Distinct counts are kept in a [DistinctCountCache], keyed by the generations of the tables
//! counted, which every change of a table renews.

use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::Triples;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::IntoLazy;
use polars_core::utils::polars_arrow::array::{Array, PrimitiveArray};
use polars_core::frame::DataFrame;
use polars_core::prelude::{ChunkAgg, Column, DataType, UInt32Chunked};
use rayon::prelude::*;
use representation::dataset::QueryGraph;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern};
use std::collections::{HashMap, HashSet};
use utils::fast_path::{self, FastPath};
use std::sync::Mutex;

/// The number of distinct counts kept.
const DISTINCT_CACHE_SIZE: usize = 32;

/// Exact distinct subject (or object) counts over sets of tables, most recent first. A count is
/// keyed by whether it counts subjects and by the sorted generations of its tables: every insert,
/// delete and merge of a table gives it a new generation (see `Triples::generation`), and
/// generations are unique, so a key names one state of the triples it counted. A write makes the
/// old keys unreachable; they age out.
#[derive(Default)]
pub struct DistinctCountCache {
    entries: Mutex<Vec<((bool, Vec<u64>), usize)>>,
}

impl Clone for DistinctCountCache {
    fn clone(&self) -> Self {
        DistinctCountCache::default()
    }
}

impl DistinctCountCache {
    fn get(&self, key: &(bool, Vec<u64>)) -> Option<usize> {
        let mut entries = self.entries.lock().unwrap();
        let i = entries.iter().position(|(k, _)| k == key)?;
        let e = entries.remove(i);
        let n = e.1;
        entries.insert(0, e);
        Some(n)
    }

    fn insert(&self, key: (bool, Vec<u64>), n: usize) {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|(k, _)| k != &key);
        entries.insert(0, (key, n));
        entries.truncate(DISTINCT_CACHE_SIZE);
    }
}

/// The term kinds a FILTER can ask for.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Iri,
    Blank,
    Literal,
}

impl Kind {
    fn matches(self, t: &BaseRDFNodeType) -> bool {
        match self {
            Kind::Iri => t.is_iri(),
            Kind::Blank => t.is_blank_node(),
            Kind::Literal => matches!(t, BaseRDFNodeType::Literal(..)),
        }
    }
}

/// A condition on the subject (true) or object (false) of a table: its type is (or, negated,
/// is not) of a kind.
type Condition = (bool, Kind, bool);

/// What one aggregate counts.
#[derive(Clone, Copy)]
enum Counted {
    /// All solutions (COUNT(*), COUNT(?v) of a variable of the pattern, COUNT(DISTINCT *)).
    Solutions,
    DistinctSubjects,
    DistinctPredicates,
    DistinctObjects,
}

impl Triplestore {
    /// Answers the aggregates if they are all counts over one triple pattern (with an optional
    /// filter on the kinds of its subject and object) that the stored tables answer directly.
    /// Returns None for anything else, which then takes the general path.
    ///
    /// `variables` are the GROUP BY variables: none, or only the predicate variable of the
    /// pattern, which gives the counts per predicate.
    pub(crate) fn count_from_tables(
        &self,
        inner: &GraphPattern,
        variables: &[Variable],
        aggregates: &[(Variable, AggregateExpression)],
        dataset: &QueryGraph,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let QueryGraph::NamedGraph(graph) = dataset else {
            return Ok(None);
        };
        let (patterns, filter) = match inner {
            GraphPattern::Bgp { patterns } => (patterns, None),
            GraphPattern::Filter { expr, inner } => match inner.as_ref() {
                GraphPattern::Bgp { patterns } => (patterns, Some(expr)),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let [tp] = patterns.as_slice() else {
            return Ok(None);
        };
        let TermPattern::Variable(s) = &tp.subject else {
            return Ok(None);
        };
        let TermPattern::Variable(o) = &tp.object else {
            return Ok(None);
        };
        let p = match &tp.predicate {
            NamedNodePattern::Variable(p) => {
                if p == s || p == o {
                    return Ok(None);
                }
                Some(p)
            }
            NamedNodePattern::NamedNode(_) => None,
        };
        if s == o || aggregates.is_empty() {
            return Ok(None);
        }
        let group_by = match variables {
            [] => None,
            [v] if Some(v) == p && aggregates.iter().all(|(a, _)| a != v) => Some(v),
            _ => return Ok(None),
        };
        let mut conditions = vec![];
        if let Some(expr) = filter {
            if !kind_conditions(expr, s, o, &mut conditions) {
                return Ok(None);
            }
        }
        let mut counted = vec![];
        for (_, a) in aggregates {
            let c = match a {
                AggregateExpression::CountSolutions { .. } => Counted::Solutions,
                AggregateExpression::FunctionCall {
                    name: AggregateFunction::Count,
                    expr: Expression::Variable(v),
                    distinct,
                } => {
                    if v == s {
                        if *distinct { Counted::DistinctSubjects } else { Counted::Solutions }
                    } else if v == o {
                        if *distinct { Counted::DistinctObjects } else { Counted::Solutions }
                    } else if Some(v) == p {
                        if *distinct { Counted::DistinctPredicates } else { Counted::Solutions }
                    } else {
                        // A variable the pattern does not bind counts 0, which is rare enough
                        // to leave to the general path.
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            };
            counted.push(c);
        }

        // The tables the pattern and the filter select, with their predicate.
        let mut tables: Vec<(&oxrdf::NamedNode, &Triples)> = vec![];
        if let Some(m) = self.graph_triples_map.get(graph) {
            let preds: Vec<_> = match &tp.predicate {
                NamedNodePattern::Variable(_) => m.iter().collect(),
                NamedNodePattern::NamedNode(nn) => m.get_key_value(nn).into_iter().collect(),
            };
            for (pred, ts) in preds {
                for t in ts.values() {
                    if t.height() > 0
                        && conditions.iter().all(|(subject, kind, positive)| {
                            let ty = if *subject { &t.subject_type } else { &t.object_type };
                            kind.matches(ty) == *positive
                        })
                    {
                        tables.push((pred, t));
                    }
                }
            }
        }

        if let Some(group_by) = group_by {
            return self.counts_per_predicate(group_by, aggregates, &counted, tables);
        }

        let mut columns = vec![];
        let mut types = HashMap::new();
        for ((v, _), c) in aggregates.iter().zip(counted) {
            let n = match c {
                Counted::Solutions => tables.iter().map(|(_, t)| t.height()).sum(),
                Counted::DistinctPredicates => {
                    tables.iter().map(|(p, _)| *p).collect::<HashSet<_>>().len()
                }
                Counted::DistinctSubjects => self.cached_distinct_values(&tables, true)?,
                Counted::DistinctObjects => self.cached_distinct_values(&tables, false)?,
            };
            columns.push(Column::new(v.as_str().into(), [n as i64]));
            types.insert(
                v.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                    .into_default_input_rdf_node_state(),
            );
        }
        Ok(Some(SolutionMappings::new(
            DataFrame::new(1, columns).unwrap().lazy(),
            types,
            1,
        )))
    }
}

impl Triplestore {
    /// The counts per predicate: one row for each predicate of the tables (which are in the
    /// order of their predicates), with the predicate as global IRI codes. The predicates are
    /// counted in parallel. A predicate with a single table whose only segment has an exact
    /// count reads no triples; the others are counted over their stored columns, and not kept
    /// in the [DistinctCountCache], which is too small for one count per predicate.
    fn counts_per_predicate(
        &self,
        group_by: &Variable,
        aggregates: &[(Variable, AggregateExpression)],
        counted: &[Counted],
        tables: Vec<(&oxrdf::NamedNode, &Triples)>,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let mut groups: Vec<Vec<(&oxrdf::NamedNode, &Triples)>> = vec![];
        for (pred, t) in tables {
            match groups.last_mut() {
                Some(g) if g[0].0 == pred => g.push((pred, t)),
                _ => groups.push(vec![(pred, t)]),
            }
        }
        let predicates: Vec<Option<&str>> = groups.iter().map(|g| Some(g[0].0.as_str())).collect();
        let codes = self.global_cats.read()?.maybe_encode_iri_slice(&predicates);
        // Every stored predicate has a code; without one, the general path answers.
        let Some(codes) = codes.into_iter().collect::<Option<Vec<u32>>>() else {
            return Ok(None);
        };
        fast_path::hit(FastPath::CountGroupedByPredicate);
        let distinct = |g: &[(&oxrdf::NamedNode, &Triples)], subject: bool| {
            if let [(_, t)] = g {
                if let Some(n) = t.exact_distinct(!subject) {
                    fast_path::hit(FastPath::CountDistinctSegmentExact);
                    return Ok(n);
                }
            }
            distinct_values(g, subject)
        };
        let rows = groups
            .par_iter()
            .map(|g| {
                counted
                    .iter()
                    .map(|c| {
                        Ok(match c {
                            Counted::Solutions => g.iter().map(|(_, t)| t.height()).sum(),
                            Counted::DistinctPredicates => 1,
                            Counted::DistinctSubjects => distinct(g, true)?,
                            Counted::DistinctObjects => distinct(g, false)?,
                        } as i64)
                    })
                    .collect::<Result<Vec<i64>, SparqlError>>()
            })
            .collect::<Result<Vec<_>, SparqlError>>()?;
        let mut columns = vec![Column::new(group_by.as_str().into(), codes)];
        let mut types = HashMap::from([(
            group_by.as_str().to_string(),
            RDFNodeState::from_bases(BaseRDFNodeType::IRI, BaseCatState::CategoricalNative(None)),
        )]);
        for (i, (v, _)) in aggregates.iter().enumerate() {
            let counts: Vec<i64> = rows.iter().map(|r| r[i]).collect();
            columns.push(Column::new(v.as_str().into(), counts));
            types.insert(
                v.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                    .into_default_input_rdf_node_state(),
            );
        }
        let height = groups.len();
        Ok(Some(SolutionMappings::new(
            DataFrame::new(height, columns).unwrap().lazy(),
            types,
            height,
        )))
    }

    /// [distinct_values], from the exact count of a single table's only segment, or from the
    /// cache when none of the tables changed since they were last counted.
    fn cached_distinct_values(
        &self,
        tables: &[(&oxrdf::NamedNode, &Triples)],
        subject: bool,
    ) -> Result<usize, SparqlError> {
        if let [(_, t)] = tables {
            if let Some(n) = t.exact_distinct(!subject) {
                fast_path::hit(FastPath::CountDistinctSegmentExact);
                return Ok(n);
            }
        }
        let mut generations: Vec<u64> = tables.iter().map(|(_, t)| t.generation()).collect();
        generations.sort_unstable();
        let key = (subject, generations);
        if let Some(n) = self.distinct_count_cache.get(&key) {
            fast_path::hit(FastPath::CountDistinctCached);
            return Ok(n);
        }
        let n = distinct_values(tables, subject)?;
        self.distinct_count_cache.insert(key, n);
        Ok(n)
    }
}

/// Reads a filter made of isIRI/isURI, isBLANK and isLITERAL of the subject or object variable,
/// their negations and conjunctions. False if the filter is anything else.
fn kind_conditions(
    expr: &Expression,
    s: &Variable,
    o: &Variable,
    out: &mut Vec<Condition>,
) -> bool {
    match expr {
        Expression::And(a, b) => {
            kind_conditions(a, s, o, out) && kind_conditions(b, s, o, out)
        }
        Expression::Not(inner) => {
            let mut inner_conditions = vec![];
            if !kind_conditions(inner, s, o, &mut inner_conditions) {
                return false;
            }
            // Only a single negated test: !(a && b) is not a conjunction.
            let [(subject, kind, positive)] = inner_conditions.as_slice() else {
                return false;
            };
            out.push((*subject, *kind, !positive));
            true
        }
        Expression::FunctionCall(f, args) => {
            let kind = match f {
                Function::IsIri => Kind::Iri,
                Function::IsBlank => Kind::Blank,
                Function::IsLiteral => Kind::Literal,
                _ => return false,
            };
            let [Expression::Variable(v)] = args.as_slice() else {
                return false;
            };
            if v == s {
                out.push((true, kind, true));
            } else if v == o {
                out.push((false, kind, true));
            } else {
                return false;
            }
            true
        }
        _ => false,
    }
}

/// The number of distinct subjects (or objects) of the tables: per stored type, the distinct
/// stored values. Terms of different types are different terms, and the stored type of a term
/// is fixed (a datatype with its time zone offset, see `BaseRDFNodeType`), so the counts per
/// type add up.
///
/// IRIs, blank nodes and strings are stored as dictionary codes, numbered per type, so their
/// distinct values are the bits set in a bitset over the codes. Other literals are counted with
/// `n_unique`.
fn distinct_values(
    tables: &[(&oxrdf::NamedNode, &Triples)],
    subject: bool,
) -> Result<usize, SparqlError> {
    let c = if subject { SUBJECT_COL_NAME } else { OBJECT_COL_NAME };
    let mut by_type: HashMap<&BaseRDFNodeType, Vec<Column>> = HashMap::new();
    for (_, t) in tables {
        let ty = if subject { &t.subject_type } else { &t.object_type };
        by_type.entry(ty).or_default().extend(t.stored_columns(c)?);
    }
    let counts = by_type
        .into_iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(ty, columns)| distinct_of_type(ty, columns))
        .collect::<Result<Vec<usize>, SparqlError>>()?;
    Ok(counts.into_iter().sum())
}

/// The distinct values of the stored columns of one type.
fn distinct_of_type(ty: &BaseRDFNodeType, columns: Vec<Column>) -> Result<usize, SparqlError> {
    if ty.stored_cat() {
        if ty.is_lang_string() {
            if let Some(n) = distinct_lang_strings(&columns) {
                return Ok(n);
            }
        } else if columns.iter().all(|c| c.dtype() == &DataType::UInt32) {
            let chunked: Vec<&UInt32Chunked> = columns.iter().map(|c| c.u32().unwrap()).collect();
            return Ok(distinct_codes_count(&chunked));
        }
    }
    let mut columns = columns.into_iter();
    let Some(first) = columns.next() else {
        return Ok(0);
    };
    let mut s = first.take_materialized_series();
    for c in columns {
        s.append(c.as_materialized_series())
            .map_err(TriplestoreError::LazyLoadError)?;
    }
    Ok(s.n_unique().map_err(TriplestoreError::LazyLoadError)?)
}

/// Inputs up to this many codes are counted on one thread.
const SERIAL_CODES: usize = 1 << 16;

/// The number of distinct codes in the columns: the bits set in a bitset over the codes. The
/// columns are cut into pieces of about equal length, each thread sets the bits of its pieces
/// in a bitset of its own, and the bitsets are ORed and counted word by word. Codes too sparse
/// for a bitset are sorted instead.
fn distinct_codes_count(columns: &[&UInt32Chunked]) -> usize {
    let rows: usize = columns.iter().map(|c| c.len() - c.null_count()).sum();
    let Some(max) = columns.iter().filter_map(|c| c.max()).max() else {
        return 0;
    };
    let words = (max as usize >> 6) + 1;
    // A bitset of at most 2 bytes per code (plus 512 KiB), else sort.
    if words > rows / 4 + (1 << 16) {
        let mut codes = Vec::with_capacity(rows);
        for c in columns {
            for arr in c.downcast_iter() {
                if arr.null_count() == 0 {
                    codes.extend_from_slice(arr.values().as_slice());
                } else {
                    codes.extend(arr.iter().flatten().copied());
                }
            }
        }
        codes.par_sort_unstable();
        codes.dedup();
        fast_path::hit(FastPath::CountDistinctSorted);
        return codes.len();
    }

    fast_path::hit_if(
        rows <= SERIAL_CODES,
        FastPath::CountDistinctBitset,
        FastPath::CountDistinctBitsetParallel,
    );
    let threads = if rows <= SERIAL_CODES {
        1
    } else {
        rayon::current_num_threads().clamp(1, rows / SERIAL_CODES)
    };
    // Pieces of at most `piece` codes; arrays with nulls (not expected in stored columns) are
    // kept whole.
    let piece = rows.div_ceil(threads * 4).max(1);
    let mut pieces: Vec<(&[u32], Option<&PrimitiveArray<u32>>)> = vec![];
    for c in columns {
        for arr in c.downcast_iter() {
            if arr.null_count() > 0 {
                pieces.push((&[], Some(arr)));
            } else {
                pieces.extend(arr.values().as_slice().chunks(piece).map(|s| (s, None)));
            }
        }
    }
    let set_bits = |bits: &mut [u64], pieces: &[(&[u32], Option<&PrimitiveArray<u32>>)]| {
        for (values, with_nulls) in pieces {
            let mut set = |u: u32| bits[(u >> 6) as usize] |= 1u64 << (u & 63);
            if let Some(arr) = with_nulls {
                arr.iter().flatten().for_each(|u| set(*u));
            } else {
                values.iter().for_each(|u| set(*u));
            }
        }
    };
    if threads == 1 {
        let mut bits = vec![0u64; words];
        set_bits(&mut bits, &pieces);
        return bits.iter().map(|w| w.count_ones() as usize).sum();
    }
    let per_thread = pieces.len().div_ceil(threads);
    let bitsets: Vec<Vec<u64>> = pieces
        .par_chunks(per_thread)
        .map(|ps| {
            let mut bits = vec![0u64; words];
            set_bits(&mut bits, ps);
            bits
        })
        .collect();
    (0..words)
        .into_par_iter()
        .with_min_len(4096)
        .map(|w| bitsets.iter().fold(0u64, |acc, b| acc | b[w]).count_ones() as usize)
        .sum()
}

/// The number of distinct language-tagged strings, stored as a struct of the value code and the
/// language code: the distinct pairs. None if the columns are not stored that way.
fn distinct_lang_strings(columns: &[Column]) -> Option<usize> {
    let mut pairs: Vec<u64> = vec![];
    for c in columns {
        let st = c.struct_().ok()?;
        let values = st.field_by_name(LANG_STRING_VALUE_FIELD).ok()?;
        let langs = st.field_by_name(LANG_STRING_LANG_FIELD).ok()?;
        let values = values.u32().ok()?;
        let langs = langs.u32().ok()?;
        pairs.reserve(values.len());
        for (v, l) in values.iter().zip(langs.iter()) {
            if let (Some(v), Some(l)) = (v, l) {
                pairs.push(((l as u64) << 32) | v as u64);
            }
        }
    }
    pairs.par_sort_unstable();
    pairs.dedup();
    Some(pairs.len())
}
