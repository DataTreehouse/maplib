//! Index nested-loop evaluation of small, selective basic graph patterns on dictionary codes,
//! outside Polars (stage 1 of `docs/design/inl_bgp.md`).
//!
//! A basic graph pattern without incoming solutions is answered by probing the sorted segments
//! of each pattern's table with the codes that the patterns before it bound, starting from a
//! pattern with a constant subject or object. It covers tables in memory with a dictionary in
//! memory, constant predicates, one table per pattern once the types of the constants and of the
//! bound variables are applied, and subjects and objects that are constants or variables (blank
//! nodes are variables). For anything else, and when the estimated or actual work is too large,
//! it declines and the Polars path answers, so it never changes results.
//!
//! The bindings are columns of `u64` slots, one row per partial solution, and each pattern
//! extends all rows at once: for every row it probes the pattern's table with the codes that row
//! binds. A slot is a dictionary code for variables of coded types, and a (segment, row)
//! reference into the table that bound the variable for types stored as native values, which are
//! gathered only at the end.

use super::{StoredTriples, Triples, TriplesSegment};
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use crate::storage::PredicateTriples;
use representation::dataset::{NamedGraph, QueryGraph};
use oxrdf::vocab::xsd;
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    Column, IdxCa, IdxSize, IntoColumn, NewChunkedArray, PlSmallStr, UInt32Chunked,
};
use representation::cats::maps::CatMaps;
use representation::cats::Cats;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::cmp::Ordering;
use std::collections::HashMap;
use utils::cancel::Cancelled;
use utils::fast_path::{hit, FastPath};

/// Set to `0` to turn index nested loops off (a kill switch); on otherwise.
pub const INL_ENV: &str = "MAPLIB_INL";
/// With `1`, says on stderr why index nested loops declined a basic graph pattern.
pub const INL_DEBUG_ENV: &str = "MAPLIB_INL_DEBUG";
/// The most estimated work (about one unit per code compared) before declining, see [max_work].
pub const INL_MAX_WORK_ENV: &str = "MAPLIB_INL_MAX_WORK";
/// The most rows the bindings may have, see [max_rows].
pub const INL_MAX_ROWS_ENV: &str = "MAPLIB_INL_MAX_ROWS";

const DEFAULT_MAX_WORK: usize = 4_000_000;
const DEFAULT_MAX_ROWS: usize = 100_000;
/// Work done while running may exceed the estimate by this factor before the loop gives up.
const RUN_WORK_FACTOR: usize = 4;
/// Cancellation is checked after this many rows of a step.
const CANCEL_CHECK_ROWS: usize = 4096;

pub fn inl_enabled() -> bool {
    !std::env::var(INL_ENV).is_ok_and(|v| v.trim() == "0")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// The most estimated work for a basic graph pattern to be evaluated with index nested loops:
/// about the number of codes compared, i.e. a few milliseconds.
fn max_work() -> usize {
    env_usize(INL_MAX_WORK_ENV, DEFAULT_MAX_WORK)
}

/// The most rows of the bindings, estimated before and counted while running.
fn max_rows() -> usize {
    env_usize(INL_MAX_ROWS_ENV, DEFAULT_MAX_ROWS)
}

/// Why index nested loops declined.
#[derive(Clone, Copy, Debug)]
enum Decline {
    /// The patterns: a variable predicate, a constant that is not a coded term, a variable twice
    /// in a pattern, no constant to start from, or patterns not connected to the others.
    Shape,
    /// Several tables (mixed types), language-tagged strings or blank node subjects.
    Types,
    /// Tables or dictionary not in memory, or columns laid out in ways it does not read.
    Storage,
    /// Too much estimated work or too many estimated rows.
    Estimate,
    /// Too much work or too many rows while running.
    Aborted,
}

impl Decline {
    fn path(self) -> FastPath {
        match self {
            Decline::Shape => FastPath::InlDeclinedShape,
            Decline::Types => FastPath::InlDeclinedTypes,
            Decline::Storage => FastPath::InlDeclinedStorage,
            Decline::Estimate => FastPath::InlDeclinedEstimate,
            Decline::Aborted => FastPath::InlAborted,
        }
    }
}

type R<T> = Result<T, Stop>;

/// Why the loop stopped: it declined, or the query was cancelled.
enum Stop {
    Declined,
    Cancelled(Cancelled),
}

impl From<Cancelled> for Stop {
    fn from(c: Cancelled) -> Self {
        Stop::Cancelled(c)
    }
}

/// Counts and, with [INL_DEBUG_ENV], reports a decline.
fn decline<T>(why: Decline, detail: &str) -> R<T> {
    hit(why.path());
    if std::env::var(INL_DEBUG_ENV).is_ok_and(|v| v == "1") {
        eprintln!("INL declined ({why:?}): {detail}");
    }
    Err(Stop::Declined)
}

trait OrDecline<T> {
    fn or_decline(self, why: Decline, detail: &str) -> R<T>;
}

impl<T> OrDecline<T> for Option<T> {
    fn or_decline(self, why: Decline, detail: &str) -> R<T> {
        match self {
            Some(t) => Ok(t),
            None => decline(why, detail),
        }
    }
}

/// The values of a column of codes, chunk by chunk; None with nulls or not codes.
fn u32_chunks(c: &Column) -> Option<Vec<&[u32]>> {
    let ca = c.as_materialized_series().u32().ok()?;
    if ca.null_count() > 0 {
        return None;
    }
    Some(ca.downcast_iter().map(|arr| arr.values().as_slice()).collect())
}

fn mem_df(st: &StoredTriples) -> Option<&DataFrame> {
    match st {
        StoredTriples::TriplesInMemory(m) => m.df.as_ref(),
        _ => None,
    }
}

/// A chunk of the subject sort of a segment (or of the write buffer): subject codes sorted by
/// their strings (unsorted in the buffer), the object codes aligned with them when the object
/// type is coded, and the offset of the chunk in the segment.
struct Part<'a> {
    subj: &'a [u32],
    obj_codes: Option<&'a [u32]>,
    offset: usize,
}

/// A segment of a table (or its write buffer) as slices.
struct SegView<'a> {
    /// The segment, for its Bloom filters; None for the buffer.
    seg: Option<&'a TriplesSegment>,
    parts: Vec<Part<'a>>,
    /// The object sort, chunk by chunk: object codes sorted by their strings, and the subjects
    /// in that order. None when there is none, or when its chunks can not be read.
    osort: Option<Vec<(&'a [u32], &'a [u32])>>,
    /// The object column of the subject sort, for gathering native values.
    obj: &'a Column,
    rows: usize,
}

/// A table of one predicate and one subject and object type, in memory.
struct TableView<'a> {
    segs: Vec<SegView<'a>>,
    rows: usize,
    /// Rows per distinct subject and per distinct object, where the segments know their
    /// distinct counts (summed over segments, so an upper bound on the distinct count).
    subject_fanout: f64,
    object_fanout: Option<f64>,
    /// Whether every segment has a usable object sort.
    object_sorted: bool,
}

/// The parts of one frame of subjects and objects, chunk by chunk.
fn frame_parts(df: &DataFrame, obj_coded: bool) -> R<Vec<Part<'_>>> {
    let subj = u32_chunks(df.column(SUBJECT_COL_NAME).unwrap())
        .or_decline(Decline::Storage, "subject column is not codes")?;
    let obj_codes = if obj_coded {
        let o = u32_chunks(df.column(OBJECT_COL_NAME).unwrap())
            .or_decline(Decline::Storage, "object column is not codes")?;
        if o.len() != subj.len() || o.iter().zip(&subj).any(|(o, s)| o.len() != s.len()) {
            return decline(Decline::Storage, "subject and object chunks differ");
        }
        Some(o)
    } else {
        None
    };
    let mut offset = 0;
    let mut parts = Vec::with_capacity(subj.len());
    for (i, s) in subj.into_iter().enumerate() {
        parts.push(Part {
            subj: s,
            obj_codes: obj_codes.as_ref().map(|o| o[i]),
            offset,
        });
        offset += s.len();
    }
    Ok(parts)
}

impl<'a> TableView<'a> {
    fn new(triples: &'a Triples) -> R<TableView<'a>> {
        if triples.subject_type != BaseRDFNodeType::IRI {
            return decline(Decline::Types, "subjects are not IRIs");
        }
        if triples.object_type.is_lang_string() {
            return decline(Decline::Types, "language-tagged string objects");
        }
        let obj_coded = triples.object_type.stored_cat();
        let mut segs = Vec::with_capacity(triples.segments.len() + 1);
        let mut distinct_s = Some(0usize);
        let mut distinct_o = Some(0usize);
        let mut object_sorted = obj_coded;
        for seg in &triples.segments {
            let df = seg
                .subject_sort
                .as_ref()
                .and_then(mem_df)
                .or_decline(Decline::Storage, "segment not in memory")?;
            let parts = frame_parts(df, obj_coded)?;
            let osort = match &seg.object_sort {
                Some(os) if obj_coded => mem_df(os).and_then(|odf| {
                    let o = u32_chunks(odf.column(OBJECT_COL_NAME).ok()?)?;
                    let s = u32_chunks(odf.column(SUBJECT_COL_NAME).ok()?)?;
                    if o.len() != s.len() || o.iter().zip(&s).any(|(o, s)| o.len() != s.len()) {
                        return None;
                    }
                    Some(o.into_iter().zip(s).collect::<Vec<_>>())
                }),
                _ => None,
            };
            object_sorted &= osort.is_some();
            distinct_s = distinct_s
                .zip(seg.distinct_count(false))
                .map(|(a, b)| a + b);
            distinct_o = distinct_o
                .zip(seg.distinct_count(true))
                .map(|(a, b)| a + b);
            segs.push(SegView {
                seg: Some(seg),
                parts,
                osort,
                obj: df.column(OBJECT_COL_NAME).unwrap(),
                rows: df.height(),
            });
        }
        if let Some(b) = triples.buffer.as_ref().filter(|b| b.height() > 0) {
            segs.push(SegView {
                seg: None,
                parts: frame_parts(b, obj_coded)?,
                osort: None,
                obj: b.column(OBJECT_COL_NAME).unwrap(),
                rows: b.height(),
            });
            // The buffer's rows are new subjects and objects at worst.
            distinct_s = distinct_s.map(|d| d + b.height());
            distinct_o = distinct_o.map(|d| d + b.height());
            // The buffer is scanned for bound objects.
            object_sorted = false;
        }
        let rows: usize = segs.iter().map(|s| s.rows).sum();
        let fanout = |d: Option<usize>| d.filter(|d| *d > 0).map(|d| rows as f64 / d as f64);
        Ok(TableView {
            segs,
            rows,
            // Without distinct counts, a subject is assumed to have few triples; the run-time
            // guard catches the exceptions.
            subject_fanout: fanout(distinct_s).unwrap_or(1.0),
            object_fanout: fanout(distinct_o),
            object_sorted,
        })
    }

    /// The cost of one binary search in every segment of the table.
    fn seek_cost(&self) -> usize {
        self.segs
            .iter()
            .map(|s| (usize::BITS - s.rows.leading_zeros()) as usize + 1)
            .sum()
    }

    /// The expected rows of one probe for a bound object.
    fn object_fanout(&self) -> f64 {
        // Without distinct objects (no object sort), assume the square root of the table's
        // rows; the run-time guard catches the exceptions.
        self.object_fanout
            .unwrap_or_else(|| (self.rows as f64).sqrt().max(1.0))
    }

    /// The cost of one probe for a bound object.
    fn object_probe_cost(&self) -> usize {
        if self.object_sorted {
            self.seek_cost()
        } else {
            self.rows
        }
    }
}

/// Orders codes by their strings, without decoding them.
struct Order<'c> {
    maps: &'c CatMaps,
}

impl Order<'_> {
    #[inline]
    fn cmp(&self, a: u32, b: u32) -> Ordering {
        self.maps.cmp_codes(a, b).unwrap_or(Ordering::Equal)
    }

    /// The rows of `sorted` holding `code`: a binary search by string, then the run of equal
    /// codes (equal strings have equal codes).
    #[inline]
    fn equal_range(&self, sorted: &[u32], code: u32) -> std::ops::Range<usize> {
        let lo = sorted.partition_point(|c| self.cmp(*c, code) == Ordering::Less);
        let mut hi = lo;
        while hi < sorted.len() && sorted[hi] == code {
            hi += 1;
        }
        lo..hi
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Pos {
    Const(u32),
    /// Index into the variables.
    Var(usize),
}

struct Pat<'a> {
    view: TableView<'a>,
    s: Pos,
    o: Pos,
    /// How objects are ordered, for coded objects.
    o_order: Option<Order<'a>>,
}

struct VarInfo {
    name: String,
    ty: BaseRDFNodeType,
    /// For a variable of a type stored as native values: the pattern that binds it.
    native_from: Option<usize>,
}

/// A (segment, row) reference to a native value.
fn pack(seg: usize, row: usize) -> u64 {
    ((seg as u64) << 32) | row as u64
}

/// The code of a constant (None if the dictionary does not have it) and its type.
fn encode_const(t: &TermPattern, cats: &Cats) -> R<(Option<u32>, BaseRDFNodeType)> {
    match t {
        TermPattern::NamedNode(nn) => Ok((
            cats.maybe_encode_iri_slice(&[Some(nn.as_str())])
                .pop()
                .flatten(),
            BaseRDFNodeType::IRI,
        )),
        TermPattern::Literal(l) if l.datatype() == xsd::STRING && l.language().is_none() => Ok((
            cats.maybe_encode_literals(&[Some(l.value())], xsd::STRING.into_owned())
                .pop()
                .flatten(),
            BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None),
        )),
        _ => decline(Decline::Shape, "constant that is not an IRI or a string"),
    }
}

/// The name of a variable or blank node, as the Polars path names its column.
fn var_name(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Evaluates the basic graph pattern with index nested loops over the tables of `graph`.
/// `Ok(None)` when it declines (the Polars path then answers), an error if the query was
/// cancelled. `query_max_rows` is the query's row limit, which the Polars path reports.
pub(crate) fn inl_bgp(
    patterns: &[TriplePattern],
    graph: &PredicateTriples,
    cats: &Cats,
    query_max_rows: Option<usize>,
) -> Result<Option<EagerSolutionMappings>, Cancelled> {
    match run(patterns, graph, cats, query_max_rows) {
        Ok(sm) => {
            hit(FastPath::InlTaken);
            if sm.mappings.height() == 0 {
                hit(FastPath::InlEmpty);
            }
            Ok(Some(sm))
        }
        Err(Stop::Declined) => Ok(None),
        Err(Stop::Cancelled(c)) => Err(c),
    }
}

impl crate::Triplestore {
    /// [inl_bgp] over the graph of the dataset, when the dataset is one stored graph.
    pub(crate) fn inl_bgp(
        &self,
        patterns: &[TriplePattern],
        dataset: &QueryGraph,
        query_settings: &QuerySettings,
    ) -> Result<Option<EagerSolutionMappings>, SparqlError> {
        let graph = match dataset {
            QueryGraph::NamedGraph(ng) => Some(ng.clone()),
            QueryGraph::QueryDataset(qd) if qd.default.len() == 1 => {
                Some(NamedGraph::from_maybe_named_node(Some(&qd.default[0])))
            }
            _ => None,
        };
        let Some(g) = graph.as_ref().and_then(|g| self.graph_triples_map.get(g)) else {
            let _ = decline::<()>(Decline::Shape, "the dataset is not one stored graph");
            return Ok(None);
        };
        let cats = self.global_cats.read()?;
        Ok(inl_bgp(patterns, g, &cats, query_settings.max_rows)?)
    }
}

fn run(
    patterns: &[TriplePattern],
    graph: &PredicateTriples,
    cats: &Cats,
    query_max_rows: Option<usize>,
) -> R<EagerSolutionMappings> {
    if patterns.is_empty() {
        return decline(Decline::Shape, "no patterns");
    }
    let iri_maps = &cats.get_cat_encs(&BaseRDFNodeType::IRI).maps;
    if iri_maps.as_in_memory().is_none() {
        return decline(Decline::Storage, "dictionary not in memory");
    }
    let s_order = Order { maps: iri_maps };
    let max_rows = query_max_rows.map_or(max_rows(), |m| m.min(max_rows()));
    let max_work = max_work();

    // Resolve each pattern to one table and its positions.
    let mut vars: Vec<VarInfo> = vec![];
    let mut pats: Vec<Pat> = Vec::with_capacity(patterns.len());
    // A constant without a code matches nothing: the result is empty, with the types of the
    // tables, as the Polars path gives it.
    let mut unknown_constant = false;
    for tp in patterns {
        let NamedNodePattern::NamedNode(p) = &tp.predicate else {
            return decline(Decline::Shape, "variable predicate");
        };
        let s_var = var_name(&tp.subject);
        let o_var = var_name(&tp.object);
        if s_var.is_some() && s_var == o_var {
            return decline(Decline::Shape, "variable twice in a pattern");
        }
        let s_const = match &s_var {
            None => Some(encode_const(&tp.subject, cats)?),
            Some(_) => None,
        };
        let o_const = match &o_var {
            None => Some(encode_const(&tp.object, cats)?),
            Some(_) => None,
        };
        let tables = graph
            .get(p)
            .or_decline(Decline::Shape, "predicate not in the graph")?;
        let known = |name: &Option<String>| {
            name.as_ref()
                .and_then(|n| vars.iter().find(|v| &v.name == n).map(|v| v.ty.clone()))
        };
        let (s_known, o_known) = (known(&s_var), known(&o_var));
        let candidates: Vec<&Triples> = tables
            .iter()
            .filter(|((st, ot), t)| {
                t.height() > 0
                    && s_const.as_ref().is_none_or(|(_, ty)| ty == st)
                    && o_const.as_ref().is_none_or(|(_, ty)| ty == ot)
                    && s_known.as_ref().is_none_or(|ty| ty == st)
                    && o_known.as_ref().is_none_or(|ty| ty == ot)
            })
            .map(|(_, t)| t)
            .collect();
        if candidates.len() != 1 {
            return decline(
                Decline::Types,
                &format!("{} tables for {p} after types", candidates.len()),
            );
        }
        let t = candidates[0];
        let view = TableView::new(t)?;
        let mut var_idx = |name: &str, ty: &BaseRDFNodeType| -> usize {
            if let Some(i) = vars.iter().position(|v| v.name == name) {
                i
            } else {
                vars.push(VarInfo {
                    name: name.to_string(),
                    ty: ty.clone(),
                    native_from: None,
                });
                vars.len() - 1
            }
        };
        let s = match (s_const, &s_var) {
            (Some((Some(c), _)), _) => Pos::Const(c),
            (Some((None, _)), _) => {
                unknown_constant = true;
                Pos::Const(u32::MAX)
            }
            (None, Some(v)) => Pos::Var(var_idx(v, &t.subject_type)),
            (None, None) => unreachable!(),
        };
        let o = match (o_const, &o_var) {
            (Some((Some(c), _)), _) => Pos::Const(c),
            (Some((None, _)), _) => {
                unknown_constant = true;
                Pos::Const(u32::MAX)
            }
            (None, Some(v)) => {
                let i = var_idx(v, &t.object_type);
                if !t.object_type.stored_cat() {
                    if vars[i].native_from.is_some() {
                        return decline(Decline::Types, "join on a native value");
                    }
                    vars[i].native_from = Some(pats.len());
                }
                Pos::Var(i)
            }
            (None, None) => unreachable!(),
        };
        let o_order = if t.object_type.stored_cat() {
            let maps = &cats.get_cat_encs(&t.object_type).maps;
            if maps.as_in_memory().is_none() {
                return decline(Decline::Storage, "dictionary not in memory");
            }
            Some(Order { maps })
        } else {
            None
        };
        pats.push(Pat { view, s, o, o_order });
    }
    if unknown_constant {
        return Ok(empty(&vars, &pats));
    }

    let order = plan(&pats, &vars, &s_order, max_work, max_rows)?;

    // The nested loops, one pattern at a time over all rows.
    let mut cols: Vec<Option<Vec<u64>>> = vec![None; vars.len()];
    let mut n_rows = 1usize;
    let mut work = 0usize;
    let max_run_work = max_work.saturating_mul(RUN_WORK_FACTOR);
    for &pi in &order {
        let p = &pats[pi];
        let mut parent: Vec<u32> = Vec::new();
        let mut new_s: Vec<u64> = Vec::new();
        let mut new_o: Vec<u64> = Vec::new();
        let s_new = matches!(p.s, Pos::Var(v) if cols[v].is_none());
        let o_new = matches!(p.o, Pos::Var(v) if cols[v].is_none());
        let get = |pos: Pos, row: usize| -> Option<u32> {
            match pos {
                Pos::Const(c) => Some(c),
                Pos::Var(v) => cols[v].as_ref().map(|c| c[row] as u32),
            }
        };
        for row in 0..n_rows {
            if row % CANCEL_CHECK_ROWS == CANCEL_CHECK_ROWS - 1 {
                utils::cancel::check()?;
            }
            let s = get(p.s, row);
            let o = if o_new { None } else { get(p.o, row) };
            match (s, o) {
                (Some(s), o) => {
                    for (si, seg) in p.view.segs.iter().enumerate() {
                        if seg.seg.is_some_and(|sg| !sg.may_hold_code(s, false)) {
                            continue;
                        }
                        for part in &seg.parts {
                            let range = if seg.seg.is_some() {
                                work += (usize::BITS - part.subj.len().leading_zeros()) as usize;
                                s_order.equal_range(part.subj, s)
                            } else {
                                work += part.subj.len();
                                0..part.subj.len()
                            };
                            work += range.len();
                            for r in range {
                                if part.subj[r] != s {
                                    continue;
                                }
                                match o {
                                    Some(o) => {
                                        if part.obj_codes.is_some_and(|oc| oc[r] == o) {
                                            parent.push(row as u32);
                                        }
                                    }
                                    None => {
                                        parent.push(row as u32);
                                        new_o.push(match part.obj_codes {
                                            Some(oc) => oc[r] as u64,
                                            None => pack(si, part.offset + r),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                (None, Some(o)) => {
                    // Only coded objects are bound here (native variables are bound once).
                    let ord = p.o_order.as_ref().unwrap();
                    for seg in &p.view.segs {
                        if seg.seg.is_some_and(|sg| !sg.may_hold_code(o, true)) {
                            continue;
                        }
                        match &seg.osort {
                            Some(chunks) => {
                                for (os, ss) in chunks {
                                    work += (usize::BITS - os.len().leading_zeros()) as usize;
                                    let range = ord.equal_range(os, o);
                                    work += range.len();
                                    for r in range {
                                        parent.push(row as u32);
                                        new_s.push(ss[r] as u64);
                                    }
                                }
                            }
                            None => {
                                for part in &seg.parts {
                                    let oc = part.obj_codes.unwrap();
                                    work += oc.len();
                                    for (r, c) in oc.iter().enumerate() {
                                        if *c == o {
                                            parent.push(row as u32);
                                            new_s.push(part.subj[r] as u64);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                (None, None) => unreachable!("the plan binds a position of every pattern"),
            }
            if parent.len() > max_rows {
                return decline(Decline::Aborted, "too many rows");
            }
            if work > max_run_work {
                return decline(Decline::Aborted, "too much work");
            }
        }
        utils::cancel::check()?;
        // Gather the old columns by parent row and add the new ones.
        for c in cols.iter_mut().flatten() {
            *c = parent.iter().map(|r| c[*r as usize]).collect();
        }
        if let (true, Pos::Var(v)) = (s_new, p.s) {
            cols[v] = Some(new_s);
        }
        if let (true, Pos::Var(v)) = (o_new, p.o) {
            cols[v] = Some(new_o);
        }
        n_rows = parent.len();
        if n_rows == 0 {
            return Ok(empty(&vars, &pats));
        }
    }

    // Build the frame: codes as they are, native values gathered from their segments.
    let mut out_cols = Vec::with_capacity(vars.len());
    for (vi, v) in vars.iter().enumerate() {
        let slots = cols[vi].as_ref().unwrap();
        let name = PlSmallStr::from_str(&v.name);
        let col = if let Some(pi) = v.native_from {
            gather_native(&pats[pi].view, slots, name)
                .or_decline(Decline::Storage, "native values could not be gathered")?
        } else {
            UInt32Chunked::from_vec(name, slots.iter().map(|c| *c as u32).collect()).into_column()
        };
        out_cols.push(col);
    }
    let df = DataFrame::new(n_rows, out_cols).unwrap();
    Ok(EagerSolutionMappings::new(df, types(&vars)))
}

fn types(vars: &[VarInfo]) -> HashMap<String, RDFNodeState> {
    vars.iter()
        .map(|v| {
            (
                v.name.clone(),
                RDFNodeState::from_bases(v.ty.clone(), v.ty.default_stored_cat_state()),
            )
        })
        .collect()
}

/// No solutions, with the columns typed as the tables type them.
fn empty(vars: &[VarInfo], pats: &[Pat]) -> EagerSolutionMappings {
    let cols = vars
        .iter()
        .map(|v| {
            let name = PlSmallStr::from_str(&v.name);
            match v.native_from {
                Some(pi) => {
                    let mut c = pats[pi].view.segs[0].obj.slice(0, 0);
                    c.rename(name);
                    c
                }
                None => UInt32Chunked::from_vec(name, vec![]).into_column(),
            }
        })
        .collect();
    EagerSolutionMappings::new(DataFrame::new(0, cols).unwrap(), types(vars))
}

/// The order of the patterns, or a decline when the estimated work or rows are too large.
///
/// The first pattern is the one with a constant and the fewest matches, counted exactly by a
/// binary search (or a scan of a table that is small enough). Then always a pattern connected
/// to the bound variables: both ends bound (a check), a bound subject, a bound object with an
/// object sort, a bound object without; ties by the smaller fanout. The rows and the work are
/// estimated along the way from the tables' fanouts.
fn plan(
    pats: &[Pat],
    vars: &[VarInfo],
    s_order: &Order,
    max_work: usize,
    max_rows: usize,
) -> R<Vec<usize>> {
    let mut seed: Option<(usize, usize)> = None;
    let mut work = 0usize;
    for (i, p) in pats.iter().enumerate() {
        let count = match (p.s, p.o) {
            (Pos::Const(s), _) => {
                work += p.view.seek_cost();
                count_subject(&p.view, s_order, s)
            }
            (_, Pos::Const(o)) => {
                if work + p.view.object_probe_cost() > max_work {
                    continue;
                }
                work += p.view.object_probe_cost();
                count_object(&p.view, p.o_order.as_ref().unwrap(), o)
            }
            _ => continue,
        };
        if seed.is_none_or(|(_, c)| count < c) {
            seed = Some((i, count));
        }
    }
    let Some((start, count)) = seed else {
        return decline(
            Decline::Shape,
            "no constant subject or object to start from",
        );
    };
    let mut done = vec![false; pats.len()];
    let mut bound = vec![false; vars.len()];
    let mut order = Vec::with_capacity(pats.len());
    let mut rows = count as f64;
    let mark = |i: usize, done: &mut Vec<bool>, bound: &mut Vec<bool>, order: &mut Vec<usize>| {
        done[i] = true;
        order.push(i);
        for pos in [pats[i].s, pats[i].o] {
            if let Pos::Var(v) = pos {
                bound[v] = true;
            }
        }
    };
    mark(start, &mut done, &mut bound, &mut order);
    while done.iter().any(|d| !d) {
        let is_bound = |pos: Pos| match pos {
            Pos::Const(_) => true,
            Pos::Var(v) => bound[v],
        };
        // (pattern, class, fanout, cost per probe) of the connected patterns.
        let next = (0..pats.len())
            .filter(|i| !done[*i])
            .filter_map(|i| {
                let p = &pats[i];
                let v = &p.view;
                match (is_bound(p.s), is_bound(p.o)) {
                    (true, true) => Some((i, 0, 1.0, v.seek_cost())),
                    (true, false) => Some((i, 1, v.subject_fanout, v.seek_cost())),
                    (false, true) if v.object_sorted => {
                        Some((i, 2, v.object_fanout(), v.seek_cost()))
                    }
                    (false, true) => Some((i, 3, v.object_fanout(), v.rows)),
                    (false, false) => None,
                }
            })
            .min_by(|a, b| {
                (a.1, a.2)
                    .partial_cmp(&(b.1, b.2))
                    .unwrap_or(Ordering::Equal)
            });
        let Some((i, _, fanout, cost)) = next else {
            return decline(Decline::Shape, "patterns not connected");
        };
        work = work.saturating_add((rows.ceil() as usize).saturating_mul(cost));
        rows *= fanout;
        if work > max_work || rows > max_rows as f64 {
            return decline(
                Decline::Estimate,
                &format!("estimated work {work}, rows {rows:.0}"),
            );
        }
        mark(i, &mut done, &mut bound, &mut order);
    }
    Ok(order)
}

fn count_subject(view: &TableView, ord: &Order, s: u32) -> usize {
    let mut n = 0;
    for seg in &view.segs {
        for p in &seg.parts {
            n += if seg.seg.is_some() {
                ord.equal_range(p.subj, s).len()
            } else {
                p.subj.iter().filter(|c| **c == s).count()
            };
        }
    }
    n
}

fn count_object(view: &TableView, ord: &Order, o: u32) -> usize {
    let mut n = 0;
    for seg in &view.segs {
        match &seg.osort {
            Some(chunks) => {
                for (os, _) in chunks {
                    n += ord.equal_range(os, o).len();
                }
            }
            None => {
                for p in &seg.parts {
                    n += p.obj_codes.unwrap().iter().filter(|c| **c == o).count();
                }
            }
        }
    }
    n
}

/// The native object values of the (segment, row) slots, in slot order.
fn gather_native(view: &TableView, slots: &[u64], name: PlSmallStr) -> Option<Column> {
    let mut by_seg: Vec<Vec<IdxSize>> = vec![vec![]; view.segs.len()];
    let mut pos_in: Vec<(u32, u32)> = Vec::with_capacity(slots.len());
    for s in slots {
        let (si, r) = ((s >> 32) as usize, (s & 0xffff_ffff) as IdxSize);
        pos_in.push((si as u32, by_seg[si].len() as u32));
        by_seg[si].push(r);
    }
    let mut all: Option<Column> = None;
    let mut base = vec![0u32; view.segs.len()];
    let mut acc = 0u32;
    for (si, rows) in by_seg.into_iter().enumerate() {
        base[si] = acc;
        acc += rows.len() as u32;
        if rows.is_empty() {
            continue;
        }
        let idx = IdxCa::from_vec(PlSmallStr::EMPTY, rows);
        let part = view.segs[si].obj.take(&idx).ok()?;
        all = Some(match all {
            None => part,
            Some(mut a) => {
                a.append(&part).ok()?;
                a
            }
        });
    }
    let order: Vec<IdxSize> = pos_in
        .iter()
        .map(|(si, k)| (base[*si as usize] + *k) as IdxSize)
        .collect();
    let idx = IdxCa::from_vec(PlSmallStr::EMPTY, order);
    let mut out = all?.take(&idx).ok()?;
    out.rename(name);
    Some(out)
}
