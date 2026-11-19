//! PROTOTYPE: index nested-loop evaluation of small, selective basic graph patterns on
//! dictionary codes, outside Polars. See `docs/design/inl_bgp.md`.
//!
//! Scope of the prototype: in-memory storage, constant predicates, one table per predicate
//! after the types of the constants and bound variables are applied, subjects and objects that
//! are constants or variables (no blank nodes), every pattern after the first sharing a variable
//! with the patterns before it. Anything else returns `None` and the Polars path answers.
//!
//! The bindings are kept as columns of `u64` slots, one row per partial solution, and each
//! pattern extends all rows at once (a breadth-first, batch-at-a-time nested loop): for every
//! row it probes the pattern's table with the codes that row binds. A slot is a dictionary code
//! for variables of coded types, and a (segment, row) reference into the table that bound the
//! variable for types stored as native values, which are gathered only at the end.

use super::{StoredTriples, Triples, TriplesSegment};
use crate::storage::PredicateTriples;
use oxrdf::vocab::xsd;
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, IdxCa, IdxSize, IntoColumn, Series, UInt32Chunked};
use polars_core::prelude::{NewChunkedArray, PlSmallStr};
use representation::cats::maps::CatMaps;
use representation::cats::Cats;
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::cmp::Ordering;
use std::collections::HashMap;

/// The environment variable that turns the prototype on.
pub const INL_ENV: &str = "MAPLIB_INL";

pub fn inl_enabled() -> bool {
    std::env::var(INL_ENV).is_ok_and(|v| v == "1")
}

/// PROTOTYPE: with `MAPLIB_INL_DEBUG=1`, says on stderr why INL declined.
pub(crate) fn why<T>(x: Option<T>, reason: &str) -> Option<T> {
    if x.is_none() && std::env::var("MAPLIB_INL_DEBUG").is_ok_and(|v| v == "1") {
        eprintln!("INL declined: {reason}");
    }
    x
}

/// The most rows the bindings may grow to before the prototype gives up.
const MAX_ROWS: usize = 100_000;

/// One sorted segment (or the unsorted write buffer) of a table, as slices.
struct Seg<'a> {
    /// Subject codes, sorted by their strings (unsorted for the buffer).
    subj: &'a [u32],
    /// Object column, aligned with `subj`.
    obj: &'a Column,
    /// Object codes when the object type is dictionary coded.
    obj_codes: Option<&'a [u32]>,
    /// The object sort: object codes sorted by their strings, and the subjects in that order.
    osort: Option<(&'a [u32], &'a [u32])>,
    /// The segment, for its Bloom filters; None for the buffer.
    seg: Option<&'a TriplesSegment>,
}

/// A table of one predicate and one subject and object type, in memory.
struct TableView<'a> {
    triples: &'a Triples,
    segs: Vec<Seg<'a>>,
}

fn u32_slice(c: &Column) -> Option<&[u32]> {
    c.u32().ok()?.cont_slice().ok()
}

fn mem_df(st: &StoredTriples) -> Option<&DataFrame> {
    match st {
        StoredTriples::TriplesInMemory(m) => m.df.as_ref(),
        _ => None,
    }
}

impl<'a> TableView<'a> {
    fn new(triples: &'a Triples) -> Option<TableView<'a>> {
        if !triples.subject_type.stored_cat() || triples.object_type.is_lang_string() {
            return why(None, "subject not coded or lang string objects");
        }
        let obj_coded = triples.object_type.stored_cat();
        let mut segs = Vec::with_capacity(triples.segments.len() + 1);
        for seg in &triples.segments {
            let df = why(mem_df(seg.subject_sort.as_ref()?), "segment not in memory")?;
            let subj = why(
                u32_slice(df.column(SUBJECT_COL_NAME).ok()?),
                "subject column not one chunk",
            )?;
            let obj = df.column(OBJECT_COL_NAME).ok()?;
            let obj_codes = if obj_coded {
                Some(why(u32_slice(obj), "object column not one chunk")?)
            } else {
                None
            };
            // An object sort that is not contiguous is not used: bound objects are scanned.
            let osort = match &seg.object_sort {
                Some(os) if obj_coded => mem_df(os).and_then(|odf| {
                    Some((
                        u32_slice(odf.column(OBJECT_COL_NAME).ok()?)?,
                        u32_slice(odf.column(SUBJECT_COL_NAME).ok()?)?,
                    ))
                }),
                _ => None,
            };
            if seg.object_sort.is_some() && obj_coded && osort.is_none() {
                why::<()>(None, "object sort not one chunk (scanning instead)");
            }
            segs.push(Seg {
                subj,
                obj,
                obj_codes,
                osort,
                seg: Some(seg),
            });
        }
        if let Some(b) = triples.buffer.as_ref().filter(|b| b.height() > 0) {
            let subj = u32_slice(b.column(SUBJECT_COL_NAME).ok()?)?;
            let obj = b.column(OBJECT_COL_NAME).ok()?;
            let obj_codes = if obj_coded { Some(u32_slice(obj)?) } else { None };
            segs.push(Seg {
                subj,
                obj,
                obj_codes,
                osort: None,
                seg: None,
            });
        }
        Some(TableView { triples, segs })
    }
}

/// Orders codes by their strings.
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
}

#[derive(Clone)]
struct VarInfo {
    name: String,
    ty: BaseRDFNodeType,
    /// For a variable of a type stored as native values: the pattern that binds it.
    native_from: Option<usize>,
}

fn pack(seg: usize, row: usize) -> u64 {
    ((seg as u64) << 32) | row as u64
}

fn encode_const(t: &TermPattern, cats: &Cats) -> Result<Option<(u32, BaseRDFNodeType)>, ()> {
    match t {
        TermPattern::NamedNode(nn) => {
            let c = cats.maybe_encode_iri_slice(&[Some(nn.as_str())]).pop().flatten();
            Ok(c.map(|c| (c, BaseRDFNodeType::IRI)))
        }
        TermPattern::Literal(l) if l.datatype() == xsd::STRING && l.language().is_none() => {
            let c = cats
                .maybe_encode_literals(&[Some(l.value())], xsd::STRING.into_owned())
                .pop()
                .flatten();
            Ok(c.map(|c| (c, BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None))))
        }
        _ => Err(()),
    }
}

/// Evaluates the basic graph pattern with index nested loops over the tables of `graph`, or
/// returns None if the prototype does not cover it. `Some(None)` would mean no solutions; the
/// prototype returns None for that as well, so the general path builds the typed empty result.
pub(crate) fn inl_bgp(
    patterns: &[TriplePattern],
    graph: &PredicateTriples,
    cats: &Cats,
) -> Option<EagerSolutionMappings> {
    if patterns.is_empty() {
        return None;
    }
    let mut vars: Vec<VarInfo> = vec![];
    let var_idx = |vars: &mut Vec<VarInfo>, name: &str, ty: &BaseRDFNodeType| -> Option<usize> {
        if let Some(i) = vars.iter().position(|v| v.name == name) {
            if &vars[i].ty != ty {
                return None;
            }
            Some(i)
        } else {
            vars.push(VarInfo {
                name: name.to_string(),
                ty: ty.clone(),
                native_from: None,
            });
            Some(vars.len() - 1)
        }
    };

    // Resolve each pattern to one table and its positions.
    let mut pats: Vec<Pat> = Vec::with_capacity(patterns.len());
    for tp in patterns {
        let NamedNodePattern::NamedNode(p) = &tp.predicate else {
            return None;
        };
        let tables = why(graph.get(p), "predicate not in graph")?;
        let s_const = match &tp.subject {
            TermPattern::Variable(_) => None,
            t => Some(why(encode_const(t, cats).ok().flatten(), "subject constant")?),
        };
        let o_const = match &tp.object {
            TermPattern::Variable(_) => None,
            t => Some(why(encode_const(t, cats).ok().flatten(), "object constant")?),
        };
        let s_var = if let TermPattern::Variable(v) = &tp.subject {
            Some(v.as_str())
        } else {
            None
        };
        let o_var = if let TermPattern::Variable(v) = &tp.object {
            Some(v.as_str())
        } else {
            None
        };
        if s_var.is_some() && s_var == o_var {
            return None;
        }
        let known = |name: Option<&str>| {
            name.and_then(|n| vars.iter().find(|v| v.name == n).map(|v| v.ty.clone()))
        };
        let (s_known, o_known) = (known(s_var), known(o_var));
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
            return why(None, &format!("{} tables for {}", candidates.len(), p));
        }
        let t = candidates[0];
        let view = why(TableView::new(t), "table view")?;
        let s = match (s_const, s_var) {
            (Some((c, _)), _) => Pos::Const(c),
            (None, Some(v)) => Pos::Var(var_idx(&mut vars, v, &t.subject_type)?),
            _ => return None,
        };
        let o = match (o_const, o_var) {
            (Some((c, _)), _) => Pos::Const(c),
            (None, Some(v)) => {
                let i = var_idx(&mut vars, v, &t.object_type)?;
                if !t.object_type.stored_cat() {
                    if vars[i].native_from.is_some() {
                        // A join on a native value: not in the prototype.
                        return None;
                    }
                    vars[i].native_from = Some(pats.len());
                }
                Pos::Var(i)
            }
            _ => return None,
        };
        pats.push(Pat { view, s, o });
    }

    let order_maps = |ty: &BaseRDFNodeType| {
        let maps = &cats.get_cat_encs(ty).maps;
        maps.as_in_memory().map(|_| maps)
    };
    let s_order = Order {
        maps: why(order_maps(&BaseRDFNodeType::IRI), "IRI dictionary not in memory")?,
    };
    let mut o_orders: Vec<Option<Order>> = Vec::with_capacity(pats.len());
    for p in &pats {
        let ot = &p.view.triples.object_type;
        if p.view.triples.subject_type != BaseRDFNodeType::IRI {
            return why(None, "blank node subjects");
        }
        o_orders.push(if ot.stored_cat() {
            Some(Order {
                maps: why(order_maps(ot), "object dictionary not in memory")?,
            })
        } else {
            None
        });
    }

    // Order: start from the pattern with a constant and the fewest matches, then always take a
    // connected pattern, bound subjects first (their fanout is small), both ends bound first of all.
    let mut done = vec![false; pats.len()];
    let mut bound = vec![false; vars.len()];
    let mut order = Vec::with_capacity(pats.len());
    let start = (0..pats.len())
        .filter(|i| matches!(pats[*i].s, Pos::Const(_)) || matches!(pats[*i].o, Pos::Const(_)))
        .min_by_key(|i| {
            let p = &pats[*i];
            match (p.s, p.o) {
                (Pos::Const(s), _) => count_subject(&p.view, &s_order, s),
                (_, Pos::Const(o)) => count_object(&p.view, o_orders[*i].as_ref(), o),
                _ => usize::MAX,
            }
        });
    let start = why(start, "no seed")?;
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
    while order.len() < pats.len() {
        let is_bound = |pos: Pos| match pos {
            Pos::Const(_) => true,
            Pos::Var(v) => bound[v],
        };
        let next = (0..pats.len())
            .filter(|i| !done[*i] && (is_bound(pats[*i].s) || is_bound(pats[*i].o)))
            .min_by_key(|i| match (is_bound(pats[*i].s), is_bound(pats[*i].o)) {
                (true, true) => 0,
                (true, false) => 1,
                _ => 2,
            });
        let next = why(next, "not connected")?;
        mark(next, &mut done, &mut bound, &mut order);
    }

    // The nested loops, one pattern at a time over all rows.
    let mut cols: Vec<Option<Vec<u64>>> = vec![None; vars.len()];
    let mut n_rows = 1usize;
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
        let o_coded = p.view.triples.object_type.stored_cat();
        for row in 0..n_rows {
            let s = get(p.s, row);
            let o = if o_new { None } else { get(p.o, row) };
            match (s, o) {
                (Some(s), o) => {
                    for (si, seg) in p.view.segs.iter().enumerate() {
                        if seg.seg.is_some_and(|sg| !sg.may_hold_code(s, false)) {
                            continue;
                        }
                        let range = if seg.seg.is_some() {
                            s_order.equal_range(seg.subj, s)
                        } else {
                            0..seg.subj.len()
                        };
                        for r in range {
                            if seg.subj[r] != s {
                                continue;
                            }
                            if let Some(o) = o {
                                if seg.obj_codes.is_some_and(|oc| oc[r] == o) {
                                    parent.push(row as u32);
                                }
                            } else {
                                parent.push(row as u32);
                                new_o.push(if o_coded {
                                    seg.obj_codes.unwrap()[r] as u64
                                } else {
                                    pack(si, r)
                                });
                            }
                        }
                    }
                }
                (None, Some(o)) => {
                    let ord = o_orders[pi].as_ref()?;
                    for seg in &p.view.segs {
                        if seg.seg.is_some_and(|sg| !sg.may_hold_code(o, true)) {
                            continue;
                        }
                        if let Some((os, ss)) = seg.osort {
                            for r in ord.equal_range(os, o) {
                                parent.push(row as u32);
                                new_s.push(ss[r] as u64);
                            }
                        } else {
                            let oc = seg.obj_codes?;
                            for (r, c) in oc.iter().enumerate() {
                                if *c == o {
                                    parent.push(row as u32);
                                    new_s.push(seg.subj[r] as u64);
                                }
                            }
                        }
                    }
                }
                (None, None) => return None,
            }
            if parent.len() > MAX_ROWS {
                return why(None, "row guard");
            }
        }
        // Gather the old columns by parent row and add the new ones.
        for c in cols.iter_mut().flatten() {
            *c = parent.iter().map(|r| c[*r as usize]).collect();
        }
        if s_new {
            if let Pos::Var(v) = p.s {
                cols[v] = Some(new_s);
            }
        }
        if o_new {
            if let Pos::Var(v) = p.o {
                cols[v] = Some(new_o);
            }
        }
        n_rows = parent.len();
        if n_rows == 0 {
            // The prototype leaves typed empty results to the general path.
            return why(None, "empty result");
        }
    }

    // Build the frame: codes as they are, native values gathered from their segments.
    let mut out_cols = Vec::with_capacity(vars.len());
    let mut types = HashMap::new();
    for (vi, v) in vars.iter().enumerate() {
        let slots = cols[vi].as_ref()?;
        let name = PlSmallStr::from_str(&v.name);
        let col = if let Some(pi) = v.native_from {
            gather_native(&pats[pi].view, slots, name)?
        } else {
            UInt32Chunked::from_vec(name, slots.iter().map(|c| *c as u32).collect()).into_column()
        };
        out_cols.push(col);
        types.insert(
            v.name.clone(),
            RDFNodeState::from_bases(v.ty.clone(), v.ty.default_stored_cat_state()),
        );
    }
    let df = DataFrame::new(n_rows, out_cols).ok()?;
    Some(EagerSolutionMappings::new(df, types))
}

fn count_subject(view: &TableView, ord: &Order, s: u32) -> usize {
    view.segs
        .iter()
        .map(|seg| {
            if seg.seg.is_some() {
                ord.equal_range(seg.subj, s).len()
            } else {
                seg.subj.iter().filter(|c| **c == s).count()
            }
        })
        .sum()
}

fn count_object(view: &TableView, ord: Option<&Order>, o: u32) -> usize {
    view.segs
        .iter()
        .map(|seg| match (seg.osort, ord) {
            (Some((os, _)), Some(ord)) => ord.equal_range(os, o).len(),
            _ => seg
                .obj_codes
                .map(|oc| oc.iter().filter(|c| **c == o).count())
                .unwrap_or(usize::MAX / 4),
        })
        .sum()
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
    let mut parts: Vec<Series> = vec![];
    let mut base = vec![0u32; view.segs.len()];
    let mut acc = 0u32;
    for (si, rows) in by_seg.iter().enumerate() {
        base[si] = acc;
        acc += rows.len() as u32;
        if rows.is_empty() {
            continue;
        }
        let idx = IdxCa::from_vec(PlSmallStr::EMPTY, rows.clone());
        parts.push(view.segs[si].obj.as_materialized_series().take(&idx).ok()?);
    }
    let mut all = parts.pop()?;
    while let Some(p) = parts.pop() {
        let mut p = p;
        p.append(&all).ok()?;
        all = p;
    }
    let order: Vec<IdxSize> = pos_in
        .iter()
        .map(|(si, k)| (base[*si as usize] + *k) as IdxSize)
        .collect();
    let idx = IdxCa::from_vec(PlSmallStr::EMPTY, order);
    let mut out = all.take(&idx).ok()?;
    out.rename(name);
    Some(out.into_column())
}
