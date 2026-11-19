//! Estimates of the number of solutions of triple patterns and of joins of them, from the
//! statistics of the stored tables ([crate::statistics]), and the cost-based order of the triple
//! patterns of a basic graph pattern that they give.
//!
//! A triple pattern with a constant predicate matches the triples of that predicate's tables.
//! Joins are estimated from the sketches of the joined columns ([ColumnSketch::join_size]) where
//! both have them, and otherwise as follows.
//! A bound subject keeps the share `1 / distinct subjects` of them per subject; a bound object
//! keeps its count among the most common objects, or the average count of the other objects. A
//! join on shared variables gives `left × right / max(distinct values of the variable on either
//! side)` for its most selective shared variable (the usual independence assumption, applied to
//! one key only, since keys of one pattern are mostly correlated), and every variable keeps at
//! most as many distinct values as there are solutions.

use super::Triplestore;
use crate::statistics::sketch::ColumnSketch;
use crate::statistics::TableStats;
use oxrdf::{NamedNode, Term};
use query_processing::pushdowns::{PushdownValues, Pushdowns};
use representation::dataset::{NamedGraph, QueryGraph};
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use utils::fast_path::{self, FastPath};

/// More constant values than this for a subject or object are estimated from their number
/// alone, not one by one.
const MAX_VALUES_LOOKED_UP: usize = 64;

/// Whether basic graph patterns are planned from the table statistics. On unless
/// `MAPLIB_TABLE_STATISTICS=off`, which keeps the order of the patterns by connection alone, as
/// before statistics, e.g. to compare plans.
pub(crate) fn statistics_enabled() -> bool {
    // Read for every basic graph pattern, so that it can be changed between queries.
    !matches!(
        std::env::var("MAPLIB_TABLE_STATISTICS").as_deref(),
        Ok("off") | Ok("0") | Ok("false")
    )
}

/// More constant values than this for a subject or object are counted without checking which
/// fit a table's column.
const MAX_VALUES_CHECKED: usize = 1024;

/// A pattern is taken before the next one in the order of the query only when its join is
/// estimated to be at least this many times smaller.
const CLEARLY_CHEAPER: f64 = 2.0;

/// A join on a variable is estimated from the sketches of its columns when there are at most
/// this many pairs of them (a pattern with an unbound predicate has a column per table).
const MAX_SKETCH_PAIRS: usize = 64;

/// The sketches of the columns a variable is bound from, together with their tables' rows.
type Sketches = Vec<Arc<ColumnSketch>>;

/// The estimated rows of a join of `left` and `right` solutions on a variable bound from the
/// columns of `a` and `b`: their join size relative to the product of their tables' rows.
fn sketch_join_rows(left: f64, right: f64, a: &Sketches, b: &Sketches) -> Option<f64> {
    if a.is_empty() || b.is_empty() || a.len() * b.len() > MAX_SKETCH_PAIRS {
        return None;
    }
    let na: f64 = a.iter().map(|s| s.rows as f64).sum();
    let nb: f64 = b.iter().map(|s| s.rows as f64).sum();
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    let mut j = 0.0;
    for x in a {
        for y in b {
            j += x.join_size(y);
        }
    }
    Some(left * right * j / (na * nb))
}

/// The estimated solutions of one triple pattern on its own.
#[derive(Debug, Clone)]
pub(crate) struct PatternEstimate {
    pub rows: f64,
    /// The estimated number of distinct values of each variable or blank node of the pattern.
    pub distinct: HashMap<String, f64>,
    /// The sketches of the columns each variable is bound from, when they have them.
    pub sketches: HashMap<String, Sketches>,
    /// The stored types each variable can have: a join on a variable whose two sides have no
    /// type in common has no solutions.
    pub types: HashMap<String, HashSet<BaseRDFNodeType>>,
    /// Whether all the tables the estimate comes from were read for statistics.
    pub analyzed: bool,
}

/// The estimated solutions of the triple patterns joined so far.
#[derive(Debug, Clone)]
pub(crate) struct JoinEstimate {
    pub rows: f64,
    pub distinct: HashMap<String, f64>,
    pub sketches: HashMap<String, Sketches>,
    pub types: HashMap<String, HashSet<BaseRDFNodeType>>,
    pub analyzed: bool,
}

impl JoinEstimate {
    /// The estimate before any pattern: the incoming solutions, or one empty solution.
    pub(crate) fn start(sm: &Option<SolutionMappings>) -> JoinEstimate {
        match sm {
            None => JoinEstimate {
                rows: 1.0,
                distinct: HashMap::new(),
                sketches: HashMap::new(),
                types: HashMap::new(),
                analyzed: true,
            },
            Some(sm) => {
                let rows = sm.height_estimate as f64;
                JoinEstimate {
                    rows,
                    distinct: sm
                        .rdf_node_types
                        .keys()
                        .map(|k| (k.clone(), rows.max(1.0)))
                        .collect(),
                    sketches: HashMap::new(),
                    // The types of the incoming solutions are not looked at.
                    types: HashMap::new(),
                    // The incoming estimate is the best there is for the solutions so far.
                    analyzed: true,
                }
            }
        }
    }

    /// The estimated number of solutions after joining with `p`: the smallest estimate over the
    /// shared variables, each from the sketches of its columns where both sides have them, and
    /// otherwise from the larger number of distinct values (independence).
    pub(crate) fn joined_rows(&self, p: &PatternEstimate) -> f64 {
        let mut best: Option<f64> = None;
        for (v, dp) in &p.distinct {
            if let Some(ds) = self.distinct.get(v) {
                if let (Some(a), Some(b)) = (self.types.get(v), p.types.get(v)) {
                    if a.is_disjoint(b) {
                        fast_path::hit(FastPath::BgpEstimateTypeDisjoint);
                        return 0.0;
                    }
                }
                let from_sketches = match (self.sketches.get(v), p.sketches.get(v)) {
                    (Some(a), Some(b)) => sketch_join_rows(self.rows, p.rows, a, b),
                    _ => None,
                };
                let e = from_sketches
                    .unwrap_or_else(|| self.rows * p.rows / ds.max(*dp).max(1.0));
                best = Some(best.map_or(e, |b: f64| b.min(e)));
            }
        }
        best.unwrap_or(self.rows * p.rows)
    }

    pub(crate) fn shares_variables(&self, p: &PatternEstimate) -> bool {
        p.distinct.keys().any(|v| self.distinct.contains_key(v))
    }

    /// Joins `p` into the estimate.
    pub(crate) fn join(&mut self, p: &PatternEstimate) {
        let rows = self.joined_rows(p).max(1.0);
        for (v, dp) in &p.distinct {
            let d = match self.distinct.get(v) {
                Some(ds) => ds.min(*dp),
                None => *dp,
            };
            // A shared variable keeps the sketches of the side with fewer distinct values,
            // which bounds it more.
            if let Some(ps) = p.sketches.get(v) {
                let keep_ours = self.sketches.contains_key(v)
                    && self.distinct.get(v).is_some_and(|ds| ds <= dp);
                if !keep_ours {
                    self.sketches.insert(v.clone(), ps.clone());
                }
            }
            self.distinct.insert(v.clone(), d);
            if let Some(pt) = p.types.get(v) {
                let t = match self.types.get(v) {
                    Some(st) => st.intersection(pt).cloned().collect(),
                    None => pt.clone(),
                };
                self.types.insert(v.clone(), t);
            }
        }
        for d in self.distinct.values_mut() {
            *d = d.min(rows).max(1.0);
        }
        self.rows = rows;
        self.analyzed = self.analyzed && p.analyzed;
    }
}

/// The graphs a pattern is matched in.
fn dataset_graphs(dataset: &QueryGraph) -> Vec<NamedGraph> {
    match dataset {
        QueryGraph::NamedGraph(ng) => vec![ng.clone()],
        QueryGraph::QueryDataset(qd) => qd
            .default
            .iter()
            .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
            .collect(),
    }
}

/// Whether a term can be stored in a table column of type `t`.
fn term_fits(term: &Term, t: &BaseRDFNodeType) -> bool {
    match (term, t) {
        (Term::NamedNode(_), BaseRDFNodeType::IRI) => true,
        (Term::BlankNode(_), BaseRDFNodeType::BlankNode) => true,
        (Term::Literal(l), BaseRDFNodeType::Literal(dt, _)) => l.datatype() == dt.as_ref(),
        _ => false,
    }
}

/// The values pushed down for the variable of a subject or object. None when there are none.
fn values_of<'a>(tp: &'a TermPattern, pushdowns: &'a Pushdowns) -> Option<&'a PushdownValues> {
    match tp {
        TermPattern::Variable(v) => pushdowns.variables_values.get(v.as_str()),
        _ => None,
    }
}

/// The number of pushed down values that can be stored in a column of type `t`: its codes of
/// that type and its terms that fit. Codes are counted without decoding them.
fn fitting_count(vs: &PushdownValues, t: &BaseRDFNodeType) -> usize {
    vs.codes.get(t).map_or(0, |c| c.len()) + vs.terms.iter().filter(|x| term_fits(x, t)).count()
}

/// The estimated number of triples of a table (with object type `t`) whose object is one of the
/// pushed down values: by code for the codes, and by term for the terms. Only for a few values
/// (see [MAX_VALUES_LOOKED_UP]).
fn fitting_frequency(vs: &PushdownValues, t: &BaseRDFNodeType, s: &TableStats) -> f64 {
    let by_code: f64 = vs
        .codes
        .get(t)
        .map_or(0.0, |c| c.iter().map(|c| s.object_frequency_code(*c)).sum());
    let by_term: f64 = vs
        .terms
        .iter()
        .filter(|x| term_fits(x, t))
        .map(|x| s.object_frequency(x))
        .sum();
    by_code + by_term
}

fn constant_term(tp: &TermPattern) -> Option<Term> {
    match tp {
        TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
        TermPattern::Literal(l) => Some(Term::Literal(l.clone())),
        _ => None,
    }
}

fn name_of(tp: &TermPattern) -> Option<String> {
    match tp {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

impl Triplestore {
    /// The tables a triple pattern reads, with their statistics, and the predicate of each.
    fn pattern_tables(
        &self,
        tp: &TriplePattern,
        pushdowns: &Pushdowns,
        dataset: &QueryGraph,
    ) -> Vec<(NamedNode, (BaseRDFNodeType, BaseRDFNodeType), Arc<TableStats>)> {
        let predicates: Option<HashSet<NamedNode>> = match &tp.predicate {
            NamedNodePattern::NamedNode(n) => Some(HashSet::from([n.clone()])),
            NamedNodePattern::Variable(v) => pushdowns
                .variables_values
                .get(v.as_str())
                .and_then(|vs| self.pushed_predicates(vs, dataset)),
        };
        let mut out = vec![];
        for g in dataset_graphs(dataset) {
            let Some(pm) = self.graph_triples_map.get(&g) else {
                continue;
            };
            // A pattern with an unbound predicate reads every table, but does not read them
            // for statistics: that would read the whole store for one query.
            let read = predicates.is_some();
            let mut add = |p: &NamedNode, tm: &crate::graph_map::TypesMap| {
                for (types, t) in tm {
                    out.push((p.clone(), types.clone(), self.table_stats(&g, p, types, t, read)));
                }
            };
            match &predicates {
                Some(ps) => {
                    for p in ps {
                        if let Some(tm) = pm.get(p) {
                            add(p, tm);
                        }
                    }
                }
                None => {
                    for (p, tm) in pm {
                        add(p, tm);
                    }
                }
            }
        }
        out
    }

    /// The predicates of the queried graphs among the values pushed down for a variable
    /// predicate. The graphs have few predicates, so their codes are looked up in the pushed codes
    /// rather than the codes decoded. None (every predicate) when more than
    /// [MAX_VALUES_CHECKED] values are pushed.
    fn pushed_predicates(
        &self,
        vs: &PushdownValues,
        dataset: &QueryGraph,
    ) -> Option<HashSet<NamedNode>> {
        let mut out: HashSet<NamedNode> = vs
            .terms
            .iter()
            .filter_map(|t| match t {
                Term::NamedNode(nn) => Some(nn.clone()),
                _ => None,
            })
            .collect();
        let codes = match vs.codes.get(&BaseRDFNodeType::IRI) {
            Some(c) if !c.is_empty() => c,
            _ => return Some(out),
        };
        if vs.len() > MAX_VALUES_CHECKED {
            return None;
        }
        let mut candidates: Vec<NamedNode> = vec![];
        for g in dataset_graphs(dataset) {
            if let Some(pm) = self.graph_triples_map.get(&g) {
                candidates.extend(pm.keys().cloned());
            }
        }
        candidates.sort();
        candidates.dedup();
        let strs: Vec<Option<&str>> = candidates.iter().map(|p| Some(p.as_str())).collect();
        let found = self
            .global_cats
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .maybe_encode_iri_slice(&strs);
        for (p, c) in candidates.into_iter().zip(found) {
            if c.is_some_and(|c| codes.binary_search(&c).is_ok()) {
                out.insert(p);
            }
        }
        Some(out)
    }

    /// The estimated solutions of a triple pattern on its own, with the values pushed down so
    /// far.
    pub(crate) fn estimate_triple_pattern(
        &self,
        tp: &TriplePattern,
        pushdowns: &Pushdowns,
        dataset: &QueryGraph,
    ) -> PatternEstimate {
        let subject_const = constant_term(&tp.subject);
        let object_const = constant_term(&tp.object);
        let subject_values = values_of(&tp.subject, pushdowns);
        let object_values = values_of(&tp.object, pushdowns);
        let subject_name = name_of(&tp.subject);
        let object_name = name_of(&tp.object);
        let same_subject_object = subject_name.is_some() && subject_name == object_name;

        let mut rows = 0.0;
        let mut subject_distinct = 0.0;
        let mut object_distinct = 0.0;
        let mut predicates: HashSet<NamedNode> = HashSet::new();
        let mut analyzed = true;
        let mut subject_sketches: Sketches = vec![];
        let mut object_sketches: Sketches = vec![];
        let mut subject_types: HashSet<BaseRDFNodeType> = HashSet::new();
        let mut object_types: HashSet<BaseRDFNodeType> = HashSet::new();
        for (p, (st, ot), s) in self.pattern_tables(tp, pushdowns, dataset) {
            if s.rows == 0 {
                continue;
            }
            analyzed &= s.analyzed;
            let n = s.rows as f64;
            let ds = s.distinct_subjects.max(1) as f64;
            let dobj = s.distinct_objects.max(1) as f64;
            // The share of the table's subjects that are bound, and how many are.
            let (subject_share, subject_k) = match (&subject_const, &subject_values) {
                (Some(t), _) => {
                    if term_fits(t, &st) {
                        (1.0 / ds, 1.0)
                    } else {
                        (0.0, 0.0)
                    }
                }
                (None, Some(vs)) => {
                    // Many values are taken to all fit the column: looking at each costs more
                    // than the estimate is worth.
                    let k = if vs.len() > MAX_VALUES_CHECKED {
                        vs.len()
                    } else {
                        fitting_count(vs, &st)
                    } as f64;
                    ((k / ds).min(1.0), k.min(ds))
                }
                (None, None) => (1.0, ds),
            };
            if subject_share == 0.0 {
                continue;
            }
            // The number of the table's triples with a bound object, and how many objects are
            // bound.
            let (object_rows, object_k) = match (&object_const, &object_values) {
                (Some(t), _) => {
                    if term_fits(t, &ot) {
                        (Some(s.object_frequency(t).min(n)), 1.0)
                    } else {
                        (Some(0.0), 0.0)
                    }
                }
                (None, Some(vs)) => {
                    let (r, k) = if vs.len() > MAX_VALUES_CHECKED {
                        (vs.len() as f64 * n / dobj, vs.len() as f64)
                    } else {
                        let k = fitting_count(vs, &ot);
                        let r = if k <= MAX_VALUES_LOOKED_UP {
                            fitting_frequency(vs, &ot, &s)
                        } else {
                            k as f64 * n / dobj
                        };
                        (r, k as f64)
                    };
                    (Some(r.min(n)), k.min(dobj))
                }
                (None, None) => (None, dobj),
            };
            if object_rows == Some(0.0) {
                continue;
            }
            let mut r = match object_rows {
                Some(or) => or * subject_share,
                None => n * subject_share,
            };
            if same_subject_object {
                // `?x :p ?x`: a triple whose subject is its object.
                r /= ds.max(dobj);
            }
            rows += r;
            // Distinct values of the ends: at most the solutions they are in.
            subject_distinct += subject_k.min(r);
            object_distinct += object_k.min(r);
            subject_types.insert(st.clone());
            object_types.insert(ot.clone());
            if let Some(sk) = &s.subject_sketch {
                subject_sketches.push(sk.clone());
            }
            if let Some(sk) = &s.object_sketch {
                object_sketches.push(sk.clone());
            }
            predicates.insert(p);
        }
        let mut distinct = HashMap::new();
        if let Some(sn) = &subject_name {
            distinct.insert(sn.clone(), subject_distinct.max(1.0));
        }
        if let Some(on) = &object_name {
            let d = object_distinct.max(1.0);
            distinct
                .entry(on.clone())
                .and_modify(|x: &mut f64| *x = x.min(d))
                .or_insert(d);
        }
        if let NamedNodePattern::Variable(v) = &tp.predicate {
            distinct.insert(v.as_str().to_string(), (predicates.len() as f64).max(1.0));
        }
        let mut sketches = HashMap::new();
        if !same_subject_object {
            if let Some(sn) = &subject_name {
                if !subject_sketches.is_empty() {
                    sketches.insert(sn.clone(), subject_sketches);
                }
            }
            if let Some(on) = &object_name {
                if !object_sketches.is_empty() {
                    sketches.insert(on.clone(), object_sketches);
                }
            }
        }
        let mut types = HashMap::new();
        if let Some(sn) = &subject_name {
            types.insert(sn.clone(), subject_types.clone());
        }
        if let Some(on) = &object_name {
            let t = match types.get(on) {
                Some(st) => object_types.intersection(st).cloned().collect(),
                None => object_types,
            };
            types.insert(on.clone(), t);
        }
        if let NamedNodePattern::Variable(v) = &tp.predicate {
            types.insert(v.as_str().to_string(), HashSet::from([BaseRDFNodeType::IRI]));
        }
        PatternEstimate {
            rows,
            distinct,
            sketches,
            types,
            analyzed,
        }
    }

    /// Orders the triple patterns of a basic graph pattern by estimated cost: each time the
    /// pattern connected to those before it whose join gives the fewest solutions, when that is
    /// at least [CLEARLY_CHEAPER] times fewer than for the next connected pattern in the order
    /// of the query, and otherwise that next one. A pattern is only taken before a connected one
    /// when none is connected.
    pub(crate) fn order_triple_patterns_by_estimates(
        &self,
        tps: &[TriplePattern],
        sm: &Option<SolutionMappings>,
        pushdowns: &Pushdowns,
        dataset: &QueryGraph,
    ) -> Vec<TriplePattern> {
        let estimates: Vec<PatternEstimate> = tps
            .iter()
            .map(|tp| self.estimate_triple_pattern(tp, pushdowns, dataset))
            .collect();
        let mut state = JoinEstimate::start(sm);
        let mut remaining: Vec<usize> = (0..tps.len()).collect();
        let mut ordered = Vec::with_capacity(tps.len());
        let mut overruled = false;
        while !remaining.is_empty() {
            let any_connected = !state.distinct.is_empty()
                && remaining
                    .iter()
                    .any(|i| state.shares_variables(&estimates[*i]));
            // The first candidate in the order of the query, and the cheapest one: the
            // cheapest is only taken when it is estimated to be at least [CLEARLY_CHEAPER] times
            // cheaper, since a small difference in estimates is mostly their error.
            let mut first: Option<(usize, f64)> = None;
            let mut best: Option<(usize, f64)> = None;
            for (pos, i) in remaining.iter().enumerate() {
                let e = &estimates[*i];
                if any_connected && !state.shares_variables(e) {
                    continue;
                }
                let cost = state.joined_rows(e);
                if first.is_none() {
                    first = Some((pos, cost));
                }
                if best.is_none_or(|(_, c)| cost < c) {
                    best = Some((pos, cost));
                }
            }
            let (first_pos, first_cost) = first.unwrap();
            let (best_pos, best_cost) = best.unwrap();
            let pos = if best_cost * CLEARLY_CHEAPER <= first_cost {
                best_pos
            } else {
                first_pos
            };
            overruled |= pos != first_pos;
            let i = remaining.remove(pos);
            state.join(&estimates[i]);
            ordered.push(tps[i].clone());
        }
        fast_path::hit_if(overruled, FastPath::BgpOrderStatistics, FastPath::BgpOrderWritten);
        ordered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::vocab::xsd;

    fn pattern(rows: f64, var: &str, types: &[BaseRDFNodeType]) -> PatternEstimate {
        PatternEstimate {
            rows,
            distinct: HashMap::from([(var.to_string(), rows)]),
            sketches: HashMap::new(),
            types: HashMap::from([(var.to_string(), types.iter().cloned().collect())]),
            analyzed: true,
        }
    }

    #[test]
    fn joins_on_variables_with_no_common_type_are_empty() {
        // ?a :hasEmergencyPlan ?v (booleans) . ?v :crossSectionArea ?x (IRI subjects)
        let boolean = BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None);
        let mut state = JoinEstimate::start(&None);
        state.join(&pattern(1_000.0, "v", &[boolean.clone()]));
        assert_eq!(state.joined_rows(&pattern(5_000.0, "v", &[BaseRDFNodeType::IRI])), 0.0);
        // A column with both types still joins on the shared one.
        let mixed = pattern(5_000.0, "v", &[BaseRDFNodeType::IRI, boolean.clone()]);
        assert!(state.joined_rows(&mixed) > 0.0);
        // After joining with the mixed column, only booleans are left for ?v.
        state.join(&mixed);
        assert_eq!(state.types["v"], HashSet::from([boolean]));
        assert_eq!(state.joined_rows(&pattern(10.0, "v", &[BaseRDFNodeType::IRI])), 0.0);
    }
}
