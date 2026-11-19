//! COUNT(*) over small join shapes answered from the stored dictionary codes, without building
//! the joined rows: e.g.
//! `SELECT (COUNT(*) AS ?c) WHERE { ?s rdf:type ?o1 . ?s era:hasEmergencyPlan ?o2 }`.
//!
//! The pattern is read as a sum (UNION) of terms. A term is a basic graph pattern whose triple
//! patterns have constant predicates and variable subjects and objects, with attachments that
//! share at most one variable (the key) with it:
//! - `FILTER EXISTS { P }` and `FILTER NOT EXISTS { P }`,
//! - `OPTIONAL { P }` (without a filter),
//! - `MINUS { P }`, which removes nothing when it shares no variable.
//!
//! The variables and triple patterns of a term must form a forest (no cycles). Then the number of
//! solutions is a product of sums along the tree: rooted at a variable `v`, the weight of a value
//! `x` of `v` is the product, over the patterns below `v`, of the sum of the weights of their other
//! ends over the stored rows with `x` at `v`, and the count is the sum of the root's weights. An
//! attachment with key `k` multiplies the weight of each value `x` of `k` by a function of the
//! number `n` of solutions of `P` with `k = x`: `n > 0` (EXISTS), `n = 0` (NOT EXISTS, MINUS),
//! `max(1, n)` (OPTIONAL). A join of a UNION distributes over its branches.
//!
//! Weights are kept per stored type and dictionary code. The tables of a graph are deduplicated
//! and disjoint, every table holds one subject type and one object type, and the codes of a type
//! are global, so equal terms have equal (type, code) pairs. A variable that two patterns join on
//! (or that an attachment is keyed on) must have dictionary coded types, anything else falls back
//! to the general path. Tables whose type at a join variable is not stored at its other patterns
//! cannot join and are left out first.

use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::{PredicateTriples, Triples};
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::IntoLazy;
use polars_core::frame::DataFrame;
use polars_core::prelude::Column;
use representation::dataset::QueryGraph;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use utils::fast_path::{self, FastPath};

/// The largest number of UNION terms the pattern may expand to.
const MAX_TERMS: usize = 16;

/// Why the fast path stopped: the general path answers, or loading failed.
enum Stop {
    Fallback,
    Error(SparqlError),
}

impl From<TriplestoreError> for Stop {
    fn from(e: TriplestoreError) -> Self {
        Stop::Error(e.into())
    }
}

type R<T> = Result<T, Stop>;

/// A hasher for dictionary codes.
#[derive(Default)]
struct CodeHasher(u64);

impl Hasher for CodeHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0.rotate_left(8) ^ *b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    fn write_u32(&mut self, i: u32) {
        let h = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = h ^ (h >> 29);
    }
}

type CodeMap = HashMap<u32, u64, BuildHasherDefault<CodeHasher>>;

/// Weights by the dictionary codes of one type. Codes that are not present weigh 0.
enum KeyMap {
    /// The weights of the codes from `min`.
    Dense { min: u32, vals: Vec<u64> },
    /// A map, with a bitmap of its codes from `min` to test codes quickly.
    Sparse { map: CodeMap, min: u32, bits: Vec<u64> },
    /// Only the total weight of the values of a type that is not dictionary coded. It is kept
    /// as long as no other factor needs the weights of single values of the type.
    Total(u64),
}

impl KeyMap {
    fn from_map(map: CodeMap) -> KeyMap {
        let (min, max) = map
            .keys()
            .fold((u32::MAX, 0), |(a, b), k| (a.min(*k), b.max(*k)));
        let mut bits = vec![];
        if !map.is_empty() {
            bits = vec![0u64; ((max - min) as usize >> 6) + 1];
            for k in map.keys() {
                let i = (k - min) as usize;
                bits[i >> 6] |= 1 << (i & 63);
            }
        }
        KeyMap::Sparse { map, min, bits }
    }

    fn from_pairs(pairs: Vec<(u32, u64)>) -> KeyMap {
        if pairs.is_empty() {
            return KeyMap::from_map(CodeMap::default());
        }
        let (min, max) = pairs
            .iter()
            .fold((u32::MAX, 0), |(a, b), (k, _)| (a.min(*k), b.max(*k)));
        let range = (max - min) as usize + 1;
        if dense_enough(range, pairs.len()) {
            let mut vals = vec![0u64; range];
            for (k, v) in pairs {
                vals[(k - min) as usize] = v;
            }
            KeyMap::Dense { min, vals }
        } else {
            let mut map = CodeMap::with_capacity_and_hasher(pairs.len(), Default::default());
            map.extend(pairs);
            KeyMap::from_map(map)
        }
    }

    fn is_total(&self) -> bool {
        matches!(self, KeyMap::Total(_))
    }

    /// Multiplies the weight of a code that is present by `f`.
    fn scale(&mut self, k: u32, f: u64) {
        match self {
            KeyMap::Dense { min, vals } => {
                if let Some(v) = vals.get_mut(k.wrapping_sub(*min) as usize) {
                    *v = v.saturating_mul(f);
                }
            }
            KeyMap::Sparse { map, .. } => {
                if let Some(v) = map.get_mut(&k) {
                    *v = v.saturating_mul(f);
                }
            }
            KeyMap::Total(_) => unreachable!("codes of a type that is not dictionary coded"),
        }
    }

    /// The weight of a code. Not for a `Total`, whose callers stop first.
    #[inline]
    fn get(&self, k: u32) -> u64 {
        match self {
            KeyMap::Dense { min, vals } => {
                vals.get(k.wrapping_sub(*min) as usize).copied().unwrap_or(0)
            }
            KeyMap::Sparse { map, min, bits } => {
                let i = k.wrapping_sub(*min) as usize;
                match bits.get(i >> 6) {
                    Some(w) if w & (1 << (i & 63)) != 0 => map.get(&k).copied().unwrap_or(0),
                    _ => 0,
                }
            }
            KeyMap::Total(_) => unreachable!("codes of a type that is not dictionary coded"),
        }
    }

    /// The codes with a weight above 0, with their weights.
    fn nonzero(&self) -> Vec<(u32, u64)> {
        match self {
            KeyMap::Dense { min, vals } => vals
                .iter()
                .enumerate()
                .filter(|(_, v)| **v > 0)
                .map(|(i, v)| (min + i as u32, *v))
                .collect(),
            KeyMap::Sparse { map, .. } => map
                .iter()
                .filter(|(_, v)| **v > 0)
                .map(|(k, v)| (*k, *v))
                .collect(),
            KeyMap::Total(_) => unreachable!("codes of a type that is not dictionary coded"),
        }
    }

    /// An upper bound on the number of codes with a weight above 0.
    fn size(&self) -> usize {
        match self {
            KeyMap::Dense { vals, .. } => vals.len(),
            KeyMap::Sparse { map, .. } => map.len(),
            KeyMap::Total(_) => 1,
        }
    }

    fn sum(&self) -> u64 {
        match self {
            KeyMap::Dense { vals, .. } => vals.iter().fold(0, |a, v| a.saturating_add(*v)),
            KeyMap::Sparse { map, .. } => map.values().fold(0, |a, v| a.saturating_add(*v)),
            KeyMap::Total(v) => *v,
        }
    }
}

/// Whether a dense array over the range of codes is cheaper than a map of `n` codes: zeroing a
/// word costs about as much as a map insert does for 16 to 32 of them.
fn dense_enough(range: usize, n: usize) -> bool {
    range <= 16 * n + 4096
}

/// Weights by type and code. Types that are not present weigh 0.
type Plain = HashMap<BaseRDFNodeType, KeyMap>;

/// Applies `f` to the weights above 0 (with no code for a `Total`), leaving out those that
/// become 0.
fn plain_map(
    p: &Plain,
    mut f: impl FnMut(&BaseRDFNodeType, Option<u32>, u64) -> R<u64>,
) -> R<Plain> {
    let mut out = Plain::new();
    for (t, m) in p {
        if let KeyMap::Total(v) = m {
            let w = f(t, None, *v)?;
            if w > 0 {
                out.insert(t.clone(), KeyMap::Total(w));
            }
            continue;
        }
        let mut pairs = vec![];
        for (k, v) in m.nonzero() {
            let w = f(t, Some(k), v)?;
            if w > 0 {
                pairs.push((k, w));
            }
        }
        if !pairs.is_empty() {
            out.insert(t.clone(), KeyMap::from_pairs(pairs));
        }
    }
    Ok(out)
}

/// The weight in `p` of a code (or, with no code, of every value of a type that is not
/// dictionary coded, which needs `p` not to hold the type).
fn plain_get(p: &Plain, t: &BaseRDFNodeType, k: Option<u32>) -> R<u64> {
    match (p.get(t), k) {
        (None, _) => Ok(0),
        (Some(m), Some(k)) if !m.is_total() => Ok(m.get(k)),
        _ => Err(Stop::Fallback),
    }
}

fn plain_add(a: Plain, b: Plain) -> R<Plain> {
    if a.is_empty() {
        return Ok(b);
    }
    if b.is_empty() {
        return Ok(a);
    }
    let mut out = Plain::new();
    let types: HashSet<_> = a.keys().chain(b.keys()).cloned().collect();
    for t in types {
        let ms: Vec<&KeyMap> = [a.get(&t), b.get(&t)].into_iter().flatten().collect();
        if ms.iter().any(|m| m.is_total()) {
            if ms.iter().all(|m| m.is_total()) {
                let v = ms.iter().fold(0u64, |a, m| a.saturating_add(m.sum()));
                out.insert(t, KeyMap::Total(v));
                continue;
            }
            return Err(Stop::Fallback);
        }
        let mut map = CodeMap::default();
        for m in ms {
            for (k, v) in m.nonzero() {
                let e = map.entry(k).or_insert(0);
                *e = e.saturating_add(v);
            }
        }
        out.insert(t, KeyMap::from_pairs(map.into_iter().collect()));
    }
    Ok(out)
}

/// What an attachment does with the number `n` of its solutions for a key value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Kind {
    /// FILTER EXISTS: n > 0.
    Exists,
    /// FILTER NOT EXISTS and MINUS: n = 0.
    NotExists,
    /// OPTIONAL: max(1, n).
    Optional,
}

impl Kind {
    #[inline]
    fn apply(self, n: u64) -> u64 {
        match self {
            Kind::Exists => (n > 0) as u64,
            Kind::NotExists => (n == 0) as u64,
            Kind::Optional => n.max(1),
        }
    }
}

/// The weights of the values of a variable: the product of `plain` (1 when None) and the
/// attachment functions of `mods`, which are kept apart while there is no `plain`, as they
/// weigh the values they do not hold 1.
#[derive(Default)]
struct Weights {
    plain: Option<Plain>,
    mods: Vec<(Plain, Kind)>,
}

impl Weights {
    /// Multiplies by weights that are 0 where they are not present.
    fn mul_plain(&mut self, msg: Plain) -> R<()> {
        let new = match &self.plain {
            None => plain_map(&msg, |t, k, v| {
                let mut v = v;
                for (m, kind) in &self.mods {
                    v = v.saturating_mul(kind.apply(plain_get(m, t, k)?));
                }
                Ok(v)
            })?,
            Some(p) => {
                let mut out = Plain::new();
                for (t, a) in p {
                    let Some(b) = msg.get(t) else { continue };
                    if a.is_total() || b.is_total() {
                        return Err(Stop::Fallback);
                    }
                    let (small, large) = if a.size() <= b.size() { (a, b) } else { (b, a) };
                    let pairs: Vec<_> = small
                        .nonzero()
                        .into_iter()
                        .filter_map(|(k, v)| {
                            let w = v.saturating_mul(large.get(k));
                            (w > 0).then_some((k, w))
                        })
                        .collect();
                    if !pairs.is_empty() {
                        out.insert(t.clone(), KeyMap::from_pairs(pairs));
                    }
                }
                out
            }
        };
        self.plain = Some(new);
        self.mods.clear();
        Ok(())
    }

    fn apply(&mut self, kind: Kind, n: Plain) -> R<()> {
        match &mut self.plain {
            // NOT EXISTS and OPTIONAL weigh the values `n` does not hold 1: change the others in
            // place.
            Some(p) if kind != Kind::Exists => {
                for (t, m) in &n {
                    let Some(target) = p.get_mut(t) else { continue };
                    if m.is_total() || target.is_total() {
                        return Err(Stop::Fallback);
                    }
                    for (k, c) in m.nonzero() {
                        target.scale(k, kind.apply(c));
                    }
                }
            }
            Some(p) => {
                let mut new = Plain::new();
                for (t, m) in &n {
                    let Some(target) = p.get(t) else { continue };
                    if m.is_total() || target.is_total() {
                        return Err(Stop::Fallback);
                    }
                    let pairs: Vec<_> = if m.size() <= target.size() {
                        m.nonzero()
                            .into_iter()
                            .filter_map(|(k, _)| {
                                let v = target.get(k);
                                (v > 0).then_some((k, v))
                            })
                            .collect()
                    } else {
                        target.nonzero().into_iter().filter(|(k, _)| m.get(*k) > 0).collect()
                    };
                    if !pairs.is_empty() {
                        new.insert(t.clone(), KeyMap::from_pairs(pairs));
                    }
                }
                self.plain = Some(new);
            }
            None if kind == Kind::Exists => {
                if n.values().any(|m| m.is_total()) {
                    return Err(Stop::Fallback);
                }
                self.mul_plain(plain_map(&n, |_, _, _| Ok(1))?)?
            }
            None => self.mods.push((n, kind)),
        }
        Ok(())
    }

    fn is_zero(&self) -> bool {
        self.plain.as_ref().is_some_and(|p| p.is_empty())
    }

    /// The weights of the values of a type, which must be single codes or none.
    fn lookup(&self, t: &BaseRDFNodeType) -> R<Lookup<'_>> {
        let (zero, plain) = match &self.plain {
            None => (false, None),
            Some(p) => match p.get(t) {
                None => (true, None),
                Some(m) => (false, Some(m)),
            },
        };
        let mods: Vec<_> = self
            .mods
            .iter()
            .filter_map(|(p, kind)| p.get(t).map(|m| (m, *kind)))
            .collect();
        if plain.is_some_and(|m| m.is_total()) || mods.iter().any(|(m, _)| m.is_total()) {
            return Err(Stop::Fallback);
        }
        Ok(Lookup { zero, plain, mods })
    }
}

/// The weights of the codes of one type.
struct Lookup<'w> {
    /// Every value weighs 0.
    zero: bool,
    plain: Option<&'w KeyMap>,
    mods: Vec<(&'w KeyMap, Kind)>,
}

impl Lookup<'_> {
    /// Every value weighs 1 (attachment functions weigh the values they do not hold 1).
    fn is_one(&self) -> bool {
        !self.zero && self.plain.is_none() && self.mods.is_empty()
    }

    #[inline]
    fn value(&self, k: u32) -> u64 {
        let v = self.plain.map_or(1, |m| m.get(k));
        if v == 0 {
            return 0;
        }
        self.mods
            .iter()
            .fold(v, |v, (m, kind)| v.saturating_mul(kind.apply(m.get(k))))
    }
}

/// A triple pattern with a constant predicate, and the tables it can match.
#[derive(Clone)]
struct Edge<'a> {
    s: String,
    o: String,
    tables: Vec<&'a Triples>,
}

impl Edge<'_> {
    fn height(&self) -> usize {
        self.tables.iter().map(|t| t.height()).sum()
    }

    fn other(&self, v: &str) -> &str {
        if self.s == v {
            &self.o
        } else {
            &self.s
        }
    }

    fn ty<'t>(t: &'t Triples, subject: bool) -> &'t BaseRDFNodeType {
        if subject {
            &t.subject_type
        } else {
            &t.object_type
        }
    }
}

#[derive(Clone)]
struct Att<'a> {
    kind: Kind,
    /// The variable it shares with the term, if any.
    key: Option<String>,
    /// The terms of its pattern, summed.
    sub: Vec<Term<'a>>,
}

#[derive(Clone, Default)]
struct Term<'a> {
    edges: Vec<Edge<'a>>,
    atts: Vec<Att<'a>>,
}

impl<'a> Term<'a> {
    fn main_vars(&self) -> HashSet<&str> {
        self.edges
            .iter()
            .flat_map(|e| [e.s.as_str(), e.o.as_str()])
            .collect()
    }

    fn all_vars(&self) -> HashSet<String> {
        let mut out: HashSet<String> = self.main_vars().into_iter().map(String::from).collect();
        for a in &self.atts {
            for t in &a.sub {
                out.extend(t.all_vars());
            }
        }
        out
    }

    fn merge(&self, other: &Term<'a>) -> Term<'a> {
        let mut t = self.clone();
        t.edges.extend(other.edges.iter().cloned());
        t.atts.extend(other.atts.iter().cloned());
        t
    }

    /// Adds an attachment with the terms of `sub`. For MINUS (`minus`), terms that share no
    /// variable remove nothing.
    fn attach(&mut self, kind: Kind, sub: Vec<Term<'a>>, minus: bool) -> R<()> {
        let all = self.all_vars();
        let main: HashSet<String> = self.main_vars().into_iter().map(String::from).collect();
        let mut key: Option<Option<String>> = None;
        let mut kept = vec![];
        for d in sub {
            let shared: Vec<String> = d.all_vars().intersection(&all).cloned().collect();
            if minus && shared.is_empty() {
                continue;
            }
            // A shared variable must be bound in every solution on both sides.
            let d_main = d.main_vars();
            if shared.len() > 1
                || shared
                    .iter()
                    .any(|v| !main.contains(v) || !d_main.contains(v.as_str()))
            {
                return Err(Stop::Fallback);
            }
            let k = shared.into_iter().next();
            match &key {
                Some(prev) if prev != &k => return Err(Stop::Fallback),
                _ => key = Some(k),
            }
            kept.push(d);
        }
        if let Some(key) = key {
            self.atts.push(Att { kind, key, sub: kept });
        }
        Ok(())
    }

    /// Checks that the term (and the terms of its attachments) are forests, and that the
    /// variables of each attachment other than its key are its own.
    fn validate(&self) -> R<()> {
        let mut parent: HashMap<&str, &str> = HashMap::new();
        fn find<'s>(parent: &mut HashMap<&'s str, &'s str>, v: &'s str) -> &'s str {
            let mut r = v;
            while let Some(p) = parent.get(r).copied() {
                if p == r {
                    break;
                }
                r = p;
            }
            parent.insert(v, r);
            r
        }
        for e in &self.edges {
            parent.entry(&e.s).or_insert(&e.s);
            parent.entry(&e.o).or_insert(&e.o);
            let (a, b) = (find(&mut parent, &e.s), find(&mut parent, &e.o));
            if a == b {
                return Err(Stop::Fallback);
            }
            parent.insert(a, b);
        }
        let main: HashSet<String> = self.main_vars().into_iter().map(String::from).collect();
        let att_vars: Vec<HashSet<String>> = self
            .atts
            .iter()
            .map(|a| {
                let mut vs: HashSet<String> =
                    a.sub.iter().flat_map(|t| t.all_vars()).collect();
                if let Some(k) = &a.key {
                    vs.remove(k);
                }
                vs
            })
            .collect();
        for (i, vs) in att_vars.iter().enumerate() {
            if !vs.is_disjoint(&main)
                || att_vars
                    .iter()
                    .enumerate()
                    .any(|(j, other)| i != j && !vs.is_disjoint(other))
            {
                return Err(Stop::Fallback);
            }
        }
        for a in &self.atts {
            for t in &a.sub {
                t.validate()?;
            }
        }
        Ok(())
    }

    /// Leaves out the tables whose type at a variable is not stored at every other pattern of
    /// the variable, as their rows join with nothing.
    fn prune(&mut self) {
        loop {
            let mut types: HashMap<&str, Vec<HashSet<BaseRDFNodeType>>> = HashMap::new();
            for e in &self.edges {
                for subject in [true, false] {
                    let v = if subject { &e.s } else { &e.o };
                    types.entry(v).or_default().push(
                        e.tables
                            .iter()
                            .map(|t| Edge::ty(t, subject).clone())
                            .collect(),
                    );
                }
            }
            let allowed: HashMap<String, HashSet<BaseRDFNodeType>> = types
                .into_iter()
                .filter(|(_, sets)| sets.len() > 1)
                .map(|(v, sets)| {
                    let mut it = sets.into_iter();
                    let first = it.next().unwrap();
                    let inter = it.fold(first, |a, b| a.intersection(&b).cloned().collect());
                    (v.to_string(), inter)
                })
                .collect();
            let mut changed = false;
            for e in &mut self.edges {
                let before = e.tables.len();
                e.tables.retain(|t| {
                    allowed.get(&e.s).is_none_or(|a| a.contains(&t.subject_type))
                        && allowed.get(&e.o).is_none_or(|a| a.contains(&t.object_type))
                });
                changed |= e.tables.len() != before;
            }
            if !changed {
                break;
            }
        }
        for a in &mut self.atts {
            for t in &mut a.sub {
                t.prune();
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.edges.iter().any(|e| e.tables.is_empty())
    }
}

/// The patterns and attachments that meet at each variable of a term.
struct Shape<'t, 'a> {
    term: &'t Term<'a>,
    edges_at: HashMap<&'t str, Vec<usize>>,
    atts_at: HashMap<&'t str, Vec<usize>>,
}

impl<'t, 'a> Shape<'t, 'a> {
    fn new(term: &'t Term<'a>) -> Self {
        let mut edges_at: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, e) in term.edges.iter().enumerate() {
            edges_at.entry(&e.s).or_default().push(i);
            edges_at.entry(&e.o).or_default().push(i);
        }
        let mut atts_at: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, a) in term.atts.iter().enumerate() {
            if let Some(k) = &a.key {
                atts_at.entry(k).or_default().push(i);
            }
        }
        Shape {
            term,
            edges_at,
            atts_at,
        }
    }

    /// The product of the attachments that share no variable.
    fn scalar_factor(&self) -> R<u64> {
        let mut f = 1u64;
        for a in &self.term.atts {
            if a.key.is_none() {
                f = f.saturating_mul(a.kind.apply(subs_total(&a.sub)?));
            }
        }
        Ok(f)
    }

    /// The variables of the component of `v`.
    fn component(&self, v: &'t str, seen: &mut HashSet<&'t str>) -> Vec<&'t str> {
        let mut out = vec![];
        let mut stack = vec![v];
        seen.insert(v);
        while let Some(x) = stack.pop() {
            out.push(x);
            for e in &self.edges_at[x] {
                let u = self.term.edges[*e].other(x);
                if let Some(u) = self.edges_at.get_key_value(u).map(|(k, _)| *k) {
                    if seen.insert(u) {
                        stack.push(u);
                    }
                }
            }
        }
        out
    }

    fn n_factors(&self, v: &str) -> usize {
        self.edges_at.get(v).map_or(0, |e| e.len()) + self.atts_at.get(v).map_or(0, |a| a.len())
    }

    /// The number of solutions of the component with root `root`. Only the total is needed:
    /// - the patterns and EXISTS at the root are weighed from the cheapest, each only on the values
    ///   the ones before it hold,
    /// - the last pattern is not keyed, but summed with the weights of the others,
    /// - NOT EXISTS, MINUS and OPTIONAL weigh the values they do not match 1. When they are
    ///   cheaper than the last pattern, they correct its total on the values they match, otherwise
    ///   they are weighed on the values of all the patterns.
    fn component_total(&self, root: &'t str) -> R<u64> {
        enum Factor {
            Edge(usize),
            Exists(usize),
        }
        let atts_at: Vec<usize> = self.atts_at.get(root).cloned().unwrap_or_default();
        let mut items: Vec<(usize, Factor)> = self.edges_at[root]
            .iter()
            .map(|e| (self.term.edges[*e].height(), Factor::Edge(*e)))
            .collect();
        let mut others = vec![];
        for a in atts_at {
            let att = &self.term.atts[a];
            if att.kind == Kind::Exists {
                items.push((sub_cost(&att.sub), Factor::Exists(a)));
            } else {
                others.push(a);
            }
        }
        items.sort_by_key(|(c, _)| *c);
        let others_cost: usize = others.iter().map(|a| sub_cost(&self.term.atts[*a].sub)).sum();
        let fuse = match items.last() {
            Some((c, Factor::Edge(_))) => others.is_empty() || others_cost <= *c,
            _ => false,
        };
        let last = if fuse { items.pop() } else { None };
        let mut w: Option<Plain> = None;
        for (_, item) in items {
            let msg = match item {
                Factor::Edge(e) => {
                    let edge = &self.term.edges[e];
                    let child = self.child_weights(root, e)?;
                    edge_scan(edge, edge.s == root, &child, w.as_ref(), true, None)?.0
                }
                Factor::Exists(a) => {
                    let n = subs_rooted(&self.term.atts[a].sub, root, w.as_ref())?;
                    if n.values().any(|m| m.is_total()) {
                        return Err(Stop::Fallback);
                    }
                    plain_map(&n, |_, _, _| Ok(1))?
                }
            };
            w = Some(plain_mul(w, msg)?);
            if w.as_ref().is_some_and(|p| p.is_empty()) {
                return Ok(0);
            }
        }
        let Some((_, Factor::Edge(last))) = last else {
            // Every pattern is weighed: weigh the attachments on their values.
            let mut ws = Weights {
                plain: w,
                mods: vec![],
            };
            for a in others {
                let att = &self.term.atts[a];
                let n = subs_rooted(&att.sub, root, ws.plain.as_ref())?;
                ws.apply(att.kind, n)?;
            }
            let p = ws.plain.unwrap_or_default();
            return Ok(p.values().fold(0, |a, m| a.saturating_add(m.sum())));
        };
        let le = &self.term.edges[last];
        let lchild = self.child_weights(root, last)?;
        let wv = |t: &BaseRDFNodeType, k: u32| -> u64 {
            match &w {
                None => 1,
                Some(p) => p.get(t).map_or(0, |m| if m.is_total() { 0 } else { m.get(k) }),
            }
        };
        let (_, total) =
            edge_scan(le, le.s == root, &lchild, w.as_ref(), false, Some(&|_, _, fw| fw))?;
        if others.is_empty() || total == 0 {
            return Ok(total);
        }
        let mut ns = vec![];
        for a in others {
            let att = &self.term.atts[a];
            let n = subs_rooted(&att.sub, root, w.as_ref())?;
            if n.values().any(|m| m.is_total()) {
                return Err(Stop::Fallback);
            }
            if !n.is_empty() {
                ns.push((n, att.kind));
            }
        }
        let corr: i128 = match ns.as_slice() {
            [] => 0,
            // The correction in one scan of the last pattern, over the values `n` holds.
            [(n, Kind::Optional)] => {
                let f = |t: &BaseRDFNodeType, k: u32, nv: u64| wv(t, k).saturating_mul(nv - 1);
                edge_scan(le, le.s == root, &lchild, Some(n), false, Some(&f))?.1 as i128
            }
            [(n, _)] => {
                let f = |t: &BaseRDFNodeType, k: u32, _: u64| wv(t, k);
                -(edge_scan(le, le.s == root, &lchild, Some(n), false, Some(&f))?.1 as i128)
            }
            _ => {
                let mut support = Plain::new();
                for (n, _) in &ns {
                    support = plain_add(support, plain_map(n, |_, _, _| Ok(1))?)?;
                }
                let (lmsg, _) = edge_scan(le, le.s == root, &lchild, Some(&support), true, None)?;
                let mut corr: i128 = 0;
                for (t, m) in &lmsg {
                    if m.is_total() {
                        return Err(Stop::Fallback);
                    }
                    for (k, e) in m.nonzero() {
                        let wk = wv(t, k);
                        if wk == 0 {
                            continue;
                        }
                        let mut g = 1u64;
                        for (n, kind) in &ns {
                            g = g.saturating_mul(kind.apply(plain_get(n, t, Some(k))?));
                        }
                        corr += e as i128 * wk as i128 * (g as i128 - 1);
                    }
                }
                corr
            }
        };
        let n = total as i128 + corr;
        if n < 0 || n >= u64::MAX as i128 {
            return Err(Stop::Fallback);
        }
        Ok(n as u64)
    }

    /// The weights of the other end of the pattern `e` at `v`. When the pattern is much smaller
    /// than the other patterns there, only the values it holds are weighed.
    fn child_weights(&self, v: &str, e: usize) -> R<Weights> {
        let edge = &self.term.edges[e];
        let u = edge.other(v);
        let smallest = self.edges_at[u]
            .iter()
            .filter(|x| **x != e)
            .map(|x| self.term.edges[*x].height())
            .min();
        let pre = match smallest {
            Some(h) if edge.height().saturating_mul(2) < h => {
                let (p, _) = edge_scan(edge, edge.s == u, &Weights::default(), None, true, None)?;
                (!p.values().any(|m| m.is_total())).then_some(p)
            }
            _ => None,
        };
        self.weights(u, Some(e), pre.as_ref())
    }

    /// The weights of the values of `v`, below its pattern `parent`. Only the weights of the
    /// values that `filter` holds are needed.
    fn weights(&self, v: &str, parent: Option<usize>, filter: Option<&Plain>) -> R<Weights> {
        let mut edges: Vec<usize> = self
            .edges_at
            .get(v)
            .map(|es| es.iter().copied().filter(|e| Some(*e) != parent).collect())
            .unwrap_or_default();
        edges.sort_by_key(|e| self.term.edges[*e].height());
        let mut atts: Vec<usize> = self.atts_at.get(v).cloned().unwrap_or_default();
        atts.sort_by_key(|a| self.term.atts[*a].kind);
        let mut w = Weights::default();
        for e in edges {
            let edge = &self.term.edges[e];
            let child = self.child_weights(v, e)?;
            let f = w.plain.as_ref().or(filter);
            let (msg, _) = edge_scan(edge, edge.s == v, &child, f, true, None)?;
            w.mul_plain(msg)?;
            if w.is_zero() {
                return Ok(w);
            }
        }
        for a in atts {
            let att = &self.term.atts[a];
            let f = w.plain.as_ref().or(filter);
            let n = subs_rooted(&att.sub, v, f)?;
            w.apply(att.kind, n)?;
            if w.is_zero() {
                return Ok(w);
            }
        }
        Ok(w)
    }
}

/// An estimate of the rows read to weigh an attachment.
fn sub_cost(sub: &[Term]) -> usize {
    sub.iter()
        .map(|t| t.edges.iter().map(|e| e.height()).sum::<usize>())
        .sum()
}

/// The product of weights (None weighs every value 1).
fn plain_mul(a: Option<Plain>, b: Plain) -> R<Plain> {
    if a.is_none() {
        return Ok(b);
    }
    let mut w = Weights { plain: a, mods: vec![] };
    w.mul_plain(b)?;
    Ok(w.plain.unwrap_or_default())
}

fn term_total(term: &Term) -> R<u64> {
    if term.is_empty() {
        return Ok(0);
    }
    let shape = Shape::new(term);
    let mut total = shape.scalar_factor()?;
    let mut seen = HashSet::new();
    let mut vars: Vec<&str> = shape.edges_at.keys().copied().collect();
    vars.sort();
    for v in vars {
        if total == 0 {
            break;
        }
        if seen.contains(v) {
            continue;
        }
        let comp = shape.component(v, &mut seen);
        // The root with the most patterns and attachments, then the smallest pattern.
        let root = comp
            .into_iter()
            .max_by_key(|x| {
                let smallest = shape.edges_at[x]
                    .iter()
                    .map(|e| term.edges[*e].height())
                    .min()
                    .unwrap_or(0);
                (shape.n_factors(x), usize::MAX - smallest, *x)
            })
            .unwrap();
        total = total.saturating_mul(shape.component_total(root)?);
    }
    Ok(total)
}

/// The number of solutions of the term by the value of its variable `k`, for the values in
/// `filter`.
fn term_rooted(term: &Term, k: &str, filter: Option<&Plain>) -> R<Plain> {
    if term.is_empty() {
        return Ok(Plain::new());
    }
    let shape = Shape::new(term);
    let mut factor = shape.scalar_factor()?;
    let mut seen = HashSet::new();
    let Some((&k, _)) = shape.edges_at.get_key_value(k) else {
        return Err(Stop::Fallback);
    };
    shape.component(k, &mut seen);
    let mut vars: Vec<&str> = shape.edges_at.keys().copied().collect();
    vars.sort();
    for v in vars {
        if factor == 0 {
            return Ok(Plain::new());
        }
        if !seen.contains(v) {
            let comp = shape.component(v, &mut seen);
            let root = comp
                .into_iter()
                .max_by_key(|x| (shape.n_factors(x), *x))
                .unwrap();
            factor = factor.saturating_mul(shape.component_total(root)?);
        }
    }
    if factor == 0 {
        return Ok(Plain::new());
    }
    let w = shape.weights(k, None, filter)?;
    let p = w.plain.unwrap_or_default();
    Ok(if factor == 1 {
        p
    } else {
        plain_map(&p, |_, _, v| Ok(v.saturating_mul(factor)))?
    })
}

fn subs_total(sub: &[Term]) -> R<u64> {
    let mut n = 0u64;
    for t in sub {
        n = n.saturating_add(term_total(t)?);
    }
    Ok(n)
}

fn subs_rooted(sub: &[Term], k: &str, filter: Option<&Plain>) -> R<Plain> {
    let mut out = Plain::new();
    for t in sub {
        out = plain_add(out, term_rooted(t, k, filter)?)?;
    }
    Ok(out)
}

fn is_coded(t: &BaseRDFNodeType) -> bool {
    t.stored_cat() && !t.is_lang_string()
}

/// The stored codes of a column, without copying a column of one chunk.
fn codes(c: &Column) -> R<Cow<'_, [u32]>> {
    let ca = c.u32().map_err(|_| Stop::Fallback)?;
    if ca.null_count() > 0 {
        return Err(Stop::Fallback);
    }
    if ca.chunks().len() == 1 {
        let a = ca.downcast_iter().next().unwrap();
        return Ok(Cow::Borrowed(a.values().as_slice()));
    }
    let mut out = Vec::with_capacity(ca.len());
    for a in ca.downcast_iter() {
        out.extend_from_slice(a.values());
    }
    Ok(Cow::Owned(out))
}

/// Accumulates weights by code.
enum Acc {
    Dense { min: u32, vals: Vec<u64> },
    Sparse(CodeMap),
}

impl Acc {
    #[inline]
    fn add(&mut self, k: u32, w: u64) {
        match self {
            Acc::Dense { min, vals } => {
                let e = &mut vals[(k - *min) as usize];
                *e = e.saturating_add(w);
            }
            Acc::Sparse(m) => {
                let e = m.entry(k).or_insert(0);
                *e = e.saturating_add(w);
            }
        }
    }

    fn finish(self) -> Option<KeyMap> {
        match self {
            Acc::Dense { min, vals } => vals
                .iter()
                .any(|v| *v > 0)
                .then_some(KeyMap::Dense { min, vals }),
            Acc::Sparse(m) => (!m.is_empty()).then(|| KeyMap::from_map(m)),
        }
    }
}

/// Sums the weights of the `child` ends of the rows of a pattern, by the code of the end at the
/// variable (the subject when `at_subject`), when `keyed`, and in total. Only the codes `filter`
/// holds are counted, and with `weigh`, the total multiplies each row by `weigh` of its type,
/// code and filter weight.
fn edge_scan(
    e: &Edge,
    at_subject: bool,
    child: &Weights,
    filter: Option<&Plain>,
    keyed: bool,
    weigh: Option<&dyn Fn(&BaseRDFNodeType, u32, u64) -> u64>,
) -> R<(Plain, u64)> {
    let (pc, cc) = if at_subject {
        (SUBJECT_COL_NAME, OBJECT_COL_NAME)
    } else {
        (OBJECT_COL_NAME, SUBJECT_COL_NAME)
    };
    let mut total = 0u64;
    // The codes of each table, by the type at the variable.
    // The stored columns of each table at the variable and at the child, by the type at the
    // variable.
    let mut by_type: HashMap<&BaseRDFNodeType, Vec<(Column, Option<Column>, Lookup<'_>)>> =
        HashMap::new();
    // The total weights by the types at the variable that are not dictionary coded.
    let mut uncoded: HashMap<&BaseRDFNodeType, u64> = HashMap::new();
    for t in &e.tables {
        let tp = Edge::ty(t, at_subject);
        let tc = Edge::ty(t, !at_subject);
        let fmap = match filter {
            Some(f) => match f.get(tp) {
                Some(m) => Some(m),
                None => continue,
            },
            None => None,
        };
        if fmap.is_some_and(|m| m.is_total()) {
            return Err(Stop::Fallback);
        }
        let lookup = child.lookup(tc)?;
        if lookup.zero {
            continue;
        }
        let one = lookup.is_one();
        if !one && !is_coded(tc) {
            return Err(Stop::Fallback);
        }
        if keyed && !is_coded(tp) {
            // Only the total weight of the values of the type is kept.
            let mut w = 0u64;
            if one {
                w = t.height() as u64;
            } else {
                for c in t.stored_columns(cc)? {
                    for k in codes(&c)?.iter() {
                        w = w.saturating_add(lookup.value(*k));
                    }
                }
            }
            total = total.saturating_add(w);
            let e = uncoded.entry(tp).or_insert(0);
            *e = e.saturating_add(w);
            continue;
        }
        if !keyed && one && fmap.is_none() {
            total = total.saturating_add(t.height() as u64);
            continue;
        }
        let need_parent = keyed || fmap.is_some();
        // The stored columns of the segments and the write buffer, in the same row order.
        let pcols = if need_parent { t.stored_columns(pc)? } else { vec![] };
        let ccols = if one { vec![] } else { t.stored_columns(cc)? };
        if !need_parent {
            // Only the child weights are needed.
            for c in &ccols {
                for k in codes(c)?.iter() {
                    total = total.saturating_add(lookup.value(*k));
                }
            }
            continue;
        }
        let mut ccols = ccols.into_iter();
        for p in pcols {
            let c = if one { None } else { ccols.next() };
            by_type.entry(tp).or_default().push((p, c, child.lookup(tc)?));
        }
    }
    let mut out: Plain = uncoded
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(t, w)| (t.clone(), KeyMap::Total(w)))
        .collect();
    for (tp, parts) in by_type {
        let fmap = filter.and_then(|f| f.get(tp));
        let mut acc = if !keyed {
            None
        } else if let Some(KeyMap::Dense { min, vals }) = fmap {
            Some(Acc::Dense {
                min: *min,
                vals: vec![0; vals.len()],
            })
        } else if let Some(m) = fmap {
            Some(Acc::Sparse(CodeMap::with_capacity_and_hasher(
                m.size(),
                Default::default(),
            )))
        } else {
            let rows: usize = parts.iter().map(|(p, _, _)| p.len()).sum();
            let (mut min, mut max) = (u32::MAX, 0);
            for (p, _, _) in &parts {
                for k in codes(p)?.iter() {
                    min = min.min(*k);
                    max = max.max(*k);
                }
            }
            if rows == 0 {
                continue;
            }
            let range = (max - min) as usize + 1;
            Some(if dense_enough(range, rows) {
                Acc::Dense {
                    min,
                    vals: vec![0; range],
                }
            } else {
                Acc::Sparse(CodeMap::with_capacity_and_hasher(rows / 2, Default::default()))
            })
        };
        for (p, c, lookup) in &parts {
            let p = codes(p)?;
            let c = match c {
                Some(c) => Some(codes(c)?),
                None => None,
            };
            for (i, k) in p.iter().enumerate() {
                let fw = match fmap {
                    Some(f) => match f.get(*k) {
                        0 => continue,
                        v => v,
                    },
                    None => 1,
                };
                let w = match &c {
                    None => 1,
                    Some(c) => lookup.value(c[i]),
                };
                if w == 0 {
                    continue;
                }
                total = total.saturating_add(match weigh {
                    Some(f) => w.saturating_mul(f(tp, *k, fw)),
                    None => w,
                });
                if let Some(acc) = &mut acc {
                    // A dense accumulator from the filter covers the codes the filter holds.
                    acc.add(*k, w);
                }
            }
        }
        if let Some(m) = acc.and_then(Acc::finish) {
            out.insert(tp.clone(), m);
        }
    }
    Ok((out, total))
}

fn term_name(t: &TermPattern) -> R<String> {
    match t {
        TermPattern::Variable(v) => Ok(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Ok(format!("_:{}", b.as_str())),
        _ => Err(Stop::Fallback),
    }
}

fn normalize<'a>(gp: &GraphPattern, preds: Option<&'a PredicateTriples>) -> R<Vec<Term<'a>>> {
    let terms = match gp {
        GraphPattern::Bgp { patterns } => {
            let mut t = Term::default();
            for tp in patterns {
                let NamedNodePattern::NamedNode(p) = &tp.predicate else {
                    return Err(Stop::Fallback);
                };
                let s = term_name(&tp.subject)?;
                let o = term_name(&tp.object)?;
                if s == o {
                    return Err(Stop::Fallback);
                }
                let tables = preds
                    .and_then(|m| m.get(p))
                    .map(|m| m.values().filter(|t| t.height() > 0).collect())
                    .unwrap_or_default();
                t.edges.push(Edge { s, o, tables });
            }
            vec![t]
        }
        GraphPattern::Join { left, right } => {
            let l = normalize(left, preds)?;
            let r = normalize(right, preds)?;
            if l.len() * r.len() > MAX_TERMS {
                return Err(Stop::Fallback);
            }
            l.iter()
                .flat_map(|a| r.iter().map(move |b| a.merge(b)))
                .collect()
        }
        GraphPattern::Union { left, right } => {
            let mut l = normalize(left, preds)?;
            l.extend(normalize(right, preds)?);
            if l.len() > MAX_TERMS {
                return Err(Stop::Fallback);
            }
            l
        }
        GraphPattern::Filter { expr, inner } => {
            let (kind, p) = match expr {
                Expression::Exists(p) => (Kind::Exists, p),
                Expression::Not(e) => match e.as_ref() {
                    Expression::Exists(p) => (Kind::NotExists, p),
                    _ => return Err(Stop::Fallback),
                },
                _ => return Err(Stop::Fallback),
            };
            let sub = normalize(p, preds)?;
            let mut terms = normalize(inner, preds)?;
            for t in &mut terms {
                t.attach(kind, sub.clone(), false)?;
            }
            terms
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression: None,
        } => {
            let sub = normalize(right, preds)?;
            let mut terms = normalize(left, preds)?;
            for t in &mut terms {
                t.attach(Kind::Optional, sub.clone(), false)?;
            }
            terms
        }
        GraphPattern::Minus { left, right } => {
            let sub = normalize(right, preds)?;
            let mut terms = normalize(left, preds)?;
            for t in &mut terms {
                t.attach(Kind::NotExists, sub.clone(), true)?;
            }
            terms
        }
        _ => return Err(Stop::Fallback),
    };
    Ok(terms)
}

impl Triplestore {
    /// Answers the aggregates if they are all COUNT(*) over a join shape that the stored codes
    /// answer directly (see the module comment). Returns None for anything else, which then
    /// takes the general path.
    pub(crate) fn count_joins_from_codes(
        &self,
        inner: &GraphPattern,
        aggregates: &[(Variable, AggregateExpression)],
        dataset: &QueryGraph,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let QueryGraph::NamedGraph(graph) = dataset else {
            return Ok(None);
        };
        if aggregates.is_empty() {
            return Ok(None);
        }
        let preds = self.graph_triples_map.get(graph);
        let mut terms = match normalize(inner, preds) {
            Ok(t) => t,
            Err(Stop::Fallback) => return Ok(None),
            Err(Stop::Error(e)) => return Err(e),
        };
        for (_, a) in aggregates {
            match a {
                AggregateExpression::CountSolutions { distinct: false } => {}
                // A variable of the patterns of every term is bound in every solution.
                AggregateExpression::FunctionCall {
                    name: AggregateFunction::Count,
                    expr: Expression::Variable(v),
                    distinct: false,
                } if terms.iter().all(|t| t.main_vars().contains(v.as_str())) => {}
                _ => return Ok(None),
            }
        }
        let mut n = 0u64;
        for t in &mut terms {
            if t.validate().is_err() {
                return Ok(None);
            }
            t.prune();
        }
        for t in &terms {
            match term_total(t) {
                Ok(c) => n = n.saturating_add(c),
                Err(Stop::Fallback) => return Ok(None),
                Err(Stop::Error(e)) => return Err(e),
            }
        }
        if n >= i64::MAX as u64 {
            return Ok(None);
        }
        let mut columns = vec![];
        let mut types = HashMap::new();
        for (v, _) in aggregates {
            columns.push(Column::new(v.as_str().into(), [n as i64]));
            types.insert(
                v.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                    .into_default_input_rdf_node_state(),
            );
        }
        fast_path::hit(FastPath::CountJoinsOnCodes);
        Ok(Some(SolutionMappings::new(
            DataFrame::new(1, columns).unwrap().lazy(),
            types,
            1,
        )))
    }
}
