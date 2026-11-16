//! Path searches over a graph whose nodes are numbered 0..n: breadth first search (one source,
//! bidirectional between two nodes, or exact-length layers), Dijkstra with node weights, all
//! shortest paths and Yen's k shortest simple paths. The graph is stored as compressed sparse
//! rows in both directions, and every edge carries a label: the index of the predicate it came
//! from, times two, plus one if the edge is followed against the direction of its triple.
//!
//! The searches use dense scratch arrays of size n that are cleared in O(1) with an epoch
//! stamp, so a search costs only what it visits. Many sources are searched in parallel, one
//! scratch per worker.

use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

pub const NONE: u32 = u32::MAX;

/// Which way the edges are followed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Forward,
    Backward,
    // Only the tests build it so far; the search code already handles it.
    #[cfg_attr(not(test), allow(dead_code))]
    Both,
}

impl Direction {
    pub fn reversed(self) -> Direction {
        match self {
            Direction::Forward => Direction::Backward,
            Direction::Backward => Direction::Forward,
            Direction::Both => Direction::Both,
        }
    }
}

/// Edges as compressed sparse rows: the edges from u are `targets[offsets[u]..offsets[u + 1]]`.
#[derive(Debug, Default)]
pub struct Csr {
    offsets: Vec<usize>,
    targets: Vec<u32>,
    labels: Vec<u32>,
}

impl Csr {
    /// From edges (from, to, label), with n nodes.
    pub fn from_edges(n: usize, from: &[u32], to: &[u32], labels: &[u32]) -> Csr {
        let mut offsets = vec![0usize; n + 1];
        for f in from {
            offsets[*f as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut next = offsets[..n].to_vec();
        let mut targets = vec![0u32; from.len()];
        let mut out_labels = vec![0u32; from.len()];
        for i in 0..from.len() {
            let f = from[i] as usize;
            targets[next[f]] = to[i];
            out_labels[next[f]] = labels[i];
            next[f] += 1;
        }
        Csr {
            offsets,
            targets,
            labels: out_labels,
        }
    }

    #[inline]
    fn row(&self, u: u32) -> (&[u32], &[u32]) {
        let u = u as usize;
        if u + 1 < self.offsets.len() {
            let (a, b) = (self.offsets[u], self.offsets[u + 1]);
            (&self.targets[a..b], &self.labels[a..b])
        } else {
            (&[], &[])
        }
    }

    pub fn n_edges(&self) -> usize {
        self.targets.len()
    }
}

/// A graph over the nodes 0..n with edges in both directions.
#[derive(Debug, Default)]
pub struct SearchGraph {
    pub n: usize,
    /// Edges u -> v of triples u p v, labelled 2p.
    fwd: Csr,
    /// Edges v -> u of triples u p v, labelled 2p + 1.
    bwd: Csr,
}

impl SearchGraph {
    /// From the edges of the triples `from[i] p[i] to[i]`, where p is the predicate index.
    #[cfg(test)]
    pub fn new(n: usize, from: &[u32], to: &[u32], predicates: &[u32]) -> SearchGraph {
        let fwd_labels: Vec<u32> = predicates.iter().map(|p| 2 * p).collect();
        let bwd_labels: Vec<u32> = predicates.iter().map(|p| 2 * p + 1).collect();
        let (fwd, bwd) = rayon::join(
            || Csr::from_edges(n, from, to, &fwd_labels),
            || Csr::from_edges(n, to, from, &bwd_labels),
        );
        SearchGraph { n, fwd, bwd }
    }

    /// From labelled edges `from[i] -> to[i]`, followed forward. The reverse edges get the label
    /// with its last bit flipped, so an edge 2p along a triple is 2p + 1 backwards and vice versa.
    pub fn from_labelled(n: usize, from: &[u32], to: &[u32], labels: &[u32]) -> SearchGraph {
        let back: Vec<u32> = labels.iter().map(|l| l ^ 1).collect();
        let (fwd, bwd) = rayon::join(
            || Csr::from_edges(n, from, to, labels),
            || Csr::from_edges(n, to, from, &back),
        );
        SearchGraph { n, fwd, bwd }
    }

    pub fn n_edges(&self) -> usize {
        self.fwd.n_edges()
    }

    /// Calls `f(v, label)` for the edges from u. The label says how the edge is followed from u
    /// to v: 2p along a triple u p v, 2p + 1 against a triple v p u.
    #[inline]
    pub fn for_each_neighbour(&self, dir: Direction, u: u32, mut f: impl FnMut(u32, u32)) {
        if matches!(dir, Direction::Forward | Direction::Both) {
            let (t, l) = self.fwd.row(u);
            for i in 0..t.len() {
                f(t[i], l[i]);
            }
        }
        if matches!(dir, Direction::Backward | Direction::Both) {
            let (t, l) = self.bwd.row(u);
            for i in 0..t.len() {
                f(t[i], l[i]);
            }
        }
    }
}

/// The targets of a search from a source.
#[derive(Debug, Clone)]
pub enum Targets {
    /// Every node reached is a target.
    All,
    /// Only these nodes are targets.
    Set(Arc<Vec<u32>>),
}

#[derive(Debug, Clone)]
pub struct SearchParams {
    pub direction: Direction,
    /// Paths shorter than this (in edges) are not returned.
    pub min_length: u32,
    pub max_length: Option<u32>,
    /// Node weights: the cost of entering a node. With weights, the paths of least cost are
    /// searched for (Dijkstra), otherwise those with the fewest edges.
    pub weights: Option<Arc<Vec<f64>>>,
    pub max_cost: Option<f64>,
    /// Paths do not pass through these nodes, but may start or end at them.
    pub avoid: Option<Arc<Vec<bool>>>,
    /// The number of nearest targets per source.
    pub limit: Option<usize>,
    /// The number of shortest paths per source and target (Yen's algorithm).
    pub k: usize,
    /// All shortest paths per source and target, up to `max_paths`.
    pub all: bool,
    pub max_paths: usize,
}

impl Default for SearchParams {
    fn default() -> Self {
        SearchParams {
            direction: Direction::Forward,
            min_length: 0,
            max_length: None,
            weights: None,
            max_cost: None,
            avoid: None,
            limit: None,
            k: 1,
            all: false,
            max_paths: 1000,
        }
    }
}

impl SearchParams {
    #[inline]
    fn avoided(&self, u: u32) -> bool {
        match &self.avoid {
            Some(a) => a.get(u as usize).copied().unwrap_or(false),
            None => false,
        }
    }

    #[inline]
    fn weight(&self, u: u32) -> f64 {
        match &self.weights {
            Some(w) => w[u as usize],
            None => 1.0,
        }
    }
}

/// A path found: `nodes` from the source to the target, and `labels[i]` the label of the edge
/// from `nodes[i]` to `nodes[i + 1]`.
#[derive(Debug, Clone, PartialEq)]
pub struct FoundPath {
    pub source: u32,
    pub target: u32,
    /// The rank of the path among the paths from the source to the target, from 0.
    pub path_id: u32,
    pub cost: f64,
    pub nodes: Vec<u32>,
    pub labels: Vec<u32>,
}

impl FoundPath {
    pub fn length(&self) -> u32 {
        self.labels.len() as u32
    }
}

/// Dense per-node arrays reused between searches.
pub struct Scratch {
    epoch: u32,
    seen: Vec<u32>,
    depth: Vec<u32>,
    parent: Vec<u32>,
    parent_label: Vec<u32>,
    target_mark: Vec<u32>,
    cost: Vec<f64>,
    // The other side of a bidirectional search.
    seen_b: Vec<u32>,
    depth_b: Vec<u32>,
    parent_b: Vec<u32>,
    parent_label_b: Vec<u32>,
    /// Further parents of nodes with several shortest paths (all shortest paths).
    extra_parents: HashMap<u32, Vec<(u32, u32)>>,
}

impl Scratch {
    pub fn new(n: usize) -> Scratch {
        Scratch {
            epoch: 0,
            seen: vec![0; n],
            depth: vec![0; n],
            parent: vec![NONE; n],
            parent_label: vec![0; n],
            target_mark: vec![0; n],
            cost: vec![],
            seen_b: vec![],
            depth_b: vec![],
            parent_b: vec![],
            parent_label_b: vec![],
            extra_parents: HashMap::new(),
        }
    }

    fn next_epoch(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            // Wrapped around: clear the stamps once.
            self.seen.iter_mut().for_each(|x| *x = 0);
            self.target_mark.iter_mut().for_each(|x| *x = 0);
            self.seen_b.iter_mut().for_each(|x| *x = 0);
            self.epoch = 1;
        }
        self.extra_parents.clear();
    }

    #[inline]
    fn is_target(&self, u: u32) -> bool {
        self.target_mark[u as usize] == self.epoch
    }

    fn ensure_cost(&mut self) {
        if self.cost.len() != self.seen.len() {
            self.cost = vec![0.0; self.seen.len()];
        }
    }

    fn ensure_backward(&mut self) {
        let n = self.seen.len();
        if self.seen_b.len() != n {
            self.seen_b = vec![0; n];
            self.depth_b = vec![0; n];
            self.parent_b = vec![NONE; n];
            self.parent_label_b = vec![0; n];
        }
    }

    /// The path to `t` along the parents, from the root of the search.
    fn path_to(&self, t: u32) -> (Vec<u32>, Vec<u32>) {
        let mut nodes = vec![t];
        let mut labels = vec![];
        let mut u = t;
        while self.parent[u as usize] != NONE {
            labels.push(self.parent_label[u as usize]);
            u = self.parent[u as usize];
            nodes.push(u);
        }
        nodes.reverse();
        labels.reverse();
        (nodes, labels)
    }

    /// All paths to `t` along all parents (the first parent and the extra ones), at most `max`.
    fn all_paths_to(&self, t: u32, max: usize) -> Vec<(Vec<u32>, Vec<u32>)> {
        let mut out = vec![];
        // Depth first from t back to the root, keeping the nodes and labels from t.
        let mut nodes = vec![t];
        let mut labels: Vec<u32> = vec![];
        self.all_paths_rec(t, &mut nodes, &mut labels, &mut out, max);
        out
    }

    fn all_paths_rec(
        &self,
        u: u32,
        nodes: &mut Vec<u32>,
        labels: &mut Vec<u32>,
        out: &mut Vec<(Vec<u32>, Vec<u32>)>,
        max: usize,
    ) {
        if out.len() >= max {
            return;
        }
        let p = self.parent[u as usize];
        if p == NONE {
            let mut n = nodes.clone();
            let mut l = labels.clone();
            n.reverse();
            l.reverse();
            out.push((n, l));
            return;
        }
        let mut parents = vec![(p, self.parent_label[u as usize])];
        if let Some(extra) = self.extra_parents.get(&u) {
            parents.extend_from_slice(extra);
        }
        for (p, l) in parents {
            nodes.push(p);
            labels.push(l);
            self.all_paths_rec(p, nodes, labels, out, max);
            nodes.pop();
            labels.pop();
            if out.len() >= max {
                return;
            }
        }
    }
}

/// Restrictions on the edges and nodes of a search, used for the spur searches of Yen's
/// algorithm.
#[derive(Default)]
struct Bans {
    nodes: HashSet<u32>,
    edges: HashSet<(u32, u32, u32)>,
}

impl Bans {
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }
}

/// The shortest paths from `source` to its targets, as described by `params`.
pub fn search(
    graph: &SearchGraph,
    source: u32,
    targets: &Targets,
    params: &SearchParams,
    scratch: &mut Scratch,
) -> Vec<FoundPath> {
    if params.weights.is_none() {
        if let Some(c) = params.max_cost {
            // Without weights the cost of a path is its length.
            let mut p = params.clone();
            p.max_cost = None;
            let m = if c < 0.0 {
                0
            } else {
                c.floor().min(u32::MAX as f64) as u32
            };
            p.max_length = Some(p.max_length.map_or(m, |l| l.min(m)));
            if c < 0.0 {
                return vec![];
            }
            return search(graph, source, targets, &p, scratch);
        }
    }
    if params.k > 1 && !params.all {
        // The k shortest paths to each of the nearest targets.
        let mut single = params.clone();
        single.k = 1;
        let first = search(graph, source, targets, &single, scratch);
        let mut out = vec![];
        for p in first {
            out.extend(yen(graph, p, params, scratch));
        }
        return out;
    }
    if let Targets::Set(set) = targets {
        if set.len() == 1
            && !(set[0] == source && params.min_length > 0)
            && params.weights.is_none()
            && !params.all
            && params.limit.is_none_or(|l| l >= 1)
        {
            return bidirectional(graph, source, set[0], params, scratch, &Bans::default())
                .into_iter()
                .collect();
        }
    }
    if params.weights.is_some() {
        dijkstra(graph, source, targets, params, scratch, &Bans::default())
    } else {
        bfs(graph, source, targets, params, scratch, &Bans::default())
    }
}

fn mark_targets(targets: &Targets, scratch: &mut Scratch) -> Option<usize> {
    match targets {
        Targets::All => None,
        Targets::Set(set) => {
            let mut n = 0;
            for t in set.iter() {
                let t = *t as usize;
                if t < scratch.target_mark.len() && scratch.target_mark[t] != scratch.epoch {
                    scratch.target_mark[t] = scratch.epoch;
                    n += 1;
                }
            }
            Some(n)
        }
    }
}

fn collect_found(
    source: u32,
    found: &[u32],
    params: &SearchParams,
    scratch: &Scratch,
    cost_of: impl Fn(u32, &[u32]) -> f64,
) -> Vec<FoundPath> {
    let mut out = Vec::with_capacity(found.len());
    for t in found {
        if params.all {
            for (i, (nodes, labels)) in scratch
                .all_paths_to(*t, params.max_paths)
                .into_iter()
                .enumerate()
            {
                out.push(FoundPath {
                    source,
                    target: *t,
                    path_id: i as u32,
                    cost: cost_of(*t, &nodes),
                    nodes,
                    labels,
                });
            }
        } else {
            let (nodes, labels) = scratch.path_to(*t);
            out.push(FoundPath {
                source,
                target: *t,
                path_id: 0,
                cost: cost_of(*t, &nodes),
                nodes,
                labels,
            });
        }
    }
    out
}

/// Breadth first search from `source`, one layer at a time, until all targets (or `limit` of
/// them) are found or the maximum length is reached.
fn bfs(
    graph: &SearchGraph,
    source: u32,
    targets: &Targets,
    params: &SearchParams,
    scratch: &mut Scratch,
    bans: &Bans,
) -> Vec<FoundPath> {
    if source as usize >= graph.n || bans.nodes.contains(&source) {
        return vec![];
    }
    scratch.next_epoch();
    let n_targets = mark_targets(targets, scratch);
    let is_target = |s: &Scratch, u: u32| n_targets.is_none() || s.is_target(u);
    let wanted = match (n_targets, params.limit) {
        (Some(n), Some(l)) => Some(n.min(l)),
        (Some(n), None) => Some(n),
        (None, l) => l,
    };
    let mut found = vec![];
    let e = scratch.epoch;
    scratch.seen[source as usize] = e;
    scratch.depth[source as usize] = 0;
    scratch.parent[source as usize] = NONE;
    if params.min_length == 0 && is_target(scratch, source) {
        found.push(source);
    }
    // With a minimum length, the source is a target only by a cycle back to it: (last node,
    // label, length) of the shortest one.
    let want_cycle = params.min_length > 0 && is_target(scratch, source);
    let mut cycle: Option<(u32, u32, u32)> = None;
    let mut frontier = vec![source];
    let mut next = vec![];
    let mut depth = 0u32;
    while !frontier.is_empty() {
        if wanted.is_some_and(|w| found.len() + cycle.is_some() as usize >= w) {
            break;
        }
        // The query timed out or was cancelled: the caller discards the result.
        if utils::cancel::stopped().is_some() {
            break;
        }
        if params.max_length.is_some_and(|m| depth >= m) {
            break;
        }
        for &u in &frontier {
            if u != source && params.avoided(u) {
                continue;
            }
            graph.for_each_neighbour(params.direction, u, |v, l| {
                if !bans.is_empty() && (bans.nodes.contains(&v) || bans.edges.contains(&(u, v, l)))
                {
                    return;
                }
                if v == source {
                    if want_cycle && cycle.is_none() && depth + 1 >= params.min_length {
                        cycle = Some((u, l, depth + 1));
                    }
                    return;
                }
                let vi = v as usize;
                if scratch.seen[vi] != e {
                    scratch.seen[vi] = e;
                    scratch.depth[vi] = depth + 1;
                    scratch.parent[vi] = u;
                    scratch.parent_label[vi] = l;
                    next.push(v);
                } else if params.all && scratch.depth[vi] == depth + 1 && v != source {
                    scratch.extra_parents.entry(v).or_default().push((u, l));
                }
            });
        }
        depth += 1;
        if depth >= params.min_length {
            for &v in &next {
                if is_target(scratch, v) {
                    found.push(v);
                }
            }
        }
        std::mem::swap(&mut frontier, &mut next);
        next.clear();
    }
    let mut out = collect_found(source, &found, params, scratch, |t, _| {
        scratch.depth[t as usize] as f64
    });
    if let Some((u, l, len)) = cycle {
        let (mut nodes, mut labels) = scratch.path_to(u);
        nodes.push(source);
        labels.push(l);
        let p = FoundPath {
            source,
            target: source,
            path_id: 0,
            cost: len as f64,
            nodes,
            labels,
        };
        // in order of length among the other targets
        let at = out
            .iter()
            .position(|f| f.length() > len)
            .unwrap_or(out.len());
        out.insert(at, p);
    }
    if let Some(w) = wanted {
        // keep whole groups of paths per target (all shortest paths)
        let mut targets = HashSet::new();
        out.retain(|f| {
            targets.insert(f.target);
            targets.len() <= w
        });
    }
    out
}

#[derive(Copy, Clone, PartialEq)]
struct HeapItem(f64, u32);

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // A min-heap on the cost, then the node.
        other
            .0
            .partial_cmp(&self.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.1.cmp(&self.1))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra's algorithm from `source`, where entering a node costs its weight.
fn dijkstra(
    graph: &SearchGraph,
    source: u32,
    targets: &Targets,
    params: &SearchParams,
    scratch: &mut Scratch,
    bans: &Bans,
) -> Vec<FoundPath> {
    if source as usize >= graph.n || bans.nodes.contains(&source) {
        return vec![];
    }
    scratch.ensure_cost();
    scratch.next_epoch();
    let n_targets = mark_targets(targets, scratch);
    let wanted = match (n_targets, params.limit) {
        (Some(n), Some(l)) => Some(n.min(l)),
        (Some(n), None) => Some(n),
        (None, l) => l,
    };
    let e = scratch.epoch;
    // seen == e: reached; target_mark is reused below only for marking targets, so settled
    // nodes are tracked by depth's top bit.
    const SETTLED: u32 = 1 << 31;
    let mut found = vec![];
    let mut heap = BinaryHeap::new();
    scratch.seen[source as usize] = e;
    scratch.cost[source as usize] = 0.0;
    scratch.depth[source as usize] = 0;
    scratch.parent[source as usize] = NONE;
    heap.push(HeapItem(0.0, source));
    let mut pops = 0u32;
    while let Some(HeapItem(c, u)) = heap.pop() {
        pops = pops.wrapping_add(1);
        if pops.is_multiple_of(4096) && utils::cancel::stopped().is_some() {
            break;
        }
        let ui = u as usize;
        if scratch.depth[ui] & SETTLED != 0 || c > scratch.cost[ui] {
            continue;
        }
        if params.max_cost.is_some_and(|m| c > m) {
            break;
        }
        scratch.depth[ui] |= SETTLED;
        let hops = scratch.depth[ui] & !SETTLED;
        if hops >= params.min_length && (n_targets.is_none() || scratch.is_target(u)) {
            found.push(u);
            if wanted.is_some_and(|w| found.len() >= w) {
                break;
            }
        }
        if u != source && params.avoided(u) {
            continue;
        }
        if params.max_length.is_some_and(|m| hops >= m) {
            continue;
        }
        graph.for_each_neighbour(params.direction, u, |v, l| {
            if !bans.is_empty() && (bans.nodes.contains(&v) || bans.edges.contains(&(u, v, l))) {
                return;
            }
            let vi = v as usize;
            let w = params.weight(v).max(0.0);
            let nc = c + w;
            if scratch.seen[vi] != e {
                scratch.seen[vi] = e;
                scratch.cost[vi] = nc;
                scratch.depth[vi] = hops + 1;
                scratch.parent[vi] = u;
                scratch.parent_label[vi] = l;
                heap.push(HeapItem(nc, v));
            } else if scratch.depth[vi] & SETTLED == 0 {
                if nc < scratch.cost[vi] {
                    scratch.cost[vi] = nc;
                    scratch.depth[vi] = hops + 1;
                    scratch.parent[vi] = u;
                    scratch.parent_label[vi] = l;
                    if params.all {
                        scratch.extra_parents.remove(&v);
                    }
                    heap.push(HeapItem(nc, v));
                } else if params.all && nc == scratch.cost[vi] && v != source {
                    scratch.extra_parents.entry(v).or_default().push((u, l));
                }
            }
        });
    }
    // All shortest paths to a target have the same cost.
    collect_found(source, &found, params, scratch, |t, _| {
        scratch.cost[t as usize]
    })
}

/// Breadth first search from both ends at once, expanding the smaller frontier one layer at a
/// time, for one source and one target.
fn bidirectional(
    graph: &SearchGraph,
    source: u32,
    target: u32,
    params: &SearchParams,
    scratch: &mut Scratch,
    bans: &Bans,
) -> Option<FoundPath> {
    let n = graph.n as u32;
    if source >= n || target >= n || bans.nodes.contains(&source) || bans.nodes.contains(&target) {
        return None;
    }
    if source == target {
        return if params.min_length == 0 {
            Some(FoundPath {
                source,
                target,
                path_id: 0,
                cost: 0.0,
                nodes: vec![source],
                labels: vec![],
            })
        } else {
            None
        };
    }
    scratch.ensure_backward();
    scratch.next_epoch();
    let e = scratch.epoch;
    scratch.seen[source as usize] = e;
    scratch.depth[source as usize] = 0;
    scratch.parent[source as usize] = NONE;
    scratch.seen_b[target as usize] = e;
    scratch.depth_b[target as usize] = 0;
    scratch.parent_b[target as usize] = NONE;
    let mut ff = vec![source];
    let mut fb = vec![target];
    let (mut df, mut db) = (0u32, 0u32);
    let max = params.max_length.unwrap_or(u32::MAX);
    let back_dir = params.direction.reversed();
    let mut next = vec![];
    // The best meeting point: (length, node on the forward side, node on the backward side,
    // label of the edge between them in path order).
    let mut best: Option<(u32, u32, u32, u32)> = None;
    while !ff.is_empty() && !fb.is_empty() && df + db < max {
        if utils::cancel::stopped().is_some() {
            break;
        }
        let forward = ff.len() <= fb.len();
        if forward {
            for &u in &ff {
                if u != source && params.avoided(u) {
                    continue;
                }
                graph.for_each_neighbour(params.direction, u, |v, l| {
                    if !bans.is_empty()
                        && (bans.nodes.contains(&v) || bans.edges.contains(&(u, v, l)))
                    {
                        return;
                    }
                    let vi = v as usize;
                    if scratch.seen_b[vi] == e && (v == target || !params.avoided(v)) {
                        let len = df + 1 + scratch.depth_b[vi];
                        if best.is_none_or(|b| len < b.0) {
                            best = Some((len, u, v, l));
                        }
                    }
                    if scratch.seen[vi] != e {
                        scratch.seen[vi] = e;
                        scratch.depth[vi] = df + 1;
                        scratch.parent[vi] = u;
                        scratch.parent_label[vi] = l;
                        next.push(v);
                    }
                });
            }
            df += 1;
            std::mem::swap(&mut ff, &mut next);
        } else {
            for &v in &fb {
                if v != target && params.avoided(v) {
                    continue;
                }
                graph.for_each_neighbour(back_dir, v, |u, l| {
                    // The edge is followed from u to v in path order, the other way round.
                    let pl = l ^ 1;
                    if !bans.is_empty()
                        && (bans.nodes.contains(&u) || bans.edges.contains(&(u, v, pl)))
                    {
                        return;
                    }
                    let ui = u as usize;
                    if scratch.seen[ui] == e && (u == source || !params.avoided(u)) {
                        let len = scratch.depth[ui] + 1 + db;
                        if best.is_none_or(|b| len < b.0) {
                            best = Some((len, u, v, pl));
                        }
                    }
                    if scratch.seen_b[ui] != e {
                        scratch.seen_b[ui] = e;
                        scratch.depth_b[ui] = db + 1;
                        scratch.parent_b[ui] = v;
                        scratch.parent_label_b[ui] = pl;
                        next.push(u);
                    }
                });
            }
            db += 1;
            std::mem::swap(&mut fb, &mut next);
        }
        next.clear();
        if best.is_some() {
            break;
        }
    }
    let (len, u, v, l) = best?;
    if len > max || len < params.min_length {
        return None;
    }
    let (mut nodes, mut labels) = scratch.path_to(u);
    labels.push(l);
    let mut w = v;
    nodes.push(w);
    while scratch.parent_b[w as usize] != NONE {
        labels.push(scratch.parent_label_b[w as usize]);
        w = scratch.parent_b[w as usize];
        nodes.push(w);
    }
    Some(FoundPath {
        source,
        target,
        path_id: 0,
        cost: len as f64,
        nodes,
        labels,
    })
}

/// The shortest path from `source` to `target` avoiding `bans`.
fn restricted_shortest(
    graph: &SearchGraph,
    source: u32,
    target: u32,
    params: &SearchParams,
    scratch: &mut Scratch,
    bans: &Bans,
) -> Option<FoundPath> {
    let mut p = params.clone();
    p.k = 1;
    p.all = false;
    p.limit = None;
    p.min_length = 0;
    if params.weights.is_some() {
        let targets = Targets::Set(Arc::new(vec![target]));
        dijkstra(graph, source, &targets, &p, scratch, bans).pop()
    } else {
        bidirectional(graph, source, target, &p, scratch, bans)
    }
}

fn path_cost(params: &SearchParams, nodes: &[u32]) -> f64 {
    if params.weights.is_some() {
        nodes[1..].iter().map(|u| params.weight(*u).max(0.0)).sum()
    } else {
        (nodes.len() - 1) as f64
    }
}

#[derive(PartialEq)]
struct Candidate(f64, Vec<u32>, Vec<u32>);

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .partial_cmp(&self.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.1.len().cmp(&self.1.len()))
            .then_with(|| other.1.cmp(&self.1))
            .then_with(|| other.2.cmp(&self.2))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Yen's algorithm: the `params.k` shortest simple paths from the source to the target of
/// `first`, which is a shortest path.
fn yen(
    graph: &SearchGraph,
    first: FoundPath,
    params: &SearchParams,
    scratch: &mut Scratch,
) -> Vec<FoundPath> {
    let (source, target) = (first.source, first.target);
    let max_len = params.max_length.unwrap_or(u32::MAX);
    let mut a: Vec<FoundPath> = vec![first];
    let mut b: BinaryHeap<Candidate> = BinaryHeap::new();
    let mut in_b: HashSet<(Vec<u32>, Vec<u32>)> = HashSet::new();
    if source == target {
        return a;
    }
    while a.len() < params.k {
        if utils::cancel::stopped().is_some() {
            break;
        }
        let prev = a.last().unwrap().clone();
        for i in 0..prev.nodes.len() - 1 {
            let spur = prev.nodes[i];
            let root_nodes = &prev.nodes[..=i];
            let root_labels = &prev.labels[..i];
            if i as u32 >= max_len {
                break;
            }
            let mut bans = Bans::default();
            for p in &a {
                if p.nodes.len() > i + 1
                    && &p.nodes[..=i] == root_nodes
                    && &p.labels[..i] == root_labels
                {
                    bans.edges.insert((p.nodes[i], p.nodes[i + 1], p.labels[i]));
                }
            }
            if i > 0 && params.avoided(spur) {
                // Paths do not pass through avoided nodes.
                continue;
            }
            for n in &root_nodes[..i] {
                bans.nodes.insert(*n);
            }
            let mut sp = params.clone();
            sp.max_length = Some(max_len - i as u32);
            if let Some(spur_path) = restricted_shortest(graph, spur, target, &sp, scratch, &bans) {
                let mut nodes = root_nodes[..i].to_vec();
                nodes.extend_from_slice(&spur_path.nodes);
                let mut labels = root_labels.to_vec();
                labels.extend_from_slice(&spur_path.labels);
                if (labels.len() as u32) < params.min_length {
                    continue;
                }
                let key = (nodes.clone(), labels.clone());
                if !in_b.contains(&key) && !a.iter().any(|p| p.nodes == nodes && p.labels == labels)
                {
                    in_b.insert(key);
                    b.push(Candidate(path_cost(params, &nodes), nodes, labels));
                }
            }
        }
        let Some(Candidate(cost, nodes, labels)) = b.pop() else {
            break;
        };
        in_b.remove(&(nodes.clone(), labels.clone()));
        if params.max_cost.is_some_and(|m| cost > m) {
            break;
        }
        a.push(FoundPath {
            source,
            target,
            path_id: a.len() as u32,
            cost,
            nodes,
            labels,
        });
    }
    a
}

/// A search job: a source and its targets.
pub struct Job {
    pub source: u32,
    pub targets: Targets,
}

/// Runs the searches in parallel, with one scratch per worker. The number of workers is
/// limited so that the scratch arrays fit in `scratch_budget_bytes`.
pub fn search_many(
    graph: &SearchGraph,
    jobs: &[Job],
    params: &SearchParams,
    scratch_budget_bytes: usize,
) -> Vec<FoundPath> {
    if jobs.is_empty() {
        return vec![];
    }
    let per_scratch = (graph.n.max(1)) * 48;
    let max_workers = (scratch_budget_bytes / per_scratch).max(1);
    let workers = rayon::current_num_threads()
        .min(max_workers)
        .min(jobs.len())
        .max(1);
    let chunk = jobs.len().div_ceil(workers);
    // The workers check the token of the query, which is current on this thread only.
    let token = utils::cancel::current();
    let parts: Vec<Vec<FoundPath>> = jobs
        .par_chunks(chunk)
        .map(|chunk| {
            utils::cancel::with_token(token.clone(), || {
                let mut scratch = Scratch::new(graph.n);
                let mut out = vec![];
                for job in chunk {
                    if utils::cancel::stopped().is_some() {
                        break;
                    }
                    out.extend(search(
                        graph,
                        job.source,
                        &job.targets,
                        params,
                        &mut scratch,
                    ));
                }
                out
            })
        })
        .collect();
    parts.into_iter().flatten().collect()
}

/// The nodes reached from `source` by walks of exactly d edges, for d = 1..=max_depth, passed
/// to `visit(d, nodes)` one layer at a time. A node can be in several layers. `visit` returns
/// false to stop. For length-constrained path predicates.
#[cfg(test)]
pub fn exact_length_layers(
    graph: &SearchGraph,
    source: u32,
    direction: Direction,
    max_depth: u32,
    scratch: &mut Scratch,
    mut visit: impl FnMut(u32, &[u32]) -> bool,
) {
    if source as usize >= graph.n {
        return;
    }
    let mut frontier = vec![source];
    let mut next = vec![];
    for d in 1..=max_depth {
        if utils::cancel::stopped().is_some() {
            return;
        }
        scratch.next_epoch();
        let e = scratch.epoch;
        for &u in &frontier {
            graph.for_each_neighbour(direction, u, |v, _| {
                if scratch.seen[v as usize] != e {
                    scratch.seen[v as usize] = e;
                    next.push(v);
                }
            });
        }
        if next.is_empty() || !visit(d, &next) {
            return;
        }
        std::mem::swap(&mut frontier, &mut next);
        next.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A small deterministic random number generator.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u32 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) as u32
        }
    }

    fn random_graph(seed: u64, n: u32, m: usize, n_preds: u32) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
        let mut r = Lcg(seed);
        let mut edges = HashSet::new();
        while edges.len() < m {
            edges.insert((r.next() % n, r.next() % n, r.next() % n_preds));
        }
        let mut edges: Vec<_> = edges.into_iter().collect();
        edges.sort();
        let from = edges.iter().map(|e| e.0).collect();
        let to = edges.iter().map(|e| e.1).collect();
        let p = edges.iter().map(|e| e.2).collect();
        (from, to, p)
    }

    /// Adjacency as (v, label) lists, by direction, for the reference implementations.
    fn naive_adj(
        n: u32,
        from: &[u32],
        to: &[u32],
        p: &[u32],
        dir: Direction,
    ) -> Vec<Vec<(u32, u32)>> {
        let mut adj = vec![vec![]; n as usize];
        for i in 0..from.len() {
            if matches!(dir, Direction::Forward | Direction::Both) {
                adj[from[i] as usize].push((to[i], 2 * p[i]));
            }
            if matches!(dir, Direction::Backward | Direction::Both) {
                adj[to[i] as usize].push((from[i], 2 * p[i] + 1));
            }
        }
        adj
    }

    fn naive_bfs(adj: &[Vec<(u32, u32)>], s: u32, avoid: &[bool]) -> Vec<Option<u32>> {
        let mut d = vec![None; adj.len()];
        d[s as usize] = Some(0);
        let mut q = VecDeque::from([s]);
        while let Some(u) = q.pop_front() {
            if u != s && avoid[u as usize] {
                continue;
            }
            for (v, _) in &adj[u as usize] {
                if d[*v as usize].is_none() {
                    d[*v as usize] = Some(d[u as usize].unwrap() + 1);
                    q.push_back(*v);
                }
            }
        }
        d
    }

    fn naive_dijkstra(adj: &[Vec<(u32, u32)>], s: u32, w: &[f64]) -> Vec<Option<f64>> {
        let n = adj.len();
        let mut d: Vec<Option<f64>> = vec![None; n];
        let mut done = vec![false; n];
        d[s as usize] = Some(0.0);
        loop {
            let mut best = None;
            for u in 0..n {
                if !done[u] {
                    if let Some(c) = d[u] {
                        if best.is_none_or(|(_, bc)| c < bc) {
                            best = Some((u, c));
                        }
                    }
                }
            }
            let Some((u, c)) = best else { break };
            done[u] = true;
            for (v, _) in &adj[u] {
                let nc = c + w[*v as usize];
                if d[*v as usize].is_none_or(|x| nc < x) {
                    d[*v as usize] = Some(nc);
                }
            }
        }
        d
    }

    /// All simple paths from s to t of at most max_len edges, by depth first search.
    fn naive_simple_paths(
        adj: &[Vec<(u32, u32)>],
        s: u32,
        t: u32,
        max_len: usize,
    ) -> Vec<(Vec<u32>, Vec<u32>)> {
        fn rec(
            adj: &[Vec<(u32, u32)>],
            u: u32,
            t: u32,
            max_len: usize,
            nodes: &mut Vec<u32>,
            labels: &mut Vec<u32>,
            out: &mut Vec<(Vec<u32>, Vec<u32>)>,
        ) {
            if u == t {
                out.push((nodes.clone(), labels.clone()));
                return;
            }
            if labels.len() >= max_len {
                return;
            }
            for (v, l) in &adj[u as usize] {
                if !nodes.contains(v) {
                    nodes.push(*v);
                    labels.push(*l);
                    rec(adj, *v, t, max_len, nodes, labels, out);
                    nodes.pop();
                    labels.pop();
                }
            }
        }
        let mut out = vec![];
        rec(adj, s, t, max_len, &mut vec![s], &mut vec![], &mut out);
        out
    }

    fn check_path(adj: &[Vec<(u32, u32)>], p: &FoundPath) {
        assert_eq!(p.nodes[0], p.source);
        assert_eq!(*p.nodes.last().unwrap(), p.target);
        assert_eq!(p.nodes.len(), p.labels.len() + 1);
        for i in 0..p.labels.len() {
            assert!(
                adj[p.nodes[i] as usize].contains(&(p.nodes[i + 1], p.labels[i])),
                "edge {} -> {} with label {} is not in the graph",
                p.nodes[i],
                p.nodes[i + 1],
                p.labels[i]
            );
        }
    }

    const DIRS: [Direction; 3] = [Direction::Forward, Direction::Backward, Direction::Both];

    #[test]
    fn bfs_matches_reference_on_random_graphs() {
        for seed in 0..30u64 {
            let n = 40 + (seed as u32 % 5) * 10;
            let (from, to, p) = random_graph(seed, n, (n as usize) * 2, 3);
            let g = SearchGraph::new(n as usize, &from, &to, &p);
            let mut scratch = Scratch::new(n as usize);
            let mut r = Lcg(seed + 1000);
            let avoid: Vec<bool> = (0..n).map(|_| r.next().is_multiple_of(7)).collect();
            for dir in DIRS {
                for use_avoid in [false, true] {
                    let adj = naive_adj(n, &from, &to, &p, dir);
                    let no_avoid = vec![false; n as usize];
                    let av = if use_avoid { &avoid } else { &no_avoid };
                    for s in [0, 3, 17] {
                        let reference = naive_bfs(&adj, s, av);
                        let params = SearchParams {
                            direction: dir,
                            avoid: Some(Arc::new(av.clone())),
                            ..Default::default()
                        };
                        // To all nodes.
                        let found = search(&g, s, &Targets::All, &params, &mut scratch);
                        let mut got = vec![None; n as usize];
                        for f in &found {
                            check_path(&adj, f);
                            got[f.target as usize] = Some(f.length());
                        }
                        assert_eq!(got, reference, "seed {seed} dir {dir:?} source {s}");
                        // To each node on its own (bidirectional search).
                        for t in 0..n {
                            let one = Targets::Set(Arc::new(vec![t]));
                            let found = search(&g, s, &one, &params, &mut scratch);
                            assert_eq!(
                                found.first().map(|f| f.length()),
                                reference[t as usize],
                                "bidirectional seed {seed} dir {dir:?} {s} -> {t}"
                            );
                            for f in &found {
                                check_path(&adj, f);
                            }
                        }
                        // The nearest 3 of a target set, with a maximum length.
                        let set: Vec<u32> = (0..n).filter(|x| x % 4 == 1).collect();
                        let limited = SearchParams {
                            limit: Some(3),
                            max_length: Some(4),
                            ..params.clone()
                        };
                        let found = search(
                            &g,
                            s,
                            &Targets::Set(Arc::new(set.clone())),
                            &limited,
                            &mut scratch,
                        );
                        let mut expected: Vec<u32> = set
                            .iter()
                            .filter_map(|t| reference[*t as usize])
                            .filter(|d| *d <= 4)
                            .collect();
                        expected.sort();
                        expected.truncate(3);
                        let mut got: Vec<u32> = found.iter().map(|f| f.length()).collect();
                        got.sort();
                        assert_eq!(got, expected, "limit seed {seed} dir {dir:?} source {s}");
                    }
                }
            }
        }
    }

    #[test]
    fn dijkstra_matches_reference() {
        for seed in 0..20u64 {
            let n = 50;
            let (from, to, p) = random_graph(seed, n, 150, 2);
            let g = SearchGraph::new(n as usize, &from, &to, &p);
            let mut r = Lcg(seed + 7);
            let w: Vec<f64> = (0..n).map(|_| (r.next() % 10) as f64 + 0.5).collect();
            let mut scratch = Scratch::new(n as usize);
            for dir in DIRS {
                let adj = naive_adj(n, &from, &to, &p, dir);
                let reference = naive_dijkstra(&adj, 0, &w);
                let params = SearchParams {
                    direction: dir,
                    weights: Some(Arc::new(w.clone())),
                    ..Default::default()
                };
                let found = search(&g, 0, &Targets::All, &params, &mut scratch);
                let mut got = vec![None; n as usize];
                for f in &found {
                    check_path(&adj, f);
                    let c: f64 = f.nodes[1..].iter().map(|u| w[*u as usize]).sum();
                    assert!((c - f.cost).abs() < 1e-9);
                    got[f.target as usize] = Some(f.cost);
                }
                assert_eq!(got, reference, "seed {seed} dir {dir:?}");
            }
        }
    }

    #[test]
    fn all_shortest_and_k_shortest_match_reference() {
        for seed in 0..25u64 {
            let n = 14;
            let (from, to, p) = random_graph(seed, n, 30, 2);
            let g = SearchGraph::new(n as usize, &from, &to, &p);
            let mut scratch = Scratch::new(n as usize);
            for dir in DIRS {
                let adj = naive_adj(n, &from, &to, &p, dir);
                for t in 1..n {
                    let mut simple = naive_simple_paths(&adj, 0, t, 7);
                    simple.sort_by_key(|(n, _)| n.len());
                    let targets = Targets::Set(Arc::new(vec![t]));
                    // All shortest paths.
                    let params = SearchParams {
                        direction: dir,
                        all: true,
                        max_length: Some(7),
                        ..Default::default()
                    };
                    let found = search(&g, 0, &targets, &params, &mut scratch);
                    let shortest = simple.first().map(|(n, _)| n.len());
                    let mut expected: Vec<_> = simple
                        .iter()
                        .filter(|(n, _)| Some(n.len()) == shortest)
                        .cloned()
                        .collect();
                    expected.sort();
                    let mut got: Vec<_> = found
                        .iter()
                        .map(|f| (f.nodes.clone(), f.labels.clone()))
                        .collect();
                    got.sort();
                    assert_eq!(got, expected, "all shortest seed {seed} {dir:?} 0 -> {t}");
                    // The 4 shortest simple paths: the lengths must match.
                    let params = SearchParams {
                        direction: dir,
                        k: 4,
                        max_length: Some(7),
                        ..Default::default()
                    };
                    let found = search(&g, 0, &targets, &params, &mut scratch);
                    let expected: Vec<usize> =
                        simple.iter().take(4).map(|(n, _)| n.len() - 1).collect();
                    let got: Vec<usize> = found.iter().map(|f| f.length() as usize).collect();
                    assert_eq!(got, expected, "k shortest seed {seed} {dir:?} 0 -> {t}");
                    let mut distinct = HashSet::new();
                    for f in &found {
                        check_path(&adj, f);
                        let mut nodes = f.nodes.clone();
                        nodes.sort();
                        nodes.dedup();
                        assert_eq!(nodes.len(), f.nodes.len(), "not simple");
                        assert!(distinct.insert((f.nodes.clone(), f.labels.clone())));
                    }
                }
            }
        }
    }

    #[test]
    fn edge_cases() {
        // 0 -> 1 -> 2 -> 0 (a cycle), 1 -> 2 twice with different predicates, 3 alone.
        let from = [0, 1, 1, 2];
        let to = [1, 2, 2, 0];
        let p = [0, 0, 1, 0];
        let g = SearchGraph::new(4, &from, &to, &p);
        let mut s = Scratch::new(4);
        let params = SearchParams::default();
        let one = |t: u32| Targets::Set(Arc::new(vec![t]));
        // Source = target.
        let f = search(&g, 0, &one(0), &params, &mut s);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].nodes, vec![0]);
        // No path.
        assert!(search(&g, 0, &one(3), &params, &mut s).is_empty());
        // Out of range.
        assert!(search(&g, 0, &one(9), &params, &mut s).is_empty());
        // Parallel edges give two shortest paths.
        let all = SearchParams {
            all: true,
            ..Default::default()
        };
        let f = search(&g, 0, &one(2), &all, &mut s);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].labels, vec![0, 0]);
        assert_eq!(f[1].labels, vec![0, 2]);
        // Minimum length: 0 -> 0 needs the cycle.
        let min = SearchParams {
            min_length: 1,
            ..Default::default()
        };
        let f = search(&g, 0, &Targets::All, &min, &mut s);
        let mut t: Vec<_> = f.iter().map(|f| (f.target, f.length())).collect();
        t.sort();
        assert_eq!(t, vec![(0, 3), (1, 1), (2, 2)]);
        let f = search(&g, 0, &one(0), &min, &mut s);
        assert_eq!(f[0].nodes, vec![0, 1, 2, 0]);
        // Maximum length.
        let max = SearchParams {
            max_length: Some(1),
            ..Default::default()
        };
        assert!(search(&g, 0, &one(2), &max, &mut s).is_empty());
        // Exact length layers around the cycle.
        let mut layers = vec![];
        exact_length_layers(&g, 0, Direction::Forward, 4, &mut s, |d, nodes| {
            layers.push((d, nodes.to_vec()));
            true
        });
        assert_eq!(
            layers,
            vec![(1, vec![1]), (2, vec![2]), (3, vec![0]), (4, vec![1])]
        );
    }
}
