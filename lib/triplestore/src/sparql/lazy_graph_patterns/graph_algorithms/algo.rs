//! Graph algorithms over nodes 0..n and an edge list: connected components (union-find),
//! strongly connected components (iterative Tarjan), and bridges and articulation points
//! (iterative depth-first search with low links). Removed nodes take no part: they and their
//! edges are left out. Everything is iterative, so deep graphs (long lines, chains) don't
//! overflow the stack.

pub const NONE: u32 = u32::MAX;

/// Edges as compressed sparse rows: the neighbours of u are `targets[offsets[u]..offsets[u + 1]]`,
/// each with the index of its edge.
pub struct Adjacency {
    offsets: Vec<usize>,
    targets: Vec<u32>,
    edges: Vec<u32>,
}

impl Adjacency {
    /// Edge i goes from `from[i]` to `to[i]`; undirected edges are listed both ways.
    pub fn new(n: usize, from: &[u32], to: &[u32], undirected: bool) -> Adjacency {
        let mut offsets = vec![0usize; n + 1];
        for (&f, &t) in from.iter().zip(to) {
            offsets[f as usize + 1] += 1;
            if undirected {
                offsets[t as usize + 1] += 1;
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let total = offsets[n];
        let mut next = offsets[..n].to_vec();
        let mut targets = vec![0u32; total];
        let mut edges = vec![0u32; total];
        for (i, (&f, &t)) in from.iter().zip(to).enumerate() {
            let (f, t) = (f as usize, t as usize);
            targets[next[f]] = t as u32;
            edges[next[f]] = i as u32;
            next[f] += 1;
            if undirected {
                targets[next[t]] = f as u32;
                edges[next[t]] = i as u32;
                next[t] += 1;
            }
        }
        Adjacency {
            offsets,
            targets,
            edges,
        }
    }

    #[inline]
    fn range(&self, u: u32) -> (usize, usize) {
        (self.offsets[u as usize], self.offsets[u as usize + 1])
    }
}

/// Components numbered 0.. by decreasing size, ties broken by the smallest node: `of[u]` is the
/// component of node u (NONE for removed nodes) and `sizes[c]` the number of nodes of c.
#[derive(Debug, Clone, PartialEq)]
pub struct Components {
    pub of: Vec<u32>,
    pub sizes: Vec<u64>,
}

impl Components {
    /// From arbitrary labels per node (NONE for removed nodes).
    fn from_labels(labels: Vec<u32>) -> Components {
        // the size and smallest node of each label
        let mut size: Vec<u64> = vec![0; labels.len()];
        let mut first: Vec<u32> = vec![NONE; labels.len()];
        for (u, &l) in labels.iter().enumerate() {
            if l != NONE {
                size[l as usize] += 1;
                if first[l as usize] == NONE {
                    first[l as usize] = u as u32;
                }
            }
        }
        let mut used: Vec<u32> = (0..labels.len() as u32)
            .filter(|&l| size[l as usize] > 0)
            .collect();
        used.sort_unstable_by(|&a, &b| {
            size[b as usize]
                .cmp(&size[a as usize])
                .then(first[a as usize].cmp(&first[b as usize]))
        });
        let mut rank = vec![NONE; labels.len()];
        for (r, &l) in used.iter().enumerate() {
            rank[l as usize] = r as u32;
        }
        let sizes = used.iter().map(|&l| size[l as usize]).collect();
        let of = labels
            .into_iter()
            .map(|l| if l == NONE { NONE } else { rank[l as usize] })
            .collect();
        Components { of, sizes }
    }
}

impl Components {
    /// Renumbers the components by decreasing size, ties broken by the smallest `order` of their
    /// nodes instead of the smallest node, so that the numbering does not depend on how the nodes
    /// happen to be numbered (e.g. two builds of the same graph).
    pub fn renumbered(self, order: &[u64]) -> Components {
        let k = self.sizes.len();
        let mut least = vec![u64::MAX; k];
        for (u, &c) in self.of.iter().enumerate() {
            if c != NONE {
                least[c as usize] = least[c as usize].min(order[u]);
            }
        }
        let mut ids: Vec<usize> = (0..k).collect();
        ids.sort_unstable_by(|&a, &b| {
            self.sizes[b]
                .cmp(&self.sizes[a])
                .then(least[a].cmp(&least[b]))
        });
        let mut rank = vec![0u32; k];
        for (r, &c) in ids.iter().enumerate() {
            rank[c] = r as u32;
        }
        Components {
            sizes: ids.iter().map(|&c| self.sizes[c]).collect(),
            of: self
                .of
                .into_iter()
                .map(|c| if c == NONE { NONE } else { rank[c as usize] })
                .collect(),
        }
    }
}

fn is_removed(removed: Option<&[bool]>, u: usize) -> bool {
    removed.is_some_and(|r| r[u])
}

/// The connected components of the undirected graph.
pub fn connected_components(
    n: usize,
    from: &[u32],
    to: &[u32],
    removed: Option<&[bool]>,
) -> Components {
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut rank: Vec<u8> = vec![0; n];
    fn find(parent: &mut [u32], mut u: u32) -> u32 {
        while parent[u as usize] != u {
            let p = parent[u as usize];
            parent[u as usize] = parent[p as usize];
            u = p;
        }
        u
    }
    for (&a, &b) in from.iter().zip(to) {
        if is_removed(removed, a as usize) || is_removed(removed, b as usize) {
            continue;
        }
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra == rb {
            continue;
        }
        let (ra, rb) = if rank[ra as usize] < rank[rb as usize] {
            (rb, ra)
        } else {
            (ra, rb)
        };
        parent[rb as usize] = ra;
        if rank[ra as usize] == rank[rb as usize] {
            rank[ra as usize] += 1;
        }
    }
    let labels = (0..n)
        .map(|u| {
            if is_removed(removed, u) {
                NONE
            } else {
                find(&mut parent, u as u32)
            }
        })
        .collect();
    Components::from_labels(labels)
}

/// The strongly connected components of the directed graph (edges from -> to).
pub fn strongly_connected_components(
    n: usize,
    from: &[u32],
    to: &[u32],
    removed: Option<&[bool]>,
) -> Components {
    let adj = Adjacency::new(n, from, to, false);
    let mut index = vec![NONE; n];
    let mut low = vec![0u32; n];
    let mut on_stack = vec![false; n];
    let mut labels = vec![NONE; n];
    let mut stack: Vec<u32> = vec![];
    // the call stack: (node, position in its adjacency)
    let mut calls: Vec<(u32, usize)> = vec![];
    let mut next_index = 0u32;
    let mut next_label = 0u32;
    for root in 0..n as u32 {
        if index[root as usize] != NONE || is_removed(removed, root as usize) {
            continue;
        }
        index[root as usize] = next_index;
        low[root as usize] = next_index;
        next_index += 1;
        stack.push(root);
        on_stack[root as usize] = true;
        calls.push((root, adj.range(root).0));
        while let Some(&mut (u, ref mut pos)) = calls.last_mut() {
            let end = adj.range(u).1;
            if *pos < end {
                let v = adj.targets[*pos];
                *pos += 1;
                if is_removed(removed, v as usize) {
                    continue;
                }
                if index[v as usize] == NONE {
                    index[v as usize] = next_index;
                    low[v as usize] = next_index;
                    next_index += 1;
                    stack.push(v);
                    on_stack[v as usize] = true;
                    calls.push((v, adj.range(v).0));
                } else if on_stack[v as usize] {
                    low[u as usize] = low[u as usize].min(index[v as usize]);
                }
            } else {
                calls.pop();
                if let Some(&(p, _)) = calls.last() {
                    low[p as usize] = low[p as usize].min(low[u as usize]);
                }
                if low[u as usize] == index[u as usize] {
                    loop {
                        let w = stack.pop().unwrap();
                        on_stack[w as usize] = false;
                        labels[w as usize] = next_label;
                        if w == u {
                            break;
                        }
                    }
                    next_label += 1;
                }
            }
        }
    }
    Components::from_labels(labels)
}

/// The bridges and articulation points of the undirected multigraph, and its components.
#[derive(Debug, Clone, PartialEq)]
pub struct Cuts {
    pub components: Components,
    /// (edge index, the number of nodes cut off): the nodes on the smaller side.
    pub bridges: Vec<(u32, u64)>,
    /// (node, the number of nodes cut off): the nodes that are no longer connected to the largest
    /// remaining part of the component when the node is removed.
    pub articulation_points: Vec<(u32, u64)>,
}

/// Bridges and articulation points. Parallel edges are not bridges, and self loops are ignored.
/// With `weights`, a node counts as its weight in the numbers of nodes cut off (e.g. 1 for the
/// nodes of one kind, 0 for the others).
pub fn cuts(
    n: usize,
    from: &[u32],
    to: &[u32],
    removed: Option<&[bool]>,
    weights: Option<&[u64]>,
) -> Cuts {
    let w = |u: usize| weights.map_or(1, |w| w[u]);
    let adj = Adjacency::new(n, from, to, true);
    let mut disc = vec![NONE; n];
    let mut low = vec![0u32; n];
    let mut sub = vec![0u64; n];
    let mut parent_edge = vec![NONE; n];
    let mut tree_of = vec![NONE; n];
    // per node: the pieces it separates (child subtrees with low >= its discovery time)
    let mut piece_count = vec![0u32; n];
    let mut piece_sum = vec![0u64; n];
    let mut piece_max = vec![0u64; n];
    // bridges by the child node below them
    let mut bridge_children: Vec<u32> = vec![];
    let mut roots: Vec<u32> = vec![];
    let mut calls: Vec<(u32, usize)> = vec![];
    let mut time = 0u32;
    for root in 0..n as u32 {
        if disc[root as usize] != NONE || is_removed(removed, root as usize) {
            continue;
        }
        let tree = roots.len() as u32;
        roots.push(root);
        disc[root as usize] = time;
        low[root as usize] = time;
        time += 1;
        sub[root as usize] = w(root as usize);
        tree_of[root as usize] = tree;
        calls.push((root, adj.range(root).0));
        while let Some(&mut (u, ref mut pos)) = calls.last_mut() {
            let end = adj.range(u).1;
            if *pos < end {
                let (v, e) = (adj.targets[*pos], adj.edges[*pos]);
                *pos += 1;
                if v == u || e == parent_edge[u as usize] || is_removed(removed, v as usize) {
                    continue;
                }
                if disc[v as usize] == NONE {
                    disc[v as usize] = time;
                    low[v as usize] = time;
                    time += 1;
                    sub[v as usize] = w(v as usize);
                    parent_edge[v as usize] = e;
                    tree_of[v as usize] = tree;
                    calls.push((v, adj.range(v).0));
                } else {
                    low[u as usize] = low[u as usize].min(disc[v as usize]);
                }
            } else {
                calls.pop();
                if let Some(&(p, _)) = calls.last() {
                    let (pu, uu) = (p as usize, u as usize);
                    low[pu] = low[pu].min(low[uu]);
                    sub[pu] += sub[uu];
                    if low[uu] > disc[pu] {
                        bridge_children.push(u);
                    }
                    if low[uu] >= disc[pu] {
                        piece_count[pu] += 1;
                        piece_sum[pu] += sub[uu];
                        piece_max[pu] = piece_max[pu].max(sub[uu]);
                    }
                }
            }
        }
    }
    let tree_size: Vec<u64> = roots.iter().map(|&r| sub[r as usize]).collect();
    let bridges = bridge_children
        .into_iter()
        .map(|c| {
            let size = tree_size[tree_of[c as usize] as usize];
            let below = sub[c as usize];
            (parent_edge[c as usize], below.min(size - below))
        })
        .collect();
    let mut articulation_points = vec![];
    for u in 0..n {
        if disc[u] == NONE {
            continue;
        }
        let is_root = parent_edge[u] == NONE;
        let separates = if is_root {
            piece_count[u] >= 2
        } else {
            piece_count[u] >= 1
        };
        if separates {
            let size = tree_size[tree_of[u] as usize];
            let rest = size - w(u) - piece_sum[u];
            let largest = piece_max[u].max(rest);
            articulation_points.push((u as u32, size - w(u) - largest));
        }
    }
    let components = Components::from_labels(tree_of);
    Cuts {
        components,
        bridges,
        articulation_points,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components_are_ranked_by_size() {
        // 0-1-2, 3-4, 5 alone
        let c = connected_components(6, &[0, 1, 3], &[1, 2, 4], None);
        assert_eq!(c.of, vec![0, 0, 0, 1, 1, 2]);
        assert_eq!(c.sizes, vec![3, 2, 1]);
    }

    #[test]
    fn removed_nodes_split_components() {
        // 0-1-2-3, remove 1
        let removed = vec![false, true, false, false];
        let c = connected_components(4, &[0, 1, 2], &[1, 2, 3], Some(&removed));
        assert_eq!(c.of, vec![1, NONE, 0, 0]);
        assert_eq!(c.sizes, vec![2, 1]);
    }

    #[test]
    fn strong_components() {
        // 0 -> 1 -> 2 -> 0, 2 -> 3, 3 -> 4 -> 3
        let c = strongly_connected_components(5, &[0, 1, 2, 2, 3, 4], &[1, 2, 0, 3, 4, 3], None);
        assert_eq!(c.of, vec![0, 0, 0, 1, 1]);
        assert_eq!(c.sizes, vec![3, 2]);
    }

    #[test]
    fn bridges_and_articulation_points() {
        // triangle 0-1-2, bridge 2-3, path 3-4, 4-5 parallel twice
        let from = [0, 1, 2, 2, 3, 4, 4];
        let to = [1, 2, 0, 3, 4, 5, 5];
        let c = cuts(6, &from, &to, None, None);
        let mut bridges = c.bridges.clone();
        bridges.sort();
        // edges 3 (2-3) and 4 (3-4); 4-5 is doubled, so not a bridge
        assert_eq!(bridges, vec![(3, 3), (4, 2)]);
        let mut ap = c.articulation_points.clone();
        ap.sort();
        // 2 cuts off {3, 4, 5}... the largest remaining part is {3, 4, 5} (3) vs {0, 1} (2)
        assert_eq!(ap, vec![(2, 2), (3, 2), (4, 1)]);
        assert_eq!(c.components.sizes, vec![6]);
    }

    #[test]
    fn renumbering_follows_the_order_not_the_node_numbers() {
        // two components of 2 nodes: {0, 1} and {2, 3}; by node numbers {0, 1} comes first
        let c = connected_components(4, &[0, 2], &[1, 3], None);
        assert_eq!(c.of, vec![0, 0, 1, 1]);
        // with an order where node 3 comes first, {2, 3} is component 0
        let r = c.renumbered(&[10, 11, 12, 5]);
        assert_eq!(r.of, vec![1, 1, 0, 0]);
        assert_eq!(r.sizes, vec![2, 2]);
    }

    #[test]
    fn weighted_cut_off() {
        // 0 - 1 - 2 - 3 - 4, only the even nodes count
        let weights = [1, 0, 1, 0, 1];
        let c = cuts(5, &[0, 1, 2, 3], &[1, 2, 3, 4], None, Some(&weights));
        let mut ap = c.articulation_points.clone();
        ap.sort();
        // removing 2 leaves {0} and {4}: one of them is cut off
        assert_eq!(ap, vec![(1, 1), (2, 1), (3, 1)]);
        let mut bridges = c.bridges.clone();
        bridges.sort();
        assert_eq!(bridges, vec![(0, 1), (1, 1), (2, 1), (3, 1)]);
    }

    #[test]
    fn deep_chain_does_not_overflow() {
        let n = 1_000_000u32;
        let from: Vec<u32> = (0..n - 1).collect();
        let to: Vec<u32> = (1..n).collect();
        let c = cuts(n as usize, &from, &to, None, None);
        assert_eq!(c.bridges.len(), (n - 1) as usize);
        assert_eq!(c.articulation_points.len(), (n - 2) as usize);
        let s = strongly_connected_components(n as usize, &from, &to, None);
        assert_eq!(s.sizes.len(), n as usize);
    }
}
