use std::collections::{BTreeSet, HashMap, HashSet};

/// The classes ordered so that every superclass comes before its subclasses, and for each class
/// the parents that its template should instantiate.
pub struct Ordering {
    pub ordering: Vec<String>,
    pub parents: HashMap<String, Vec<String>>,
    /// The classes placed to break subclass cycles, empty if the hierarchy is acyclic.
    pub broken_cycle_members: Vec<String>,
}

/// Order classes so that every superclass comes before its subclasses.
///
/// `edges` are (child, parent) pairs from rdfs:subClassOf. Self-loops are ignored. Cycles are
/// broken deterministically, since classes in a subclass cycle are equivalent and would otherwise
/// instantiate each other forever. Redundant parents (those that are also ancestors of another
/// parent) are pruned, so that the triples of a superclass are produced only once.
pub fn topological_sort<'a>(
    classes: impl IntoIterator<Item = &'a String>,
    edges: impl IntoIterator<Item = &'a (String, String)>,
) -> Ordering {
    let mut nodes: BTreeSet<&str> = classes.into_iter().map(|c| c.as_str()).collect();
    let mut parents: HashMap<&str, HashSet<&str>> = HashMap::new();
    for (child, parent) in edges {
        nodes.insert(child);
        nodes.insert(parent);
        if child == parent {
            continue;
        }
        parents.entry(child).or_default().insert(parent);
    }

    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for (child, ps) in &parents {
        for p in ps {
            children.entry(p).or_default().push(child);
        }
    }

    let mut remaining_in_degree: HashMap<&str, usize> = nodes
        .iter()
        .map(|n| (*n, parents.get(n).map(|p| p.len()).unwrap_or(0)))
        .collect();
    let mut placed: HashSet<&str> = HashSet::new();
    let mut ordering: Vec<&str> = Vec::with_capacity(nodes.len());
    let mut ready: BTreeSet<&str> = nodes
        .iter()
        .filter(|n| remaining_in_degree[*n] == 0)
        .copied()
        .collect();
    let mut broken_cycle_members = vec![];

    while ordering.len() < nodes.len() {
        if ready.is_empty() {
            // Only cycles remain, break one by placing its smallest member.
            let n = *nodes.iter().find(|x| !placed.contains(*x)).unwrap();
            broken_cycle_members.push(n.to_string());
            ready.insert(n);
        }
        let n = ready.pop_first().unwrap();
        if placed.contains(n) {
            continue;
        }
        placed.insert(n);
        ordering.push(n);
        if let Some(cs) = children.get(n) {
            for c in cs {
                if placed.contains(c) {
                    continue;
                }
                let d = remaining_in_degree.get_mut(c).unwrap();
                *d -= 1;
                if *d == 0 {
                    ready.insert(c);
                }
            }
        }
    }

    let position: HashMap<&str, usize> =
        ordering.iter().enumerate().map(|(i, n)| (*n, i)).collect();
    // Only keep edges to parents that were placed before the child (drops cycle edges).
    let mut direct_parents: HashMap<&str, Vec<&str>> = HashMap::new();
    for n in &ordering {
        let mut ps: Vec<&str> = parents
            .get(n)
            .map(|ps| {
                ps.iter()
                    .filter(|p| position[*p] < position[n])
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        ps.sort_by_key(|p| position[p]);
        direct_parents.insert(n, ps);
    }

    let mut ancestors: HashMap<&str, HashSet<&str>> = HashMap::new();
    for n in &ordering {
        let mut a = HashSet::new();
        for p in &direct_parents[n] {
            a.insert(*p);
            a.extend(ancestors[p].iter().copied());
        }
        ancestors.insert(n, a);
    }

    let mut pruned = HashMap::with_capacity(ordering.len());
    for n in &ordering {
        let ps = &direct_parents[n];
        let kept: Vec<String> = ps
            .iter()
            .filter(|p| !ps.iter().any(|q| q != *p && ancestors[q].contains(*p)))
            .map(|p| p.to_string())
            .collect();
        pruned.insert(n.to_string(), kept);
    }
    Ordering {
        ordering: ordering.into_iter().map(|n| n.to_string()).collect(),
        parents: pruned,
        broken_cycle_members,
    }
}
