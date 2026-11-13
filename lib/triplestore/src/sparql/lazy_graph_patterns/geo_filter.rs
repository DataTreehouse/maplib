//! Helpers for evaluating GeoSPARQL filters with the geo index: splitting a filter into
//! conjuncts, checking where a variable is bound, and splitting a pattern into two independent
//! sides for a spatial join.

use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

/// The conjuncts of a filter expression: `a && (b && c)` gives `[a, b, c]`.
pub(crate) fn conjuncts(expression: &Expression) -> Vec<&Expression> {
    let mut out = vec![];
    let mut stack = vec![expression];
    while let Some(e) = stack.pop() {
        if let Expression::And(l, r) = e {
            stack.push(r);
            stack.push(l);
        } else {
            out.push(e);
        }
    }
    out
}

/// Joins conjuncts back into one expression, `None` if there are none.
pub(crate) fn conjunction(conjuncts: Vec<&Expression>) -> Option<Expression> {
    conjuncts
        .into_iter()
        .cloned()
        .reduce(|l, r| Expression::And(Box::new(l), Box::new(r)))
}

/// Whether every solution of the pattern binds the variable through a triple pattern, so that
/// restricting the variable to some values before evaluating the pattern gives the same result
/// as filtering on it afterwards.
pub(crate) fn binds_variable_in_triple(gp: &GraphPattern, variable: &str) -> bool {
    match gp {
        GraphPattern::Bgp { patterns } => patterns
            .iter()
            .any(|tp| triple_variables(tp).iter().any(|v| v == variable)),
        GraphPattern::Join { left, right } => {
            binds_variable_in_triple(left, variable) || binds_variable_in_triple(right, variable)
        }
        GraphPattern::LeftJoin { left, .. } | GraphPattern::Minus { left, .. } => {
            binds_variable_in_triple(left, variable)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner } => binds_variable_in_triple(inner, variable),
        GraphPattern::Extend {
            inner, variable: v, ..
        } => v.as_str() != variable && binds_variable_in_triple(inner, variable),
        // Projection, ordering, slicing, grouping and the rest change which solutions or
        // variables exist, so restricting before them is not the same as filtering after.
        _ => false,
    }
}

/// The variable names of a triple pattern. Blank nodes act as variables in a pattern, so they
/// are included with a `_:` prefix to connect the patterns they occur in.
fn triple_variables(tp: &TriplePattern) -> Vec<String> {
    let mut out = vec![];
    for t in [&tp.subject, &tp.object] {
        match t {
            TermPattern::Variable(v) => out.push(v.as_str().to_string()),
            TermPattern::BlankNode(b) => out.push(format!("_:{}", b.as_str())),
            _ => {}
        }
    }
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        out.push(v.as_str().to_string());
    }
    out
}

fn pattern_variables(gp: &GraphPattern) -> Vec<String> {
    if let GraphPattern::Bgp { patterns } = gp {
        patterns.iter().flat_map(triple_variables).collect()
    } else {
        let mut out = vec![];
        gp.on_in_scope_variable(|v| out.push(v.as_str().to_string()));
        out
    }
}

/// Splits a basic graph pattern or a join of patterns into two parts that share no variables,
/// one binding `a` and the other binding `b`. Joining the two parts on a spatial relation then
/// replaces evaluating the relation over their cross product.
pub(crate) fn split_for_spatial_join(
    gp: &GraphPattern,
    a: &str,
    b: &str,
) -> Option<(GraphPattern, GraphPattern)> {
    let mut leaves = vec![];
    flatten(gp, &mut leaves)?;
    if leaves.len() < 2 {
        return None;
    }
    let vars: Vec<Vec<String>> = leaves.iter().map(pattern_variables).collect();

    // Union-find over the leaves, connected by shared variables.
    let mut parent: Vec<usize> = (0..leaves.len()).collect();
    fn find(parent: &mut Vec<usize>, i: usize) -> usize {
        let mut r = i;
        while parent[r] != r {
            r = parent[r];
        }
        let mut i = i;
        while parent[i] != r {
            let next = parent[i];
            parent[i] = r;
            i = next;
        }
        r
    }
    let mut first_with_var: HashMap<&str, usize> = HashMap::new();
    for (i, vs) in vars.iter().enumerate() {
        for v in vs {
            if let Some(j) = first_with_var.get(v.as_str()) {
                let (ri, rj) = (find(&mut parent, i), find(&mut parent, *j));
                parent[ri] = rj;
            } else {
                first_with_var.insert(v, i);
            }
        }
    }
    let comp_a = find(&mut parent, *first_with_var.get(a)?);
    let comp_b = find(&mut parent, *first_with_var.get(b)?);
    if comp_a == comp_b {
        return None;
    }
    let mut left = vec![];
    let mut right = vec![];
    for (i, leaf) in leaves.into_iter().enumerate() {
        // Components with neither variable are a cross product anyway, they go on the left.
        if find(&mut parent, i) == comp_b {
            right.push(leaf);
        } else {
            left.push(leaf);
        }
    }
    Some((rebuild(left), rebuild(right)))
}

/// Flattens a tree of joins and basic graph patterns into one pattern per triple pattern and one
/// per other pattern. Returns `None` for patterns that are not a join or basic graph pattern.
fn flatten(gp: &GraphPattern, out: &mut Vec<GraphPattern>) -> Option<()> {
    match gp {
        GraphPattern::Bgp { patterns } => {
            for tp in patterns {
                out.push(GraphPattern::Bgp {
                    patterns: vec![tp.clone()],
                });
            }
            Some(())
        }
        GraphPattern::Join { left, right } => {
            flatten(left, out)?;
            flatten(right, out)
        }
        GraphPattern::Path { .. } | GraphPattern::Values { .. } | GraphPattern::Graph { .. } => {
            out.push(gp.clone());
            Some(())
        }
        _ => None,
    }
}

fn rebuild(leaves: Vec<GraphPattern>) -> GraphPattern {
    let mut triples = vec![];
    let mut others = vec![];
    let mut seen = HashSet::new();
    for leaf in leaves {
        match leaf {
            GraphPattern::Bgp { patterns } => {
                for tp in patterns {
                    if seen.insert(tp.clone()) {
                        triples.push(tp);
                    }
                }
            }
            other => others.push(other),
        }
    }
    let mut gp = if triples.is_empty() {
        None
    } else {
        Some(GraphPattern::Bgp { patterns: triples })
    };
    for o in others {
        gp = Some(match gp {
            None => o,
            Some(gp) => GraphPattern::Join {
                left: Box::new(gp),
                right: Box::new(o),
            },
        });
    }
    gp.unwrap()
}
