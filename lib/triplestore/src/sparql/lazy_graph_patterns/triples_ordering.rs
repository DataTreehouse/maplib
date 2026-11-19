use oxrdf::{BlankNode, Variable};
use query_processing::pushdowns::{PushdownValues, Pushdowns};
use representation::solution_mapping::SolutionMappings;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub fn order_triple_patterns(
    tps: &[TriplePattern],
    sm: &Option<SolutionMappings>,
    pushdowns: &Pushdowns,
) -> Vec<TriplePattern> {
    let mut candidates = tps.to_owned();
    let mut ordering = vec![];
    let mut visited: HashSet<_> = if let Some(sm) = sm {
        sm.rdf_node_types
            .keys()
            .map(|x| x.as_str().to_string())
            .collect()
    } else {
        HashSet::new()
    };
    while !candidates.is_empty() {
        let tp = candidates
            .iter()
            .min_by(|t1, t2| strictly_before(t1, t2, &visited, &pushdowns.variables_values))
            .unwrap();
        let pos = candidates.iter().position(|x| x == tp).unwrap();
        let tp = candidates.remove(pos);
        for v in variables(&tp) {
            visited.insert(v.as_str().to_string());
        }
        for b in blank_nodes(&tp) {
            visited.insert(b.to_string());
        }
        ordering.push(tp);
    }
    ordering
}

// Metaphor here is that quantity is cost to include, so less is better.
fn strictly_before(
    t1: &TriplePattern,
    t2: &TriplePattern,
    visited: &HashSet<String>,
    variable_values: &HashMap<String, PushdownValues>,
) -> Ordering {
    let t1_connected = is_connected(t1, visited);
    let t2_connected = is_connected(t2, visited);
    if t1_connected && !t2_connected {
        return Ordering::Less;
    }
    if !t1_connected && t2_connected {
        return Ordering::Greater;
    }
    if let NamedNodePattern::Variable(v1) = &t1.predicate {
        if let NamedNodePattern::Variable(v2) = &t2.predicate {
            if variable_values.contains_key(v1.as_str())
                && !variable_values.contains_key(v2.as_str())
            {
                return Ordering::Less;
            } else if !variable_values.contains_key(v1.as_str())
                && variable_values.contains_key(v2.as_str())
            {
                return Ordering::Greater;
            }
            if visited.contains(v1.as_str()) && !visited.contains(v2.as_str()) {
                return Ordering::Less;
            }
            if !visited.contains(v1.as_str()) && visited.contains(v2.as_str()) {
                return Ordering::Greater;
            }
            //Todo find the least costly among the two
        } else {
            return Ordering::Greater;
        }
    } else if let NamedNodePattern::Variable(_) = &t2.predicate {
        return Ordering::Less;
    }

    // we rely on Polars to do the rest in the query optimizer.
    Ordering::Equal
}

fn is_connected(tp: &TriplePattern, visited: &HashSet<String>) -> bool {
    let tp_vars = variables(tp);
    for v in &tp_vars {
        if visited.contains(v.as_str()) {
            return true;
        }
    }
    let tp_blanks = blank_nodes(tp);
    for b in tp_blanks {
        if visited.contains(&b.to_string()) {
            return true;
        }
    }
    false
}

fn variables(tp: &TriplePattern) -> Vec<&Variable> {
    let mut vs = vec![];
    if let TermPattern::Variable(v) = &tp.subject {
        vs.push(v);
    }
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        vs.push(v);
    }
    if let TermPattern::Variable(v) = &tp.object {
        vs.push(v);
    }
    vs
}

fn blank_nodes(tp: &TriplePattern) -> Vec<&BlankNode> {
    let mut bs = vec![];
    if let TermPattern::BlankNode(b) = &tp.subject {
        bs.push(b);
    }
    if let TermPattern::BlankNode(b) = &tp.object {
        bs.push(b);
    }
    bs
}

/// Splits ordered triple patterns into groups that are joined into the solution one at a time.
///
/// A pattern that introduces new variables is grouped with the later patterns that bind no other
/// new variables, when those patterns share more of the already bound variables. The group is then
/// joined on all those variables at once. For example, with ?k and ?r bound,
/// `?t ex:kind ?k . ?t ex:region ?r` is joined on (?k, ?r): joining `?t ex:kind ?k` first would
/// join on ?k alone, and the intermediate result could be quadratic in size.
pub fn group_triple_patterns(
    ordered: Vec<TriplePattern>,
    sm: &Option<SolutionMappings>,
) -> Vec<Vec<TriplePattern>> {
    let mut visited: HashSet<String> = if let Some(sm) = sm {
        sm.rdf_node_types.keys().cloned().collect()
    } else {
        HashSet::new()
    };
    let mut remaining: Vec<Option<TriplePattern>> = ordered.into_iter().map(Some).collect();
    let mut groups = vec![];
    for i in 0..remaining.len() {
        let Some(tp) = remaining[i].take() else {
            continue;
        };
        let tp_names = names(&tp);
        let new: HashSet<_> = tp_names
            .iter()
            .filter(|x| !visited.contains(*x))
            .cloned()
            .collect();
        let mut group = vec![tp];
        if !new.is_empty() && tp_names.iter().any(|x| visited.contains(x)) {
            let mut keys: HashSet<_> = tp_names.iter().filter(|x| visited.contains(*x)).collect();
            let keys_before = keys.len();
            let mut members = vec![];
            for (j, other) in remaining.iter().enumerate().skip(i + 1) {
                let Some(other) = other else { continue };
                let other_names = names(other);
                let binds_only_new = other_names
                    .iter()
                    .all(|x| visited.contains(x) || new.contains(x));
                if binds_only_new && other_names.iter().any(|x| new.contains(x)) {
                    members.push(j);
                    for x in &other_names {
                        if visited.contains(x) {
                            keys.insert(visited.get(x).unwrap());
                        }
                    }
                }
            }
            if keys.len() > keys_before {
                for j in members {
                    group.push(remaining[j].take().unwrap());
                }
            }
        }
        for tp in &group {
            visited.extend(names(tp));
        }
        groups.push(group);
    }
    groups
}

/// The column names of the variables and blank nodes of a triple pattern.
fn names(tp: &TriplePattern) -> Vec<String> {
    let mut ns: Vec<String> = variables(tp)
        .into_iter()
        .map(|v| v.as_str().to_string())
        .collect();
    ns.extend(blank_nodes(tp).into_iter().map(|b| b.to_string()));
    ns
}
