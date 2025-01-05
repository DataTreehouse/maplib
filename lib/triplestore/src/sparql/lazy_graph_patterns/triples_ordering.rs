use oxrdf::{BlankNode, Term, Variable};
use query_processing::pushdowns::Pushdowns;
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
    variable_values: &HashMap<String, HashSet<Term>>,
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
