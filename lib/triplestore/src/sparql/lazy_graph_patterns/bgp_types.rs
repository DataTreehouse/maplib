//! Proving a basic graph pattern empty from the types of the stored triples, before reading any.
//!
//! The triples of a predicate are stored in one table per pair of subject and object types. A
//! variable can only be bound to a type that every position it has in the pattern can hold: in
//! `?a ex:p ?b . ?b ex:q ?c`, `?b` must be both an object type of `ex:p` and a subject type of
//! `ex:q`. When no type is left for some variable, or a constant predicate has no triples, the
//! pattern has no solutions.

use super::Triplestore;
use oxrdf::NamedNode;
use polars::prelude::{lit, IntoLazy, NULL};
use polars_core::frame::DataFrame;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

/// A stored type, with the time zone of a literal left out: literal types are compared by
/// datatype only, which can only keep more types.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum Kind {
    Iri,
    BlankNode,
    Literal(NamedNode),
    TripleTerm,
}

fn kind(t: &BaseRDFNodeType) -> Option<Kind> {
    match t {
        BaseRDFNodeType::IRI => Some(Kind::Iri),
        BaseRDFNodeType::BlankNode => Some(Kind::BlankNode),
        BaseRDFNodeType::Literal(dt, _) => Some(Kind::Literal(dt.clone())),
        BaseRDFNodeType::TripleTerm => Some(Kind::TripleTerm),
        // Anything may match.
        BaseRDFNodeType::None => None,
    }
}

fn name_of(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

impl Triplestore {
    fn query_graphs(dataset: &QueryGraph) -> Vec<NamedGraph> {
        match dataset {
            QueryGraph::NamedGraph(ng) => vec![ng.clone()],
            QueryGraph::QueryDataset(qd) => qd
                .default
                .iter()
                .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                .collect(),
        }
    }

    /// The (subject, object) kinds of the stored triples of a predicate in the queried graphs.
    /// None in a position means it may hold anything.
    fn predicate_kinds(
        &self,
        predicate: &NamedNode,
        graphs: &[NamedGraph],
    ) -> (Option<HashSet<Kind>>, Option<HashSet<Kind>>) {
        let mut subjects = Some(HashSet::new());
        let mut objects = Some(HashSet::new());
        for g in graphs {
            let Some(m) = self.graph_triples_map.get(g).and_then(|m| m.get(predicate)) else {
                continue;
            };
            for (s, o) in m.keys() {
                match (kind(s), &mut subjects) {
                    (Some(k), Some(set)) => {
                        set.insert(k);
                    }
                    _ => subjects = None,
                }
                match (kind(o), &mut objects) {
                    (Some(k), Some(set)) => {
                        set.insert(k);
                    }
                    _ => objects = None,
                }
            }
        }
        (subjects, objects)
    }

    /// Whether the triple patterns (joined) can have no solutions in the queried graphs, from the
    /// types of the stored triples alone. Patterns with a variable predicate say nothing about
    /// their subject and object.
    pub(crate) fn bgp_proven_empty(&self, patterns: &[TriplePattern], dataset: &QueryGraph) -> bool {
        if patterns.is_empty() {
            return false;
        }
        let graphs = Self::query_graphs(dataset);
        // The kinds each variable or blank node can still have.
        let mut possible: HashMap<String, HashSet<Kind>> = HashMap::new();
        let mut restrict = |name: String, kinds: HashSet<Kind>| -> bool {
            let entry = possible.entry(name).or_insert_with(|| kinds.clone());
            entry.retain(|k| kinds.contains(k));
            entry.is_empty()
        };
        let iri: HashSet<Kind> = [Kind::Iri].into_iter().collect();
        for tp in patterns {
            match &tp.predicate {
                NamedNodePattern::Variable(v) => {
                    if restrict(v.as_str().to_string(), iri.clone()) {
                        return true;
                    }
                }
                NamedNodePattern::NamedNode(p) => {
                    let (subjects, objects) = self.predicate_kinds(p, &graphs);
                    if subjects.as_ref().is_some_and(|s| s.is_empty())
                        || objects.as_ref().is_some_and(|o| o.is_empty())
                    {
                        // No triples with the predicate.
                        return true;
                    }
                    for (term, kinds) in [(&tp.subject, subjects), (&tp.object, objects)] {
                        let Some(kinds) = kinds else { continue };
                        let empty = match term {
                            TermPattern::NamedNode(_) => !kinds.contains(&Kind::Iri),
                            TermPattern::Literal(l) => {
                                !kinds.contains(&Kind::Literal(l.datatype().into_owned()))
                            }
                            _ => match name_of(term) {
                                Some(name) => restrict(name, kinds),
                                None => false,
                            },
                        };
                        if empty {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// The solutions of a basic graph pattern proven empty: none, with the variables and blank
    /// nodes of the patterns that `solution_mappings` does not have as unbound (untyped) columns,
    /// as a pattern without matching triples gives them.
    pub(crate) fn empty_bgp_solution_mappings(
        &self,
        patterns: &[TriplePattern],
        solution_mappings: Option<SolutionMappings>,
    ) -> SolutionMappings {
        let (mut mappings, mut types) = match solution_mappings {
            Some(sm) => (sm.mappings.limit(0), sm.rdf_node_types),
            None => (DataFrame::empty().lazy(), HashMap::new()),
        };
        let mut names = vec![];
        for tp in patterns {
            names.extend(name_of(&tp.subject));
            if let NamedNodePattern::Variable(v) = &tp.predicate {
                names.push(v.as_str().to_string());
            }
            names.extend(name_of(&tp.object));
        }
        let dt = BaseRDFNodeType::None.default_stored_polars_data_type();
        let mut new_columns = vec![];
        for name in names {
            if types.contains_key(&name) {
                continue;
            }
            new_columns.push(
                lit(NULL).cast(dt.clone()).alias(name.as_str()),
            );
            types.insert(
                name,
                BaseRDFNodeType::None.into_default_stored_rdf_node_state(),
            );
        }
        if !new_columns.is_empty() {
            mappings = mappings.with_columns(new_columns);
        }
        SolutionMappings::new(mappings.limit(0), types, 0)
    }
}
