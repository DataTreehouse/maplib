//! The values a variable is known to take, pushed down into the patterns that bind it.
//!
//! Values that come from solutions are mostly dictionary codes (IRIs, blank nodes and string
//! literals). They are kept as codes, so that the triple lookups that use them need not decode
//! them into terms and then look the terms up in the dictionary again.

use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use polars::prelude::Column;
use representation::cats::Cats;
use representation::multitype::extract_column_from_multitype;
use representation::polars_to_rdf::basic_rdf_node_type_column_to_term_vec;
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState};
use std::collections::{BTreeMap, HashSet};

/// A set of values of a variable. A pushed down set may hold more values than the variable can
/// take (a superset), never fewer: the patterns that use it still join with, or filter by, the
/// solutions it came from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushdownValues {
    /// Values given as terms: the constants of the query, and values of types that are not
    /// dictionary codes or are coded in a local dictionary.
    pub terms: HashSet<Term>,
    /// Global dictionary codes of values of the types that tables store as codes (see
    /// [is_code_type]), sorted and without duplicates.
    pub codes: BTreeMap<BaseRDFNodeType, Vec<u32>>,
}

/// Whether values of the type are stored in triple tables as global dictionary codes, one code
/// per value. Language tagged strings are two codes, so they are kept as terms.
pub fn is_code_type(bt: &BaseRDFNodeType) -> bool {
    bt.stored_cat() && !bt.is_lang_string()
}

/// The type of the term, as the key of its codes in [PushdownValues::codes].
pub fn term_base_type(t: &Term) -> BaseRDFNodeType {
    match t {
        Term::NamedNode(_) => BaseRDFNodeType::IRI,
        Term::BlankNode(_) => BaseRDFNodeType::BlankNode,
        Term::Literal(l) => BaseRDFNodeType::Literal(l.datatype().into_owned(), None),
        Term::Triple(_) => BaseRDFNodeType::TripleTerm,
    }
}

impl PushdownValues {
    pub fn from_terms(terms: HashSet<Term>) -> Self {
        PushdownValues {
            terms,
            codes: BTreeMap::new(),
        }
    }

    /// The values of a column of solutions: the codes of the parts of the column that hold
    /// global dictionary codes, and the terms of the other parts. `distinct_terms` says whether
    /// to deduplicate the other parts before converting them to terms (for large columns).
    pub fn from_column(
        column: &Column,
        state: &RDFNodeState,
        cats: &representation::cats::LockedCats,
        distinct_terms: bool,
    ) -> Self {
        let mut out = PushdownValues::default();
        let multi = state.is_multi();
        for (bt, bs) in &state.map {
            if matches!(bt, BaseRDFNodeType::None) {
                continue;
            }
            let part = if multi {
                extract_column_from_multitype(column, bt)
            } else {
                column.clone()
            };
            if is_code_type(bt) && matches!(bs, BaseCatState::CategoricalNative(None)) {
                if let Ok(u) = part.u32() {
                    let mut codes: Vec<u32> = u.iter().flatten().collect();
                    codes.sort_unstable();
                    codes.dedup();
                    out.codes.insert(bt.clone(), codes);
                    continue;
                }
            }
            let part = if distinct_terms {
                part.unique().unwrap()
            } else {
                part
            };
            let terms = basic_rdf_node_type_column_to_term_vec(&part, bt, bs, cats.clone(), false);
            out.terms.extend(terms.into_iter().flatten());
        }
        out
    }

    /// The number of values (a value given both as a term and as a code counts twice).
    pub fn len(&self) -> usize {
        self.terms.len() + self.codes.values().map(|c| c.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.codes.values().all(|c| c.is_empty())
    }

    /// Whether some value is of the type.
    pub fn has_type(&self, bt: &BaseRDFNodeType) -> bool {
        self.codes.get(bt).is_some_and(|c| !c.is_empty())
            || self.terms.iter().any(|t| &term_base_type(t) == bt)
    }

    /// Whether every value is an IRI.
    pub fn all_iris(&self) -> bool {
        self.terms.iter().all(|t| matches!(t, Term::NamedNode(_)))
            && self
                .codes
                .iter()
                .all(|(bt, c)| c.is_empty() || matches!(bt, BaseRDFNodeType::IRI))
    }

    /// Adds the values of `other`.
    pub fn union_with(&mut self, other: PushdownValues) {
        self.terms.extend(other.terms);
        for (bt, codes) in other.codes {
            match self.codes.get_mut(&bt) {
                Some(mine) => *mine = union_sorted(mine, &codes),
                None => {
                    self.codes.insert(bt, codes);
                }
            }
        }
    }

    /// The values in both sets, or more. Codes are intersected with codes, and terms with
    /// terms. Terms of a type that the other set has codes of are kept as they are, without
    /// encoding them: they are the constants of a query, a few at most.
    pub fn intersection(self, other: PushdownValues) -> PushdownValues {
        let mut terms = HashSet::new();
        for t in &self.terms {
            if other.terms.contains(t) || other.codes.contains_key(&term_base_type(t)) {
                terms.insert(t.clone());
            }
        }
        for t in other.terms {
            if self.codes.contains_key(&term_base_type(&t)) {
                terms.insert(t);
            }
        }
        let mut codes = BTreeMap::new();
        let mut other_codes = other.codes;
        for (bt, mine) in self.codes {
            if let Some(theirs) = other_codes.remove(&bt) {
                codes.insert(bt, intersect_sorted(&mine, &theirs));
            }
        }
        PushdownValues { terms, codes }
    }

    /// The values as terms, decoding the codes.
    pub fn to_terms(&self, cats: &Cats) -> HashSet<Term> {
        let mut out = self.terms.clone();
        for (bt, codes) in &self.codes {
            let Some(enc) = cats.get_enc(bt) else {
                continue;
            };
            let us: Vec<Option<u32>> = codes.iter().map(|u| Some(*u)).collect();
            for s in enc.maps.decode_batch(&us).into_iter().flatten() {
                out.insert(match bt {
                    BaseRDFNodeType::IRI => Term::NamedNode(NamedNode::new_unchecked(s)),
                    BaseRDFNodeType::BlankNode => Term::BlankNode(BlankNode::new_unchecked(s)),
                    BaseRDFNodeType::Literal(dt, _) => {
                        if dt.as_ref() == xsd::STRING {
                            Term::Literal(Literal::new_simple_literal(s))
                        } else {
                            Term::Literal(Literal::new_typed_literal(s, dt.clone()))
                        }
                    }
                    // Not code types (see is_code_type), so they have no codes here.
                    BaseRDFNodeType::None | BaseRDFNodeType::TripleTerm => continue,
                });
            }
        }
        out
    }

    /// The global codes of the values of the type: the codes, and the codes of the terms of the
    /// type that are in the dictionary, sorted and without duplicates. None if values of the
    /// type are not stored as codes.
    pub fn codes_of_type(&self, bt: &BaseRDFNodeType, cats: &Cats) -> Option<Vec<u32>> {
        if !is_code_type(bt) {
            return None;
        }
        let strs: Vec<Option<&str>> = self
            .terms
            .iter()
            .filter(|t| &term_base_type(t) == bt)
            .map(|t| {
                Some(match t {
                    Term::NamedNode(nn) => nn.as_str(),
                    Term::BlankNode(bl) => bl.as_str(),
                    Term::Literal(l) => l.value(),
                    // Triple terms are not a code type, so they are not of the type `bt`.
                    Term::Triple(_) => unreachable!("triple terms are not a code type"),
                })
            })
            .collect();
        let mut codes = self.codes.get(bt).cloned().unwrap_or_default();
        if !strs.is_empty() {
            let found = match bt {
                BaseRDFNodeType::IRI => cats.maybe_encode_iri_slice(&strs),
                BaseRDFNodeType::BlankNode => cats.maybe_encode_blanks(&strs),
                BaseRDFNodeType::Literal(dt, _) => cats.maybe_encode_literals(&strs, dt.clone()),
                BaseRDFNodeType::None | BaseRDFNodeType::TripleTerm => vec![],
            };
            let n = codes.len();
            codes.extend(found.into_iter().flatten());
            if codes.len() > n {
                codes.sort_unstable();
                codes.dedup();
            }
        }
        Some(codes)
    }

    /// Keeps the values that can be subjects: IRIs and blank nodes.
    pub fn subjects_only(mut self) -> PushdownValues {
        self.terms
            .retain(|t| matches!(t, Term::NamedNode(_) | Term::BlankNode(_)));
        self.codes
            .retain(|bt, _| matches!(bt, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode));
        self
    }
}

fn union_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> Term {
        Term::NamedNode(NamedNode::new_unchecked(s))
    }

    #[test]
    fn sorted_set_operations() {
        assert_eq!(union_sorted(&[1, 3, 5], &[2, 3, 6]), vec![1, 2, 3, 5, 6]);
        assert_eq!(intersect_sorted(&[1, 3, 5, 7], &[3, 4, 7]), vec![3, 7]);
        assert_eq!(intersect_sorted(&[], &[3]), Vec::<u32>::new());
    }

    #[test]
    fn intersection_of_codes_and_terms_keeps_a_superset() {
        let codes = PushdownValues {
            terms: HashSet::new(),
            codes: BTreeMap::from([(BaseRDFNodeType::IRI, vec![1, 2, 3])]),
        };
        let terms = PushdownValues::from_terms(HashSet::from([
            iri("http://a"),
            Term::Literal(Literal::new_simple_literal("x")),
        ]));
        let both = codes.clone().intersection(terms.clone());
        // The IRI may be one of the codes, the literal is of a type the codes have none of.
        assert_eq!(both.terms, HashSet::from([iri("http://a")]));
        assert!(both.codes.is_empty());
        let other = PushdownValues {
            terms: HashSet::new(),
            codes: BTreeMap::from([(BaseRDFNodeType::IRI, vec![2, 3, 4])]),
        };
        assert_eq!(
            codes.intersection(other).codes,
            BTreeMap::from([(BaseRDFNodeType::IRI, vec![2, 3])])
        );
    }
}
