use oxrdf::Term;
use rayon::prelude::IntoParallelRefIterator;
use representation::polars_to_rdf::column_as_terms;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;
use std::collections::{HashMap, HashSet};
use polars::prelude::IntoLazy;

pub const SMALL_HEIGHT: usize = 50;

#[derive(Clone)]
pub struct Pushdowns {
    pub variables: HashMap<String, HashSet<Term>>,
}

impl Pushdowns {
    pub(crate) fn new() -> Pushdowns {
        Pushdowns {
            variables: Default::default(),
        }
    }
}

impl Pushdowns {
    pub(crate) fn add_from_solution_mappings(&mut self, sm: SolutionMappings) -> SolutionMappings {
        if sm.height_upper_bound <= SMALL_HEIGHT {
            let eager_sm = sm.as_eager();
            let colnames = eager_sm.mappings.get_column_names();
            let columns = eager_sm.mappings.columns(&colnames).unwrap();
            //Todo: why not par?
            let pushdowns: HashMap<_, _> = columns
                .into_iter()
                .map(|x| {
                    let name = x.name().as_str();
                    let maybe_terms = column_as_terms(x, eager_sm.rdf_node_types.get(name).unwrap());
                    let terms: HashSet<_> = maybe_terms
                        .into_iter()
                        .filter(|x| x.is_some())
                        .map(|x| x.unwrap())
                        .collect();
                    (name.to_string(), terms)
                })
                .collect();
            self.variables = conjunction(&mut self.variables, pushdowns);
            eager_sm.as_lazy()
        } else {
            sm
        }
    }
}

impl Pushdowns {
    pub fn add_variable_pushdowns(&mut self, e: &Expression) {
        if let Some(pushdowns) = find_variable_pushdowns(e) {
            self.variables = conjunction(&mut self.variables, pushdowns);
        }
    }
}

fn find_variable_pushdowns(e: &Expression) -> Option<HashMap<String, HashSet<Term>>> {
    match e {
        Expression::If(_, left, right) | Expression::Or(left, right) => {
            let left = find_variable_pushdowns(left);
            let right = find_variable_pushdowns(right);
            if let (Some(left), Some(mut right)) = (left, right) {
                let mut new_map = HashMap::new();
                for (k, mut v) in left {
                    if let Some(vr) = right.remove(&k) {
                        v.extend(vr.into_iter());
                    }
                    new_map.insert(k, v);
                }
                Some(new_map)
            } else {
                None
            }
        }
        Expression::And(left, right) => {
            let left = find_variable_pushdowns(left);
            let right = find_variable_pushdowns(right);
            if let (Some(mut left), Some(right)) = (left, right) {
                Some(conjunction(&mut left, right))
            } else {
                None
            }
        }
        Expression::Equal(left, right) => {
            if let Expression::Variable(left) = left.as_ref() {
                let mut terms = HashSet::new();
                if let Expression::NamedNode(nn) = right.as_ref() {
                    terms.insert(Term::NamedNode(nn.clone()));
                } else if let Expression::Literal(l) = right.as_ref() {
                    terms.insert(Term::Literal(l.clone()));
                } else {
                    return None;
                }
                Some(HashMap::from([(left.as_str().to_string(), terms)]))
            } else if let Expression::Variable(right) = right.as_ref() {
                let mut terms = HashSet::new();
                if let Expression::NamedNode(nn) = left.as_ref() {
                    terms.insert(Term::NamedNode(nn.clone()));
                } else if let Expression::Literal(l) = left.as_ref() {
                    terms.insert(Term::Literal(l.clone()));
                } else {
                    return None;
                }
                Some(HashMap::from([(right.as_str().to_string(), terms)]))
            } else {
                None
            }
        }
        Expression::In(v, values) => {
            if let Expression::Variable(v) = v.as_ref() {
                let mut terms = HashSet::new();
                for e in values {
                    if let Expression::NamedNode(nn) = e {
                        terms.insert(Term::NamedNode(nn.clone()));
                    } else if let Expression::Literal(l) = e {
                        terms.insert(Term::Literal(l.clone()));
                    } else {
                        return None;
                    }
                }
                Some(HashMap::from([(v.as_str().to_string(), terms)]))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn conjunction(
    left: &mut HashMap<String, HashSet<Term>>,
    mut right: HashMap<String, HashSet<Term>>,
) -> HashMap<String, HashSet<Term>> {
    let mut new_map = HashMap::new();
    for (k, v) in left.drain() {
        if let Some(vr) = right.remove(&k) {
            // Todo: improve this thing.
            new_map.insert(k, v.intersection(&vr).cloned().collect());
        } else {
            new_map.insert(k, v);
        }
    }
    new_map.extend(right.into_iter());
    new_map
}
