use oxrdf::vocab::rdfs;
use oxrdf::{Term, Variable};
use query_processing::type_constraints::{
    conjunction_variable_type, equal_variable_type, ConstraintExpr, PossibleTypes,
};
use representation::polars_to_rdf::column_as_terms;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

pub const SMALL_HEIGHT: usize = 100;
pub const OWL_REAL: &str = "http://www.w3.org/2002/07/owl#real";

//Todos: pushdowns from joins..

#[derive(Clone, Debug)]
pub struct Pushdowns {
    pub variables_values: HashMap<String, HashSet<Term>>,
    pub variables_type_constraints: HashMap<String, PossibleTypes>,
}

impl Pushdowns {
    pub(crate) fn remove_variable(&mut self, v: &Variable) {
        self.variables_values.remove(v.as_str());
        self.variables_type_constraints.remove(v.as_str());
    }
}

impl Default for Pushdowns {
    fn default() -> Self {
        Self::new()
    }
}

impl Pushdowns {
    pub fn new() -> Pushdowns {
        Pushdowns {
            variables_values: Default::default(),
            variables_type_constraints: Default::default(),
        }
    }
}

impl Pushdowns {
    pub(crate) fn add_from_solution_mappings(&mut self, sm: SolutionMappings) -> SolutionMappings {
        if sm.height_estimate <= SMALL_HEIGHT {
            let eager_sm = sm.as_eager();
            let colnames = eager_sm.mappings.get_column_names();
            let columns = eager_sm.mappings.columns(&colnames).unwrap();
            //Todo: why not par?
            let pushdowns: HashMap<_, _> = columns
                .into_iter()
                .map(|x| {
                    let name = x.name().as_str();
                    let maybe_terms =
                        column_as_terms(x, eager_sm.rdf_node_types.get(name).unwrap());
                    let terms: HashSet<_> = maybe_terms.into_iter().flatten().collect();
                    (name.to_string(), terms)
                })
                .collect();
            self.variables_values = conjunction(&mut self.variables_values, pushdowns);
            eager_sm.as_lazy()
        } else {
            sm
        }
    }

    pub(crate) fn add_patterns_pushdowns(&mut self, patterns: &Vec<TriplePattern>) {
        for pattern in patterns {
            if let TermPattern::Variable(v) = &pattern.subject {
                self.iri_or_blanknode_constraint(v)
            }
            if let NamedNodePattern::Variable(v) = &pattern.predicate {
                self.iri_or_blanknode_constraint(v)
            }
        }
    }

    pub(crate) fn add_graph_pattern_pushdowns(&mut self, gp: &GraphPattern) {
        match gp {
            GraphPattern::Bgp { patterns } => {
                self.add_patterns_pushdowns(patterns);
            }
            GraphPattern::Join { left, right } => {
                self.add_graph_pattern_pushdowns(left);
                self.add_graph_pattern_pushdowns(right);
            }
            GraphPattern::LeftJoin { left, .. } => self.add_graph_pattern_pushdowns(left),
            GraphPattern::Minus { left, .. } => self.add_graph_pattern_pushdowns(left),
            GraphPattern::Filter { inner, expr } => {
                self.add_graph_pattern_pushdowns(inner);
                self.add_filter_variable_pushdowns(expr);
            }
            GraphPattern::Union { left: _, right: _ } => {
                //Consider what to do, must make corresponding change to graph pattern behaviour.
            }
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => {
                self.add_graph_pattern_pushdowns(inner);
                if let Expression::Variable(expr_variable) = expression {
                    if self.variables_values.contains_key(expr_variable.as_str()) {
                        self.variables_values.insert(
                            variable.as_str().to_string(),
                            self.variables_values
                                .get(expr_variable.as_str())
                                .unwrap()
                                .clone(),
                        );
                    }
                }
            }
            GraphPattern::Values {
                bindings,
                variables,
            } => {
                for (i, v) in variables.iter().enumerate() {
                    let mut terms = HashSet::new();
                    let mut types = HashSet::new();
                    for gs in bindings {
                        let g = gs.get(i).unwrap();
                        if let Some(g) = g {
                            terms.insert(g.clone());
                            match g {
                                GroundTerm::NamedNode(_) => {
                                    types.insert(BaseRDFNodeType::IRI);
                                }
                                GroundTerm::Literal(l) => {
                                    types.insert(BaseRDFNodeType::Literal(
                                        l.datatype().into_owned(),
                                    ));
                                }
                            }
                        }
                    }
                    let use_terms: HashSet<_> = terms
                        .into_iter()
                        .map(|x| match x {
                            GroundTerm::NamedNode(nn) => Term::NamedNode(nn),
                            GroundTerm::Literal(l) => Term::Literal(l),
                        })
                        .collect();
                    if !use_terms.is_empty() {
                        if let Some(t) = self.variables_values.get_mut(v.as_str()) {
                            t.extend(use_terms);
                        } else {
                            self.variables_values
                                .insert(v.as_str().to_string(), use_terms);
                        }
                    }
                    if !types.is_empty() {
                        let mut types_iter = types.into_iter();
                        let init_ctr =
                            ConstraintExpr::Constraint(Box::new(types_iter.next().unwrap()));
                        let ctr = types_iter.fold(init_ctr, |acc, elem| {
                            ConstraintExpr::Or(
                                Box::new(acc),
                                Box::new(ConstraintExpr::Constraint(Box::new(elem))),
                            )
                        });
                        if let Some(t) = self.variables_type_constraints.get_mut(v.as_str()) {
                            t.and_ctr(ctr)
                        } else {
                            self.variables_type_constraints
                                .insert(v.as_str().to_string(), PossibleTypes::singular_ctr(ctr));
                        }
                    }
                }
            }
            GraphPattern::OrderBy { inner, .. } => self.add_graph_pattern_pushdowns(inner),
            GraphPattern::Distinct { inner, .. } => self.add_graph_pattern_pushdowns(inner),
            GraphPattern::Reduced { inner } => self.add_graph_pattern_pushdowns(inner),
            GraphPattern::Slice { inner, .. } => {
                self.add_graph_pattern_pushdowns(inner);
            }
            GraphPattern::Group {
                inner: _,
                variables: _,
                ..
            }
            | GraphPattern::Project {
                inner: _,
                variables: _,
            } => {
                //Todo..
            }
            _ => {}
        }
    }

    pub fn add_filter_variable_pushdowns(&mut self, e: &Expression) {
        if let Some(pushdowns) = find_variable_pushdowns(e) {
            self.variables_values = conjunction(&mut self.variables_values, pushdowns);
        }
        if let Some(type_constraints) = find_variable_type_constraints(e) {
            self.variables_type_constraints =
                conjunction_variable_type(&mut self.variables_type_constraints, type_constraints);
        }
    }

    pub fn limit_to_variables(&mut self, variables: &[Variable]) {
        let mut new_pushdown_variables = HashMap::new();
        let mut new_type_constraints = HashMap::new();

        for v in variables {
            if let Some((k, v)) = self.variables_values.remove_entry(v.as_str()) {
                new_pushdown_variables.insert(k, v);
            }
            if let Some((k, v)) = self.variables_type_constraints.remove_entry(v.as_str()) {
                new_type_constraints.insert(k, v);
            }
        }

        self.variables_values = new_pushdown_variables;
        self.variables_type_constraints = new_type_constraints;
    }

    fn iri_or_blanknode_constraint(&mut self, v: &Variable) {
        let ctr = ConstraintExpr::Or(
            Box::new(ConstraintExpr::Constraint(Box::new(BaseRDFNodeType::IRI))),
            Box::new(ConstraintExpr::Constraint(Box::new(
                BaseRDFNodeType::BlankNode,
            ))),
        );
        if let Some(constraint) = self.variables_type_constraints.get_mut(v.as_str()) {
            constraint.and_ctr(ctr);
        } else {
            self.variables_type_constraints
                .insert(v.as_str().to_string(), PossibleTypes::singular_ctr(ctr));
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
                        new_map.insert(k, v);
                    }
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

fn find_variable_type_constraints(e: &Expression) -> Option<HashMap<String, PossibleTypes>> {
    match e {
        Expression::If(_, left, right) | Expression::Or(left, right) => {
            let left = find_variable_type_constraints(left);
            let right = find_variable_type_constraints(right);
            if let (Some(left), Some(mut right)) = (left, right) {
                let mut new_map = HashMap::new();
                for (k, mut v) in left {
                    if let Some(vr) = right.remove(&k) {
                        v.or_other(vr);
                        new_map.insert(k, v);
                    }
                }
                return Some(new_map);
            }
        }
        Expression::And(left, right) => {
            let left = find_variable_type_constraints(left);
            let right = find_variable_type_constraints(right);
            if let (Some(mut left), Some(right)) = (left, right) {
                return Some(conjunction_variable_type(&mut left, right));
            }
        }
        Expression::Equal(left, right) => {
            if let Some((v, t)) = equal_variable_type(left, right) {
                return Some(HashMap::from([(
                    v.as_str().to_string(),
                    PossibleTypes::singular(t),
                )]));
            } else if let Some((v, t)) = equal_variable_type(right, left) {
                return Some(HashMap::from([(
                    v.as_str().to_string(),
                    PossibleTypes::singular(t),
                )]));
            }
        }
        Expression::FunctionCall(f, args) => match f {
            Function::IsIri => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(BaseRDFNodeType::IRI),
                        )]));
                    }
                }
            }
            Function::IsBlank => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(BaseRDFNodeType::BlankNode),
                        )]));
                    }
                }
            }
            Function::IsLiteral => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(BaseRDFNodeType::Literal(
                                rdfs::LITERAL.into_owned(),
                            )),
                        )]));
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
    None
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
    new_map.extend(right);
    new_map
}
