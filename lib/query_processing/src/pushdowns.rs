pub use crate::pushdown_values::PushdownValues;
use crate::type_constraints::{
    conjunction_variable_type, datetime_datatypes, equal_variable_possible_types,
    ConstraintBaseRDFNodeType, ConstraintExpr, PossibleTypes,
};
use oxrdf::vocab::rdfs;
use oxrdf::{Literal, NamedNodeRef, Term, Variable};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::cats::LockedCats;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use utils::fast_path::{self, FastPath};

/// Solution mappings estimated to have at most this many rows are collected to check whether
/// their values can be pushed down.
pub const CHECK_SMALL_HEIGHT: usize = 100_000;
/// The values of every variable of solution mappings with at most this many rows are pushed down.
pub const SMALL_HEIGHT: usize = 500;
/// The values of the variables of the pattern being evaluated are pushed down from solution
/// mappings with at most this many rows. Pushing values down costs time proportional to the
/// number of distinct values, which is small compared with scanning large triple tables.
pub const MEDIUM_HEIGHT: usize = 100_000;
/// Values are pushed down from more than [SMALL_HEIGHT] rows only if the triple table the
/// pattern reads has at least this many times more rows. Otherwise sorting the values, looking
/// them up and joining the rows found costs more than joining the whole table within the plan:
/// e.g. 28,880 subjects pushed into a table of 528,699 rows (18 times larger) took 6 ms against
/// 1.6 ms for the join. Between 32 and 128 the difference is within measurement noise; a lower
/// factor keeps pushing into large tables, which saves reading them in disk storage.
pub const MEDIUM_TABLE_FACTOR: usize = 64;
/// Solutions estimated to have at most this many rows are not collected to push their values down
/// into a pattern whose triple table has at most this many rows: joining the whole table within
/// the query's plan costs less than a collect, and a star or chain of such patterns is then
/// collected once.
pub const SMALL_TABLE_HEIGHT: usize = 16_384;
pub const OWL_REAL: &str = "http://www.w3.org/2002/07/owl#real";

//Todos: pushdowns from joins..

#[derive(Clone, Debug)]
pub struct Pushdowns {
    /// The values each variable is known to take (or a superset of them), see [PushdownValues].
    pub variables_values: HashMap<String, PushdownValues>,
    pub variables_type_constraints: HashMap<String, PossibleTypes>,
    /// For the graph patterns of an expression (EXISTS): what is known about the solutions the
    /// expression is evaluated on, from the pattern that gave them. `None` if nothing is.
    pub expression_input: Option<ExpressionInput>,
    /// Use the incoming solutions of a pattern as they are, without collecting them to push
    /// their values down: they are then evaluated once, within the query's plan.
    pub keep_lazy: bool,
}

/// What the pattern that gave some solutions says about them (see [Pushdowns]).
#[derive(Clone, Debug)]
pub struct ExpressionInput {
    /// The variables that may be unbound in some of the solutions.
    pub maybe_unbound: HashSet<String>,
    /// Whether evaluating the solutions again may give other values (RAND(), UUID(), BNODE(),
    /// NOW(), SERVICE or a custom function).
    pub may_change: bool,
}

impl Pushdowns {
    pub fn remove_variable(&mut self, v: &Variable) {
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
            expression_input: None,
            keep_lazy: false,
        }
    }
}

impl Pushdowns {
    /// Pushes down the values of the variables of `sm`. When `sm` has more than [SMALL_HEIGHT]
    /// rows, only the values of the variables in `focus` (those of the pattern about to be
    /// evaluated) are pushed down, and only when `table_height` (the number of triples the
    /// pattern would otherwise read, if known) is at least [MEDIUM_TABLE_FACTOR] times larger.
    pub fn add_from_solution_mappings(
        &mut self,
        sm: SolutionMappings,
        global_cats: LockedCats,
        focus: &[&str],
        table_height: Option<usize>,
    ) -> SolutionMappings {
        let medium_height = match table_height {
            Some(t) => MEDIUM_HEIGHT.min(t / MEDIUM_TABLE_FACTOR),
            None => MEDIUM_HEIGHT,
        };
        // Joining a small table within the plan only pays when the solutions so far are
        // estimated small too: a collect also replaces a loose estimate (e.g. that of a lookup
        // by a constant object) by the real height, which decides the engine of the query.
        fast_path::hit_when(
            !self.keep_lazy
                && sm.height_estimate <= SMALL_TABLE_HEIGHT
                && table_height.is_some_and(|t| t <= SMALL_TABLE_HEIGHT),
            FastPath::BgpSmallTableInPlan,
        );
        if self.keep_lazy
            || sm.height_estimate > CHECK_SMALL_HEIGHT
            || (table_height.is_some_and(|t| t <= SMALL_TABLE_HEIGHT)
                && sm.height_estimate <= SMALL_TABLE_HEIGHT)
        {
            return sm;
        }
        // Variables with more known values than rows of `sm` would be constrained further.
        let could_constrain = |v: &str, height: usize| match self.variables_values.get(v) {
            Some(vv) => vv.len() > height,
            None => true,
        };
        if !sm
            .rdf_node_types
            .keys()
            .any(|v| could_constrain(v, sm.height_estimate))
        {
            return sm;
        }
        let eager_sm = sm.as_eager();
        let height = eager_sm.mappings.height();
        let columns: Vec<_> = if height <= SMALL_HEIGHT {
            eager_sm.mappings.columns().iter().collect()
        } else if height <= medium_height {
            eager_sm
                .mappings
                .columns()
                .iter()
                .filter(|c| {
                    let name = c.name().as_str();
                    focus.contains(&name) && could_constrain(name, height)
                })
                .collect()
        } else {
            vec![]
        };
        // Triple terms are not pushed down yet: their ids cannot be looked up as codes of the
        // string dictionaries. A variable that may hold them gets no pushed down values, so
        // that its triple terms are not left out; the join with its solutions keeps the matches.
        let columns: Vec<_> = columns
            .into_iter()
            .filter(|c| {
                !eager_sm
                    .rdf_node_types
                    .get(c.name().as_str())
                    .is_some_and(|t| t.map.contains_key(&BaseRDFNodeType::TripleTerm))
            })
            .collect();
        if !columns.is_empty() {
            fast_path::hit(FastPath::BgpValuesPushed);
            let pushdowns: HashMap<_, _> = columns
                .into_par_iter()
                .map(|x| {
                    let name = x.name().as_str();
                    // Dictionary codes are pushed down as they are; only the values of other
                    // types are converted to terms.
                    let values = PushdownValues::from_column(
                        x,
                        eager_sm.rdf_node_types.get(name).unwrap(),
                        &global_cats,
                        height > SMALL_HEIGHT,
                    );
                    (name.to_string(), values)
                })
                .collect();
            self.variables_values = conjunction(&mut self.variables_values, pushdowns);
        }
        eager_sm.as_lazy()
    }

    pub fn add_patterns_pushdowns(&mut self, patterns: &Vec<TriplePattern>) {
        for pattern in patterns {
            if let TermPattern::Variable(v) = &pattern.subject {
                self.iri_or_blanknode_constraint(v)
            }
            if let NamedNodePattern::Variable(v) = &pattern.predicate {
                self.iri_or_blanknode_constraint(v)
            }
        }
    }

    pub fn add_graph_pattern_pushdowns(&mut self, gp: &GraphPattern) {
        self.add_graph_pattern_pushdowns_impl(gp, None);
    }

    fn add_graph_pattern_pushdowns_impl(
        &mut self,
        gp: &GraphPattern,
        only_vars: Option<&Vec<&str>>,
    ) {
        match gp {
            GraphPattern::Bgp { patterns } => {
                self.add_patterns_pushdowns(patterns);
            }
            GraphPattern::Join { left, right } => {
                self.add_graph_pattern_pushdowns_impl(left, only_vars);
                self.add_graph_pattern_pushdowns_impl(right, only_vars);
            }
            GraphPattern::LeftJoin { left, .. } => {
                self.add_graph_pattern_pushdowns_impl(left, only_vars)
            }
            GraphPattern::Minus { left, .. } | GraphPattern::Lateral { left, .. } => {
                self.add_graph_pattern_pushdowns_impl(left, only_vars)
            }
            GraphPattern::Filter { inner, expr } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars);
                self.add_filter_variable_pushdowns(expr, only_vars);
            }
            GraphPattern::Union { left: _, right: _ } => {
                //Consider what to do, must make corresponding change to graph pattern behaviour.
            }
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars);
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
                    if let Some(only_vars) = &only_vars {
                        if !only_vars.contains(&v.as_str()) {
                            continue;
                        }
                    }
                    // An UNDEF value is compatible with anything, so nothing is pushed down.
                    if bindings.iter().any(|gs| gs.get(i).unwrap().is_none()) {
                        continue;
                    }
                    let mut terms = HashSet::new();
                    let mut literal_types = HashSet::new();
                    let mut iri = false;
                    let mut triple = false;
                    for gs in bindings {
                        let g = gs.get(i).unwrap();
                        if let Some(g) = g {
                            terms.insert(g.clone());
                            match g {
                                GroundTerm::NamedNode(..) => {
                                    iri = true;
                                }
                                GroundTerm::Literal(l) => {
                                    literal_types.insert(l.datatype());
                                }
                                GroundTerm::Triple(_) => {
                                    triple = true;
                                }
                            }
                        }
                    }
                    let mut types: Vec<_> = literal_types
                        .into_iter()
                        .map(|l| ConstraintBaseRDFNodeType::Literal(l.into_owned()))
                        .collect();
                    if iri {
                        types.push(ConstraintBaseRDFNodeType::IRI);
                    }
                    if triple {
                        types.push(ConstraintBaseRDFNodeType::TripleTerm);
                    }
                    let use_terms: HashSet<_> = terms
                        .into_iter()
                        .map(|x| match x {
                            GroundTerm::NamedNode(nn) => Term::NamedNode(nn),
                            GroundTerm::Literal(l) => Term::Literal(l),
                            t @ GroundTerm::Triple(_) => Term::from(t),
                        })
                        .collect();
                    if !use_terms.is_empty() {
                        let use_terms = PushdownValues::from_terms(use_terms);
                        if let Some(t) = self.variables_values.get_mut(v.as_str()) {
                            t.union_with(use_terms);
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
            GraphPattern::OrderBy { inner, .. } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars)
            }
            GraphPattern::Distinct { inner, .. } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars)
            }
            GraphPattern::Reduced { inner } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars)
            }
            GraphPattern::Slice { inner, .. } => {
                self.add_graph_pattern_pushdowns_impl(inner, only_vars);
            }
            GraphPattern::Group {
                inner: i,
                variables: vs,
                ..
            }
            | GraphPattern::Project {
                inner: i,
                variables: vs,
            } => {
                let new_variables: Vec<_> = if let Some(only_vars) = only_vars {
                    vs.iter()
                        .map(|x| x.as_str())
                        .filter(|x| only_vars.contains(x))
                        .collect()
                } else {
                    vs.iter().map(|x| x.as_str()).collect()
                };
                if !new_variables.is_empty() {
                    self.add_graph_pattern_pushdowns_impl(i, Some(&new_variables));
                }
            }
            _ => {}
        }
    }

    pub fn add_filter_variable_pushdowns(&mut self, e: &Expression, only_vars: Option<&Vec<&str>>) {
        if let Some(mut pushdowns) = find_variable_pushdowns(e) {
            if let Some(only_vars) = only_vars {
                let to_remove: Vec<_> = pushdowns
                    .keys()
                    .filter(|k| only_vars.contains(&k.as_str()))
                    .cloned()
                    .collect();
                for k in to_remove {
                    pushdowns.remove(k.as_str());
                }
            }
            let pushdowns = pushdowns
                .into_iter()
                .map(|(k, v)| (k, PushdownValues::from_terms(v)))
                .collect();
            self.variables_values = conjunction(&mut self.variables_values, pushdowns);
        }
        if let Some(mut type_constraints) = find_variable_type_constraints(e) {
            if let Some(only_vars) = only_vars {
                let to_remove: Vec<_> = type_constraints
                    .keys()
                    .filter(|k| only_vars.contains(&k.as_str()))
                    .cloned()
                    .collect();
                for k in to_remove {
                    type_constraints.remove(k.as_str());
                }
            }
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
            Box::new(ConstraintExpr::Constraint(Box::new(
                ConstraintBaseRDFNodeType::IRI,
            ))),
            Box::new(ConstraintExpr::Constraint(Box::new(
                ConstraintBaseRDFNodeType::BlankNode,
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
                        v.extend(vr);
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
            if let (Some(left), Some(right)) = (left, right) {
                Some(terms_conjunction(left, right))
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
                    terms.extend(equal_literal_terms(l)?);
                } else {
                    return None;
                }
                Some(HashMap::from([(left.as_str().to_string(), terms)]))
            } else if let Expression::Variable(right) = right.as_ref() {
                let mut terms = HashSet::new();
                if let Expression::NamedNode(nn) = left.as_ref() {
                    terms.insert(Term::NamedNode(nn.clone()));
                } else if let Expression::Literal(l) = left.as_ref() {
                    terms.extend(equal_literal_terms(l)?);
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
                        terms.extend(equal_literal_terms(l)?);
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
            if let Some((v, t)) = equal_variable_possible_types(left, right) {
                return Some(HashMap::from([(v.as_str().to_string(), t)]));
            } else if let Some((v, t)) = equal_variable_possible_types(right, left) {
                return Some(HashMap::from([(v.as_str().to_string(), t)]));
            }
        }
        Expression::FunctionCall(f, args) => match f {
            Function::IsIri => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(ConstraintBaseRDFNodeType::IRI),
                        )]));
                    }
                }
            }
            Function::IsBlank => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(ConstraintBaseRDFNodeType::BlankNode),
                        )]));
                    }
                }
            }
            Function::IsLiteral if args.len() == 1 => {
                if let Expression::Variable(v) = args.first().unwrap() {
                    return Some(HashMap::from([(
                        v.as_str().to_string(),
                        PossibleTypes::singular(ConstraintBaseRDFNodeType::Literal(
                            rdfs::LITERAL.into_owned(),
                        )),
                    )]));
                }
            }
            _ => {}
        },
        _ => {}
    }
    None
}

fn conjunction(
    left: &mut HashMap<String, PushdownValues>,
    mut right: HashMap<String, PushdownValues>,
) -> HashMap<String, PushdownValues> {
    let mut new_map = HashMap::new();
    for (k, v) in left.drain() {
        if let Some(vr) = right.remove(&k) {
            new_map.insert(k, v.intersection(vr));
        } else {
            new_map.insert(k, v);
        }
    }
    new_map.extend(right);
    new_map
}

fn terms_conjunction(
    mut left: HashMap<String, HashSet<Term>>,
    mut right: HashMap<String, HashSet<Term>>,
) -> HashMap<String, HashSet<Term>> {
    let mut new_map = HashMap::new();
    for (k, v) in left.drain() {
        if let Some(vr) = right.remove(&k) {
            new_map.insert(k, v.intersection(&vr).cloned().collect());
        } else {
            new_map.insert(k, v);
        }
    }
    new_map.extend(right);
    new_map
}

/// The terms that a variable can be bound to when it is = to the literal, or None if they
/// cannot be listed, in which case the value is not pushed down.
///
/// A dateTime is equal to the same instant as an xsd:dateTime or an xsd:dateTimeStamp, so the
/// value is pushed down with both datatypes. A number is equal to the same value of any numeric
/// datatype (with type promotion), see [numeric_equal_terms].
fn equal_literal_terms(l: &Literal) -> Option<Vec<Term>> {
    if representation::literal_is_datetime(l.datatype()) {
        Some(
            datetime_datatypes()
                .into_iter()
                .map(|dt| Term::Literal(Literal::new_typed_literal(l.value(), dt)))
                .collect(),
        )
    } else if representation::literal_is_numeric(l.datatype()) {
        numeric_equal_terms(l)
    } else if crate::expressions::durations::is_duration_datatype(l.datatype()) {
        // Durations with other lexical forms and duration datatypes can be equal, e.g.
        // "P1D"^^xsd:dayTimeDuration = "PT24H"^^xsd:dayTimeDuration.
        None
    } else {
        Some(vec![Term::Literal(l.clone())])
    }
}

/// The values of all numeric datatypes that are = to an integer or decimal literal.
///
/// Each numeric datatype has at most one value that is equal to such a literal: the same
/// number for the integer types (when in range) and xsd:decimal, and the nearest double or
/// float, since the literal is promoted to xsd:double or xsd:float when compared with one.
/// For an xsd:double or xsd:float literal, many decimals are equal to it after promotion,
/// so the values cannot be listed and None is returned.
fn numeric_equal_terms(l: &Literal) -> Option<Vec<Term>> {
    use oxrdf::vocab::xsd;
    let dt = l.datatype();
    if matches!(dt, xsd::DOUBLE | xsd::FLOAT) {
        return None;
    }
    let lexical = l.value().trim();
    let unsigned = lexical.trim_start_matches(['+', '-']);
    let negative = lexical.starts_with('-');
    let (int_part, frac_part) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if int_part.is_empty() && frac_part.is_empty()
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let frac_part = frac_part.trim_end_matches('0');
    let int_part = match int_part.trim_start_matches('0') {
        "" => "0",
        x => x,
    };
    let sign = if negative && (int_part != "0" || !frac_part.is_empty()) {
        "-"
    } else {
        ""
    };
    let decimal = if frac_part.is_empty() {
        format!("{sign}{int_part}")
    } else {
        format!("{sign}{int_part}.{frac_part}")
    };
    let value: f64 = decimal.parse().ok()?;
    let mut terms = vec![
        Term::Literal(Literal::new_typed_literal(&decimal, xsd::DECIMAL)),
        Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DOUBLE)),
        Term::Literal(Literal::new_typed_literal(
            (value as f32).to_string(),
            xsd::FLOAT,
        )),
    ];
    if frac_part.is_empty() {
        // The integer types that the integer is in the range of.
        let n: i128 = decimal.parse().ok()?;
        let ranges: [(NamedNodeRef, i128, i128); 13] = [
            (xsd::INTEGER, i64::MIN as i128, i64::MAX as i128),
            (xsd::LONG, i64::MIN as i128, i64::MAX as i128),
            (xsd::INT, i32::MIN as i128, i32::MAX as i128),
            (xsd::SHORT, i16::MIN as i128, i16::MAX as i128),
            (xsd::BYTE, i8::MIN as i128, i8::MAX as i128),
            (xsd::NON_POSITIVE_INTEGER, i64::MIN as i128, 0),
            (xsd::NEGATIVE_INTEGER, i64::MIN as i128, -1),
            (xsd::NON_NEGATIVE_INTEGER, 0, u64::MAX as i128),
            (xsd::POSITIVE_INTEGER, 1, u64::MAX as i128),
            (xsd::UNSIGNED_LONG, 0, u64::MAX as i128),
            (xsd::UNSIGNED_INT, 0, u32::MAX as i128),
            (xsd::UNSIGNED_SHORT, 0, u16::MAX as i128),
            (xsd::UNSIGNED_BYTE, 0, u8::MAX as i128),
        ];
        for (dt, min, max) in ranges {
            if (min..=max).contains(&n) {
                terms.push(Term::Literal(Literal::new_typed_literal(n.to_string(), dt)));
            }
        }
    }
    Some(terms)
}
