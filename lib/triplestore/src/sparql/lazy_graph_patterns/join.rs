use super::Triplestore;
use crate::sparql::errors::SparqlError;
use std::cmp::Ordering;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
struct BadGraphPatternProperties {
    n_cross_joins: usize,
    n_variable_predicates: usize,
}

impl BadGraphPatternProperties {
    pub(crate) fn with_additional(&mut self, other: BadGraphPatternProperties) {
        self.n_cross_joins += other.n_cross_joins;
        self.n_variable_predicates += other.n_variable_predicates;
    }
}

impl PartialEq for BadGraphPatternProperties {
    fn eq(&self, other: &Self) -> bool {
        other.n_cross_joins == self.n_cross_joins
            && other.n_variable_predicates == self.n_variable_predicates
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl PartialOrd for BadGraphPatternProperties {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.lt(other) {
            Some(Ordering::Less)
        } else if self.gt(other) {
            Some(Ordering::Greater)
        } else if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            None
        }
    }

    fn lt(&self, other: &Self) -> bool {
        self.n_cross_joins < other.n_cross_joins
            || (self.n_cross_joins == other.n_cross_joins
                && self.n_variable_predicates < other.n_variable_predicates)
    }

    fn le(&self, other: &Self) -> bool {
        self.lt(other) || self.eq(other)
    }

    fn gt(&self, other: &Self) -> bool {
        self.n_cross_joins > other.n_cross_joins
            || (self.n_cross_joins == other.n_cross_joins
                && self.n_variable_predicates > other.n_variable_predicates)
    }

    fn ge(&self, other: &Self) -> bool {
        self.gt(other) || self.eq(other)
    }
}

impl Triplestore {
    #[instrument(skip_all)]
    pub fn lazy_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let mut flat_gps = Vec::new();
        flat_gps.extend(flatten_join_gps(left, left_context));
        flat_gps.extend(flatten_join_gps(right, right_context));
        let ordered_flat_gps = order_graph_patterns(flat_gps, &solution_mappings);
        let mut output_solution_mappings = solution_mappings;
        for (gp, ctx) in ordered_flat_gps {
            output_solution_mappings = Some(self.lazy_graph_pattern(
                gp,
                output_solution_mappings,
                &ctx,
                parameters,
                pushdowns.clone(),
                query_settings,
                dataset,
            )?);
        }

        Ok(output_solution_mappings.unwrap())
    }
}

fn flatten_join_gps<'a>(
    gp: &'a GraphPattern,
    context: Context,
) -> Vec<(&'a GraphPattern, Context)> {
    let mut gps = Vec::new();
    flatten_join_gps_inner(gp, &mut gps, context);
    gps
}

fn flatten_join_gps_inner<'a>(
    gp: &'a GraphPattern,
    flat: &mut Vec<(&'a GraphPattern, Context)>,
    context: Context,
) {
    if let GraphPattern::Join { left, right } = gp {
        flatten_join_gps_inner(left, flat, context.extension_with(PathEntry::JoinLeftSide));
        flatten_join_gps_inner(
            right,
            flat,
            context.extension_with(PathEntry::JoinRightSide),
        );
    } else {
        flat.push((gp, context));
    }
}

fn find_used_variables_and_blanks(tp: &TriplePattern) -> HashSet<String> {
    let mut tp_variables = HashSet::new();
    if let TermPattern::Variable(v) = &tp.subject {
        tp_variables.insert(v.as_str().to_string());
    } else if let TermPattern::BlankNode(b) = &tp.subject {
        tp_variables.insert(b.to_string());
    }
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        tp_variables.insert(v.as_str().to_string());
    }
    if let TermPattern::Variable(v) = &tp.object {
        tp_variables.insert(v.as_str().to_string());
    } else if let TermPattern::BlankNode(b) = &tp.object {
        tp_variables.insert(b.to_string());
    }
    tp_variables
}

pub fn order_graph_patterns<'a>(
    gps: Vec<(&'a GraphPattern, Context)>,
    sm: &Option<SolutionMappings>,
) -> Vec<(&'a GraphPattern, Context)> {
    let mut candidates: HashSet<_> = (0..gps.len()).collect();
    let candidate_gps: HashMap<_, _> = gps
        .iter()
        .enumerate()
        .map(|(x, (gp, _))| (x, *gp))
        .collect();

    let mut use_vars: Option<HashSet<_>> = if let Some(sm) = &sm {
        Some(sm.rdf_node_types.keys().cloned().collect())
    } else {
        None
    };

    let mut candidate_contexts: HashMap<_, _> = gps
        .into_iter()
        .enumerate()
        .map(|(x, (_, ctx))| (x, ctx))
        .collect();

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
        let mut candidate_bad_properties = HashMap::new();
        for (i, gp) in candidates.iter().map(|i| {
            (*i, candidate_gps.get(i).unwrap())
        }) {
            let (bad_props, _) = bad_properties(gp, use_vars.as_ref());
            candidate_bad_properties.insert(i, bad_props);
        }

        let c = *candidates
            .iter()
            .min_by(|t1, t2| {
                strictly_before(t1, t2, &visited, &candidate_gps, &candidate_bad_properties)
            })
            .unwrap();

        candidates.remove(&c);
        let gp = *candidate_gps.get(&c).unwrap();
        let gpvars = variables(gp);
        visited.extend(variables(gp));
        if let Some(use_vars) = &mut use_vars {
            use_vars.extend(gpvars)
        } else {
            use_vars = Some(gpvars);
        }
        let ctx = candidate_contexts.remove(&c).unwrap();
        ordering.push((gp, ctx));
    }
    ordering
}

// Metaphor here is that quantity is cost to include, so less is better.
fn strictly_before(
    gp1: &usize,
    gp2: &usize,
    visited: &HashSet<String>,
    candidate_gps: &HashMap<usize, &GraphPattern>,
    candidate_bad_properties: &HashMap<usize, BadGraphPatternProperties>,
) -> Ordering {
    let t1_connected = is_connected(candidate_gps.get(gp1).unwrap(), visited);
    let t2_connected = is_connected(candidate_gps.get(gp2).unwrap(), visited);
    if t1_connected && !t2_connected {
        return Ordering::Less;
    }
    if !t1_connected && t2_connected {
        return Ordering::Greater;
    }
    let bp1 = candidate_bad_properties.get(gp1).unwrap();
    let bp2 = candidate_bad_properties.get(gp2).unwrap();
    bp1.partial_cmp(bp2).unwrap_or(Ordering::Equal)
}

fn is_connected(gp: &GraphPattern, visited: &HashSet<String>) -> bool {
    let gp_vars = variables(gp);
    for v in &gp_vars {
        if visited.contains(v.as_str()) {
            return true;
        }
    }
    false
}

fn variables(gp: &GraphPattern) -> HashSet<String> {
    let mut vs = HashSet::new();
    gp.on_in_scope_variable(|x| {
        vs.insert(x.as_str().to_string());
    });
    vs
}

fn bad_properties(
    gp: &GraphPattern,
    incoming_cols: Option<&HashSet<String>>,
) -> (BadGraphPatternProperties, HashSet<String>) {
    let res = match gp {
        GraphPattern::Bgp { patterns } => {
            let mut n_variable_predicates = 0;
            let mut triples_cols = HashSet::new();
            let mut patterns_to_process = Vec::from_iter(patterns.iter());
            let mut n_cross_joins = 0usize;

            for tp in patterns {
                if let NamedNodePattern::Variable(..) = tp.predicate {
                    n_variable_predicates += 1;
                }
            }
            while !patterns_to_process.is_empty() {
                let mut connected = None;
                for (i, tp) in patterns_to_process.iter().enumerate() {
                    let variables = find_used_variables_and_blanks(*tp);
                    if !triples_cols.is_disjoint(&variables) {
                        connected = Some(i);
                        triples_cols.extend(variables);
                        break;
                    } else if let Some(incoming_cols) = incoming_cols {
                        if !incoming_cols.is_disjoint(&variables) {
                            connected = Some(i);
                            triples_cols.extend(variables);
                            break;
                        }
                    }

                    if i == patterns_to_process.len() - 1 {
                        connected = Some(i);
                        if incoming_cols.is_some() || !triples_cols.is_empty() {
                            n_cross_joins += 1;
                        }
                        triples_cols.extend(variables);
                    }
                }
                if let Some(i) = connected {
                    patterns_to_process.remove(i);
                }
            }

            if let Some(incoming_vars) = incoming_cols {
                triples_cols.extend(incoming_vars.iter().cloned());
            }
            (
                BadGraphPatternProperties {
                    n_cross_joins,
                    n_variable_predicates,
                },
                triples_cols,
            )
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            let mut variables: HashSet<_> = HashSet::new();

            if let TermPattern::Variable(v) = subject {
                variables.insert(v.as_str().to_string());
            } else if let TermPattern::BlankNode(b) = subject {
                variables.insert(b.to_string());
            }
            if let TermPattern::Variable(v) = object {
                variables.insert(v.as_str().to_string());
            } else if let TermPattern::BlankNode(b) = object {
                variables.insert(b.to_string());
            }

            let n_cross_joins = if let Some(incoming_vars) = &incoming_cols {
                if incoming_vars.is_disjoint(&variables) {
                    1
                } else {
                    0
                }
            } else {
                0
            };
            (
                BadGraphPatternProperties {
                    n_cross_joins,
                    n_variable_predicates: 0,
                },
                variables,
            )
        }
        GraphPattern::Join { left, right } => {
            // Check left first
            let (mut left_properties_left_first, left_first_vars) =
                bad_properties(left, incoming_cols);
            let mut after_left_first_vars = left_first_vars.clone();
            if let Some(incoming_cols) = incoming_cols {
                after_left_first_vars.extend(incoming_cols.iter().cloned());
            }
            let (right_properties_left_first, mut left_first_updated_vars) =
                bad_properties(right, Some(&after_left_first_vars));
            left_properties_left_first.with_additional(right_properties_left_first);
            left_first_updated_vars.extend(left_first_vars);

            // Then check right
            let (mut right_properties_right_first, right_first_vars) =
                bad_properties(right, incoming_cols);
            let mut after_right_first_vars = right_first_vars.clone();
            if let Some(incoming_cols) = incoming_cols {
                after_right_first_vars.extend(incoming_cols.iter().cloned());
            }
            let (left_properties_right_first, mut right_first_updated_vars) =
                bad_properties(left, Some(&after_right_first_vars));
            right_properties_right_first.with_additional(left_properties_right_first);
            right_first_updated_vars.extend(right_first_vars);

            if left_properties_left_first < right_properties_right_first {
                (left_properties_left_first, left_first_updated_vars)
            } else {
                (right_properties_right_first, right_first_updated_vars)
            }
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            let (mut left_bp, mut left_vs) = bad_properties(left, incoming_cols);
            let (right_bp, right_vs) = bad_properties(right, None);
            left_bp.with_additional(right_bp);
            left_vs.extend(right_vs);
            (left_bp, left_vs)
        }
        GraphPattern::Filter { inner, .. } => bad_properties(inner, incoming_cols),
        GraphPattern::Union { left, right } => {
            let (mut left_p, mut left_vars) = bad_properties(left, incoming_cols);
            let (right_p, right_vars) = bad_properties(right, incoming_cols);
            left_p.with_additional(right_p);
            left_vars.extend(right_vars);
            (left_p, left_vars)
        }
        GraphPattern::Graph { name: _name, inner } => bad_properties(inner, incoming_cols),
        GraphPattern::Minus { left, right } => {
            let (mut left_properties, left_vars) = bad_properties(left, incoming_cols);
            let (right_properties, _) = bad_properties(right, incoming_cols);
            left_properties.with_additional(right_properties);
            (left_properties, left_vars)
        }
        GraphPattern::Values { variables, .. } | GraphPattern::PValues { variables, .. } => {
            let mut variables: HashSet<_> =
                variables.iter().map(|x| x.as_str().to_string()).collect();
            let n_cross_joins = if let Some(incoming_vars) = incoming_cols {
                if incoming_vars.is_disjoint(&variables) {
                    1
                } else {
                    0
                }
            } else {
                0
            };
            if let Some(incoming_vars) = incoming_cols {
                variables.extend(incoming_vars.iter().cloned());
            }
            (
                BadGraphPatternProperties {
                    n_cross_joins,
                    n_variable_predicates: 0,
                },
                variables,
            )
        }
        GraphPattern::Project { inner, variables } => {
            let (bp, _) = bad_properties(inner, None);
            let variables: HashSet<_> = variables.iter().map(|x| x.as_str().to_string()).collect();
            (bp, variables)
        }
        GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Reduced { inner, .. }
        | GraphPattern::Distinct { inner, .. } => bad_properties(inner, incoming_cols),
        GraphPattern::Extend {
            inner, variable, ..
        } => {
            let (bad, mut vs) = bad_properties(inner, incoming_cols);
            vs.insert(variable.as_str().to_string());
            (bad, vs)
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let (bp, _) = bad_properties(inner, None);
            let mut variables: HashSet<_> =
                variables.iter().map(|x| x.as_str().to_string()).collect();
            for (v, _) in aggregates {
                variables.insert(v.as_str().to_string());
            }
            (bp, variables)
        }
        GraphPattern::Service { .. } => {
            todo!()
        }
    };
    res
}
