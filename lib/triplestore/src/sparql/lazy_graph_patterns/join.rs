use super::Triplestore;
use crate::sparql::errors::SparqlError;
use std::cmp::Ordering;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use query_processing::pushdowns::Pushdowns;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::collections::{HashMap, HashSet};

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
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let use_vars: Option<HashSet<_>> = if let Some(sm) = &solution_mappings {
            Some(sm.rdf_node_types.keys().cloned().collect())
        } else {
            None
        };
        let (left_props, _) = self.bad_properties(left, use_vars.as_ref());
        let (right_props, _) = self.bad_properties(right, use_vars.as_ref());

        let output_solution_mappings = if left_props < right_props {
            let mut output_solution_mappings = self.lazy_graph_pattern(
                left,
                solution_mappings,
                &left_context,
                parameters,
                pushdowns.clone(),
                query_settings,
            )?;
            output_solution_mappings = self.lazy_graph_pattern(
                right,
                Some(output_solution_mappings),
                &right_context,
                parameters,
                pushdowns,
                query_settings,
            )?;
            output_solution_mappings
        } else {
            let mut output_solution_mappings = self.lazy_graph_pattern(
                right,
                solution_mappings,
                &right_context,
                parameters,
                pushdowns.clone(),
                query_settings,
            )?;
            output_solution_mappings = self.lazy_graph_pattern(
                left,
                Some(output_solution_mappings),
                &left_context,
                parameters,
                pushdowns,
                query_settings,
            )?;
            output_solution_mappings
        };

        Ok(output_solution_mappings)
    }

    fn bad_properties(
        &self,
        gp: &GraphPattern,
        incoming_cols: Option<&HashSet<String>>,
    ) -> (BadGraphPatternProperties, HashSet<String>) {
        match gp {
            GraphPattern::Bgp { patterns } => {
                let mut n_variable_predicates = 0;
                let mut variables: Vec<HashSet<_>> = vec![];
                for p in patterns {
                    let mut tp_variables = HashSet::new();
                    if let TermPattern::Variable(v) = &p.subject {
                        tp_variables.insert(v.as_str().to_string());
                    } else if let TermPattern::BlankNode(b) = &p.subject {
                        tp_variables.insert(b.to_string());
                    }
                    if let NamedNodePattern::Variable(v) = &p.predicate {
                        tp_variables.insert(v.as_str().to_string());
                        n_variable_predicates += 1;
                    }
                    if let TermPattern::Variable(v) = &p.object {
                        tp_variables.insert(v.as_str().to_string());
                    } else if let TermPattern::BlankNode(b) = &p.object {
                        tp_variables.insert(b.to_string());
                    }
                    let mut found_matching = false;
                    let mut matching_i = 0;
                    for (i, vs) in &mut variables.iter().enumerate() {
                        if !vs.is_disjoint(&tp_variables) {
                            found_matching = true;
                            matching_i = i;
                            break;
                        }
                    }
                    if found_matching {
                        variables.get_mut(matching_i).unwrap().extend(tp_variables);
                    } else {
                        variables.push(tp_variables);
                    }
                }
                let n_cross_joins = if let Some(incoming_vars) = &incoming_cols {
                    let mut n_cross_joins = 0usize;
                    for vs in &variables {
                        if incoming_vars.is_disjoint(vs) {
                            n_cross_joins += 1;
                        }
                    }
                    n_cross_joins
                } else {
                    variables.len() - 1
                };
                let mut all_variables = HashSet::new();
                for v in variables {
                    all_variables.extend(v);
                }
                if let Some(incoming_vars) = incoming_cols {
                    all_variables.extend(incoming_vars.iter().cloned());
                }
                (
                    BadGraphPatternProperties {
                        n_cross_joins,
                        n_variable_predicates,
                    },
                    all_variables,
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
            GraphPattern::Join { left, right } => {
                let (mut left_properties_left_first, left_first_vars) =
                    self.bad_properties(left, incoming_cols);
                let (right_properties_left_first, left_first_vars) =
                    self.bad_properties(right, Some(&left_first_vars));
                left_properties_left_first.with_additional(right_properties_left_first);

                let (mut right_properties_right_first, right_first_vars) =
                    self.bad_properties(right, incoming_cols);
                let (left_properties_right_first, right_first_vars) =
                    self.bad_properties(left, Some(&right_first_vars));
                right_properties_right_first.with_additional(left_properties_right_first);

                if left_properties_left_first < right_properties_right_first {
                    (left_properties_left_first, left_first_vars)
                } else {
                    (right_properties_right_first, right_first_vars)
                }
            }
            GraphPattern::LeftJoin { left, right, .. } => {
                let (mut left_bp, vs) = self.bad_properties(left, incoming_cols);
                let (right_bp, vs) = self.bad_properties(right, Some(&vs));
                left_bp.with_additional(right_bp);
                (left_bp, vs)
            }
            GraphPattern::Filter { inner, .. } => self.bad_properties(inner, incoming_cols),
            GraphPattern::Union { left, right } => {
                let (mut left_p, mut left_vars) = self.bad_properties(left, incoming_cols);
                let (right_p, right_vars) = self.bad_properties(right, incoming_cols);
                left_p.with_additional(right_p);
                left_vars.extend(right_vars);
                (left_p, left_vars)
            }
            GraphPattern::Graph { .. } => {
                todo!()
            }
            GraphPattern::Minus { left, right } => {
                let (mut left_properties, left_vars) = self.bad_properties(left, incoming_cols);
                let (right_properties, _) = self.bad_properties(right, incoming_cols);
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
                let (bp, _) = self.bad_properties(inner, None);
                let variables: HashSet<_> =
                    variables.iter().map(|x| x.as_str().to_string()).collect();
                (bp, variables)
            }
            GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Reduced { inner, .. }
            | GraphPattern::Distinct { inner, .. }
            | GraphPattern::Extend { inner, .. } => self.bad_properties(inner, incoming_cols),
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let (bp, _) = self.bad_properties(inner, None);
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
            GraphPattern::DT { .. } => {
                unreachable!("Should never happen")
            }
        }
    }
}
