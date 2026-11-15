use super::path::matches_zero_length;
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use std::cmp::{Ordering, Reverse};
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::graph_patterns::join_with_unbound;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Debug, Clone)]
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

impl PartialOrd for BadGraphPatternProperties {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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

impl Eq for BadGraphPatternProperties {}

impl PartialEq<Self> for BadGraphPatternProperties {
    fn eq(&self, other: &Self) -> bool {
        self.n_cross_joins == other.n_cross_joins
            && self.n_variable_predicates == other.n_variable_predicates
    }
}

impl Ord for BadGraphPatternProperties {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.lt(other) {
            Ordering::Less
        } else if self.gt(other) {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
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
        // VALUES with UNDEF are joined last, as unbound values are compatible with anything.
        // Evaluating them first would constrain the other patterns with nulls.
        let (mut ordered, with_undef): (Vec<_>, Vec<_>) =
            ordered_flat_gps.into_iter().partition(|(gp, _)| {
                !matches!(gp, GraphPattern::Values { bindings, .. }
                    if bindings.iter().any(|row| row.iter().any(|x| x.is_none())))
            });
        ordered.extend(with_undef);
        // SERVICE is evaluated after the local patterns, so that the solutions bound by them can
        // be sent with it (a bind join), and SERVICE ?var has ?var bound. Services with a
        // variable endpoint come after those with a fixed endpoint, which may bind it.
        // Path searches take their sources and targets from the other patterns of the group
        // (services included), so they are joined last.
        let (rest, path_searches): (Vec<_>, Vec<_>) = ordered
            .into_iter()
            .partition(|(gp, _)| !super::path_search::is_path_search_pattern(gp));
        let (local, mut services): (Vec<_>, Vec<_>) = rest
            .into_iter()
            .partition(|(gp, _)| !matches!(gp, GraphPattern::Service { .. }));
        services.sort_by_key(|(gp, _)| {
            matches!(
                gp,
                GraphPattern::Service {
                    name: NamedNodePattern::Variable(_),
                    ..
                }
            )
        });
        let mut ordered = local;
        ordered.extend(services);
        // A pattern that may leave a variable unbound which other patterns of the group, or the
        // incoming solution mappings, also use is evaluated on its own and joined afterwards, as
        // the unbound variable is compatible with any value. Passing the other solutions into it
        // would also join them into the inner side of an OPTIONAL or a UNION branch.
        let mut used_vars: HashMap<String, usize> = HashMap::new();
        if let Some(sm) = &solution_mappings {
            for v in sm.rdf_node_types.keys() {
                *used_vars.entry(v.clone()).or_default() += 1;
            }
        }
        for (gp, _) in &ordered {
            for v in in_scope_variable_names(gp) {
                *used_vars.entry(v).or_default() += 1;
            }
        }
        // VALUES handles its own UNDEF values when it is joined with the incoming solutions.
        let (ordered, deferred): (Vec<_>, Vec<_>) = ordered.into_iter().partition(|(gp, _)| {
            let own = in_scope_variable_names(gp);
            matches!(gp, GraphPattern::Values { .. })
                || !maybe_unbound_variables(gp)
                    .iter()
                    .any(|v| own.contains(v) && used_vars.get(v).copied().unwrap_or(0) > 1)
        });
        let mut output_maybe_unbound = HashSet::new();
        for (gp, _) in &ordered {
            output_maybe_unbound.extend(maybe_unbound_variables(gp));
        }
        let mut output_solution_mappings = solution_mappings;
        for (gp, ctx) in ordered {
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
        for (gp, ctx) in deferred {
            let sm = self.lazy_graph_pattern(
                gp,
                None,
                &ctx,
                parameters,
                pushdowns.clone(),
                query_settings,
                dataset,
            )?;
            let maybe_unbound = maybe_unbound_variables(gp);
            output_solution_mappings = Some(if let Some(output) = output_solution_mappings {
                join_with_unbound(
                    output,
                    &output_maybe_unbound,
                    sm,
                    &maybe_unbound,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?
            } else {
                sm
            });
            output_maybe_unbound.extend(maybe_unbound);
        }
        for (gp, ctx) in path_searches {
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

fn flatten_join_gps(gp: &GraphPattern, context: Context) -> Vec<(&GraphPattern, Context)> {
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

fn in_scope_variable_names(gp: &GraphPattern) -> HashSet<String> {
    let mut vs = HashSet::new();
    gp.on_in_scope_variable(|v| {
        vs.insert(v.as_str().to_string());
    });
    vs
}

/// The in-scope variables of a pattern that may be unbound in some of its solutions.
/// Variables bound by BIND are left out, although an expression error leaves them unbound.
pub(crate) fn maybe_unbound_variables(gp: &GraphPattern) -> HashSet<String> {
    match gp {
        GraphPattern::Bgp { .. }
        | GraphPattern::Path { .. }
        | GraphPattern::PValues { .. }
        | GraphPattern::Service { .. } => HashSet::new(),
        GraphPattern::Join { left, right } => {
            let mut certain = certainly_bound_variables(left);
            certain.extend(certainly_bound_variables(right));
            let mut maybe = maybe_unbound_variables(left);
            maybe.extend(maybe_unbound_variables(right));
            maybe.retain(|v| !certain.contains(v));
            maybe
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            let certain = certainly_bound_variables(left);
            let mut maybe = maybe_unbound_variables(left);
            maybe.extend(
                in_scope_variable_names(right)
                    .into_iter()
                    .filter(|v| !certain.contains(v)),
            );
            maybe
        }
        GraphPattern::Union { left, right } => {
            let left_vars = in_scope_variable_names(left);
            let right_vars = in_scope_variable_names(right);
            let mut maybe = maybe_unbound_variables(left);
            maybe.extend(maybe_unbound_variables(right));
            maybe.extend(left_vars.symmetric_difference(&right_vars).cloned());
            maybe
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => variables
            .iter()
            .enumerate()
            .filter(|(i, _)| bindings.iter().any(|row| row[*i].is_none()))
            .map(|(_, v)| v.as_str().to_string())
            .collect(),
        GraphPattern::Project { inner, variables } => {
            let mut maybe = maybe_unbound_variables(inner);
            maybe.retain(|v| variables.iter().any(|x| x.as_str() == v));
            maybe
        }
        GraphPattern::Group {
            inner, variables, ..
        } => {
            let mut maybe = maybe_unbound_variables(inner);
            maybe.retain(|v| variables.iter().any(|x| x.as_str() == v));
            maybe
        }
        GraphPattern::Minus { left: inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => maybe_unbound_variables(inner),
        #[allow(unreachable_patterns)]
        _ => HashSet::new(),
    }
}

fn certainly_bound_variables(gp: &GraphPattern) -> HashSet<String> {
    let maybe = maybe_unbound_variables(gp);
    let mut vs = in_scope_variable_names(gp);
    vs.retain(|v| !maybe.contains(v));
    vs
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

#[derive(Clone)]
struct JoinOrdering {
    variables: Option<HashSet<String>>,
    set: HashSet<usize>,
    ordering: Vec<usize>,
    bad_properties: BadGraphPatternProperties,
}

impl JoinOrdering {
    pub fn new(
        variables: Option<HashSet<String>>,
        ordering: Vec<usize>,
        bad_properties: BadGraphPatternProperties,
    ) -> Self {
        JoinOrdering {
            variables,
            set: ordering.iter().cloned().collect(),
            ordering,
            bad_properties,
        }
    }

    pub fn extend_all(
        &self,
        candidates: &Vec<usize>,
        candidates_gp_map: &HashMap<usize, &GraphPattern>,
    ) -> Vec<Self> {
        let mut extensions = Vec::new();
        for c in candidates {
            if !self.set.contains(c) {
                let gp = candidates_gp_map.get(c).unwrap();
                let (mut bp, mut vars) = bad_properties(gp, self.variables.as_ref());
                if let Some(self_vars) = &self.variables {
                    vars.extend(self_vars.iter().cloned());
                }
                bp.with_additional(self.bad_properties.clone());
                let mut ordering = self.ordering.clone();
                ordering.push(*c);
                let jo = JoinOrdering::new(Some(vars), ordering, bp);
                extensions.push(jo);
            }
        }
        extensions
    }
}

impl PartialOrd<Self> for JoinOrdering {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JoinOrdering {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bad_properties.cmp(&other.bad_properties)
    }
}

impl Eq for JoinOrdering {}

impl PartialEq for JoinOrdering {
    fn eq(&self, other: &Self) -> bool {
        self.ordering == other.ordering
    }
}

pub fn order_graph_patterns<'a>(
    gps: Vec<(&'a GraphPattern, Context)>,
    sm: &Option<SolutionMappings>,
) -> Vec<(&'a GraphPattern, Context)> {
    let candidates: Vec<_> = (0..gps.len()).collect();
    let candidate_gps: HashMap<_, _> = gps
        .iter()
        .enumerate()
        .map(|(x, (gp, _))| (x, *gp))
        .collect();

    let use_vars: Option<HashSet<_>> = sm
        .as_ref()
        .map(|sm| sm.rdf_node_types.keys().cloned().collect());

    let mut candidate_contexts: HashMap<_, _> = gps
        .into_iter()
        .enumerate()
        .map(|(x, (_, ctx))| (x, ctx))
        .collect();

    let mut ordering = vec![];

    let mut heap = BinaryHeap::new();
    let jo = JoinOrdering::new(
        use_vars,
        Vec::new(),
        BadGraphPatternProperties {
            n_cross_joins: 0,
            n_variable_predicates: 0,
        },
    );
    heap.push(Reverse(jo));
    let mut found_order = None;
    while found_order.is_none() {
        let next = heap.pop().unwrap().0;
        if next.ordering.len() == candidates.len() {
            found_order = Some(next);
        } else {
            let new = next.extend_all(&candidates, &candidate_gps);
            heap.extend(new.into_iter().map(Reverse));
        }
    }
    let found_order = found_order.unwrap();
    for c in found_order.ordering {
        ordering.push((
            *candidate_gps.get(&c).unwrap(),
            candidate_contexts.remove(&c).unwrap(),
        ));
    }
    ordering
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
                    let variables = find_used_variables_and_blanks(tp);
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
            subject,
            path,
            object,
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

            let disjoint = incoming_cols
                .map(|incoming_vars| incoming_vars.is_disjoint(&variables))
                .unwrap_or(true);
            // A zero length path matches each value its variables are bound to, which it only
            // knows when the patterns binding them are evaluated first.
            let n_cross_joins = if disjoint
                && (incoming_cols.is_some() || (!variables.is_empty() && matches_zero_length(path)))
            {
                1
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
        GraphPattern::Lateral { left, right } => {
            let (mut left_bp, mut left_vs) = bad_properties(left, incoming_cols);
            let (right_bp, right_vs) = bad_properties(right, Some(&left_vs));
            left_bp.with_additional(right_bp);
            left_vs.extend(right_vs);
            (left_bp, left_vs)
        }
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
        GraphPattern::Service { inner, .. } => {
            // Ordered after the local patterns by lazy_join, so it never causes a cross join.
            let mut variables: HashSet<String> = HashSet::new();
            inner.on_in_scope_variable(|v| {
                variables.insert(v.as_str().to_string());
            });
            if let Some(incoming_vars) = incoming_cols {
                variables.extend(incoming_vars.iter().cloned());
            }
            (
                BadGraphPatternProperties {
                    n_cross_joins: 0,
                    n_variable_predicates: 0,
                },
                variables,
            )
        }
    };
    res
}
