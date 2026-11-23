use super::{QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::path::{create_graph_pattern, need_sparse_matrix};
use crate::sparql::lazy_graph_patterns::triples_ordering::order_triple_patterns;
use oxrdf::{Literal, NamedNode, Variable};
use polars::prelude::len;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::{NamedGraph, QueryGraph};
pub(crate) use representation::debug::{DebugOutput, DebugOutputs};
use representation::polars_to_rdf::column_as_terms;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::RDFNodeState;
use spargebra::algebra::{Expression, Function, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::{HashMap, HashSet};

/// How many example terms a hint lists.
const N_EXAMPLES: usize = 3;
/// How many predicates or graphs a hint lists.
const N_NAMES: usize = 5;
/// The variables that replace constants of the query in the patterns the hints evaluate.
const HINT_SUBJECT: &str = "maplib_debug_s";
const HINT_PREDICATE: &str = "maplib_debug_p";
const HINT_OBJECT: &str = "maplib_debug_o";

pub enum PartialDebugOutput {
    PartialResults,
    DebugOutputs(Vec<DebugOutput>),
}

type Parameters<'a> = Option<&'a HashMap<String, EagerSolutionMappings>>;

impl Triplestore {
    pub fn debug(
        &self,
        q: &Query,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        qs: &QuerySettings,
        graph: Option<&NamedGraph>,
    ) -> Result<DebugOutputs, SparqlError> {
        match q {
            Query::Select {
                dataset, pattern, ..
            }
            | Query::Construct {
                dataset, pattern, ..
            }
            | Query::Ask {
                dataset, pattern, ..
            }
            | Query::Describe {
                dataset, pattern, ..
            } => {
                let qg = &self.query_graph(dataset, graph, qs);
                let partial = self.debug_gp(pattern, parameters, qs, qg)?;
                let dbg = match partial {
                    // This is only called for queries without solution mappings.
                    PartialDebugOutput::PartialResults => vec![DebugOutput::NoExplanation],
                    PartialDebugOutput::DebugOutputs(v) => v,
                };
                Ok(DebugOutputs::new(dbg))
            }
        }
    }

    fn debug_gp(
        &self,
        gp: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        match gp {
            GraphPattern::Bgp { patterns } => self.debug_patterns(patterns, qs, qg),
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.debug_path(subject, path, object, qs, qg),
            GraphPattern::Join { left, right } => self.debug_join(left, right, parameters, qs, qg),

            GraphPattern::Filter { expr, inner } => {
                self.debug_filter(expr, inner, parameters, qs, qg)
            }
            GraphPattern::Union { left, right } => {
                self.debug_union(left, right, parameters, qs, qg)
            }
            GraphPattern::Minus { left, right } => {
                self.debug_minus(left, right, parameters, qs, qg)
            }
            GraphPattern::Values { .. } | GraphPattern::PValues { .. } => {
                self.debug_whole_gp(gp, parameters, qs, qg)
            }
            GraphPattern::Graph { name, inner } => match name {
                NamedNodePattern::NamedNode(nn) => {
                    let qg = QueryGraph::NamedGraph(NamedGraph::from_maybe_named_node(Some(nn)));
                    self.debug_gp(inner, parameters, qs, &qg)
                }
                // The inner pattern is matched against every named graph, so there is no
                // single graph to explain it in. Report the GRAPH pattern as a whole.
                NamedNodePattern::Variable(_) => self.debug_whole_gp(gp, parameters, qs, qg),
            },
            // These have solution mappings whenever the inner pattern has.
            GraphPattern::Extend { inner, .. }
            | GraphPattern::LeftJoin { left: inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner, .. }
            | GraphPattern::Reduced { inner, .. }
            | GraphPattern::Group { inner, .. } => self.debug_gp(inner, parameters, qs, qg),
            // The right side of LATERAL and the OFFSET of a slice can leave nothing of an
            // inner pattern with solution mappings.
            GraphPattern::Lateral { left: inner, .. } | GraphPattern::Slice { inner, .. } => {
                match self.debug_gp(inner, parameters, qs, qg)? {
                    PartialDebugOutput::PartialResults => {
                        self.debug_whole_gp(gp, parameters, qs, qg)
                    }
                    dbg => Ok(dbg),
                }
            }
            // The remote endpoint is not asked why it has no results, only whether it has any.
            GraphPattern::Service { .. } => self.debug_whole_gp(gp, parameters, qs, qg),
        }
    }

    fn debug_eval(
        &self,
        gp: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        self.lazy_graph_pattern(
            gp,
            None,
            &Context::new(),
            parameters,
            Pushdowns::new(),
            qs,
            qg,
        )
    }

    fn has_no_results(
        &self,
        gp: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<bool, SparqlError> {
        let sm = self.debug_eval(gp, parameters, qs, qg)?;
        let df = sm
            .mappings
            .first()
            .collect()
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        Ok(df.height() == 0)
    }

    /// Evaluates the pattern as a whole and reports it when it has no solution mappings.
    fn debug_whole_gp(
        &self,
        gp: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        if self.has_no_results(gp, parameters, qs, qg)? {
            Ok(PartialDebugOutput::DebugOutputs(vec![
                DebugOutput::NoResultsGraphPattern(gp.clone()),
            ]))
        } else {
            Ok(PartialDebugOutput::PartialResults)
        }
    }

    fn debug_patterns(
        &self,
        patterns: &Vec<TriplePattern>,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        if patterns.is_empty() {
            return Ok(PartialDebugOutput::PartialResults);
        }
        // First check that the triple patterns have results individually
        let mut errs = vec![];
        for p in patterns {
            if self.has_no_results(&bgp(vec![p.clone()]), None, qs, qg)? {
                let hints = best_effort(self.triple_pattern_hints(p, qs, qg))?;
                errs.push(DebugOutput::NoResultsTriplePattern(p.clone(), hints));
            }
        }
        if !errs.is_empty() {
            Ok(PartialDebugOutput::DebugOutputs(errs))
        } else {
            // Check that triple patterns have results in unison
            let mut processed_patterns = vec![];
            let patterns = order_triple_patterns(patterns, &None, &Pushdowns::new());
            for p in patterns {
                processed_patterns.push(p);
                if self.has_no_results(&bgp(processed_patterns.clone()), None, qs, qg)? {
                    let bad_pattern = processed_patterns.pop().unwrap();
                    let hints = best_effort(self.join_hints(
                        &bgp(processed_patterns.clone()),
                        &bgp(vec![bad_pattern.clone()]),
                        None,
                        qs,
                        qg,
                    ))?;
                    return Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsJoiningTriplePattern(
                            processed_patterns,
                            bad_pattern,
                            hints,
                        ),
                    ]));
                }
            }
            Ok(PartialDebugOutput::PartialResults)
        }
    }

    fn debug_path(
        &self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let needs_sparse = need_sparse_matrix(ppe);
        if needs_sparse {
            let gp = GraphPattern::Path {
                subject: subject.clone(),
                path: ppe.clone(),
                object: object.clone(),
            };
            if self.has_no_results(&gp, None, qs, qg)? {
                Ok(PartialDebugOutput::DebugOutputs(vec![
                    DebugOutput::NoResultsPath(ppe.clone()),
                ]))
            } else {
                Ok(PartialDebugOutput::PartialResults)
            }
        } else {
            let mut intermediaries = vec![];
            let gp = create_graph_pattern(ppe, subject, object, &mut intermediaries);
            self.debug_gp(&gp, None, qs, qg)
        }
    }

    fn debug_filter(
        &self,
        expression: &Expression,
        inner: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg = self.debug_gp(inner, parameters, qs, qg)?;
        match dbg {
            PartialDebugOutput::PartialResults => {
                if self.has_no_results(&filter(expression, inner), parameters, qs, qg)? {
                    let hints =
                        best_effort(self.filter_hints(expression, inner, parameters, qs, qg))?;
                    Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsFilter(inner.clone(), expression.clone(), hints),
                    ]))
                } else {
                    Ok(PartialDebugOutput::PartialResults)
                }
            }
            dbg => Ok(dbg),
        }
    }

    fn debug_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, parameters, qs, qg)?;
        match dbg_left {
            PartialDebugOutput::PartialResults => {
                let dbg_right = self.debug_gp(right, parameters, qs, qg)?;
                match dbg_right {
                    PartialDebugOutput::PartialResults => {
                        let join = GraphPattern::Join {
                            left: Box::new(left.clone()),
                            right: Box::new(right.clone()),
                        };
                        if self.has_no_results(&join, parameters, qs, qg)? {
                            let hints =
                                best_effort(self.join_hints(left, right, parameters, qs, qg))?;
                            Ok(PartialDebugOutput::DebugOutputs(vec![
                                DebugOutput::NoResultsJoin(left.clone(), right.clone(), hints),
                            ]))
                        } else {
                            Ok(PartialDebugOutput::PartialResults)
                        }
                    }
                    dbg_right => Ok(dbg_right),
                }
            }
            dbg_left => Ok(dbg_left),
        }
    }

    fn debug_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, parameters, qs, qg)?;
        match dbg_left {
            PartialDebugOutput::PartialResults => {
                let minus = GraphPattern::Minus {
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                };
                if self.has_no_results(&minus, parameters, qs, qg)? {
                    Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsMinus(left.clone(), right.clone()),
                    ]))
                } else {
                    Ok(PartialDebugOutput::PartialResults)
                }
            }
            dbg_left => Ok(dbg_left),
        }
    }

    fn debug_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, parameters, qs, qg)?;

        if let PartialDebugOutput::DebugOutputs(mut left_dbg) = dbg_left {
            let dbg_right = self.debug_gp(right, parameters, qs, qg)?;

            if let PartialDebugOutput::DebugOutputs(right_dbg) = dbg_right {
                left_dbg.extend(right_dbg);
                return Ok(PartialDebugOutput::DebugOutputs(left_dbg));
            }
        }
        Ok(PartialDebugOutput::PartialResults)
    }

    /// What is known about a triple pattern without solution mappings: which of its constant
    /// terms the graph does not have, and what the graph has in their place.
    fn triple_pattern_hints(
        &self,
        tp: &TriplePattern,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<Vec<String>, SparqlError> {
        let mut hints = vec![];
        let any_subject = TermPattern::Variable(Variable::new_unchecked(HINT_SUBJECT));
        let any_predicate = NamedNodePattern::Variable(Variable::new_unchecked(HINT_PREDICATE));
        let any_object = TermPattern::Variable(Variable::new_unchecked(HINT_OBJECT));
        let pattern = |s: &TermPattern, p: &NamedNodePattern, o: &TermPattern| {
            bgp(vec![TriplePattern {
                subject: s.clone(),
                predicate: p.clone(),
                object: o.clone(),
            }])
        };
        let subject_is_constant = is_constant(&tp.subject);
        let object_is_constant = is_constant(&tp.object);

        if let NamedNodePattern::NamedNode(p) = &tp.predicate {
            let predicate_only = pattern(&any_subject, &tp.predicate, &any_object);
            if self.has_no_results(&predicate_only, None, qs, qg)? {
                hints.push(format!(
                    "The predicate {p} is not used in the queried {}",
                    graphs_word(qg)
                ));
                let elsewhere = self.other_graphs_with_predicate(p, qg);
                if !elsewhere.is_empty() {
                    hints.push(format!(
                        "It is used in {}",
                        list_names(elsewhere.iter().map(graph_name).collect())
                    ));
                }
                let similar = self.predicates_with_local_name(p, qg);
                if !similar.is_empty() {
                    hints.push(format!(
                        "Predicates with the same local name: {}",
                        list_names(similar.iter().map(|x| x.to_string()).collect())
                    ));
                }
                return Ok(hints);
            }
        }

        if !matches!(tp.object, TermPattern::Literal(_))
            && tp.subject != tp.object
            && (subject_is_constant || object_is_constant)
        {
            let reversed = pattern(&tp.object, &tp.predicate, &tp.subject);
            if !self.has_no_results(&reversed, None, qs, qg)? {
                hints.push(
                    "There are solution mappings when the subject and the object change places"
                        .to_string(),
                );
            }
        }

        let mut subject_exists = false;
        if subject_is_constant {
            let subject_only = pattern(&tp.subject, &any_predicate, &any_object);
            subject_exists = !self.has_no_results(&subject_only, None, qs, qg)?;
            if !subject_exists {
                hints.push(format!(
                    "{} is not the subject of any triple in the queried {}",
                    tp.subject,
                    graphs_word(qg)
                ));
            }
        }

        if object_is_constant {
            // Terms that are written as the object is, but are not the same term: a literal
            // with another datatype or language tag, or an IRI where the object is a string.
            let o = Variable::new_unchecked(HINT_OBJECT);
            let same_text = Expression::And(
                Box::new(Expression::Equal(
                    Box::new(Expression::FunctionCall(
                        Function::Str,
                        vec![Expression::Variable(o.clone())],
                    )),
                    Box::new(Expression::Literal(Literal::new_simple_literal(term_text(
                        &tp.object,
                    )))),
                )),
                Box::new(Expression::Not(Box::new(Expression::SameTerm(
                    Box::new(Expression::Variable(o.clone())),
                    Box::new(term_expression(&tp.object)),
                )))),
            );
            let look_alikes = self.example_terms(
                &filter(
                    &same_text,
                    &pattern(&any_subject, &tp.predicate, &any_object),
                ),
                &o,
                qs,
                qg,
            )?;
            if !look_alikes.is_empty() {
                hints.push(format!(
                    "The object is written like these objects of the predicate, which are other terms: {}",
                    look_alikes.join(", ")
                ));
            }
            if subject_is_constant && subject_exists {
                let others = self.example_terms(
                    &pattern(&tp.subject, &tp.predicate, &any_object),
                    &o,
                    qs,
                    qg,
                )?;
                if others.is_empty() {
                    if matches!(tp.predicate, NamedNodePattern::NamedNode(_)) {
                        hints.push(format!("{} has no triples with the predicate", tp.subject));
                    }
                } else {
                    hints.push(format!(
                        "The objects of the subject and the predicate are instead, for example: {}",
                        others.join(", ")
                    ));
                }
            }
            if subject_is_constant
                && self.has_no_results(
                    &pattern(&any_subject, &tp.predicate, &tp.object),
                    None,
                    qs,
                    qg,
                )?
            {
                hints.push(format!(
                    "No triple{} has the object {}",
                    if matches!(tp.predicate, NamedNodePattern::NamedNode(_)) {
                        " with the predicate"
                    } else {
                        ""
                    },
                    tp.object
                ));
            } else if !subject_is_constant && look_alikes.is_empty() {
                let others = self.example_terms(
                    &pattern(&any_subject, &tp.predicate, &any_object),
                    &o,
                    qs,
                    qg,
                )?;
                if !others.is_empty() {
                    hints.push(format!(
                        "The objects of the predicate are instead, for example: {}",
                        others.join(", ")
                    ));
                }
            }
        }
        Ok(hints)
    }

    /// The first distinct terms of a variable of a graph pattern, written as in SPARQL.
    fn example_terms(
        &self,
        gp: &GraphPattern,
        variable: &Variable,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<Vec<String>, SparqlError> {
        let gp = GraphPattern::Slice {
            inner: Box::new(GraphPattern::Distinct {
                inner: Box::new(GraphPattern::Project {
                    inner: Box::new(gp.clone()),
                    variables: vec![variable.clone()],
                }),
            }),
            start: 0,
            length: Some(N_EXAMPLES),
        };
        let SolutionMappings {
            mappings,
            rdf_node_types,
            ..
        } = self.debug_eval(&gp, None, qs, qg)?;
        let df = mappings
            .collect()
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        let (Ok(column), Some(t)) = (
            df.column(variable.as_str()),
            rdf_node_types.get(variable.as_str()),
        ) else {
            return Ok(vec![]);
        };
        Ok(column_as_terms(column, t, self.global_cats.clone(), false)
            .into_iter()
            .flatten()
            .map(|t| t.to_string())
            .collect())
    }

    /// The graphs that are not queried, but have triples with the predicate.
    fn other_graphs_with_predicate(&self, p: &NamedNode, qg: &QueryGraph) -> Vec<NamedGraph> {
        let queried = queried_graphs(qg);
        let mut out: Vec<NamedGraph> = self
            .graph_triples_map
            .iter()
            .filter(|(g, m)| {
                !queried.contains(g) && m.get(p).is_some_and(|tables| !tables.is_empty())
            })
            .map(|(g, _)| g.clone())
            .collect();
        out.sort_by_key(graph_name);
        out
    }

    /// The other predicates of the queried graphs whose IRI ends with the same name, in any
    /// case: the predicate that is meant when a namespace or the case of a letter is wrong.
    fn predicates_with_local_name(&self, p: &NamedNode, qg: &QueryGraph) -> Vec<NamedNode> {
        let name = local_name(p.as_str()).to_lowercase();
        let mut out: Vec<NamedNode> = vec![];
        if name.is_empty() {
            return out;
        }
        for g in queried_graphs(qg) {
            if let Some(m) = self.graph_triples_map.get(&g) {
                for (other, tables) in m.iter() {
                    if other != p
                        && !tables.is_empty()
                        && local_name(other.as_str()).to_lowercase() == name
                        && !out.contains(other)
                    {
                        out.push(other.clone());
                    }
                }
            }
        }
        out.sort();
        out
    }

    /// Why two graph patterns that each have solution mappings have none together: the
    /// variables they join on, and whether these have terms of the same kinds on both sides.
    fn join_hints(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<Vec<String>, SparqlError> {
        let left_types = self.debug_eval(left, parameters, qs, qg)?.rdf_node_types;
        let right_types = self.debug_eval(right, parameters, qs, qg)?.rdf_node_types;
        let mut shared: Vec<&String> = left_types
            .keys()
            .filter(|v| right_types.contains_key(*v))
            .collect();
        shared.sort();
        let mut hints = vec![];
        let mut mismatch = false;
        for v in &shared {
            let l = &left_types[*v];
            let r = &right_types[*v];
            if !l.map.keys().any(|t| r.map.contains_key(t)) {
                mismatch = true;
                hints.push(format!(
                    "?{v} is never the same term in both: it is {} in the first and {} in the second",
                    type_names(l),
                    type_names(r)
                ));
            }
        }
        if !mismatch && !shared.is_empty() {
            let names: Vec<String> = shared.iter().map(|v| format!("?{v}")).collect();
            hints.push(if names.len() == 1 {
                format!("They have no term of {} in common", names[0])
            } else {
                format!(
                    "They have no combination of terms of {} in common",
                    names.join(", ")
                )
            });
        }
        Ok(hints)
    }

    /// Why a filter leaves nothing of a graph pattern with solution mappings: how many it had,
    /// the kinds of terms of the variables of the expression, and which of the conditions of
    /// a conjunction are never true.
    fn filter_hints(
        &self,
        expression: &Expression,
        inner: &GraphPattern,
        parameters: Parameters,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<Vec<String>, SparqlError> {
        let mut hints = vec![];
        let SolutionMappings {
            mappings,
            rdf_node_types,
            ..
        } = self.debug_eval(inner, parameters, qs, qg)?;
        let count = mappings
            .select([len()])
            .collect()
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        if let Some(n) = count
            .columns()
            .first()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<u64>())
        {
            hints.push(format!(
                "The graph pattern has {n} solution mapping{}, the expression is true for none",
                if n == 1 { "" } else { "s" }
            ));
        }

        let mut used = HashSet::new();
        find_all_used_variables_in_expression(expression, &mut used, false, true);
        let mut used: Vec<&str> = used.iter().map(|v| v.as_str()).collect();
        used.sort();
        let mut types = vec![];
        for v in used {
            match rdf_node_types.get(v) {
                Some(t) => types.push(format!("?{v} is {}", type_names(t))),
                None => types.push(format!("?{v} is not bound by the graph pattern")),
            }
        }
        if !types.is_empty() {
            hints.push(types.join(", "));
        }

        let mut conditions = vec![];
        conjuncts(expression, &mut conditions);
        if conditions.len() > 1 {
            let mut never_true = vec![];
            for c in conditions {
                if self.has_no_results(&filter(c, inner), parameters, qs, qg)? {
                    never_true.push(c.to_string());
                }
            }
            if never_true.is_empty() {
                hints.push(
                    "Each condition of the expression is true for some solution mappings, but not all of them for the same one"
                        .to_string(),
                );
            } else {
                hints.push(format!(
                    "Never true on its own: {}",
                    never_true.join(" and: ")
                ));
            }
        }
        Ok(hints)
    }
}

/// Hints are extras: when finding them fails, the explanation is given without them.
fn best_effort(hints: Result<Vec<String>, SparqlError>) -> Result<Vec<String>, SparqlError> {
    match hints {
        Err(SparqlError::InterruptSignal) => Err(SparqlError::InterruptSignal),
        Err(SparqlError::Cancelled(c)) => Err(SparqlError::Cancelled(c)),
        Err(_) => Ok(vec![]),
        ok => ok,
    }
}

fn bgp(patterns: Vec<TriplePattern>) -> GraphPattern {
    GraphPattern::Bgp { patterns }
}

fn filter(expr: &Expression, inner: &GraphPattern) -> GraphPattern {
    GraphPattern::Filter {
        expr: expr.clone(),
        inner: Box::new(inner.clone()),
    }
}

fn is_constant(t: &TermPattern) -> bool {
    matches!(t, TermPattern::NamedNode(_) | TermPattern::Literal(_))
}

fn term_text(t: &TermPattern) -> &str {
    match t {
        TermPattern::NamedNode(nn) => nn.as_str(),
        TermPattern::Literal(l) => l.value(),
        _ => "",
    }
}

fn term_expression(t: &TermPattern) -> Expression {
    match t {
        TermPattern::NamedNode(nn) => Expression::NamedNode(nn.clone()),
        TermPattern::Literal(l) => Expression::Literal(l.clone()),
        _ => unreachable!("Only called for constant terms"),
    }
}

fn conjuncts<'a>(e: &'a Expression, out: &mut Vec<&'a Expression>) {
    if let Expression::And(l, r) = e {
        conjuncts(l, out);
        conjuncts(r, out);
    } else {
        out.push(e);
    }
}

fn queried_graphs(qg: &QueryGraph) -> Vec<NamedGraph> {
    match qg {
        QueryGraph::NamedGraph(g) => vec![g.clone()],
        QueryGraph::QueryDataset(qd) => qd
            .default
            .iter()
            .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
            .collect(),
    }
}

fn graphs_word(qg: &QueryGraph) -> &'static str {
    if queried_graphs(qg).len() == 1 {
        "graph"
    } else {
        "graphs"
    }
}

fn graph_name(g: &NamedGraph) -> String {
    match g {
        NamedGraph::DefaultGraph => "the default graph".to_string(),
        NamedGraph::NamedGraph(nn) => format!("the graph {nn}"),
        NamedGraph::BlankNode(b) => format!("the graph {b}"),
    }
}

fn local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/', ':']).next().unwrap_or("")
}

fn list_names(mut names: Vec<String>) -> String {
    let more = names.len().saturating_sub(N_NAMES);
    names.truncate(N_NAMES);
    let mut out = names.join(", ");
    if more > 0 {
        out.push_str(&format!(" and {more} more"));
    }
    out
}

/// The kinds of terms of a variable, e.g. `IRI` or `<http://www.w3.org/2001/XMLSchema#string>`.
fn type_names(t: &RDFNodeState) -> String {
    let mut names: Vec<String> = t.map.keys().map(|x| x.to_string()).collect();
    names.sort();
    names.join(" or ")
}
