use super::{dataset_or_named_graph, QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::path::{create_graph_pattern, need_sparse_matrix};
use crate::sparql::lazy_graph_patterns::triples_ordering::order_triple_patterns;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::{NamedGraph, QueryGraph};
use representation::query_context::Context;
use representation::solution_mapping::EagerSolutionMappings;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{TermPattern, TriplePattern};
use spargebra::Query;

pub enum PartialDebugOutput {
    PartialResults(EagerSolutionMappings),
    DebugOutputs(Vec<DebugOutput>),
}

pub enum DebugOutput {
    NoResultsTriplePattern(TriplePattern),
    NoResultsJoiningTriplePattern(Vec<TriplePattern>, TriplePattern),
    NoResultsJoin(GraphPattern, GraphPattern),
    NoResultsMinus(GraphPattern, GraphPattern),
    NoResultsFilter(GraphPattern, Expression),
    NoResultsPath(PropertyPathExpression),
    NoResultsGraphPattern(GraphPattern),
}

impl Triplestore {
    pub fn debug(&self, q: &Query, qs: &QuerySettings, graph:Option<&NamedGraph>) -> Result<Vec<DebugOutput>, SparqlError> {

        match q {
            Query::Select {
                dataset,
                pattern,
                ..
            } => {
                let mut pushdowns = Pushdowns::new();
                pushdowns.add_graph_pattern_pushdowns(pattern);
                let qg = &dataset_or_named_graph(dataset, graph);
                let partial = self.debug_gp(pattern, pushdowns, qs, qg)?;
                match partial {
                    PartialDebugOutput::PartialResults(_) => {Ok(vec![])}
                    PartialDebugOutput::DebugOutputs(v) => {Ok(v)}
                }
            }
            _ => todo!()
        }
    }

    fn debug_gp(
        &self,
        gp: &GraphPattern,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        match gp {
            GraphPattern::Bgp { patterns } => self.debug_patterns(patterns, pushdowns, qs, qg),
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.debug_path(subject, path, object, pushdowns, qs, qg),
            GraphPattern::Join { left, right } => self.debug_join(left, right, pushdowns, qs, qg),

            GraphPattern::Filter { expr, inner } => {
                self.debug_filter(expr, inner, pushdowns, qs, qg)
            }
            GraphPattern::Union { left, right } => self.debug_union(left, right, pushdowns, qs, qg),
            GraphPattern::Minus { left, right } => self.debug_minus(left, right, pushdowns, qs, qg),
            GraphPattern::Values { .. } | GraphPattern::PValues { .. } => {
                let sm =
                    self.lazy_graph_pattern(gp, None, &Context::new(), &None, pushdowns, qs, qg)?;
                let sm = sm.as_eager(false);
                if sm.mappings.height() == 0 {
                    Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsGraphPattern(gp.clone()),
                    ]))
                } else {
                    Ok(PartialDebugOutput::PartialResults(sm))
                }
            }
            GraphPattern::Extend { inner, .. }
            | GraphPattern::LeftJoin { left: inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner, .. }
            | GraphPattern::Reduced { inner, .. }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Group { inner, .. } => self.debug_gp(inner, pushdowns, qs, qg),
            GraphPattern::Service { .. } | GraphPattern::DT { .. } | GraphPattern::Graph { .. } => todo!(),
        }
    }

    fn debug_patterns(
        &self,
        patterns: &Vec<TriplePattern>,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        if patterns.is_empty() {
            let sm = self.lazy_graph_pattern(
                &GraphPattern::Bgp { patterns: vec![] },
                None,
                &Context::new(),
                &None,
                pushdowns,
                qs,
                qg,
            )?;
            let sm = sm.as_eager(false);
            return Ok(PartialDebugOutput::PartialResults(sm));
        }
        // First check that the triple patterns have results individually
        let mut errs = vec![];
        for p in patterns {
            let sm = self.lazy_graph_pattern(
                &GraphPattern::Bgp {
                    patterns: vec![p.clone()],
                },
                None,
                &Context::new(),
                &None,
                pushdowns.clone(),
                qs,
                qg,
            )?;
            let f = sm.mappings.first().collect().unwrap();
            if f.height() == 0 {
                errs.push(DebugOutput::NoResultsTriplePattern(p.clone()));
            }
        }
        if !errs.is_empty() {
            Ok(PartialDebugOutput::DebugOutputs(errs))
        } else {
            // Check that triple patterns have results in unison
            let mut sm = None;
            let mut processed_patterns = vec![];
            let patterns = order_triple_patterns(patterns, &None, &pushdowns);
            for p in patterns {
                processed_patterns.push(p);
                sm = Some(
                    self.lazy_graph_pattern(
                        &GraphPattern::Bgp {
                            patterns: processed_patterns.clone(),
                        },
                        sm.map(|x: EagerSolutionMappings| x.as_lazy()),
                        &Context::new(),
                        &None,
                        pushdowns.clone(),
                        qs,
                        qg,
                    )?
                    .as_eager_interruptable(false)
                    .map_err(|x| SparqlError::InterruptSignal)?,
                );
                if sm.as_ref().unwrap().mappings.height() == 0 {
                    let bad_pattern = processed_patterns.pop().unwrap();
                    return Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsJoiningTriplePattern(processed_patterns, bad_pattern),
                    ]));
                }
            }
            Ok(PartialDebugOutput::PartialResults(sm.unwrap()))
        }
    }

    fn debug_path(
        &self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let needs_sparse = need_sparse_matrix(ppe);
        if needs_sparse {
            let sm = self.lazy_graph_pattern(
                &GraphPattern::Path {
                    subject: subject.clone(),
                    path: ppe.clone(),
                    object: object.clone(),
                },
                None,
                &Context::new(),
                &None,
                pushdowns,
                qs,
                qg,
            )?;
            let sm = sm.as_eager(false);
            if sm.mappings.height() == 0 {
                Ok(PartialDebugOutput::DebugOutputs(vec![
                    DebugOutput::NoResultsPath(ppe.clone()),
                ]))
            } else {
                Ok(PartialDebugOutput::PartialResults(sm))
            }
        } else {
            let mut intermediaries = vec![];
            let gp = create_graph_pattern(ppe, subject, object, &mut intermediaries);
            self.debug_gp(&gp, pushdowns, qs, qg)
        }
    }

    fn debug_filter(
        &self,
        expression: &Expression,
        inner: &GraphPattern,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg = self.debug_gp(inner, pushdowns.clone(), qs, qg)?;
        match dbg {
            PartialDebugOutput::PartialResults(sm) => {
                let sm = self.lazy_graph_pattern(
                    &GraphPattern::Filter {
                        expr: expression.clone(),
                        inner: Box::new(inner.clone()),
                    },
                    Some(sm.as_lazy()),
                    &Context::new(),
                    &None,
                    pushdowns,
                    qs,
                    qg,
                )?;
                let sm = sm.as_eager(false);
                if sm.mappings.height() == 0 {
                    Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsFilter(inner.clone(), expression.clone()),
                    ]))
                } else {
                    Ok(PartialDebugOutput::PartialResults(sm))
                }
            }
            dbg => Ok(dbg),
        }
    }

    fn debug_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, pushdowns.clone(), qs, qg)?;
        match dbg_left {
            PartialDebugOutput::PartialResults(..) => {
                let dbg_right = self.debug_gp(right, pushdowns.clone(), qs, qg)?;
                match dbg_right {
                    PartialDebugOutput::PartialResults(..) => {
                        let sm = self.lazy_graph_pattern(
                            &GraphPattern::Join {
                                left: Box::new(left.clone()),
                                right: Box::new(right.clone()),
                            },
                            None,
                            &Context::new(),
                            &None,
                            pushdowns,
                            qs,
                            qg,
                        )?;
                        let sm = sm.as_eager(false);
                        if sm.mappings.height() == 0 {
                            Ok(PartialDebugOutput::DebugOutputs(vec![
                                DebugOutput::NoResultsJoin(left.clone(), right.clone()),
                            ]))
                        } else {
                            Ok(PartialDebugOutput::PartialResults(sm))
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
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, pushdowns.clone(), qs, qg)?;
        match dbg_left {
            PartialDebugOutput::PartialResults(..) => {
                let sm = self.lazy_graph_pattern(
                    &GraphPattern::Minus {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                    },
                    None,
                    &Context::new(),
                    &None,
                    pushdowns,
                    qs,
                    qg,
                )?;
                let sm = sm.as_eager(false);
                if sm.mappings.height() == 0 {
                    Ok(PartialDebugOutput::DebugOutputs(vec![
                        DebugOutput::NoResultsMinus(left.clone(), right.clone()),
                    ]))
                } else {
                    Ok(PartialDebugOutput::PartialResults(sm))
                }
            }
            dbg_left => Ok(dbg_left),
        }
    }

    fn debug_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        pushdowns: Pushdowns,
        qs: &QuerySettings,
        qg: &QueryGraph,
    ) -> Result<PartialDebugOutput, SparqlError> {
        let dbg_left = self.debug_gp(left, pushdowns.clone(), qs, qg)?;
        match dbg_left {
            PartialDebugOutput::DebugOutputs(mut left_dbg) => {
                let dbg_right = self.debug_gp(right, pushdowns.clone(), qs, qg)?;
                match dbg_right {
                    PartialDebugOutput::DebugOutputs(right_dbg) => {
                        left_dbg.extend(right_dbg);
                        return Ok(PartialDebugOutput::DebugOutputs(left_dbg));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        let sm = self.lazy_graph_pattern(
            &GraphPattern::Union {
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            },
            None,
            &Context::new(),
            &None,
            pushdowns,
            qs,
            qg,
        )?;
        let sm = sm.as_eager(false);

        Ok(PartialDebugOutput::PartialResults(sm))
    }
}
