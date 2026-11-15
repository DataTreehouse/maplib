//! A hook into the evaluation of graph patterns, used by engines that evaluate parts of a query
//! elsewhere and let the triplestore evaluate the rest (e.g. chrontext, which runs the
//! virtualized parts of a query against a virtualized database).
//!
//! The hook is called before each graph pattern is evaluated, with the graph pattern, the
//! solution mappings it is evaluated with and its context (its position in the query). It can
//! change the solution mappings the graph pattern is then evaluated with, or return the solution
//! mappings of the graph pattern itself, in which case the triplestore does not evaluate it. To
//! evaluate a graph pattern itself, the hook can let the triplestore evaluate its inner patterns
//! with `evaluate` (which calls the hook for them in turn).
//!
//! The triplestore does not rewrite a query that is evaluated with a hook, so the contexts the
//! hook sees are the positions of the graph patterns in the query as it was given.

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::GraphPattern;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub enum HookDecision {
    /// The triplestore evaluates the graph pattern with these solution mappings.
    Evaluate(Option<SolutionMappings>),
    /// The solution mappings of the graph pattern, which the triplestore then does not evaluate.
    Evaluated(SolutionMappings),
}

/// Evaluates an inner graph pattern at its context with the given solution mappings.
pub type InnerEvaluator<'e> = dyn FnMut(
        &GraphPattern,
        Option<SolutionMappings>,
        &Context,
    ) -> Result<SolutionMappings, SparqlError>
    + 'e;

pub trait EvaluationHook: Send + Sync {
    fn before_graph_pattern(
        &self,
        triplestore: &Triplestore,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        evaluate: &mut InnerEvaluator,
    ) -> Result<HookDecision, SparqlError>;
}

/// An evaluation hook as part of the query settings.
#[derive(Clone)]
pub struct EvaluationHookRef(pub Arc<dyn EvaluationHook>);

impl Debug for EvaluationHookRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EvaluationHook")
    }
}
