use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::TriplePattern;
use std::fmt::Display;

#[derive(Clone)]
pub enum DebugOutput {
    NoResultsTriplePattern(TriplePattern),
    NoResultsJoiningTriplePattern(Vec<TriplePattern>, TriplePattern),
    NoResultsJoin(GraphPattern, GraphPattern),
    NoResultsMinus(GraphPattern, GraphPattern),
    NoResultsFilter(GraphPattern, Expression),
    NoResultsPath(PropertyPathExpression),
    NoResultsGraphPattern(GraphPattern),
    HasResults,
}

impl Display for DebugOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DebugOutput::NoResultsTriplePattern(tp) => {
                write!(
                    f,
                    "The following triple pattern has no solution mappings: {}",
                    tp
                )
            }
            DebugOutput::NoResultsJoiningTriplePattern(tps, tp) => {
                let tps_strings = tps.iter().map(|x| format!("{}", x)).collect::<Vec<_>>();
                write!(f,
                       "There were no solution mappings after joining the triple pattern:\n{}\n with the triple patterns:\n{}",
                       tp, tps_strings.join(",\n"))
            }
            DebugOutput::NoResultsJoin(left, right) => {
                write!(f, "The join between the following graph patterns produced no solution mappings:\n{}\nand:\n{}", left, right)
            }
            DebugOutput::NoResultsMinus(left, right) => {
                write!(
                    f,
                    "No solution mappings after the following minus operation:\n{}",
                    GraphPattern::Minus {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone())
                    }
                )
            }
            DebugOutput::NoResultsFilter(gp, expr) => {
                write!(f, "No solution mappings after evaluating the expression:\n{}\non the graph pattern:\n{}", expr, gp)
            }
            DebugOutput::NoResultsPath(path) => {
                write!(
                    f,
                    "No solution mappings evaluating the property path:\n{}",
                    path
                )
            }
            DebugOutput::NoResultsGraphPattern(gp) => {
                write!(
                    f,
                    "No solution mappings after the following graph pattern:\n{}",
                    gp
                )
            }
            DebugOutput::HasResults => {
                write!(f, "This query has at least one solution mapping")
            }
        }
    }
}

#[derive(Clone)]
pub struct DebugOutputs {
    pub debug_outputs: Vec<DebugOutput>,
}

impl DebugOutputs {
    pub fn new(debug_outputs: Vec<DebugOutput>) -> Self {
        Self { debug_outputs }
    }

    pub fn extend_outputs(&mut self, debug_outputs: DebugOutputs) {
        self.debug_outputs.extend(debug_outputs.debug_outputs);
    }
}

impl Display for DebugOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for output in &self.debug_outputs {
            writeln!(f, "{}", output)?;
        }
        Ok(())
    }
}
