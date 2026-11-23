use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::TriplePattern;
use std::fmt::Display;

/// Why a query, or a part of it, has no solution mappings. The hints are further findings
/// about the reported pattern, one sentence each.
#[derive(Clone, Debug)]
pub enum DebugOutput {
    NoResultsTriplePattern(TriplePattern, Vec<String>),
    NoResultsJoiningTriplePattern(Vec<TriplePattern>, TriplePattern, Vec<String>),
    NoResultsJoin(GraphPattern, GraphPattern, Vec<String>),
    NoResultsMinus(GraphPattern, GraphPattern),
    NoResultsFilter(GraphPattern, Expression, Vec<String>),
    NoResultsPath(PropertyPathExpression),
    NoResultsGraphPattern(GraphPattern),
    /// The query has no solution mappings, but no part of it was found to be the reason.
    NoExplanation,
    HasResults,
}

fn write_hints(f: &mut std::fmt::Formatter<'_>, hints: &[String]) -> std::fmt::Result {
    for hint in hints {
        write!(f, "\n  - {}", hint)?;
    }
    Ok(())
}

impl Display for DebugOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DebugOutput::NoResultsTriplePattern(tp, hints) => {
                write!(
                    f,
                    "The following triple pattern has no solution mappings: {}",
                    tp
                )?;
                write_hints(f, hints)
            }
            DebugOutput::NoResultsJoiningTriplePattern(tps, tp, hints) => {
                let tps_strings = tps.iter().map(|x| format!("{}", x)).collect::<Vec<_>>();
                write!(f,
                       "There were no solution mappings after joining the triple pattern:\n{}\n with the triple patterns:\n{}",
                       tp, tps_strings.join(",\n"))?;
                write_hints(f, hints)
            }
            DebugOutput::NoResultsJoin(left, right, hints) => {
                write!(f, "The join between the following graph patterns produced no solution mappings:\n{}\nand:\n{}", left, right)?;
                write_hints(f, hints)
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
            DebugOutput::NoResultsFilter(gp, expr, hints) => {
                write!(f, "No solution mappings after evaluating the expression:\n{}\non the graph pattern:\n{}", expr, gp)?;
                write_hints(f, hints)
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
            DebugOutput::NoExplanation => {
                write!(
                    f,
                    "This query has no solution mappings, but no part of it was found to be the reason"
                )
            }
            DebugOutput::HasResults => {
                write!(f, "This query has at least one solution mapping")
            }
        }
    }
}

#[derive(Clone, Debug)]
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
