//! Built-in reasoning profiles (RDFS, RDFS-Plus and OWL 2 RL). Reasoning is part of the
//! Datalog engine, which is not included in the open source version.

use crate::inference::DatalogError;
use representation::dataset::NamedGraph;
use std::fmt::{Display, Formatter};
use triplestore::Triplestore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningProfile {
    /// The RDFS entailment rules of RDF 1.1 Semantics, except rdfs4a/rdfs4b.
    Rdfs,
    /// RDFS without the axiomatic and reflexive triples, with owl:inverseOf,
    /// owl:SymmetricProperty, owl:TransitiveProperty, owl:equivalentClass,
    /// owl:equivalentProperty, owl:sameAs, owl:FunctionalProperty and
    /// owl:InverseFunctionalProperty.
    RdfsPlus,
    /// The OWL 2 RL/RDF rules of OWL 2 Profiles, section 4.3.
    OwlRl,
}

impl ReasoningProfile {
    pub fn parse(s: &str) -> Result<ReasoningProfile, DatalogError> {
        let norm: String = s
            .trim()
            .to_lowercase()
            .chars()
            .filter(|c| !matches!(c, '-' | '_' | ' ' | '2'))
            .collect();
        match norm.as_str() {
            "rdfs" => Ok(ReasoningProfile::Rdfs),
            "rdfsplus" | "rdfs++" => Ok(ReasoningProfile::RdfsPlus),
            "owlrl" | "rl" => Ok(ReasoningProfile::OwlRl),
            _ => Err(DatalogError::ReasoningError(format!(
                "Unknown reasoning profile `{s}`, expected one of \"owl-rl\", \"rdfs-plus\" or \"rdfs\""
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ReasoningProfile::Rdfs => "rdfs",
            ReasoningProfile::RdfsPlus => "rdfs-plus",
            ReasoningProfile::OwlRl => "owl-rl",
        }
    }

    #[allow(dead_code)]
    fn owl(&self) -> bool {
        matches!(self, ReasoningProfile::RdfsPlus | ReasoningProfile::OwlRl)
    }
}

/// A solution of a rule of OWL 2 RL whose head is `false`, i.e. an inconsistency.
#[derive(Debug, Clone)]
pub struct Violation {
    /// The name of the rule in OWL 2 Profiles, e.g. `cax-dw`.
    pub rule: String,
    pub message: String,
    /// The terms of the solution in N-Triples syntax, in the order the message refers to them.
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ReasoningResult {
    pub profile: String,
    /// The number of triples that were added to the target graph.
    pub inferred_triples: usize,
    pub violations: Vec<Violation>,
    /// True if a check reached max_violations, so that more violations may exist.
    pub violations_truncated: bool,
    /// The number of times the schema was compiled and the rules run.
    pub rounds: usize,
    /// The number of rules in the last compiled ruleset.
    pub rules: usize,
    /// The number of rule executions of the Datalog engine.
    pub rule_executions: usize,
    pub seconds: f64,
}

impl ReasoningResult {
    pub fn consistent(&self) -> bool {
        self.violations.is_empty()
    }
}

impl Display for ReasoningResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReasoningResult(profile={}, inferred_triples={}, consistent={}, violations={}{}, rounds={}, rules={}, rule_executions={}, seconds={:.3})",
            self.profile,
            self.inferred_triples,
            self.consistent(),
            self.violations.len(),
            if self.violations_truncated { "+" } else { "" },
            self.rounds,
            self.rules,
            self.rule_executions,
            self.seconds
        )
    }
}

pub fn reason(
    _triplestore: &mut Triplestore,
    _profile: ReasoningProfile,
    _graph: &NamedGraph,
    _target_graph: &NamedGraph,
    _include_graphs: Vec<NamedGraph>,
    _max_rows: Option<usize>,
    _max_violations: usize,
) -> Result<ReasoningResult, DatalogError> {
    unimplemented!("Contact Data Treehouse to try")
}
