//! The abstract syntax of SPARQL-RL (SRL) 1.2 rule sets, the rule language of the W3C Data Shapes
//! Working Group (formerly "SHACL 1.2 Rules"). See [`crate::parser::parse_srl_ruleset`].

use crate::algebra::{Expression, Function};
use crate::term::{NamedNodePattern, TermPattern, TriplePattern};
use oxrdf::{NamedNode, Variable};
use std::fmt;

/// A parsed SRL rule set.
#[derive(Debug, Clone, PartialEq)]
pub struct SrlRuleSet {
    pub base_iri: Option<String>,
    pub imports: Vec<NamedNode>,
    pub items: Vec<SrlItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SrlItem {
    Rule(SrlRule),
    /// A `DATA { ... }` block. The triples are ground (blank nodes are allowed).
    Data(Vec<TriplePattern>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SrlRule {
    pub name: Option<NamedNode>,
    pub head: Vec<TriplePattern>,
    /// False for `WHERE DATA { ... }`, where the body only matches the base graph.
    pub evaldata: bool,
    pub body: Vec<SrlElement>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SrlElement {
    Triple(TriplePattern),
    Filter(Expression),
    Negation {
        /// False for `NOT DATA { ... }`, which only matches the base graph.
        evaldata: bool,
        body: Vec<SrlElement>,
    },
    Assignment {
        variable: Variable,
        expression: Expression,
    },
    Group {
        keys: Vec<Variable>,
        body: Vec<SrlElement>,
        aggregates: Vec<(Variable, SrlAggregate)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SrlAggregateFunction {
    Count,
    Min,
    Max,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SrlAggregate {
    pub function: SrlAggregateFunction,
    pub distinct: bool,
    /// None for `COUNT(*)`.
    pub argument: Option<Variable>,
}

impl fmt::Display for SrlAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.function {
            SrlAggregateFunction::Count => "COUNT",
            SrlAggregateFunction::Min => "MIN",
            SrlAggregateFunction::Max => "MAX",
            SrlAggregateFunction::Sum => "SUM",
        };
        write!(f, "{name}(")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        match &self.argument {
            Some(v) => write!(f, "{v})"),
            None => write!(f, "*)"),
        }
    }
}

fn fmt_elements(elements: &[SrlElement], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for e in elements {
        write!(f, "{e} ")?;
    }
    Ok(())
}

impl fmt::Display for SrlElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SrlElement::Triple(t) => write!(f, "{t} ."),
            SrlElement::Filter(e) => write!(f, "FILTER({e})"),
            SrlElement::Negation { evaldata, body } => {
                write!(f, "NOT ")?;
                if !evaldata {
                    write!(f, "DATA ")?;
                }
                write!(f, "{{ ")?;
                fmt_elements(body, f)?;
                write!(f, "}}")
            }
            SrlElement::Assignment {
                variable,
                expression,
            } => write!(f, "SET({variable} := {expression})"),
            SrlElement::Group {
                keys,
                body,
                aggregates,
            } => {
                write!(f, "GROUP ")?;
                if !keys.is_empty() {
                    write!(f, "BY")?;
                    for k in keys {
                        write!(f, " {k}")?;
                    }
                    write!(f, " ")?;
                }
                write!(f, "{{ ")?;
                fmt_elements(body, f)?;
                write!(f, "}} [")?;
                let aggs: Vec<_> = aggregates
                    .iter()
                    .map(|(v, a)| format!("{v} := {a}"))
                    .collect();
                write!(f, "{}]", aggs.join(", "))
            }
        }
    }
}

impl fmt::Display for SrlRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RULE ")?;
        if let Some(name) = &self.name {
            write!(f, "{name} ")?;
        }
        write!(f, "{{ ")?;
        for t in &self.head {
            write!(f, "{t} . ")?;
        }
        write!(f, "}} WHERE ")?;
        if !self.evaldata {
            write!(f, "DATA ")?;
        }
        write!(f, "{{ ")?;
        fmt_elements(&self.body, f)?;
        write!(f, "}}")
    }
}

/// Checks that an expression only uses what SRL allows: no EXISTS, BOUND, COALESCE, RAND or
/// hash functions.
pub(crate) fn check_srl_expression(e: &Expression) -> Result<(), &'static str> {
    match e {
        Expression::NamedNode(_) | Expression::Literal(_) | Expression::Variable(_) => Ok(()),
        Expression::Exists(_) => Err("EXISTS is not allowed in SPARQL-RL expressions"),
        Expression::Bound(_) => Err("BOUND is not allowed in SPARQL-RL expressions"),
        Expression::Coalesce(_) => Err("COALESCE is not allowed in SPARQL-RL expressions"),
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => {
            check_srl_expression(a)?;
            check_srl_expression(b)
        }
        Expression::In(a, bs) => {
            check_srl_expression(a)?;
            for b in bs {
                check_srl_expression(b)?;
            }
            Ok(())
        }
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
            check_srl_expression(a)
        }
        Expression::If(a, b, c) => {
            check_srl_expression(a)?;
            check_srl_expression(b)?;
            check_srl_expression(c)
        }
        Expression::FunctionCall(func, args) => {
            match func {
                Function::Rand => return Err("RAND is not allowed in SPARQL-RL expressions"),
                Function::Md5
                | Function::Sha1
                | Function::Sha256
                | Function::Sha384
                | Function::Sha512 => {
                    return Err("Hash functions are not allowed in SPARQL-RL expressions")
                }
                _ => {}
            }
            for a in args {
                check_srl_expression(a)?;
            }
            Ok(())
        }
    }
}

/// The variables of a triple pattern, including those inside triple terms.
pub fn triple_pattern_variables(tp: &TriplePattern, out: &mut Vec<Variable>) {
    term_pattern_variables(&tp.subject, out);
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        out.push(v.clone());
    }
    term_pattern_variables(&tp.object, out);
}

fn term_pattern_variables(t: &TermPattern, out: &mut Vec<Variable>) {
    match t {
        TermPattern::Variable(v) => out.push(v.clone()),
        #[cfg(feature = "sparql-12")]
        TermPattern::Triple(t) => triple_pattern_variables(t, out),
        _ => {}
    }
}
