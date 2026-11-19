//! Counts over a single triple pattern answered straight from the stored triple tables, without
//! building solution mappings: e.g. `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }` or
//! `SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`.
//!
//! The tables of a graph are deduplicated and disjoint, and every table holds one subject type
//! and one object type, so:
//! - the number of solutions is the sum of the heights of the tables the pattern can match,
//! - a filter on the kind of the subject or object (isIRI, isBLANK, isLITERAL) selects whole
//!   tables,
//! - the distinct subjects (objects) are the distinct stored values per subject (object) type,
//!   counted over the stored columns, which are still dictionary codes for IRIs, blank nodes and
//!   strings.

use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::sparql::errors::SparqlError;
use crate::storage::Triples;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::{col, concat, IntoLazy, LazyFrame, UnionArgs};
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, DataType};
use representation::dataset::QueryGraph;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern};
use std::collections::{HashMap, HashSet};

/// The term kinds a FILTER can ask for.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Iri,
    Blank,
    Literal,
}

impl Kind {
    fn matches(self, t: &BaseRDFNodeType) -> bool {
        match self {
            Kind::Iri => t.is_iri(),
            Kind::Blank => t.is_blank_node(),
            Kind::Literal => matches!(t, BaseRDFNodeType::Literal(..)),
        }
    }
}

/// A condition on the subject (true) or object (false) of a table: its type is (or, negated,
/// is not) of a kind.
type Condition = (bool, Kind, bool);

/// What one aggregate counts.
enum Counted {
    /// All solutions (COUNT(*), COUNT(?v) of a variable of the pattern, COUNT(DISTINCT *)).
    Solutions,
    DistinctSubjects,
    DistinctPredicates,
    DistinctObjects,
}

impl Triplestore {
    /// Answers the aggregates if they are all counts over one triple pattern (with an optional
    /// filter on the kinds of its subject and object) that the stored tables answer directly.
    /// Returns None for anything else, which then takes the general path.
    pub(crate) fn count_from_tables(
        &self,
        inner: &GraphPattern,
        aggregates: &[(Variable, AggregateExpression)],
        dataset: &QueryGraph,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let QueryGraph::NamedGraph(graph) = dataset else {
            return Ok(None);
        };
        let (patterns, filter) = match inner {
            GraphPattern::Bgp { patterns } => (patterns, None),
            GraphPattern::Filter { expr, inner } => match inner.as_ref() {
                GraphPattern::Bgp { patterns } => (patterns, Some(expr)),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let [tp] = patterns.as_slice() else {
            return Ok(None);
        };
        let TermPattern::Variable(s) = &tp.subject else {
            return Ok(None);
        };
        let TermPattern::Variable(o) = &tp.object else {
            return Ok(None);
        };
        let p = match &tp.predicate {
            NamedNodePattern::Variable(p) => {
                if p == s || p == o {
                    return Ok(None);
                }
                Some(p)
            }
            NamedNodePattern::NamedNode(_) => None,
        };
        if s == o || aggregates.is_empty() {
            return Ok(None);
        }
        let mut conditions = vec![];
        if let Some(expr) = filter {
            if !kind_conditions(expr, s, o, &mut conditions) {
                return Ok(None);
            }
        }
        let mut counted = vec![];
        for (_, a) in aggregates {
            let c = match a {
                AggregateExpression::CountSolutions { .. } => Counted::Solutions,
                AggregateExpression::FunctionCall {
                    name: AggregateFunction::Count,
                    expr: Expression::Variable(v),
                    distinct,
                } => {
                    if v == s {
                        if *distinct { Counted::DistinctSubjects } else { Counted::Solutions }
                    } else if v == o {
                        if *distinct { Counted::DistinctObjects } else { Counted::Solutions }
                    } else if Some(v) == p {
                        if *distinct { Counted::DistinctPredicates } else { Counted::Solutions }
                    } else {
                        // A variable the pattern does not bind counts 0, which is rare enough
                        // to leave to the general path.
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            };
            counted.push(c);
        }

        // The tables the pattern and the filter select, with their predicate.
        let mut tables: Vec<(&oxrdf::NamedNode, &Triples)> = vec![];
        if let Some(m) = self.graph_triples_map.get(graph) {
            let preds: Vec<_> = match &tp.predicate {
                NamedNodePattern::Variable(_) => m.iter().collect(),
                NamedNodePattern::NamedNode(nn) => m.get_key_value(nn).into_iter().collect(),
            };
            for (pred, ts) in preds {
                for t in ts.values() {
                    if t.height() > 0
                        && conditions.iter().all(|(subject, kind, positive)| {
                            let ty = if *subject { &t.subject_type } else { &t.object_type };
                            kind.matches(ty) == *positive
                        })
                    {
                        tables.push((pred, t));
                    }
                }
            }
        }

        let mut columns = vec![];
        let mut types = HashMap::new();
        for ((v, _), c) in aggregates.iter().zip(counted) {
            let n = match c {
                Counted::Solutions => tables.iter().map(|(_, t)| t.height()).sum(),
                Counted::DistinctPredicates => {
                    tables.iter().map(|(p, _)| *p).collect::<HashSet<_>>().len()
                }
                Counted::DistinctSubjects => distinct_values(&tables, true)?,
                Counted::DistinctObjects => distinct_values(&tables, false)?,
            };
            columns.push(Column::new(v.as_str().into(), [n as i64]));
            types.insert(
                v.as_str().to_string(),
                BaseRDFNodeType::Literal(xsd::INTEGER.into_owned(), None)
                    .into_default_input_rdf_node_state(),
            );
        }
        Ok(Some(SolutionMappings::new(
            DataFrame::new(1, columns).unwrap().lazy(),
            types,
            1,
        )))
    }
}

/// Reads a filter made of isIRI/isURI, isBLANK and isLITERAL of the subject or object variable,
/// their negations and conjunctions. False if the filter is anything else.
fn kind_conditions(
    expr: &Expression,
    s: &Variable,
    o: &Variable,
    out: &mut Vec<Condition>,
) -> bool {
    match expr {
        Expression::And(a, b) => {
            kind_conditions(a, s, o, out) && kind_conditions(b, s, o, out)
        }
        Expression::Not(inner) => {
            let mut inner_conditions = vec![];
            if !kind_conditions(inner, s, o, &mut inner_conditions) {
                return false;
            }
            // Only a single negated test: !(a && b) is not a conjunction.
            let [(subject, kind, positive)] = inner_conditions.as_slice() else {
                return false;
            };
            out.push((*subject, *kind, !positive));
            true
        }
        Expression::FunctionCall(f, args) => {
            let kind = match f {
                Function::IsIri => Kind::Iri,
                Function::IsBlank => Kind::Blank,
                Function::IsLiteral => Kind::Literal,
                _ => return false,
            };
            let [Expression::Variable(v)] = args.as_slice() else {
                return false;
            };
            if v == s {
                out.push((true, kind, true));
            } else if v == o {
                out.push((false, kind, true));
            } else {
                return false;
            }
            true
        }
        _ => false,
    }
}

/// The number of distinct subjects (or objects) of the tables: per stored type, the distinct
/// stored values. Terms of different types are different terms, and the stored type of a term
/// is fixed (a datatype with its time zone offset, see `BaseRDFNodeType`), so the counts per
/// type add up.
fn distinct_values(
    tables: &[(&oxrdf::NamedNode, &Triples)],
    subject: bool,
) -> Result<usize, SparqlError> {
    let c = if subject { SUBJECT_COL_NAME } else { OBJECT_COL_NAME };
    let mut by_type: HashMap<&BaseRDFNodeType, Vec<LazyFrame>> = HashMap::new();
    for (_, t) in tables {
        let ty = if subject { &t.subject_type } else { &t.object_type };
        let lfs = t.get_lazy_frame_slices()?.unwrap_or_default();
        by_type
            .entry(ty)
            .or_default()
            .extend(lfs.into_iter().map(|lf| lf.select([col(c)])));
    }
    if by_type.is_empty() {
        return Ok(0);
    }
    let counts: Vec<LazyFrame> = by_type
        .into_values()
        .map(|lfs| {
            let args = UnionArgs {
                parallel: true,
                rechunk: false,
                to_supertypes: false,
                ..Default::default()
            };
            Ok(concat(lfs, args)
                .map_err(TriplestoreError::LazyLoadError)?
                .select([col(c).n_unique().cast(DataType::UInt64).alias("n")]))
        })
        .collect::<Result<_, SparqlError>>()?;
    let df = concat(counts, UnionArgs::default())
        .map_err(TriplestoreError::LazyLoadError)?
        .select([col("n").sum()])
        .collect()
        .map_err(TriplestoreError::LazyLoadError)?;
    Ok(df
        .column("n")
        .unwrap()
        .u64()
        .unwrap()
        .get(0)
        .unwrap_or(0) as usize)
}
