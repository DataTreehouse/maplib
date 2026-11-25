//! Triple term patterns of basic graph patterns: `?r rdf:reifies <<( ?s :p ?o )>>`, which the
//! reifier syntax `<< ?s :p ?o >>` is parsed into as well.
//!
//! A triple term pattern in a triple pattern is replaced by a variable of its own (named so that
//! it cannot be a SPARQL variable), constrained to triple terms, and the basic graph pattern is
//! evaluated as before. The triple term pattern is then matched against the dictionary of triple
//! terms (representation::cats::triple_terms): the partitions of its predicate (all of them for a
//! variable predicate), with its constants as filters on the stored subjects and objects. The
//! matches are joined with the solutions on the id of the triple term, and the variable is
//! dropped. A nested triple term pattern is matched the same way, inside the one it is in.

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::{Term, Variable};
use polars::prelude::{by_name, col, lit, Column, DataFrame, IntoLazy, JoinType, LazyFrame};
use query_processing::expressions::{literal_enc, named_node_enc};
use query_processing::graph_patterns::{join, union};
use query_processing::pushdowns::Pushdowns;
use query_processing::type_constraints::{ConstraintBaseRDFNodeType, PossibleTypes};
use representation::cats::triple_terms::TRIPLE_TERM_ID_COL_NAME;
use representation::cats::Cats;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Variables that stand for triple term patterns are named `#tt<n>`: `#` cannot occur in a SPARQL
/// variable name, so they do not clash with the variables of the query.
static NEXT_TRIPLE_TERM_VARIABLE: AtomicUsize = AtomicUsize::new(0);

fn new_triple_term_variable() -> Variable {
    let n = NEXT_TRIPLE_TERM_VARIABLE.fetch_add(1, Ordering::Relaxed);
    Variable::new_unchecked(format!("#tt{n}"))
}

/// The triple patterns with their triple term patterns (in subjects and objects) replaced by
/// variables, and the triple term pattern of each such variable.
pub(crate) fn extract_triple_term_patterns(
    patterns: &[TriplePattern],
) -> (Vec<TriplePattern>, Vec<(Variable, TriplePattern)>) {
    let mut extracted = vec![];
    let mut replace = |t: &TermPattern| -> TermPattern {
        if let TermPattern::Triple(tp) = t {
            let v = new_triple_term_variable();
            extracted.push((v.clone(), tp.as_ref().clone()));
            TermPattern::Variable(v)
        } else {
            t.clone()
        }
    };
    let patterns = patterns
        .iter()
        .map(|tp| TriplePattern {
            subject: replace(&tp.subject),
            predicate: tp.predicate.clone(),
            object: replace(&tp.object),
        })
        .collect();
    (patterns, extracted)
}

/// Constrains the variables of triple term patterns to triple terms, so that their triple
/// patterns only read tables of triple term objects.
pub(crate) fn constrain_triple_term_variables(
    pushdowns: &mut Pushdowns,
    variables: &[(Variable, TriplePattern)],
) {
    for (v, _) in variables {
        pushdowns.variables_type_constraints.insert(
            v.as_str().to_string(),
            PossibleTypes::singular(ConstraintBaseRDFNodeType::TripleTerm),
        );
    }
}

/// The name of the column of a term pattern that is a variable or a blank node (which is a
/// variable in a pattern), as the triple patterns name them.
fn variable_name(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Whether the constant can be a value of a column of type `bt`.
fn constant_matches_type(t: &Term, bt: &BaseRDFNodeType) -> bool {
    match t {
        Term::NamedNode(_) => bt.is_iri(),
        Term::BlankNode(_) => bt.is_blank_node(),
        Term::Literal(l) => &BaseRDFNodeType::from_literal(l) == bt,
        Term::Triple(_) => bt.is_triple_term(),
    }
}

/// Whether the triple pattern has one of the variables of the triple term patterns.
pub(crate) fn uses_any(
    tp: &TriplePattern,
    triple_term_patterns: &[(Variable, TriplePattern)],
) -> bool {
    triple_term_patterns.iter().any(|(v, _)| {
        matches!(&tp.subject, TermPattern::Variable(s) if s == v)
            || matches!(&tp.object, TermPattern::Variable(o) if o == v)
    })
}

/// The variables of a term pattern (and of its triple term patterns, which are joined with it).
fn term_variables(
    t: &TermPattern,
    triple_term_patterns: &[(Variable, TriplePattern)],
    out: &mut Vec<String>,
) {
    if let Some(name) = variable_name(t) {
        if let Some((_, tp)) = triple_term_patterns
            .iter()
            .find(|(v, _)| v.as_str() == name)
        {
            term_variables(&tp.subject, triple_term_patterns, out);
            if let NamedNodePattern::Variable(p) = &tp.predicate {
                out.push(p.as_str().to_string());
            }
            term_variables(&tp.object, triple_term_patterns, out);
        }
        out.push(name);
    } else if let TermPattern::Triple(tp) = t {
        term_variables(&tp.subject, triple_term_patterns, out);
        term_variables(&tp.object, triple_term_patterns, out);
    }
}

/// The variables a triple pattern binds, with those of its triple term patterns.
fn pattern_variables(
    tp: &TriplePattern,
    triple_term_patterns: &[(Variable, TriplePattern)],
) -> Vec<String> {
    let mut out = vec![];
    term_variables(&tp.subject, triple_term_patterns, &mut out);
    if let NamedNodePattern::Variable(p) = &tp.predicate {
        out.push(p.as_str().to_string());
    }
    term_variables(&tp.object, triple_term_patterns, &mut out);
    out
}

/// The patterns in the order chosen for them, except that a pattern that shares no variable with
/// the patterns before it (and the incoming solutions), which would be joined with them as a
/// cross product, is preceded by a pattern with triple term patterns that shares variables with
/// them. The order is chosen without the triple term patterns, so it does not know that their
/// variables connect the pattern they are in with the patterns that use their variables
/// (`?r rdf:reifies <<( ?s :p ?o )>> . ?s :p ?o`), and could otherwise join those as a cross
/// product before the triple term pattern connects them.
pub(crate) fn connect_triple_term_patterns(
    ordered: Vec<TriplePattern>,
    triple_term_patterns: &[(Variable, TriplePattern)],
    solution_mappings: &Option<SolutionMappings>,
) -> Vec<TriplePattern> {
    if triple_term_patterns.is_empty() {
        return ordered;
    }
    let mut bound: std::collections::HashSet<String> = solution_mappings
        .as_ref()
        .map(|sm| sm.rdf_node_types.keys().cloned().collect())
        .unwrap_or_default();
    let mut remaining: std::collections::VecDeque<TriplePattern> = ordered.into();
    let mut out = Vec::with_capacity(remaining.len());
    while let Some(next) = remaining.pop_front() {
        let shares = |tp: &TriplePattern, bound: &std::collections::HashSet<String>| {
            pattern_variables(tp, triple_term_patterns)
                .iter()
                .any(|v| bound.contains(v))
        };
        if !bound.is_empty() && !shares(&next, &bound) {
            if let Some(i) = remaining
                .iter()
                .position(|tp| uses_any(tp, triple_term_patterns) && shares(tp, &bound))
            {
                let connecting = remaining.remove(i).unwrap();
                bound.extend(pattern_variables(&connecting, triple_term_patterns));
                out.push(connecting);
                remaining.push_front(next);
                continue;
            }
        }
        bound.extend(pattern_variables(&next, triple_term_patterns));
        out.push(next);
    }
    out
}

impl Triplestore {
    /// A triple pattern of a basic graph pattern. A pattern with triple term patterns that
    /// shares no variable with the solutions itself, only through its triple term patterns, is
    /// evaluated on its own and joined with them, so that it is not joined with the solutions as
    /// a cross product before its triple term patterns connect them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_bgp_triple_pattern(
        &self,
        solution_mappings: Option<SolutionMappings>,
        tp: &TriplePattern,
        pending: &mut Vec<(Variable, TriplePattern)>,
        context: &representation::query_context::Context,
        pushdowns: &mut Pushdowns,
        query_settings: &crate::sparql::QuerySettings,
        dataset: &representation::dataset::QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        if let Some(sm) = &solution_mappings {
            let own: Vec<String> = [&tp.subject, &tp.object]
                .into_iter()
                .filter_map(variable_name)
                .chain(match &tp.predicate {
                    NamedNodePattern::Variable(v) => Some(v.as_str().to_string()),
                    NamedNodePattern::NamedNode(_) => None,
                })
                .collect();
            if uses_any(tp, pending) && !own.iter().any(|v| sm.rdf_node_types.contains_key(v)) {
                let unit = self.lazy_triple_pattern(
                    None,
                    tp,
                    context,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                let unit =
                    self.join_bound_triple_term_patterns(unit, pending, query_settings.max_rows)?;
                return Ok(join(
                    solution_mappings.unwrap(),
                    unit,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?);
            }
        }
        self.lazy_triple_pattern(
            solution_mappings,
            tp,
            context,
            pushdowns,
            query_settings,
            dataset,
        )
    }

    /// Joins the solutions with the matches of the triple term patterns whose variables they
    /// bind, which are removed from `pending`.
    pub(crate) fn join_bound_triple_term_patterns(
        &self,
        sm: SolutionMappings,
        pending: &mut Vec<(Variable, TriplePattern)>,
        max_rows: Option<usize>,
    ) -> Result<SolutionMappings, SparqlError> {
        let (bound, rest): (Vec<_>, Vec<_>) = std::mem::take(pending)
            .into_iter()
            .partition(|(v, _)| sm.rdf_node_types.contains_key(v.as_str()));
        *pending = rest;
        self.join_triple_term_patterns(sm, &bound, max_rows)
    }

    /// Joins the solutions of a basic graph pattern with the matches of its triple term
    /// patterns, and drops the variables that stood for them.
    pub(crate) fn join_triple_term_patterns(
        &self,
        mut sm: SolutionMappings,
        triple_term_patterns: &[(Variable, TriplePattern)],
        max_rows: Option<usize>,
    ) -> Result<SolutionMappings, SparqlError> {
        for (v, tp) in triple_term_patterns {
            let matches = self.triple_term_pattern_solution_mappings(v, tp, max_rows)?;
            sm = join(
                sm,
                matches,
                JoinType::Inner,
                self.global_cats.clone(),
                max_rows,
            )?;
            sm = drop_variable(sm, v.as_str());
        }
        Ok(sm)
    }

    /// The triple terms that match the triple term pattern, with their ids in `var` and the
    /// values of the variables of the pattern.
    fn triple_term_pattern_solution_mappings(
        &self,
        var: &Variable,
        tp: &TriplePattern,
        max_rows: Option<usize>,
    ) -> Result<SolutionMappings, SparqlError> {
        let subject_name = variable_name(&tp.subject);
        let predicate_name = match &tp.predicate {
            NamedNodePattern::Variable(v) => Some(v.as_str().to_string()),
            NamedNodePattern::NamedNode(_) => None,
        };
        // A nested triple term pattern in the object is matched like this one, and joined on
        // the id of the object.
        let nested = match &tp.object {
            TermPattern::Triple(inner) => {
                Some((new_triple_term_variable(), inner.as_ref().clone()))
            }
            _ => None,
        };
        let object_name = match &nested {
            Some((v, _)) => Some(v.as_str().to_string()),
            None => variable_name(&tp.object),
        };
        if predicate_name.is_some()
            && (predicate_name == subject_name || predicate_name == object_name)
        {
            return Err(SparqlError::NotSupportedYet(
                "the predicate variable of a triple term pattern repeated in its subject or object"
                    .to_string(),
            ));
        }

        let mut sms = vec![];
        {
            let cats = self.global_cats.read()?;
            let partitions: Vec<_> = match &tp.predicate {
                NamedNodePattern::NamedNode(p) => {
                    cats.triple_terms.partitions_with_predicate(p).collect()
                }
                NamedNodePattern::Variable(_) => cats.triple_terms.partitions().iter().collect(),
            };
            for part in partitions {
                let key = &part.key;
                let mut lf = part.lazy();
                let Some(filtered) =
                    filter_constant(lf, SUBJECT_COL_NAME, &tp.subject, &key.subject_type, &cats)?
                else {
                    continue;
                };
                lf = filtered;
                if nested.is_some() && !key.object_type.is_triple_term() {
                    continue;
                }
                if nested.is_none() {
                    let Some(filtered) =
                        filter_constant(lf, OBJECT_COL_NAME, &tp.object, &key.object_type, &cats)?
                    else {
                        continue;
                    };
                    lf = filtered;
                }
                let mut columns = vec![col(TRIPLE_TERM_ID_COL_NAME).alias(var.as_str())];
                let mut types = HashMap::from([(
                    var.as_str().to_string(),
                    BaseRDFNodeType::TripleTerm.into_default_stored_rdf_node_state(),
                )]);
                if let Some(s) = &subject_name {
                    columns.push(col(SUBJECT_COL_NAME).alias(s));
                    types.insert(
                        s.clone(),
                        key.subject_type
                            .clone()
                            .into_default_stored_rdf_node_state(),
                    );
                }
                if let Some(o) = &object_name {
                    if Some(o) == subject_name.as_ref() {
                        // The same variable as subject and object.
                        if key.subject_type != key.object_type {
                            continue;
                        }
                        lf = lf.filter(col(SUBJECT_COL_NAME).eq(col(OBJECT_COL_NAME)));
                    } else {
                        columns.push(col(OBJECT_COL_NAME).alias(o));
                        types.insert(
                            o.clone(),
                            key.object_type.clone().into_default_stored_rdf_node_state(),
                        );
                    }
                }
                if let Some(p) = &predicate_name {
                    columns.push(lit(key.predicate.as_str().to_string()).alias(p));
                    types.insert(
                        p.clone(),
                        RDFNodeState::from_bases(BaseRDFNodeType::IRI, BaseCatState::String),
                    );
                }
                sms.push(SolutionMappings {
                    mappings: lf.select(columns),
                    rdf_node_types: types,
                    height_estimate: part.height(),
                });
            }
        }
        let mut sm = if sms.is_empty() {
            let mut names = vec![var.as_str().to_string()];
            names.extend(subject_name);
            names.extend(object_name);
            names.extend(predicate_name);
            empty_solution_mappings(&names)
        } else {
            union(sms, false, self.global_cats.clone())?
        };
        if let Some((v, inner)) = &nested {
            let inner_sm = self.triple_term_pattern_solution_mappings(v, inner, max_rows)?;
            sm = join(
                sm,
                inner_sm,
                JoinType::Inner,
                self.global_cats.clone(),
                max_rows,
            )?;
            sm = drop_variable(sm, v.as_str());
        }
        Ok(sm)
    }
}

/// The rows of `lf` whose column `c` (of type `bt`) holds the constant of the term pattern, all
/// rows for a variable, and None if the column cannot hold the constant.
fn filter_constant(
    lf: LazyFrame,
    c: &str,
    t: &TermPattern,
    bt: &BaseRDFNodeType,
    cats: &Cats,
) -> Result<Option<LazyFrame>, SparqlError> {
    Ok(match t {
        TermPattern::Variable(_) | TermPattern::BlankNode(_) => Some(lf),
        TermPattern::NamedNode(nn) => {
            if !bt.is_iri() {
                return Ok(None);
            }
            named_node_enc(nn, cats).map(|e| lf.filter(col(c).eq(e)))
        }
        TermPattern::Literal(l) => {
            if !constant_matches_type(&Term::Literal(l.clone()), bt) {
                return Ok(None);
            }
            let (e, _, _) = literal_enc(l, cats)?;
            Some(lf.filter(col(c).eq(e)))
        }
        // A subject is never a triple term; nested objects are joined instead (see above).
        TermPattern::Triple(_) => None,
    })
}

fn drop_variable(mut sm: SolutionMappings, v: &str) -> SolutionMappings {
    sm.mappings = sm.mappings.drop(by_name([v], true, false));
    sm.rdf_node_types.remove(v);
    sm
}

fn empty_solution_mappings(names: &[String]) -> SolutionMappings {
    let mut columns = vec![];
    let mut types = HashMap::new();
    for n in names {
        if types.contains_key(n) {
            continue;
        }
        columns.push(Column::new_empty(
            n.into(),
            &BaseRDFNodeType::None.default_stored_polars_data_type(),
        ));
        types.insert(
            n.clone(),
            BaseRDFNodeType::None.into_default_stored_rdf_node_state(),
        );
    }
    SolutionMappings {
        mappings: DataFrame::new(0, columns).unwrap().lazy(),
        rdf_node_types: types,
        height_estimate: 0,
    }
}
