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
//!
//! When the solutions so far bind variables of a triple term pattern (VALUES ?s { ... }, or an
//! earlier pattern), it is matched before the triple pattern it is in instead: with the values
//! pushed down for its variables as filters on the dictionary, joined with the solutions, and
//! with the ids of the triple terms that matched pushed down to the triple pattern, which then
//! reads only the rows with those ids.

use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{
    by_name, col, lit, Column, DataFrame, IdxCa, IdxSize, IntoLazy, JoinType, LazyFrame, NamedFrom,
    PolarsResult, Series,
};
use query_processing::expressions::{literal_enc, named_node_enc};
use query_processing::graph_patterns::{join, union};
use query_processing::pushdown_values::PushdownValues;
use query_processing::pushdowns::Pushdowns;
use query_processing::type_constraints::{ConstraintBaseRDFNodeType, PossibleTypes};
use representation::cats::triple_terms::{
    TripleTermPartition, TripleTermPartitionKey, TRIPLE_TERM_ID_COL_NAME,
};
use representation::cats::Cats;
use representation::solution_mapping::{BaseCatState, EagerSolutionMappings, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The most solutions whose triple term ids are pushed down to the triple pattern that the
/// triple term pattern is in (see Triplestore::lazy_bgp_triple_pattern).
const MAX_PUSHED_DOWN_TRIPLE_TERMS: usize = 100_000;

/// Known ids of triple terms are looked up directly when they are at most this fraction (as
/// 1/n) of the triple terms of the partitions they could be in; with more, reading the
/// partitions is cheaper.
const ID_LOOKUP_FRACTION: usize = 4;

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

/// Whether the term pattern is the variable.
fn is_variable(t: &TermPattern, v: &Variable) -> bool {
    matches!(t, TermPattern::Variable(x) if x == v)
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
        // Triple term patterns of this pattern with variables that the solutions bind are
        // matched first, with the values pushed down for those variables, and joined with the
        // solutions. The pattern then looks up only the ids of the triple terms that matched,
        // instead of every triple term of its predicate being joined with the solutions.
        if let Some(sm) = &solution_mappings {
            let first: Vec<usize> = pending
                .iter()
                .enumerate()
                .filter(|(_, (v, ttp))| {
                    (is_variable(&tp.subject, v) || is_variable(&tp.object, v))
                        && pattern_variables(ttp, pending)
                            .iter()
                            .any(|x| sm.rdf_node_types.contains_key(x))
                })
                .map(|(i, _)| i)
                .collect();
            if !first.is_empty() {
                let mut sm = solution_mappings.unwrap();
                let mut variables = vec![];
                for i in first.into_iter().rev() {
                    let (v, ttp) = pending.remove(i);
                    let matches = self.triple_term_pattern_solution_mappings(
                        &v,
                        &ttp,
                        query_settings.max_rows,
                        &pushdowns.variables_values,
                        None,
                    )?;
                    sm = join(
                        sm,
                        matches,
                        JoinType::Inner,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?;
                    sm = push_down_triple_term_ids(sm, &v, pushdowns)?;
                    variables.push(v);
                }
                let mut sm = self.lazy_triple_pattern(
                    Some(sm),
                    tp,
                    context,
                    pushdowns,
                    query_settings,
                    dataset,
                )?;
                for v in variables {
                    pushdowns.variables_values.remove(v.as_str());
                    sm = drop_variable(sm, v.as_str());
                }
                return Ok(sm);
            }
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
                let unit = self.join_bound_triple_term_patterns(
                    unit,
                    pending,
                    query_settings.max_rows,
                    &pushdowns.variables_values,
                )?;
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
        values: &HashMap<String, PushdownValues>,
    ) -> Result<SolutionMappings, SparqlError> {
        let (bound, rest): (Vec<_>, Vec<_>) = std::mem::take(pending)
            .into_iter()
            .partition(|(v, _)| sm.rdf_node_types.contains_key(v.as_str()));
        *pending = rest;
        self.join_triple_term_patterns(sm, &bound, max_rows, values)
    }

    /// Joins the solutions of a basic graph pattern with the matches of its triple term
    /// patterns, and drops the variables that stood for them.
    pub(crate) fn join_triple_term_patterns(
        &self,
        mut sm: SolutionMappings,
        triple_term_patterns: &[(Variable, TriplePattern)],
        max_rows: Option<usize>,
        values: &HashMap<String, PushdownValues>,
    ) -> Result<SolutionMappings, SparqlError> {
        for (v, tp) in triple_term_patterns {
            // With few solutions, their triple terms are found by their ids (see
            // TripleTerms::rows_of_ids), and no other triple terms are read. The solutions are
            // collected for their ids only when the ids can be few enough to be used (see
            // ID_LOOKUP_FRACTION), judged by the estimate of their number.
            let ids_can_be_used = sm.height_estimate <= MAX_PUSHED_DOWN_TRIPLE_TERMS
                && sm.height_estimate.saturating_mul(ID_LOOKUP_FRACTION)
                    <= self.candidate_triple_terms(tp)?;
            let ids = if ids_can_be_used {
                let eager = sm.as_eager();
                let ids = if eager.mappings.height() <= MAX_PUSHED_DOWN_TRIPLE_TERMS {
                    triple_term_ids(&eager, v)?
                } else {
                    None
                };
                sm = eager.as_lazy();
                ids
            } else {
                None
            };
            let matches = self.triple_term_pattern_solution_mappings(
                v,
                tp,
                max_rows,
                values,
                ids.as_deref(),
            )?;
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

    /// The number of triple terms that a triple term pattern can match: those of the partitions
    /// of its predicate, or all of them for a variable predicate.
    fn candidate_triple_terms(&self, tp: &TriplePattern) -> Result<usize, SparqlError> {
        let cats = self.global_cats.read()?;
        Ok(match &tp.predicate {
            NamedNodePattern::NamedNode(p) => cats
                .triple_terms
                .partitions_with_predicate(p)
                .map(|p| p.height())
                .sum(),
            NamedNodePattern::Variable(_) => cats.triple_terms.len(),
        })
    }

    /// The triple terms that match the triple term pattern, with their ids in `var` and the
    /// values of the variables of the pattern, which are limited to the values pushed down for
    /// them in `values`. With `ids`, only the triple terms with those ids are matched, read
    /// from the partitions that hold them, and so are the triple terms of a nested pattern.
    fn triple_term_pattern_solution_mappings(
        &self,
        var: &Variable,
        tp: &TriplePattern,
        max_rows: Option<usize>,
        values: &HashMap<String, PushdownValues>,
        ids: Option<&[u32]>,
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
        // The ids of the triple terms of the nested pattern, when the ids of these are known:
        // the objects of the rows of these that hold triple terms.
        let mut nested_ids: Option<Vec<u32>> = None;
        let uses_ids;
        let subject_targets = pattern_targets(&tp.subject, values);
        let object_targets = if nested.is_none() {
            pattern_targets(&tp.object, values)
        } else {
            None
        };
        {
            // Held while the partitions are read, and released before the union and the
            // nested pattern, which take it again.
            let cats = self.global_cats.read()?;
            let candidates: Vec<&TripleTermPartition> = match &tp.predicate {
                NamedNodePattern::NamedNode(p) => {
                    cats.triple_terms.partitions_with_predicate(p).collect()
                }
                NamedNodePattern::Variable(_) => cats.triple_terms.partitions().iter().collect(),
            };
            // Taking the rows of many ids costs more than reading their partitions, so the
            // ids are used only when they are few compared with the triple terms they could be.
            let candidate_rows: usize = candidates.iter().map(|p| p.height()).sum();
            let ids = ids.filter(|ids| ids.len() * ID_LOOKUP_FRACTION <= candidate_rows);
            uses_ids = ids.is_some();
            // The rows to match, per partition: the rows of the ids, or the rows of the
            // partitions left after their filters. The rows of small partitions with the codes
            // looked for are found now, and those partitions can be stacked (see stack_tables);
            // large ones are filtered in the query plan, which is faster for many rows.
            let mut tables: Vec<MatchTable> = vec![];
            let mut subject_codes = HashMap::new();
            let mut object_codes = HashMap::new();
            match ids {
                Some(ids) => {
                    let taken = cats.triple_terms.rows_of_ids(ids).map_err(polars_error)?;
                    for (part, df) in taken {
                        if let NamedNodePattern::NamedNode(p) = &tp.predicate {
                            if &part.key.predicate != p {
                                continue;
                            }
                        }
                        if nested.is_some() && part.key.object_type.is_triple_term() {
                            let objects = df
                                .column(OBJECT_COL_NAME)
                                .and_then(|c| c.u32().cloned())
                                .map_err(polars_error)?;
                            nested_ids
                                .get_or_insert_with(Vec::new)
                                .extend(objects.iter().flatten());
                        }
                        tables.push(MatchTable::of(part.key.clone(), df, true));
                    }
                }
                None => {
                    // Partitions whose filters hold none of the subjects or objects looked for
                    // are skipped.
                    for p in candidates {
                        if may_hold_targets(p, &subject_targets, false, &mut subject_codes, &cats)
                            && may_hold_targets(p, &object_targets, true, &mut object_codes, &cats)
                        {
                            let small = p.height() <= MAX_SCANNED_ROWS;
                            tables.push(MatchTable::of(p.key.clone(), p.df().clone(), small));
                        }
                    }
                }
            }
            // Stacked tables keep only the rows with the codes looked for, found by a scan, so
            // that the predicates are made only for those rows. Other tables are filtered in the
            // query plan, whose filters are faster on many rows.
            let mut units = vec![];
            'units: for mut unit in
                stack_tables(tables, predicate_name.is_some()).map_err(polars_error)?
            {
                if !uses_ids && unit.stacked.is_some() {
                    for (c, targets, by_object, codes) in [
                        (
                            SUBJECT_COL_NAME,
                            &subject_targets,
                            false,
                            &mut subject_codes,
                        ),
                        (OBJECT_COL_NAME, &object_targets, true, &mut object_codes),
                    ] {
                        let bt = if by_object {
                            &unit.key.object_type
                        } else {
                            &unit.key.subject_type
                        };
                        if let Some(codes) = target_codes(targets, bt, codes, &cats) {
                            if codes.len() <= MAX_FILTER_PROBES {
                                match rows_with_codes(&unit.df, c, codes).map_err(polars_error)? {
                                    Some(kept) => unit.df = kept,
                                    None => continue 'units,
                                }
                            }
                        }
                    }
                }
                unit.add_stacked_predicates().map_err(polars_error)?;
                units.push(unit);
            }
            for unit in units {
                let key = &unit.key;
                let height = unit.df.height();
                let mut lf = unit.df.lazy();
                let Some(filtered) =
                    filter_constant(lf, SUBJECT_COL_NAME, &tp.subject, &key.subject_type, &cats)?
                else {
                    continue;
                };
                let Some(filtered) = filter_pushed_down(
                    filtered,
                    SUBJECT_COL_NAME,
                    &tp.subject,
                    &key.subject_type,
                    values,
                    &cats,
                ) else {
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
                    let Some(filtered) = filter_pushed_down(
                        filtered,
                        OBJECT_COL_NAME,
                        &tp.object,
                        &key.object_type,
                        values,
                        &cats,
                    ) else {
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
                    columns.push(if unit.stacked.is_some() {
                        col(STACKED_PREDICATE_COL_NAME).alias(p)
                    } else {
                        lit(key.predicate.as_str().to_string()).alias(p)
                    });
                    types.insert(
                        p.clone(),
                        RDFNodeState::from_bases(BaseRDFNodeType::IRI, BaseCatState::String),
                    );
                }
                sms.push(SolutionMappings {
                    mappings: lf.select(columns),
                    rdf_node_types: types,
                    height_estimate: height,
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
            let inner_sm =
                self.triple_term_pattern_solution_mappings(v, inner, max_rows, values, {
                    // Known ids of these give the ids of the nested ones; none if no row of
                    // these holds a triple term.
                    if uses_ids {
                        let mut n = nested_ids.take().unwrap_or_default();
                        n.sort_unstable();
                        n.dedup();
                        nested_ids = Some(n);
                    }
                    nested_ids.as_deref()
                })?;
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

/// The rows of `lf` whose column `c` (of type `bt`) holds one of the values pushed down for the
/// variable of the term pattern, all rows if no values are pushed down for it or values of the
/// type are not codes, and None if none of the values can be in the column.
fn filter_pushed_down(
    lf: LazyFrame,
    c: &str,
    t: &TermPattern,
    bt: &BaseRDFNodeType,
    values: &HashMap<String, PushdownValues>,
    cats: &Cats,
) -> Option<LazyFrame> {
    let Some(vs) = variable_name(t).and_then(|name| values.get(&name)) else {
        return Some(lf);
    };
    let codes = if bt.is_triple_term() {
        vs.codes.get(bt).cloned()
    } else {
        vs.codes_of_type(bt, cats)
    };
    match codes {
        None => Some(lf),
        Some(codes) if codes.is_empty() => None,
        Some(codes) => {
            let codes = Series::new("".into(), codes);
            Some(lf.filter(col(c).is_in(lit(codes).implode(false), false)))
        }
    }
}

/// The most codes looked for that are checked against the filters of each partition, and below
/// which the partitions left are filtered before they are planned. More codes are not checked:
/// they would cost more than reading the partitions.
const MAX_FILTER_PROBES: usize = 1000;

/// The values the subject or object of a triple term pattern can have: its constant, or the
/// values pushed down for its variable. None when it can have any value.
fn pattern_targets(
    t: &TermPattern,
    values: &HashMap<String, PushdownValues>,
) -> Option<PushdownValues> {
    match t {
        TermPattern::NamedNode(nn) => Some(PushdownValues::from_terms(HashSet::from([
            Term::NamedNode(nn.clone()),
        ]))),
        TermPattern::Literal(l) => {
            Some(PushdownValues::from_terms(HashSet::from([Term::Literal(
                l.clone(),
            )])))
        }
        TermPattern::Variable(_) | TermPattern::BlankNode(_) => {
            variable_name(t).and_then(|n| values.get(&n).cloned())
        }
        TermPattern::Triple(_) => None,
    }
}

/// The codes of the target values that a column of type `bt` can hold (ids for triple terms),
/// None when the targets can be any value, or values of the type are not codes. The codes of
/// each type are found once, in `codes`.
fn target_codes<'c>(
    targets: &Option<PushdownValues>,
    bt: &BaseRDFNodeType,
    codes: &'c mut HashMap<BaseRDFNodeType, Option<Vec<u32>>>,
    cats: &Cats,
) -> Option<&'c Vec<u32>> {
    let t = targets.as_ref()?;
    codes
        .entry(bt.clone())
        .or_insert_with(|| {
            if bt.is_triple_term() {
                t.codes.get(bt).cloned()
            } else {
                t.codes_of_type(bt, cats)
            }
        })
        .as_ref()
}

/// Whether the partition may hold one of the target values in its subjects (or objects, if
/// `by_object`): false only when its filter holds none of their codes, or no target can be of
/// the type of the column.
fn may_hold_targets(
    part: &TripleTermPartition,
    targets: &Option<PushdownValues>,
    by_object: bool,
    codes: &mut HashMap<BaseRDFNodeType, Option<Vec<u32>>>,
    cats: &Cats,
) -> bool {
    if !part.has_filter(by_object) {
        return true;
    }
    let bt = if by_object {
        &part.key.object_type
    } else {
        &part.key.subject_type
    };
    match target_codes(targets, bt, codes, cats) {
        None => true,
        Some(c) if c.len() > MAX_FILTER_PROBES => true,
        Some(c) => c.iter().any(|c| part.may_hold_code(*c, by_object)),
    }
}

/// Partitions of at most this many triple terms are scanned for the codes looked for before
/// the query is planned, and can then be stacked; larger ones are filtered in the plan, whose
/// filters are faster on many rows.
const MAX_SCANNED_ROWS: usize = 65_536;

/// The rows of `df` whose column `c` holds one of the codes (sorted), None if no row does, and
/// `df` as it is if the column does not hold codes. A scan of the column, which for a small
/// table costs far less than a filter in a query plan.
fn rows_with_codes(df: &DataFrame, c: &str, codes: &[u32]) -> PolarsResult<Option<DataFrame>> {
    let Ok(column) = df.column(c).and_then(|c| c.u32().cloned()) else {
        return Ok(Some(df.clone()));
    };
    let wanted = |u: u32| {
        if codes.len() <= 8 {
            codes.contains(&u)
        } else {
            codes.binary_search(&u).is_ok()
        }
    };
    let mut rows: Vec<IdxSize> = vec![];
    if column.null_count() == 0 {
        // The values of the chunks as they are, without a check per row for nulls.
        let mut i: IdxSize = 0;
        for arr in column.downcast_iter() {
            for &u in arr.values().iter() {
                if wanted(u) {
                    rows.push(i);
                }
                i += 1;
            }
        }
    } else {
        rows.extend(
            column
                .iter()
                .enumerate()
                .filter(|(_, u)| u.is_some_and(wanted))
                .map(|(i, _)| i as IdxSize),
        );
    }
    if rows.is_empty() {
        Ok(None)
    } else if rows.len() == df.height() {
        Ok(Some(df.clone()))
    } else {
        Ok(Some(df.take(&IdxCa::from_vec("rows".into(), rows))?))
    }
}

/// With more tables than this to match against a variable predicate, the small tables of the
/// same subject and object types are stacked into one, so that the query has a few branches
/// rather than one per partition (each costs about 0.1 ms to run, whatever its size).
const MAX_UNION_BRANCHES: usize = 16;
/// The column of the predicates of stacked tables.
const STACKED_PREDICATE_COL_NAME: &str = "#predicate";
/// The column of the partition of each row of a stacked table, an index into its predicates,
/// from which STACKED_PREDICATE_COL_NAME is made for the rows that are kept.
const STACKED_PART_COL_NAME: &str = "#part";

/// The rows of one or more partitions to match a triple term pattern against. `key` gives the
/// subject and object types. The predicate is the one of `key`, or for a table stacked from
/// several partitions (`stacked`, their predicates) the column STACKED_PREDICATE_COL_NAME.
/// Only `stackable` tables (small ones) are stacked.
struct MatchTable {
    key: TripleTermPartitionKey,
    df: DataFrame,
    stacked: Option<Vec<NamedNode>>,
    stackable: bool,
}

impl MatchTable {
    fn of(key: TripleTermPartitionKey, df: DataFrame, stackable: bool) -> Self {
        MatchTable {
            key,
            df,
            stacked: None,
            stackable,
        }
    }

    /// Adds the column of predicates to a stacked table, from the partition of each row.
    fn add_stacked_predicates(&mut self) -> PolarsResult<()> {
        let Some(predicates) = &self.stacked else {
            return Ok(());
        };
        let parts = self.df.column(STACKED_PART_COL_NAME)?.u32()?.clone();
        let column: Vec<&str> = parts
            .into_no_null_iter()
            .map(|i| predicates[i as usize].as_str())
            .collect();
        self.df
            .with_column(Column::new(STACKED_PREDICATE_COL_NAME.into(), column))?;
        Ok(())
    }
}

/// The tables to match, with the stackable tables of the same types stacked when the predicate
/// is a variable and there are many tables (see MAX_UNION_BRANCHES). Stacking copies the rows
/// of small tables, which costs less than a branch per partition.
fn stack_tables(
    tables: Vec<MatchTable>,
    variable_predicate: bool,
) -> PolarsResult<Vec<MatchTable>> {
    if !variable_predicate || tables.len() <= MAX_UNION_BRANCHES {
        return Ok(tables);
    }
    let mut out = vec![];
    let mut groups: BTreeMap<(BaseRDFNodeType, BaseRDFNodeType), Vec<MatchTable>> = BTreeMap::new();
    for t in tables {
        if t.stackable {
            groups
                .entry((t.key.subject_type.clone(), t.key.object_type.clone()))
                .or_default()
                .push(t);
        } else {
            out.push(t);
        }
    }
    for (_, mut group) in groups {
        if group.len() == 1 {
            out.push(group.pop().unwrap());
            continue;
        }
        let key = group[0].key.clone();
        let mut stacked: Option<DataFrame> = None;
        let mut predicates = Vec::with_capacity(group.len());
        for (i, t) in group.into_iter().enumerate() {
            let mut df = t.df;
            let parts = vec![i as u32; df.height()];
            df.with_column(Column::new(STACKED_PART_COL_NAME.into(), parts))?;
            predicates.push(t.key.predicate);
            match &mut stacked {
                None => stacked = Some(df),
                Some(s) => {
                    s.vstack_mut_owned(df)?;
                }
            }
        }
        let mut df = stacked.unwrap();
        df.rechunk_mut();
        out.push(MatchTable {
            key,
            df,
            stacked: Some(predicates),
            stackable: false,
        });
    }
    Ok(out)
}

/// Pushes the ids of the triple terms of the variable `v` of the solutions down to the triple
/// pattern that has the variable, when there are few enough solutions to collect them. The
/// variable stands for a triple term pattern, so no other pattern has it.
fn push_down_triple_term_ids(
    sm: SolutionMappings,
    v: &Variable,
    pushdowns: &mut Pushdowns,
) -> Result<SolutionMappings, SparqlError> {
    if sm.height_estimate > MAX_PUSHED_DOWN_TRIPLE_TERMS {
        return Ok(sm);
    }
    let eager = sm.as_eager();
    if eager.mappings.height() <= MAX_PUSHED_DOWN_TRIPLE_TERMS {
        if let Some(ids) = triple_term_ids(&eager, v)? {
            pushdowns.variables_values.insert(
                v.as_str().to_string(),
                PushdownValues {
                    terms: Default::default(),
                    codes: BTreeMap::from([(BaseRDFNodeType::TripleTerm, ids)]),
                },
            );
        }
    }
    Ok(eager.as_lazy())
}

/// The distinct ids of the triple terms of the variable `v` of the solutions, sorted. Empty
/// when the variable holds no triple terms (as when none matched, and the column is untyped),
/// and None when it holds other values as well, as its column is then not one of ids.
fn triple_term_ids(
    eager: &EagerSolutionMappings,
    v: &Variable,
) -> Result<Option<Vec<u32>>, SparqlError> {
    let Some(state) = eager.rdf_node_types.get(v.as_str()) else {
        return Ok(Some(vec![]));
    };
    if !state.map.contains_key(&BaseRDFNodeType::TripleTerm) {
        return Ok(Some(vec![]));
    }
    if state.map.len() > 1 {
        return Ok(None);
    }
    let column = eager
        .mappings
        .column(v.as_str())
        .and_then(|c| c.u32().cloned())
        .map_err(polars_error)?;
    let mut ids: Vec<u32> = column.iter().flatten().collect();
    ids.sort_unstable();
    ids.dedup();
    Ok(Some(ids))
}

fn polars_error(e: polars::prelude::PolarsError) -> SparqlError {
    SparqlError::TriplestoreError(crate::errors::TriplestoreError::LazyLoadError(e))
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
