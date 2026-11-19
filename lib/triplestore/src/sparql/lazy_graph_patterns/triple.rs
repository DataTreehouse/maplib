use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};

use crate::errors::TriplestoreError;
use crate::sparql::QuerySettings;
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use polars::prelude::{by_name, IntoLazy};
use polars::prelude::{col, lit, AnyValue, JoinType, UniqueKeepStrategy};
use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use query_processing::type_constraints::{ConstraintBaseRDFNodeType, PossibleTypes};
use representation::dataset::QueryGraph;
use representation::literal_iri_to_namednode;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use tracing::{instrument, trace};
use utils::fast_path::{self, FastPath};

impl Triplestore {
    #[instrument(skip_all)]
    pub fn lazy_triple_pattern(
        &self,
        solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        context: &Context,
        pushdowns: &mut Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!(
            "Processing triple pattern {:?} at {}",
            triple_pattern,
            context.as_str()
        );

        let focus = pattern_variables(triple_pattern);
        // With a constant predicate, the pushdown is weighed against the size of its table.
        let table_height = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => Some(self.predicate_height(n, dataset)),
            NamedNodePattern::Variable(_) => None,
        };
        let mut solution_mappings = solution_mappings.map(|solution_mappings| {
            pushdowns.add_from_solution_mappings(
                solution_mappings,
                self.global_cats.clone(),
                &focus,
                table_height,
            )
        });
        let subjects = create_subjects(&triple_pattern.subject, &pushdowns.variables_values);
        let subject_type_ctr = create_type_constraint(
            &triple_pattern.subject,
            &pushdowns.variables_type_constraints,
        );
        let objects = create_objects(&triple_pattern.object, &pushdowns.variables_values);
        let object_type_ctr = create_type_constraint(
            &triple_pattern.object,
            &pushdowns.variables_type_constraints,
        );
        let subject_rename = get_keep_rename_term_pattern(&triple_pattern.subject);
        let predicate_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);

        let SolutionMappings {
            mappings: lf,
            rdf_node_types: dts,
            height_estimate: new_height_upper_bound,
        } = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_multi_predicates_solution_mappings(
                Some(vec![n.to_owned()]),
                &subject_rename,
                &predicate_rename,
                &object_rename,
                &subjects,
                &objects,
                &subject_type_ctr,
                &object_type_ctr,
                dataset,
            )?,
            NamedNodePattern::Variable(v) => {
                let mut predicates: Option<HashSet<NamedNode>> = None;
                if let Some(values) = pushdowns.variables_values.get(v.as_str()) {
                    predicates = Some(
                        values
                            .iter()
                            .filter(|x| matches!(x, Term::NamedNode(_)))
                            .map(|x| {
                                if let Term::NamedNode(nn) = x {
                                    nn.clone()
                                } else {
                                    panic!("Invalid state")
                                }
                            })
                            .collect(),
                    )
                } else if let Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                    height_estimate: height_upper_bound,
                }) = solution_mappings
                {
                    if let Some(dt) = rdf_node_types.get(v.as_str()) {
                        if dt.is_iri() {
                            let mappings_df = mappings.collect().unwrap();
                            //TODO! Fix
                            let mut predicates_series = mappings_df
                                .column(v.as_str())
                                .unwrap()
                                .unique()
                                .unwrap()
                                .as_materialized_series()
                                .clone();
                            let bt = dt.get_base_type().unwrap();
                            let bs = dt.get_base_state().unwrap();
                            let cats = self.global_cats.read()?;
                            if matches!(bs, BaseCatState::CategoricalNative(..)) {
                                predicates_series = cats
                                    .decode_of_type(&predicates_series, bt, bs.get_local_cats())
                                    .map_err(TriplestoreError::DecodeError)?;
                            }

                            let predicates_iter = predicates_series.iter();
                            predicates = Some(
                                predicates_iter
                                    .filter_map(|x| match x {
                                        AnyValue::Null => None,
                                        AnyValue::String(s) => Some(literal_iri_to_namednode(s)),
                                        AnyValue::StringOwned(s) => {
                                            Some(literal_iri_to_namednode(&s))
                                        }
                                        x => panic!("Should never happen: {x}"),
                                    })
                                    .collect(),
                            );
                            solution_mappings = Some(SolutionMappings {
                                mappings: mappings_df.lazy(),
                                rdf_node_types,
                                height_estimate: height_upper_bound,
                            })
                        } else {
                            predicates = Some(HashSet::new());
                            solution_mappings = Some(SolutionMappings {
                                mappings,
                                rdf_node_types,
                                height_estimate: height_upper_bound,
                            })
                        };
                    } else {
                        solution_mappings = Some(SolutionMappings {
                            mappings,
                            rdf_node_types,
                            height_estimate: height_upper_bound,
                        });
                    }
                }
                let predicates: Option<Vec<_>> =
                    predicates.map(|predicates| predicates.into_iter().collect());

                self.get_multi_predicates_solution_mappings(
                    predicates,
                    &subject_rename,
                    &predicate_rename,
                    &object_rename,
                    &subjects,
                    &objects,
                    &subject_type_ctr,
                    &object_type_ctr,
                    dataset,
                )?
            }
        };
        let colnames: Vec<_> = dts.keys().cloned().collect();
        if let Some(SolutionMappings {
            mut mappings,
            mut rdf_node_types,
            height_estimate: height_upper_bound,
        }) = solution_mappings
        {
            let overlap: Vec<_> = colnames
                .iter()
                .filter(|x| rdf_node_types.contains_key(*x))
                .cloned()
                .collect();
            if height_upper_bound == 0 {
                // Important that overlapping cols are dropped from mappings and not from lf,
                // since we also overwrite rdf_node_types with dts correspondingly below.
                mappings = mappings.drop(by_name(overlap.iter(), true, false));
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(lf, [], [], JoinType::Cross.into()).unwrap();
                }
                rdf_node_types.extend(dts);
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                    height_estimate: height_upper_bound,
                });
            } else {
                // Each solution matches at most as many triples as share its value of a key.
                // An untyped (unbound) column on either side is no join key.
                let keys: Vec<String> = overlap
                    .iter()
                    .filter(|k| {
                        !rdf_node_types.get(*k).unwrap().is_none()
                            && !dts.get(*k).unwrap().is_none()
                    })
                    .cloned()
                    .collect();
                let per_key = self.join_key_bound(triple_pattern, &keys, dataset);
                let bound = per_key.map(|m| height_upper_bound.saturating_mul(m));
                // The join's estimate and its check for joins of many rows on shared values
                // (whose product of the input heights would otherwise be that of the whole
                // table) then use the rows a solution can match: at most `m`.
                let new_height_upper_bound = match per_key {
                    Some(m) => new_height_upper_bound.min(m),
                    None => new_height_upper_bound,
                };
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                    height_estimate: height_upper_bound,
                });
                let new_solution_mappings = SolutionMappings {
                    mappings: lf,
                    rdf_node_types: dts,
                    height_estimate: new_height_upper_bound,
                };
                let mut joined = join(
                    solution_mappings.unwrap(),
                    new_solution_mappings,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
                if let Some(b) = bound {
                    fast_path::hit_when(
                        b < joined.height_estimate,
                        FastPath::JoinEstimateBoundedPerKey,
                    );
                    joined.height_estimate = joined.height_estimate.min(b);
                }
                solution_mappings = Some(joined);
            }
        } else {
            solution_mappings = Some(SolutionMappings {
                mappings: lf,
                rdf_node_types: dts,
                height_estimate: new_height_upper_bound,
            })
        }
        Ok(solution_mappings.unwrap())
    }

    /// Joins a group of triple patterns into the solution mappings on all the bound variables
    /// they share at once, as in [`super::triples_ordering::group_triple_patterns`].
    ///
    /// Each pattern is first restricted to the values its bound variables have so far, so that the
    /// joins within the group, on its new variables, stay near the size of the solution.
    pub(crate) fn lazy_triple_pattern_group(
        &self,
        solution_mappings: SolutionMappings,
        group: &[TriplePattern],
        context: &Context,
        pushdowns: &mut Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let bound = |tp: &TriplePattern| -> Vec<String> {
            let mut names = vec![];
            for t in [&tp.subject, &tp.object] {
                if let Some(name) = get_keep_rename_term_pattern(t) {
                    names.push(name);
                }
            }
            if let Some(name) = get_keep_rename_named_node_pattern(&tp.predicate) {
                names.push(name);
            }
            names.retain(|x| solution_mappings.rdf_node_types.contains_key(x));
            names.sort();
            names.dedup();
            names
        };
        let mut shared: Vec<String> = group.iter().flat_map(bound).collect();
        shared.sort();
        shared.dedup();
        let keys_of: Vec<Vec<String>> = group.iter().map(bound).collect();
        let focus: Vec<_> = shared.iter().map(|x| x.as_str()).collect();
        let solution_mappings = pushdowns.add_from_solution_mappings(
            solution_mappings,
            self.global_cats.clone(),
            &focus,
            None,
        );
        let mut group_solution_mappings: Option<SolutionMappings> = None;
        for (tp, keys) in group.iter().zip(keys_of) {
            let mut sm =
                self.lazy_triple_pattern(None, tp, context, pushdowns, query_settings, dataset)?;
            if !keys.is_empty() {
                // An inner join with the distinct bound values keeps the matching rows only.
                let values = SolutionMappings {
                    mappings: solution_mappings
                        .mappings
                        .clone()
                        .select(keys.iter().map(|k| col(k.as_str())).collect::<Vec<_>>())
                        .unique(None, UniqueKeepStrategy::Any),
                    rdf_node_types: keys
                        .iter()
                        .map(|k| {
                            (
                                k.clone(),
                                solution_mappings.rdf_node_types.get(k).unwrap().clone(),
                            )
                        })
                        .collect(),
                    height_estimate: solution_mappings.height_estimate,
                };
                sm = join(
                    sm,
                    values,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
            }
            group_solution_mappings = Some(match group_solution_mappings {
                None => sm,
                Some(g) => join(
                    g,
                    sm,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?,
            });
        }
        Ok(join(
            solution_mappings,
            group_solution_mappings.unwrap(),
            JoinType::Inner,
            self.global_cats.clone(),
            query_settings.max_rows,
        )?)
    }
}

impl Triplestore {
    /// For a pattern with a constant predicate joined on `keys`: the largest number of its
    /// triples that share the value of a key in the subject or object position. Each solution it
    /// is joined with matches at most that many. None if not known.
    fn join_key_bound(
        &self,
        tp: &TriplePattern,
        keys: &[String],
        dataset: &QueryGraph,
    ) -> Option<usize> {
        let NamedNodePattern::NamedNode(p) = &tp.predicate else {
            return None;
        };
        let mut bound: Option<usize> = None;
        for (term, subject) in [(&tp.subject, true), (&tp.object, false)] {
            let Some(name) = get_keep_rename_term_pattern(term) else {
                continue;
            };
            if !keys.contains(&name) {
                continue;
            }
            if let Some(m) = self.predicate_max_rows_per_key(p, subject, dataset) {
                bound = Some(bound.map_or(m, |b| b.min(m)));
            }
        }
        bound
    }
}

/// The names of the variables and blank nodes of a triple pattern.
fn pattern_variables(tp: &TriplePattern) -> Vec<&str> {
    let mut vs = vec![];
    for t in [&tp.subject, &tp.object] {
        if let TermPattern::Variable(v) = t {
            vs.push(v.as_str());
        }
    }
    if let NamedNodePattern::Variable(v) = &tp.predicate {
        vs.push(v.as_str());
    }
    vs
}

fn create_type_constraint(
    term_pattern: &TermPattern,
    variable_type_constraint: &HashMap<String, PossibleTypes>,
) -> Option<PossibleTypes> {
    match term_pattern {
        TermPattern::NamedNode(..) => Some(PossibleTypes::singular(ConstraintBaseRDFNodeType::IRI)),
        TermPattern::Literal(l) => Some(PossibleTypes::singular(
            ConstraintBaseRDFNodeType::Literal(l.datatype().into_owned()),
        )),
        TermPattern::Variable(v) => variable_type_constraint.get(v.as_str()).cloned(),
        _ => None,
    }
}

pub fn create_subjects(
    term_pattern: &TermPattern,
    variable_pushdowns: &HashMap<String, HashSet<Term>>,
) -> Option<Vec<NamedOrBlankNode>> {
    if let TermPattern::NamedNode(nn) = term_pattern {
        Some(vec![NamedOrBlankNode::NamedNode(nn.clone())])
    } else if let TermPattern::Variable(v) = term_pattern {
        variable_pushdowns.get(v.as_str()).map(|terms| {
            terms
                .iter()
                .filter_map(|x| match x {
                    Term::NamedNode(nn) => Some(NamedOrBlankNode::NamedNode(nn.clone())),
                    Term::BlankNode(bl) => Some(NamedOrBlankNode::BlankNode(bl.clone())),
                    _ => None,
                })
                .collect()
        })
    } else {
        None
    }
}

pub fn create_objects(
    term_pattern: &TermPattern,
    variable_pushdowns: &HashMap<String, HashSet<Term>>,
) -> Option<Vec<Term>> {
    match term_pattern {
        TermPattern::NamedNode(nn) => Some(vec![Term::NamedNode(nn.clone())]),
        TermPattern::Literal(lit) => Some(vec![Term::Literal(lit.clone())]),
        TermPattern::Variable(v) => variable_pushdowns
            .get(v.as_str())
            .map(|terms| terms.iter().cloned().collect()),
        _ => None,
    }
}

pub fn get_keep_rename_term_pattern(term_pattern: &TermPattern) -> Option<String> {
    if let TermPattern::Variable(v) = term_pattern {
        return Some(v.as_str().to_string());
    } else if let TermPattern::BlankNode(b) = term_pattern {
        return Some(b.to_string());
    }
    None
}

fn get_keep_rename_named_node_pattern(named_node_pattern: &NamedNodePattern) -> Option<String> {
    if let NamedNodePattern::Variable(v) = named_node_pattern {
        return Some(v.as_str().to_string());
    }
    None
}
