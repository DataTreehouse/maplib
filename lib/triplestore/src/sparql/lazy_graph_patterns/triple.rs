use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;

use log::trace;
use oxrdf::{NamedNode, Subject, Term};
use polars::prelude::IntoLazy;
use polars::prelude::{col, lit, AnyValue, DataType, JoinType};
use query_processing::graph_patterns::join;
use query_processing::pushdowns::Pushdowns;
use query_processing::type_constraints::PossibleTypes;
use representation::{literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    pub fn lazy_triple_pattern(
        &mut self,
        solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        context: &Context,
        pushdowns: &mut Pushdowns,
        include_transient: bool,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!(
            "Processing triple pattern {:?} at {}",
            triple_pattern,
            context.as_str()
        );

        let mut solution_mappings = solution_mappings
            .map(|solution_mappings| pushdowns.add_from_solution_mappings(solution_mappings));
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
        let verb_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);
        let SolutionMappings {
            mappings: lf,
            rdf_node_types: dts,
            height_estimate: new_height_upper_bound,
        } = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_multi_predicates_solution_mappings(
                Some(vec![n.to_owned()]),
                &subject_rename,
                &verb_rename,
                &object_rename,
                &subjects,
                &objects,
                &subject_type_ctr,
                &object_type_ctr,
                include_transient,
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
                        if let RDFNodeType::IRI = dt {
                            let mappings_df = mappings.collect().unwrap();
                            let predicates_series = mappings_df
                                .column(v.as_str())
                                .unwrap()
                                .unique()
                                .unwrap()
                                .cast(&DataType::String)
                                .unwrap()
                                .take_materialized_series();
                            let predicates_iter = predicates_series.iter();
                            predicates = Some(
                                predicates_iter
                                    .filter_map(|x| match x {
                                        AnyValue::Null => None,
                                        AnyValue::String(s) => Some(literal_iri_to_namednode(s)),
                                        AnyValue::StringOwned(s) => {
                                            Some(literal_iri_to_namednode(&s))
                                        }
                                        x => panic!("Should never happen: {}", x),
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
                    &verb_rename,
                    &object_rename,
                    &subjects,
                    &objects,
                    &subject_type_ctr,
                    &object_type_ctr,
                    include_transient,
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
                mappings = mappings.drop(overlap.iter().map(col));
                if colnames.is_empty() {
                    mappings = mappings.filter(lit(false));
                } else {
                    mappings = mappings.join(lf, [], [], JoinType::Cross.into());
                }
                rdf_node_types.extend(dts);
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                    height_estimate: height_upper_bound,
                });
            } else {
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
                solution_mappings = Some(join(
                    solution_mappings.unwrap(),
                    new_solution_mappings,
                    JoinType::Inner,
                )?);
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
}

fn create_type_constraint(
    term_pattern: &TermPattern,
    variable_type_constraint: &HashMap<String, PossibleTypes>,
) -> Option<PossibleTypes> {
    match term_pattern {
        TermPattern::NamedNode(_) => Some(PossibleTypes::singular(BaseRDFNodeType::IRI)),
        TermPattern::Literal(l) => Some(PossibleTypes::singular(BaseRDFNodeType::Literal(
            l.datatype().into_owned(),
        ))),
        TermPattern::Variable(v) => variable_type_constraint.get(v.as_str()).cloned(),
        _ => None,
    }
}

pub fn create_subjects(
    term_pattern: &TermPattern,
    variable_pushdowns: &HashMap<String, HashSet<Term>>,
) -> Option<Vec<Subject>> {
    if let TermPattern::NamedNode(nn) = term_pattern {
        Some(vec![Subject::NamedNode(nn.clone())])
    } else if let TermPattern::Variable(v) = term_pattern {
        variable_pushdowns.get(v.as_str()).map(|terms| {
            terms
                .iter()
                .filter_map(|x| match x {
                    Term::NamedNode(nn) => Some(Subject::NamedNode(nn.clone())),
                    Term::BlankNode(bl) => Some(Subject::BlankNode(bl.clone())),
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
