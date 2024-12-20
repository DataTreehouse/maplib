use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;

use log::debug;
use oxrdf::{NamedNode, Term};
use polars::prelude::IntoLazy;
use polars::prelude::{col, lit, AnyValue, DataType, JoinType};
use query_processing::graph_patterns::join;
use representation::{literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashSet;

impl Triplestore {
    pub fn lazy_triple_pattern(
        &mut self,
        mut solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!(
            "Processing triple pattern {:?} at {}",
            triple_pattern,
            context.as_str()
        );
        let subject_term = create_term_pattern_term(&triple_pattern.subject);
        let object_term = create_term_pattern_term(&triple_pattern.object);
        let object_datatype_req = match &triple_pattern.object {
            TermPattern::NamedNode(_nn) => Some(BaseRDFNodeType::IRI),
            TermPattern::BlankNode(_) => None,
            TermPattern::Literal(l) => Some(BaseRDFNodeType::Literal(l.datatype().into_owned())),
            TermPattern::Variable(_) => None,
        };
        let subject_rename = get_keep_rename_term_pattern(&triple_pattern.subject);
        let verb_rename = get_keep_rename_named_node_pattern(&triple_pattern.predicate);
        let object_rename = get_keep_rename_term_pattern(&triple_pattern.object);

        let (
            SolutionMappings {
                mappings: lf,
                rdf_node_types: dts,
            },
            height_0,
        ) = match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => self.get_deduplicated_predicate_lf(
                n,
                &subject_rename,
                &verb_rename,
                &object_rename,
                subject_term,
                object_term,
                None, //TODO!
                object_datatype_req.as_ref(),
            )?,
            NamedNodePattern::Variable(v) => {
                let predicates: Option<HashSet<NamedNode>>;
                if let Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
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
                            })
                        } else {
                            predicates = Some(HashSet::new());
                            solution_mappings = Some(SolutionMappings {
                                mappings,
                                rdf_node_types,
                            })
                        };
                    } else {
                        solution_mappings = Some(SolutionMappings {
                            mappings,
                            rdf_node_types,
                        });
                        predicates = None;
                    }
                } else {
                    predicates = None;
                }
                let predicates: Option<Vec<_>> =
                    predicates.map(|predicates| predicates.into_iter().collect());
                self.get_predicates_lf(
                    predicates,
                    &subject_rename,
                    &verb_rename,
                    &object_rename,
                    subject_term,
                    object_term,
                    object_datatype_req.as_ref(),
                )?
            }
        };
        let colnames: Vec<_> = dts.keys().cloned().collect();
        if let Some(SolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) = solution_mappings
        {
            let overlap: Vec<_> = colnames
                .iter()
                .filter(|x| rdf_node_types.contains_key(*x))
                .cloned()
                .collect();
            if height_0 {
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
                });
            } else {
                solution_mappings = Some(SolutionMappings {
                    mappings,
                    rdf_node_types,
                });
                let new_solution_mappings = SolutionMappings {
                    mappings: lf,
                    rdf_node_types: dts,
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
            })
        }
        Ok(solution_mappings.unwrap())
    }
}

pub fn create_term_pattern_term(term_pattern: &TermPattern) -> Option<Term> {
    if let TermPattern::Literal(l) = term_pattern {
        Some(Term::Literal(l.clone()))
    } else if let TermPattern::NamedNode(nn) = term_pattern {
        Some(Term::NamedNode(nn.clone()))
    } else {
        None
    }
}

pub fn get_keep_rename_term_pattern(term_pattern: &TermPattern) -> Option<String> {
    if let TermPattern::Variable(v) = term_pattern {
        return Some(v.as_str().to_string());
    } else if let TermPattern::BlankNode(b) = term_pattern {
        return Some(b.as_str().to_string());
    }
    None
}

fn get_keep_rename_named_node_pattern(named_node_pattern: &NamedNodePattern) -> Option<String> {
    if let NamedNodePattern::Variable(v) = named_node_pattern {
        return Some(v.as_str().to_string());
    }
    None
}
