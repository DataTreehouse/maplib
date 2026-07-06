use crate::cats::LockedCats;
use crate::debug::DebugOutputs;
use crate::polars_to_rdf::{df_as_result, QuerySolutions};
use crate::query_context::Context;
use crate::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use crate::solution_mapping::EagerSolutionMappings;
use crate::{BaseRDFNodeType, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use oxttl::TurtleSerializer;
use polars::prelude::{col, lit, IntoLazy};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};

pub struct QueryResult {
    pub kind: QueryResultKind,
    pub debug: Option<DebugOutputs>,
    pub pushdown_paths: Vec<Context>,
}

#[derive(Debug)]
pub enum QueryResultKind {
    Select(EagerSolutionMappings),
    Construct(Vec<(EagerSolutionMappings, Option<NamedNode>)>),
}

impl QueryResultKind {
    pub fn json(&self, global_cats: LockedCats) -> String {
        match self {
            QueryResultKind::Select(sm) => {
                let QuerySolutions {
                    variables,
                    solutions,
                } = df_as_result(sm, global_cats);
                let json_serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json);
                let mut buffer = vec![];
                let mut serializer = json_serializer
                    .serialize_solutions_to_writer(&mut buffer, variables.clone())
                    .unwrap();
                for s in solutions {
                    let mut solvec = vec![];
                    for (i, t) in s.iter().enumerate() {
                        if let Some(t) = t {
                            solvec.push((variables.get(i).unwrap(), t.as_ref()));
                        }
                    }
                    serializer.serialize(solvec).unwrap()
                }
                serializer.finish().unwrap();
                String::from_utf8(buffer).unwrap()
            }
            QueryResultKind::Construct(c) => {
                let mut ser = TurtleSerializer::new().for_writer(vec![]);
                for (sm, predicate) in c {
                    let mut sm = sm.clone();
                    if let Some(predicate) = predicate {
                        sm.mappings = sm
                            .mappings
                            .lazy()
                            .with_column(
                                lit(rdf_named_node_to_polars_literal_value(predicate))
                                    .alias(PREDICATE_COL_NAME),
                            )
                            .select([
                                col(SUBJECT_COL_NAME),
                                col(PREDICATE_COL_NAME),
                                col(OBJECT_COL_NAME),
                            ])
                            .collect()
                            .unwrap();
                        sm.rdf_node_types.insert(
                            PREDICATE_COL_NAME.to_string(),
                            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                        );
                    }
                    let QuerySolutions {
                        variables,
                        solutions,
                    } = df_as_result(&sm, global_cats.clone());
                    for s in solutions {
                        let mut subject = None;
                        let mut predicate = None;
                        let mut object = None;
                        for (i, t) in s.into_iter().enumerate() {
                            let t = t.unwrap();
                            let v = variables.get(i).unwrap();
                            if v.as_str() == SUBJECT_COL_NAME {
                                subject = Some(match t {
                                    Term::NamedNode(nn) => NamedOrBlankNode::NamedNode(nn),
                                    Term::BlankNode(bl) => NamedOrBlankNode::BlankNode(bl),
                                    _ => todo!(),
                                });
                            } else if v.as_str() == PREDICATE_COL_NAME {
                                predicate = Some(match t {
                                    Term::NamedNode(nn) => nn,
                                    _ => panic!("Should never happen"),
                                });
                            } else if v.as_str() == OBJECT_COL_NAME {
                                object = Some(t);
                            } else {
                                panic!("Should never happen");
                            }
                        }
                        let t = Triple::new(subject.unwrap(), predicate.unwrap(), object.unwrap());
                        ser.serialize_triple(t.as_ref()).unwrap();
                    }
                }
                let res = ser.finish().unwrap();
                String::from_utf8(res).unwrap()
            }
        }
    }
}
