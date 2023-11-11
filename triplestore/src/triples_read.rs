use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::TriplesToAdd;
use oxrdf::vocab::xsd;
use polars_core::frame::DataFrame;
use polars_core::prelude::{AnyValue, NamedFrom, Series};
use representation::literals::sparql_literal_to_any_value;
use representation::RDFNodeType;
use rio_api::parser::TriplesParser;
use rio_turtle::{NTriplesParser, TurtleError, TurtleParser};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

impl Triplestore {
    pub fn read_triples(&mut self, path: &Path) -> Result<(), TriplestoreError> {
        //Copied from the documentation of rio_turtle

        let mut predicate_map = HashMap::new();
        let parse_func = &mut |t: rio_api::model::Triple| {
            let verb_key = t.predicate.iri;
            if !predicate_map.contains_key(verb_key) {
                predicate_map.insert(verb_key.to_string(), HashMap::new());
            }
            let type_map = predicate_map.get_mut(verb_key).unwrap();
            let subject_datatype = get_rio_subject_datatype(&t.subject);
            let object_datatype = get_rio_term_datatype(&t.object);
            let types_tuple = (subject_datatype, object_datatype);
            if !type_map.contains_key(&types_tuple) {
                type_map.insert(types_tuple.clone(), (vec![], vec![]));
            }

            let (subjects, objects) = type_map.get_mut(&(types_tuple)).unwrap();
            subjects.push(rio_subject_to_oxrdf_subject(&t.subject));
            objects.push(rio_term_to_oxrdf_term(&t.object));
            Ok(()) as Result<(), TurtleError>
        };

        if path.ends_with(".ttl") {
            let mut tparser = TurtleParser::new(
                BufReader::new(
                    File::open(path).map_err(|x| TriplestoreError::ReadTriplesFileError(x))?,
                ),
                None,
            );
            tparser
                .parse_all(parse_func)
                .map_err(|x| TriplestoreError::TurtleParsingError(x.to_string()))?;
        } else if path.ends_with(".nt") {
            let mut ntparser = NTriplesParser::new(BufReader::new(
                File::open(path).map_err(|x| TriplestoreError::ReadTriplesFileError(x))?,
            ));
            ntparser
                .parse_all(parse_func)
                .map_err(|x| TriplestoreError::TurtleParsingError(x.to_string()))?;
        } else {
            todo!("Have not implemented file format {:?}", path);
        }

        let mut triples_to_add = vec![];
        for (k, map) in predicate_map {
            for ((subject_dt, object_dt), (subjects, objects)) in map {
                let mut any_vec = vec![];
                for s in &subjects {
                    any_vec.push(match s {
                        oxrdf::Subject::NamedNode(nn) => AnyValue::Utf8(nn.as_str()),
                        oxrdf::Subject::BlankNode(bb) => AnyValue::Utf8(bb.as_str()),
                        _ => {
                            todo!()
                        }
                    });
                }
                let subjects_ser =
                    Series::from_any_values("subject", any_vec.as_slice(), false).unwrap();

                let mut any_vec = vec![];
                let mut found_language_tag = false;
                let mut language_tags_vec = vec![];
                for t in &objects {
                    match t {
                        oxrdf::Term::NamedNode(nn) => any_vec.push(AnyValue::Utf8(nn.as_str())),
                        oxrdf::Term::BlankNode(bb) => any_vec.push(AnyValue::Utf8(bb.as_str())),
                        oxrdf::Term::Literal(l) => {
                            if l.datatype() == xsd::STRING {
                                language_tags_vec.push(match l.language() {
                                    None => None,
                                    Some(tag) => Some(tag.to_string()),
                                });
                            }
                            any_vec.push(
                                sparql_literal_to_any_value(l.value(), &Some(l.datatype())).0,
                            );
                        }
                    }
                }
                let language_tag_ser = if found_language_tag {
                    Some(Series::new("language_tag", language_tags_vec))
                } else {
                    None
                };

                let objects_ser =
                    Series::from_any_values("object", any_vec.as_slice(), false).unwrap();

                let mut all_series = vec![subjects_ser, objects_ser];
                if let Some(language_tag_ser) = language_tag_ser {
                    all_series.push(language_tag_ser);
                }
                let df = DataFrame::new(all_series).unwrap();
                triples_to_add.push(TriplesToAdd {
                    df,
                    subject_type: subject_dt,
                    object_type: object_dt,
                    language_tag: None,
                    static_verb_column: Some(k.to_string()),
                    has_unique_subset: false,
                });
            }
        }
        self.add_triples_vec(triples_to_add, &uuid::Uuid::new_v4().to_string())?;

        Ok(())
    }
}

fn rio_term_to_oxrdf_term(t: &rio_api::model::Term) -> oxrdf::Term {
    match t {
        rio_api::model::Term::NamedNode(nn) => {
            oxrdf::Term::NamedNode(rio_named_node_to_oxrdf_named_node(nn))
        }
        rio_api::model::Term::BlankNode(bn) => {
            oxrdf::Term::BlankNode(rio_blank_node_to_oxrdf_blank_node(bn))
        }
        rio_api::model::Term::Literal(l) => oxrdf::Term::Literal(rio_literal_to_oxrdf_literal(l)),
        rio_api::model::Term::Triple(_) => {
            todo!()
        }
    }
}

fn rio_literal_to_oxrdf_literal(l: &rio_api::model::Literal) -> oxrdf::Literal {
    match l {
        rio_api::model::Literal::Simple { value } => oxrdf::Literal::new_simple_literal(*value),
        rio_api::model::Literal::LanguageTaggedString { value, language } => {
            oxrdf::Literal::new_language_tagged_literal_unchecked(*value, *language)
        }
        rio_api::model::Literal::Typed { value, datatype } => {
            oxrdf::Literal::new_typed_literal(*value, rio_named_node_to_oxrdf_named_node(datatype))
        }
    }
}

fn rio_subject_to_oxrdf_subject(s: &rio_api::model::Subject) -> oxrdf::Subject {
    match s {
        rio_api::model::Subject::NamedNode(nn) => {
            oxrdf::Subject::NamedNode(rio_named_node_to_oxrdf_named_node(nn))
        }
        rio_api::model::Subject::BlankNode(bn) => {
            oxrdf::Subject::BlankNode(rio_blank_node_to_oxrdf_blank_node(bn))
        }
        rio_api::model::Subject::Triple(_) => {
            todo!()
        }
    }
}

fn rio_blank_node_to_oxrdf_blank_node(bn: &rio_api::model::BlankNode) -> oxrdf::BlankNode {
    oxrdf::BlankNode::new_unchecked(bn.id)
}

fn rio_named_node_to_oxrdf_named_node(nn: &rio_api::model::NamedNode) -> oxrdf::NamedNode {
    oxrdf::NamedNode::new_unchecked(nn.iri)
}

fn get_rio_subject_datatype(s: &rio_api::model::Subject) -> RDFNodeType {
    match s {
        rio_api::model::Subject::NamedNode(_) => RDFNodeType::IRI,
        rio_api::model::Subject::BlankNode(_) => RDFNodeType::BlankNode,
        _ => {todo!()}
    }
}

fn get_rio_term_datatype(t: &rio_api::model::Term) -> RDFNodeType {
    match t {
        rio_api::model::Term::NamedNode(_) => RDFNodeType::IRI,
        rio_api::model::Term::BlankNode(_) => RDFNodeType::BlankNode,
        rio_api::model::Term::Literal(l) => match l {
            rio_api::model::Literal::Simple { .. }
            | rio_api::model::Literal::LanguageTaggedString { .. } => {
                RDFNodeType::Literal(xsd::STRING.into_owned())
            }
            rio_api::model::Literal::Typed { value: _, datatype } => {
                RDFNodeType::Literal(rio_named_node_to_oxrdf_named_node(datatype))
            }
        },
        _ => {
            todo!()
        }
    }
}
