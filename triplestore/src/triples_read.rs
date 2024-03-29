use super::{TripleFormat, Triplestore};
use crate::errors::TriplestoreError;
use crate::TriplesToAdd;
use oxiri::Iri;

use crate::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars_core::frame::DataFrame;
use polars_core::prelude::{AnyValue, Series};
use representation::literals::sparql_literal_to_any_value;
use representation::RDFNodeType;
use rio_api::parser::TriplesParser;
use rio_turtle::{NTriplesParser, TurtleError, TurtleParser};
use rio_xml::{RdfXmlError, RdfXmlParser};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

impl Triplestore {
    pub fn read_triples_from_path(
        &mut self,
        path: &Path,
        triple_format: Option<TripleFormat>,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let triple_format = if let Some(triple_format) = triple_format {
            triple_format
        } else {
            if path.extension() == Some("ttl".as_ref()) {
                TripleFormat::Turtle
            } else if path.extension() == Some("nt".as_ref()) {
                TripleFormat::NTriples
            } else if path.extension() == Some("xml".as_ref()) {
                TripleFormat::RDFXML
            } else {
                todo!("Have not implemented file format {:?}", path);
            }
        };

        let reader =
            BufReader::new(File::open(path).map_err(TriplestoreError::ReadTriplesFileError)?);

        self.read_triples(reader, triple_format, base_iri, transient)
    }

    pub fn read_triples_from_string(
        &mut self,
        s: &str,
        triple_format: TripleFormat,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let reader = BufReader::new(s.as_bytes());
        self.read_triples(reader, triple_format, base_iri, transient)
    }

    pub fn read_triples<R: std::io::Read>(
        &mut self,
        reader: BufReader<R>,
        triple_format: TripleFormat,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        //Copied from the documentation of rio_turtle
        let mut predicate_map = HashMap::new();
        let parser_call = self.parser_call.to_string();
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
            subjects.push(rio_subject_to_oxrdf_subject(&t.subject, &parser_call));
            objects.push(rio_term_to_oxrdf_term(&t.object, &parser_call));
        };

        let base_iri = if let Some(base_iri) = base_iri {
            Some(
                Iri::parse(base_iri)
                    .map_err(|x| TriplestoreError::InvalidBaseIri(x.to_string()))?,
            )
        } else {
            None
        };

        match triple_format {
            TripleFormat::NTriples => {
                let mut ntparser = NTriplesParser::new(reader);
                ntparser
                    .parse_all(&mut |x| -> Result<(), TurtleError> {
                        parse_func(x);
                        Ok(())
                    })
                    .map_err(|x| TriplestoreError::TurtleParsingError(x.to_string()))?;
            }
            TripleFormat::Turtle => {
                let mut tparser = TurtleParser::new(BufReader::new(reader), base_iri);
                tparser
                    .parse_all(&mut |x| {
                        parse_func(x);
                        Ok(())
                    })
                    .map_err(|x: rio_turtle::TurtleError| {
                        TriplestoreError::NTriplesParsingError(x.to_string())
                    })?;
            }
            TripleFormat::RDFXML => {
                let mut xmlparser = RdfXmlParser::new(BufReader::new(reader), base_iri);
                xmlparser
                    .parse_all(&mut |x| -> Result<(), RdfXmlError> {
                        parse_func(x);
                        Ok(())
                    })
                    .map_err(|x| TriplestoreError::XMLParsingError(x.to_string()))?;
            }
        }

        let mut triples_to_add = vec![];
        for (k, map) in predicate_map {
            for ((subject_dt, object_dt), (subjects, objects)) in map {
                let strings_iter = subjects.into_iter().map(|s| match s {
                    oxrdf::Subject::NamedNode(nn) => nn.to_string(),
                    oxrdf::Subject::BlankNode(bl) => bl.to_string(),
                });
                let mut subjects_ser = Series::from_iter(strings_iter);
                subjects_ser.rename(SUBJECT_COL_NAME);

                let any_iter: Vec<AnyValue> = objects
                    .into_iter()
                    .map(|t| match t {
                        oxrdf::Term::NamedNode(nn) => AnyValue::StringOwned(nn.to_string().into()),
                        oxrdf::Term::BlankNode(bb) => AnyValue::StringOwned(bb.to_string().into()),
                        oxrdf::Term::Literal(l) => {
                            sparql_literal_to_any_value(
                                l.value(),
                                l.language(),
                                &Some(l.datatype()),
                            )
                            .0
                        }
                    })
                    .collect();

                let objects_ser =
                    Series::from_any_values(OBJECT_COL_NAME, any_iter.as_slice(), false).unwrap();

                let all_series = vec![subjects_ser, objects_ser];
                let df = DataFrame::new(all_series).unwrap();
                triples_to_add.push(TriplesToAdd {
                    df,
                    subject_type: subject_dt,
                    object_type: object_dt,
                    static_verb_column: Some(NamedNode::new_unchecked(k.clone())),
                    has_unique_subset: false,
                });
            }
        }
        self.parser_call += 1;
        self.add_triples_vec(triples_to_add, &uuid::Uuid::new_v4().to_string(), transient)?;
        Ok(())
    }
}

fn rio_term_to_oxrdf_term(t: &rio_api::model::Term, parser_call: &str) -> oxrdf::Term {
    match t {
        rio_api::model::Term::NamedNode(nn) => {
            oxrdf::Term::NamedNode(rio_named_node_to_oxrdf_named_node(nn))
        }
        rio_api::model::Term::BlankNode(bn) => {
            oxrdf::Term::BlankNode(rio_blank_node_to_oxrdf_blank_node(bn, parser_call))
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

fn rio_subject_to_oxrdf_subject(s: &rio_api::model::Subject, parser_call: &str) -> oxrdf::Subject {
    match s {
        rio_api::model::Subject::NamedNode(nn) => {
            oxrdf::Subject::NamedNode(rio_named_node_to_oxrdf_named_node(nn))
        }
        rio_api::model::Subject::BlankNode(bn) => {
            oxrdf::Subject::BlankNode(rio_blank_node_to_oxrdf_blank_node(bn, parser_call))
        }
        rio_api::model::Subject::Triple(_) => {
            todo!()
        }
    }
}

fn rio_blank_node_to_oxrdf_blank_node(
    bn: &rio_api::model::BlankNode,
    parser_call: &str,
) -> oxrdf::BlankNode {
    oxrdf::BlankNode::new_unchecked(format!("{}_{}", bn.id, parser_call))
}

fn rio_named_node_to_oxrdf_named_node(nn: &rio_api::model::NamedNode) -> oxrdf::NamedNode {
    oxrdf::NamedNode::new_unchecked(nn.iri)
}

fn get_rio_subject_datatype(s: &rio_api::model::Subject) -> RDFNodeType {
    match s {
        rio_api::model::Subject::NamedNode(_) => RDFNodeType::IRI,
        rio_api::model::Subject::BlankNode(_) => RDFNodeType::BlankNode,
        _ => {
            todo!()
        }
    }
}

fn get_rio_term_datatype(t: &rio_api::model::Term) -> RDFNodeType {
    match t {
        rio_api::model::Term::NamedNode(_) => RDFNodeType::IRI,
        rio_api::model::Term::BlankNode(_) => RDFNodeType::BlankNode,
        rio_api::model::Term::Literal(l) => match l {
            rio_api::model::Literal::Simple { .. } => {
                RDFNodeType::Literal(xsd::STRING.into_owned())
            }
            rio_api::model::Literal::LanguageTaggedString { .. } => {
                RDFNodeType::Literal(rdf::LANG_STRING.into_owned())
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
