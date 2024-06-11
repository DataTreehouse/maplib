use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::TriplesToAdd;

use crate::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use log::debug;
use oxrdf::{BlankNode, NamedNode, Quad, Subject, Term};
use oxrdfio::{RdfFormat, RdfParser};
use polars::prelude::{as_struct, col, DataFrame, IntoLazy, LiteralValue, Series};
use rayon::iter::ParallelIterator;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator};
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_blank_node_to_polars_literal_value,
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
    rdf_owned_blank_node_to_polars_literal_value, rdf_owned_named_node_to_polars_literal_value,
};
use representation::{
    BaseRDFNodeType, BaseRDFNodeTypeRef, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

impl Triplestore {
    pub fn read_triples_from_path(
        &mut self,
        path: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let rdf_format = if let Some(rdf_format) = rdf_format {
            rdf_format
        } else {
            if path.extension() == Some("ttl".as_ref()) {
                RdfFormat::Turtle
            } else if path.extension() == Some("nt".as_ref()) {
                RdfFormat::NTriples
            } else if path.extension() == Some("xml".as_ref()) {
                RdfFormat::RdfXml
            } else {
                todo!("Have not implemented file format {:?}", path);
            }
        };

        let reader =
            BufReader::new(File::open(path).map_err(TriplestoreError::ReadTriplesFileError)?);
        self.read_triples(reader, rdf_format, base_iri, transient)
    }

    pub fn read_triples_from_string(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let reader = BufReader::new(s.as_bytes());
        self.read_triples(reader, rdf_format, base_iri, transient)
    }

    pub fn read_triples<R: std::io::Read>(
        &mut self,
        reader: BufReader<R>,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let start_parse_now = Instant::now();
        let mut parser = RdfParser::from_format(rdf_format).unchecked();
        if let Some(base_iri) = base_iri {
            parser = parser.with_base_iri(base_iri).unwrap();
        }

        let quads: Vec<_> = parser.parse_read(reader).map(|x| x.unwrap()).collect();
        debug!(
            "Parsing {} quads took {} seconds",
            quads.len(),
            start_parse_now.elapsed().as_secs_f64()
        );

        let mut predicate_map = HashMap::new();
        let parser_call = self.parser_call.to_string();

        let start_quadproc_now = Instant::now();
        for q in quads {
            let Quad {
                subject,
                predicate,
                object,
                graph_name: _,
            } = q;
            let type_map: &mut HashMap<_, HashMap<_, (Vec<Subject>, Vec<Term>)>> =
                if let Some(type_map) = predicate_map.get_mut(predicate.as_str()) {
                    type_map
                } else {
                    let verb_key = predicate.into_string();
                    predicate_map.insert(verb_key.clone(), HashMap::new());
                    predicate_map.get_mut(&verb_key).unwrap()
                };

            let subject_to_insert = subject_to_oxrdf_subject(subject, &parser_call);
            let object_to_insert = term_to_oxrdf_term(object, &parser_call);
            let subject_datatype = get_subject_datatype_ref(&subject_to_insert);
            let object_datatype = get_term_datatype_ref(&object_to_insert);
            if let Some(obj_type_map) = type_map.get_mut(subject_datatype.as_str()) {
                if let Some((subjects, objects)) = obj_type_map.get_mut(object_datatype.as_str()) {
                    subjects.push(subject_to_insert);
                    objects.push(object_to_insert);
                } else {
                    obj_type_map.insert(
                        object_datatype.as_str().to_string(),
                        (vec![subject_to_insert], vec![object_to_insert]),
                    );
                }
            } else {
                let mut obj_type_map = HashMap::new();
                let subject_datatype_string = subject_datatype.as_str().to_string();
                obj_type_map.insert(
                    object_datatype.as_str().to_string(),
                    (vec![subject_to_insert], vec![object_to_insert]),
                );
                type_map.insert(subject_datatype_string, obj_type_map);
            }
        }
        debug!(
            "Processing quads took {} seconds",
            start_quadproc_now.elapsed().as_secs_f64()
        );

        let start_tripleproc_now = Instant::now();
        let mut triples_to_add = vec![];
        for (k, map) in predicate_map {
            for (subject_dt, obj_map) in map {
                let subject_dt = BaseRDFNodeType::from_string(subject_dt);
                for (object_dt, (subjects, objects)) in obj_map {
                    let object_dt = BaseRDFNodeType::from_string(object_dt);
                    let strings_iter = subjects.into_iter().map(|s| match s {
                        Subject::NamedNode(nn) => nn.into_string(),
                        Subject::BlankNode(bl) => bl.into_string(),
                    });
                    let mut subjects_ser = Series::from_iter(strings_iter);
                    subjects_ser.rename(SUBJECT_COL_NAME);

                    let objects_ser = if object_dt.is_lang_string() {
                        let langs = objects
                            .par_iter()
                            .map(|t| match t {
                                Term::Literal(l) => {
                                    LiteralValue::String(l.language().unwrap().to_string())
                                }
                                _ => panic!("Should never happen"),
                            })
                            .collect();
                        let vals = objects
                            .into_par_iter()
                            .map(|t| match t {
                                Term::Literal(l) => {
                                    let (s, _, _) = l.destruct();
                                    LiteralValue::String(s)
                                }
                                _ => panic!("Should never happen"),
                            })
                            .collect();

                        let val_ser =
                            polars_literal_values_to_series(vals, LANG_STRING_VALUE_FIELD);
                        let lang_ser =
                            polars_literal_values_to_series(langs, LANG_STRING_LANG_FIELD);
                        let mut df = DataFrame::new(vec![val_ser, lang_ser])
                            .unwrap()
                            .lazy()
                            .with_column(
                                as_struct(vec![
                                    col(LANG_STRING_VALUE_FIELD),
                                    col(LANG_STRING_LANG_FIELD),
                                ])
                                .alias(OBJECT_COL_NAME),
                            )
                            .collect()
                            .unwrap();
                        df.drop_in_place(OBJECT_COL_NAME).unwrap()
                    } else {
                        let any_iter: Vec<_> = objects
                            .into_par_iter()
                            .map(|t| match t {
                                Term::NamedNode(nn) => {
                                    rdf_owned_named_node_to_polars_literal_value(nn)
                                }
                                Term::BlankNode(bb) => {
                                    rdf_owned_blank_node_to_polars_literal_value(bb)
                                }
                                Term::Literal(l) => rdf_literal_to_polars_literal_value(&l),
                            })
                            .collect();
                        polars_literal_values_to_series(any_iter, OBJECT_COL_NAME)
                    };

                    let all_series = vec![subjects_ser, objects_ser];
                    let mut df = DataFrame::new(all_series).unwrap();
                    // TODO: Include bad data also
                    df = df
                        .drop_nulls(Some(&[SUBJECT_COL_NAME, OBJECT_COL_NAME]))
                        .unwrap();

                    triples_to_add.push(TriplesToAdd {
                        df,
                        subject_type: subject_dt.as_rdf_node_type(),
                        object_type: object_dt.as_rdf_node_type(),
                        static_verb_column: Some(NamedNode::new_unchecked(k.clone())),
                        has_unique_subset: true,
                    });
                }
            }
        }
        debug!(
            "Creating the triples to add as DFs took {} seconds",
            start_tripleproc_now.elapsed().as_secs_f64()
        );
        self.parser_call += 1;
        self.add_triples_vec(triples_to_add, &uuid::Uuid::new_v4().to_string(), transient)?;
        Ok(())
    }
}

fn term_to_oxrdf_term(t: Term, parser_call: &str) -> Term {
    if let Term::BlankNode(bn) = t {
        Term::BlankNode(blank_node_to_oxrdf_blank_node(bn, parser_call))
    } else {
        t
    }
}

fn subject_to_oxrdf_subject(s: Subject, parser_call: &str) -> Subject {
    if let Subject::BlankNode(bn) = s {
        Subject::BlankNode(blank_node_to_oxrdf_blank_node(bn, parser_call))
    } else {
        s
    }
}

fn blank_node_to_oxrdf_blank_node(bn: BlankNode, parser_call: &str) -> BlankNode {
    BlankNode::new_unchecked(format!("{}_{}", bn.as_str(), parser_call))
}

fn get_subject_datatype_ref(s: &Subject) -> BaseRDFNodeTypeRef {
    match s {
        Subject::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Subject::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        _ => {
            todo!()
        }
    }
}

fn get_term_datatype_ref(t: &Term) -> BaseRDFNodeTypeRef {
    match t {
        Term::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Term::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        Term::Literal(l) => BaseRDFNodeTypeRef::Literal(l.datatype()),
        _ => {
            todo!()
        }
    }
}

