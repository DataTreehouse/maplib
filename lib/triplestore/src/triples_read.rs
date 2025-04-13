use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::TriplesToAdd;

use log::debug;
use memmap2::MmapOptions;
use oxrdf::{BlankNode, GraphName, NamedNode, Quad, Subject, Term};
use oxrdfio::{RdfFormat, RdfParser, RdfSyntaxError, SliceQuadParser};
use oxttl::ntriples::SliceNTriplesParser;
use oxttl::turtle::SliceTurtleParser;
use oxttl::{NTriplesParser, TurtleParser};
use polars::prelude::{as_struct, col, DataFrame, IntoLazy, LiteralValue, PlSmallStr, Series};
use polars_core::prelude::{IntoColumn, Scalar};
use rayon::iter::ParallelIterator;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator};
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_literal_to_polars_literal_value,
    rdf_owned_blank_node_to_polars_literal_value, rdf_owned_named_node_to_polars_literal_value,
};
use representation::{
    get_subject_datatype_ref, get_term_datatype_ref, BaseRDFNodeType, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::time::Instant;

type MapType = HashMap<String, HashMap<String, (Vec<Subject>, Vec<Term>)>>;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    pub fn read_triples_from_path(
        &mut self,
        path: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
    ) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        debug!(
            "Started reading triples from path {}",
            path.to_string_lossy()
        );
        let rdf_format = if let Some(rdf_format) = rdf_format {
            rdf_format
        } else if path.extension() == Some("ttl".as_ref()) {
            RdfFormat::Turtle
        } else if path.extension() == Some("nt".as_ref()) {
            RdfFormat::NTriples
        } else if path.extension() == Some("xml".as_ref()) {
            RdfFormat::RdfXml
        } else {
            todo!("Have not implemented file format {:?}", path);
        };
        let file = File::open(path).map_err(TriplestoreError::ReadTriplesFileError)?;
        let mut opt = MmapOptions::new();
        opt.stack();
        let map = unsafe { opt.map(&file).unwrap() };
        let res = self.read_triples(
            map.deref(),
            rdf_format,
            base_iri,
            transient,
            parallel,
            checked,
        );
        debug!(
            "Reading triples from path took {} seconds",
            now.elapsed().as_secs_f32()
        );
        res
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_triples_from_string(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
    ) -> Result<(), TriplestoreError> {
        self.read_triples(
            s.as_bytes(),
            rdf_format,
            base_iri,
            transient,
            parallel,
            checked,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_triples(
        &mut self,
        slice: &[u8],
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
    ) -> Result<(), TriplestoreError> {
        let start_quadproc_now = Instant::now();
        let readers =
            if (rdf_format == RdfFormat::Turtle || rdf_format == RdfFormat::NTriples) && parallel {
                let threads = if let Ok(threads) = std::thread::available_parallelism() {
                    threads.get()
                } else {
                    1
                };

                let mut readers = vec![];
                if rdf_format == RdfFormat::Turtle {
                    let mut parser = TurtleParser::new();
                    if !checked {
                        parser = parser.unchecked();
                    }
                    if let Some(base_iri) = base_iri {
                        parser = parser.with_base_iri(base_iri).unwrap();
                    }
                    for r in parser.split_slice_for_parallel_parsing(slice, threads) {
                        readers.push(MyFromSliceQuadReader {
                            parser: MyFromSliceQuadReaderKind::TurtlePar(r),
                        });
                    }
                } else if rdf_format == RdfFormat::NTriples {
                    let mut parser = NTriplesParser::new();
                    if !checked {
                        parser = parser.unchecked();
                    }
                    for r in parser.split_slice_for_parallel_parsing(slice, threads) {
                        readers.push(MyFromSliceQuadReader {
                            parser: MyFromSliceQuadReaderKind::NTriplesPar(r),
                        });
                    }
                }
                readers
            } else {
                let mut parser = RdfParser::from(rdf_format);
                if !checked {
                    parser = parser.unchecked();
                }
                if let Some(base_iri) = base_iri {
                    parser = parser.with_base_iri(base_iri).unwrap();
                }
                vec![MyFromSliceQuadReader {
                    parser: MyFromSliceQuadReaderKind::Other(parser.for_slice(slice)),
                }]
            };
        debug!("Effective parallelization for reading is {}", readers.len());

        let parser_call = self.parser_call.to_string();
        let predicate_maps: Vec<_> = readers
            .into_par_iter()
            .map(|r| create_predicate_map(r, &parser_call))
            .collect();

        let mut par_predicate_map: HashMap<String, Vec<MapType>> = HashMap::new();
        for m in predicate_maps {
            for (verb, map) in m? {
                if let Some(v) = par_predicate_map.get_mut(&verb) {
                    v.push(map);
                } else {
                    par_predicate_map.insert(verb, vec![map]);
                }
            }
        }

        let predicate_map: HashMap<String, MapType> = par_predicate_map
            .into_par_iter()
            .map(|(verb, maps)| {
                let mut subject_map: MapType = HashMap::new();
                for new_subject_map in maps {
                    for (subject_dt, new_object_map) in new_subject_map {
                        if let Some(object_map) = subject_map.get_mut(&subject_dt) {
                            for (object_dt, (new_subjects, new_objects)) in new_object_map {
                                if let Some((subjects, objects)) = object_map.get_mut(&object_dt) {
                                    subjects.extend(new_subjects);
                                    objects.extend(new_objects);
                                } else {
                                    object_map.insert(object_dt, (new_subjects, new_objects));
                                }
                            }
                        } else {
                            subject_map.insert(subject_dt, new_object_map);
                        }
                    }
                }
                (verb, subject_map)
            })
            .collect();

        debug!(
            "Processing quads took {} seconds",
            start_quadproc_now.elapsed().as_secs_f64()
        );

        let start_tripleproc_now = Instant::now();
        let triples_to_add: Vec<_> = predicate_map
            .into_par_iter()
            .map(|(k, map)| {
                let mut triples_to_add = vec![];
                for (subject_dt, obj_map) in map {
                    let subject_dt = BaseRDFNodeType::from_string(subject_dt);
                    for (object_dt, (subjects, objects)) in obj_map {
                        let object_dt = BaseRDFNodeType::from_string(object_dt);
                        let strings_iter = subjects.into_iter().map(|s| match s {
                            Subject::NamedNode(nn) => nn.into_string(),
                            Subject::BlankNode(bl) => bl.into_string(),
                        });
                        let mut subjects_ser = Series::from_iter(strings_iter);
                        subjects_ser.rename(SUBJECT_COL_NAME.into());

                        let objects_ser = particular_term_vec_to_series(objects, object_dt.clone());

                        let all_series =
                            vec![subjects_ser.into_column(), objects_ser.into_column()];
                        let mut df = DataFrame::new(all_series).unwrap();
                        // TODO: Include bad data also
                        df = df
                            .drop_nulls(Some(&[
                                SUBJECT_COL_NAME.to_string(),
                                OBJECT_COL_NAME.to_string(),
                            ]))
                            .unwrap();

                        triples_to_add.push(TriplesToAdd {
                            df,
                            subject_type: subject_dt.as_rdf_node_type(),
                            object_type: object_dt.as_rdf_node_type(),
                            static_verb_column: Some(NamedNode::new_unchecked(k.clone())),
                        });
                    }
                }
                triples_to_add
            })
            .collect();
        let triples_to_add: Vec<_> = triples_to_add.into_iter().flatten().collect();
        debug!(
            "Creating the triples to add as DFs took {} seconds",
            start_tripleproc_now.elapsed().as_secs_f64()
        );
        let start_add_triples_vec = Instant::now();
        self.parser_call += 1;
        self.add_triples_vec(
            triples_to_add,
            &uuid::Uuid::new_v4().to_string(),
            transient,
            true,
        )?;
        debug!(
            "Adding triples vec took {} seconds",
            start_add_triples_vec.elapsed().as_secs_f64()
        );
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

fn create_predicate_map(
    r: MyFromSliceQuadReader,
    parser_call: &str,
) -> Result<HashMap<String, MapType>, TriplestoreError> {
    let mut predicate_map = HashMap::new();
    for q in r {
        let Quad {
            subject,
            predicate,
            object,
            ..
        } = q.map_err(TriplestoreError::RDFSyntaxError)?;
        let type_map: &mut HashMap<_, HashMap<_, (Vec<Subject>, Vec<Term>)>> =
            if let Some(type_map) = predicate_map.get_mut(predicate.as_str()) {
                type_map
            } else {
                let verb_key = predicate.into_string();
                predicate_map.insert(verb_key.clone(), HashMap::new());
                predicate_map.get_mut(&verb_key).unwrap()
            };

        let subject_to_insert = subject_to_oxrdf_subject(subject, parser_call);
        let object_to_insert = term_to_oxrdf_term(object, parser_call);
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
    Ok(predicate_map)
}

//Adapted from proposed change to https://github.com/oxigraph/
#[must_use]
pub struct MyFromSliceQuadReader<'a> {
    pub parser: MyFromSliceQuadReaderKind<'a>,
}

pub enum MyFromSliceQuadReaderKind<'a> {
    Other(SliceQuadParser<'a>),
    TurtlePar(SliceTurtleParser<'a>),
    NTriplesPar(SliceNTriplesParser<'a>),
}

impl Iterator for MyFromSliceQuadReader<'_> {
    type Item = Result<Quad, RdfSyntaxError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match &mut self.parser {
            MyFromSliceQuadReaderKind::Other(parser) => parser.next()?,
            MyFromSliceQuadReaderKind::TurtlePar(parser) => match parser.next()? {
                Ok(triple) => Ok(triple.in_graph(GraphName::default())),
                Err(e) => Err(e.into()),
            },
            MyFromSliceQuadReaderKind::NTriplesPar(parser) => match parser.next()? {
                Ok(triple) => Ok(triple.in_graph(GraphName::default())),
                Err(e) => Err(e.into()),
            },
        })
    }
}

// These terms must be of the given dt
fn particular_term_vec_to_series(term_vec: Vec<Term>, dt: BaseRDFNodeType) -> Series {
    if dt.is_lang_string() {
        let langs = term_vec
            .par_iter()
            .map(|t| match t {
                Term::Literal(l) => LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(
                    l.language().unwrap().to_string(),
                ))),
                _ => panic!("Should never happen"),
            })
            .collect();
        let vals = term_vec
            .into_par_iter()
            .map(|t| match t {
                Term::Literal(l) => {
                    let (s, _, _) = l.destruct();
                    LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(s)))
                }
                _ => panic!("Should never happen"),
            })
            .collect();

        let val_ser = polars_literal_values_to_series(vals, LANG_STRING_VALUE_FIELD);
        let lang_ser = polars_literal_values_to_series(langs, LANG_STRING_LANG_FIELD);
        let mut df = DataFrame::new(vec![val_ser.into(), lang_ser.into()])
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
        df.drop_in_place(OBJECT_COL_NAME)
            .unwrap()
            .take_materialized_series()
    } else {
        let any_iter: Vec<_> = term_vec
            .into_par_iter()
            .map(|t| match t {
                Term::NamedNode(nn) => rdf_owned_named_node_to_polars_literal_value(nn),
                Term::BlankNode(bb) => rdf_owned_blank_node_to_polars_literal_value(bb),
                Term::Literal(l) => rdf_literal_to_polars_literal_value(&l),
            })
            .collect();
        polars_literal_values_to_series(any_iter, OBJECT_COL_NAME)
    }
}
