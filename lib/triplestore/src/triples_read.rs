use super::Triplestore;
use crate::errors::TriplestoreError;
use crate::TriplesToAdd;
use std::cmp;

use cimxml_import::{fix_cim_quad, Remapper};
use memmap2::MmapOptions;
use oxrdf::{BlankNode, GraphName, NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use oxrdfio::{
    JsonLdProfileSet, LoadedDocument, RdfFormat, RdfParser, RdfSyntaxError, SliceQuadParser,
};
use oxttl::ntriples::SliceNTriplesParser;
use oxttl::turtle::SliceTurtleParser;
use oxttl::{NTriplesParser, TurtleParser};
use polars::prelude::{concat, DataFrame, IntoLazy, UnionArgs};
use polars_core::prelude::{IntoColumn};
use rayon::iter::ParallelIterator;
use rayon::prelude::{IntoParallelIterator};
use representation::dataset::NamedGraph;
use representation::series_builder::{BySubjectType, PredMap};
use representation::{
    get_subject_datatype_ref, get_term_datatype_ref, BaseRDFNodeType, BaseRDFNodeTypeRef,
    SeriesBuilder
};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, instrument};

const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ExtendedRdfFormat {
    Normal(RdfFormat),
    CIMXML,
}

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, rdf_format, base_iri, transient, parallel, checked))]
    pub fn read_triples_from_path(
        &mut self,
        path: &Path,
        rdf_format: Option<ExtendedRdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        parallel: Option<bool>,
        checked: bool,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), TriplestoreError> {
        let now = Instant::now();
        let rdf_format = if let Some(rdf_format) = rdf_format {
            rdf_format
        } else if path.extension() == Some("ttl".as_ref()) {
            ExtendedRdfFormat::Normal(RdfFormat::Turtle)
        } else if path.extension() == Some("nt".as_ref()) {
            ExtendedRdfFormat::Normal(RdfFormat::NTriples)
        } else if path.extension() == Some("xml".as_ref())
            || path.extension() == Some("rdf".as_ref())
        {
            ExtendedRdfFormat::Normal(RdfFormat::RdfXml)
        } else if path.extension() == Some("jsonld".as_ref())
            || path.extension() == Some("json".as_ref())
        {
            ExtendedRdfFormat::Normal(RdfFormat::JsonLd {
                profile: JsonLdProfileSet::empty(),
            })
        } else {
            todo!("Have not implemented file format {:?}", path);
        };
        let file = File::open(path).map_err(TriplestoreError::ReadTriplesFileError)?;
        let opt = MmapOptions::new();
        let map = unsafe { opt.map(&file).unwrap() };
        self.read_triples(
            map.as_ref(),
            rdf_format,
            base_iri,
            transient,
            parallel,
            checked,
            graph,
            prefixes,
            triples_batch_size,
            known_contexts,
        )?;
        drop(map);

        debug!(
            "Reading triples from path took {} seconds",
            now.elapsed().as_secs_f32()
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_triples_from_string(
        &mut self,
        s: &str,
        rdf_format: ExtendedRdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: Option<bool>,
        checked: bool,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), TriplestoreError> {
        self.read_triples(
            s.as_bytes(),
            rdf_format,
            base_iri,
            transient,
            parallel,
            checked,
            graph,
            prefixes,
            triples_batch_size,
            known_contexts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn read_triples(
        &mut self,
        slice: &[u8],
        rdf_format: ExtendedRdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: Option<bool>,
        checked: bool,
        graph: &NamedGraph,
        prefixes: &HashMap<String, NamedNode>,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), TriplestoreError> {
        let use_slice = if slice.starts_with(&UTF8_BOM) {
            &slice[UTF8_BOM.len()..]
        } else {
            slice
        };

        let start_quadproc_now = Instant::now();
        let parallel = if let Some(parallel) = parallel {
            parallel
        } else {
            matches!(
                rdf_format,
                ExtendedRdfFormat::Normal(RdfFormat::NTriples)
            )
        };
        let mut readers = if matches!(
            rdf_format,
            ExtendedRdfFormat::Normal(RdfFormat::NTriples)
                | ExtendedRdfFormat::Normal(RdfFormat::Turtle)
        ) && parallel
        {
            let threads = if let Ok(threads) = std::thread::available_parallelism() {
                threads.get()
            } else {
                1
            };

            let mut readers = vec![];
            if rdf_format == ExtendedRdfFormat::Normal(RdfFormat::Turtle) {
                let mut parser = TurtleParser::new();
                for (k, v) in prefixes {
                    parser = parser.with_prefix(k, v.as_str()).unwrap();
                }
                if !checked {
                    parser = parser.lenient();
                }
                if let Some(base_iri) = base_iri {
                    parser = parser.with_base_iri(base_iri).unwrap();
                }
                for r in parser.split_slice_for_parallel_parsing(use_slice, threads) {
                    readers.push(MyFromSliceQuadReader {
                        parser: MyFromSliceQuadReaderKind::TurtlePar(r),
                    });
                }
            } else if rdf_format == ExtendedRdfFormat::Normal(RdfFormat::NTriples) {
                let mut parser = NTriplesParser::new();
                if !checked {
                    parser = parser.lenient();
                }
                for r in parser.split_slice_for_parallel_parsing(use_slice, threads) {
                    readers.push(MyFromSliceQuadReader {
                        parser: MyFromSliceQuadReaderKind::NTriplesPar(r),
                    });
                }
            }
            readers
        } else {
            let use_format = match rdf_format {
                ExtendedRdfFormat::Normal(n) => n,
                ExtendedRdfFormat::CIMXML => RdfFormat::RdfXml,
            };
            let mut parser = RdfParser::from(use_format.clone());
            if !checked {
                parser = parser.lenient();
            }
            if let Some(base_iri) = &base_iri {
                parser = parser.with_base_iri(base_iri).unwrap();
            }
            let mut for_slice = parser.for_slice(use_slice);
            if matches!(use_format, RdfFormat::JsonLd { .. }) {
                for_slice = for_slice.with_document_loader(move |url| {
                    if let Some(doc) = known_contexts.get(url) {
                        Ok(LoadedDocument {
                            url: url.to_string(),
                            content: doc.clone().into_bytes(),
                            format: RdfFormat::JsonLd {
                                profile: JsonLdProfileSet::empty(),
                            },
                        })
                    } else {
                        Err(Box::new(TriplestoreError::MissingContext(url.to_string())))
                    }
                });
            }
            if matches!(rdf_format, ExtendedRdfFormat::CIMXML) {
                vec![MyFromSliceQuadReader {
                    parser: MyFromSliceQuadReaderKind::CIMXML(for_slice, base_iri.clone()),
                }]
            } else {
                vec![MyFromSliceQuadReader {
                    parser: MyFromSliceQuadReaderKind::Other(for_slice),
                }]
            }
        };
        debug!("Effective parallelization for reading is {}", readers.len());

        let parser_call = self.parser_call.to_string();
        while !readers.is_empty() {
            let reader_batch_size = triples_batch_size / cmp::max(1, readers.len());
            let readers_predicate_maps: Vec<_> = readers
                .into_par_iter()
                .map(|r| create_predicate_map(r, &rdf_format, &parser_call, reader_batch_size))
                .collect();

            let mut updated_readers = vec![];
            let mut predicate_maps = vec![];
            for rp in readers_predicate_maps {
                let (reader, predicate_map) = rp?;
                if let Some(reader) = reader {
                    updated_readers.push(reader);
                }
                predicate_maps.push(predicate_map);
            }
            readers = updated_readers;

            let mut all_builders: Vec<(
                NamedGraph,
                NamedNode,
                BaseRDFNodeType,
                BaseRDFNodeType,
                SeriesBuilder,
                SeriesBuilder,
            )> = Vec::new();

            for map in predicate_maps.into_iter() {
                for (gr, pred_map) in map {
                    let use_graph = if matches!(graph, NamedGraph::DefaultGraph) {
                        NamedGraph::from(&gr)
                    } else {
                        graph.clone()
                    };
                    for (pred, bst) in pred_map.into_iter() {
                        let pred_nn = NamedNode::new_unchecked(pred);
                        for (subj, bot) in bst {
                            for (obj, (subjects, objects)) in bot {
                                all_builders.push((
                                    use_graph.clone(),
                                    pred_nn.clone(),
                                    subj.clone(),
                                    obj.clone(),
                                    subjects,
                                    objects,
                                ));
                            }
                        }
                    }
                }
            }
            debug!(
                "Processing quads took {} seconds",
                start_quadproc_now.elapsed().as_secs_f64()
            );

            let start_tripleproc_now = Instant::now();
            let triples_to_add: Vec<_> = all_builders
                .into_par_iter()
                .map(
                    |(graph, predicate, subject_type, object_type, subjects, objects)| {
                        let l = subjects.len();
                        let df = DataFrame::new(
                            l,
                            vec![
                                subjects.into_series(SUBJECT_COL_NAME).into_column(),
                                objects.into_series(OBJECT_COL_NAME).into_column(),
                            ],
                        )
                        .unwrap();
                        TriplesToAdd {
                            df,
                            subject_type: subject_type.clone(),
                            object_type: object_type.clone(),
                            predicate: Some(predicate),
                            graph,
                            subject_cat_state: subject_type.default_input_cat_state(),
                            predicate_cat_state: None,
                            object_cat_state: subject_type.default_input_cat_state(),
                        }
                    },
                )
                .collect();

            let mut tta_map = HashMap::new();
            for triple in triples_to_add.into_iter() {
                let k = (triple.graph.clone(), triple.predicate.clone(), triple.subject_type.clone(), triple.object_type.clone());
                if !tta_map.contains_key(&k) {
                    tta_map.insert(k.clone(), Vec::new());
                }
                tta_map.get_mut(&k).unwrap().push(triple);
            }

            let ttas:Vec<_> = tta_map.into_par_iter().map(|(k,mut ttas)| {
                if ttas.len() == 1 {
                    ttas.pop().unwrap()
                } else {
                    let (graph, predicate, subject_type, object_type) = k;
                    let mut lfs = Vec::with_capacity(ttas.len());
                    for tta in ttas {
                        lfs.push(tta.df.lazy());
                    }
                    let df = concat(lfs, UnionArgs {
                        parallel: true,
                        rechunk: false,
                        to_supertypes: false,
                        diagonal: false,
                        strict: false,
                        from_partitioned_ds: false,
                        maintain_order: false,
                    }).unwrap().collect().unwrap();
                    let subject_cat_state = subject_type.default_input_cat_state();
                    let object_cat_state = object_type.default_input_cat_state();
                    TriplesToAdd {
                        df,
                        subject_type,
                        object_type,
                        predicate,
                        graph,
                        subject_cat_state,
                        object_cat_state,
                        predicate_cat_state: None,
                    }
                }
            }).collect();

            debug!(
                "Creating the triples to add as DFs took {} seconds",
                start_tripleproc_now.elapsed().as_secs_f64()
            );
            let start_add_triples_vec = Instant::now();
            self.parser_call += 1;
            self.add_triples_vec(ttas, transient)?;
            debug!(
                "Adding triples vec took {} seconds",
                start_add_triples_vec.elapsed().as_secs_f64()
            );
        }
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

fn subject_to_oxrdf_subject(s: NamedOrBlankNode, parser_call: &str) -> NamedOrBlankNode {
    if let NamedOrBlankNode::BlankNode(bn) = s {
        NamedOrBlankNode::BlankNode(blank_node_to_oxrdf_blank_node(bn, parser_call))
    } else {
        s
    }
}

fn blank_node_to_oxrdf_blank_node(bn: BlankNode, parser_call: &str) -> BlankNode {
    BlankNode::new_unchecked(format!("{}_{}", bn.as_str(), parser_call))
}

fn create_predicate_map<'a>(
    mut r: MyFromSliceQuadReader<'a>,
    rdf_format: &ExtendedRdfFormat,
    parser_call: &str,
    max_iterations: usize,
) -> Result<
    (
        Option<MyFromSliceQuadReader<'a>>,
        HashMap<GraphName, PredMap>,
    ),
    TriplestoreError,
> {
    let cim_remapper = if matches!(rdf_format, ExtendedRdfFormat::CIMXML) {
        Some(Remapper::new())
    } else {
        None
    };
    let mut graph_predicate_map = HashMap::new();
    let mut subj_type_map: HashMap<String, BaseRDFNodeType> = HashMap::new();
    let mut obj_type_map: HashMap<String, BaseRDFNodeType> = HashMap::new();
    let mut unparseable = Vec::new();
    let mut empty_iter = false;
    let mut reached_max = false;
    let mut i = 0usize;
    while !empty_iter && !reached_max {
        if let Some(q) = r.next() {
            let Quad {
                subject,
                predicate,
                object,
                graph_name,
            } = q.map_err(TriplestoreError::RDFSyntaxError)?;
            let predicate_map =
                if let Some(predicate_map) = graph_predicate_map.get_mut(&graph_name) {
                    predicate_map
                } else {
                    graph_predicate_map.insert(graph_name.clone(), HashMap::new());
                    graph_predicate_map.get_mut(&graph_name).unwrap()
                };
            let type_map: &mut BySubjectType =
                if let Some(type_map) = predicate_map.get_mut(predicate.as_str()) {
                    type_map
                } else {
                    let predicate_key = predicate.as_str().to_string();
                    predicate_map.insert(predicate_key.clone(), HashMap::new());
                    predicate_map.get_mut(&predicate_key).unwrap()
                };

            let subject_to_insert = subject_to_oxrdf_subject(subject, parser_call);
            let object_to_insert = term_to_oxrdf_term(object, parser_call);
            let subject_ref_datatype = get_subject_datatype_ref(&subject_to_insert);
            let object_ref_datatype = get_term_datatype_ref(&object_to_insert);
            //Remap cim here
            let object_ref_datatype = if let Some(remapper) = &cim_remapper {
                remapper.remap_predicate_datatype(&predicate, &object_ref_datatype)
            } else {
                object_ref_datatype
            };
            let subject_type = get_or_insert_dt(subject_ref_datatype, &mut subj_type_map);
            let object_type = get_or_insert_dt(object_ref_datatype, &mut obj_type_map);
            if !type_map.contains_key(&subject_type) {
                type_map.insert(subject_type.clone(), HashMap::new());
            }

            let obj_type_map = type_map.get_mut(&subject_type).unwrap();
            if !obj_type_map.contains_key(&object_type) {
                obj_type_map.insert(
                    object_type.clone(),
                    (
                        SeriesBuilder::new(&subject_type),
                        SeriesBuilder::new(&object_type),
                    ),
                );
            }
            let (subjects, objects) = obj_type_map.get_mut(&object_type).unwrap();
            match objects.parse_term(&object_to_insert) {
                Ok(()) => {
                    subjects.push_named_or_blank(&subject_to_insert);
                }
                Err(e) => {
                    unparseable.push((Triple::new(subject_to_insert, predicate, object_to_insert), e));
                }
            }
        } else {
            empty_iter = true;
        }
        if i >= max_iterations {
            reached_max = true;
        }
        i += 1;
    }
    let out_r = if empty_iter { None } else { Some(r) };
    Ok((out_r, graph_predicate_map))
}

//Adapted from proposed change to https://github.com/oxigraph/
#[must_use]
pub struct MyFromSliceQuadReader<'a> {
    pub parser: MyFromSliceQuadReaderKind<'a>,
}

pub enum MyFromSliceQuadReaderKind<'a> {
    Other(SliceQuadParser<'a>),
    CIMXML(SliceQuadParser<'a>, Option<String>),
    TurtlePar(SliceTurtleParser<'a>),
    NTriplesPar(SliceNTriplesParser<'a>),
}

impl Iterator for MyFromSliceQuadReader<'_> {
    type Item = Result<Quad, RdfSyntaxError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match &mut self.parser {
            MyFromSliceQuadReaderKind::Other(parser) => parser.next()?,
            MyFromSliceQuadReaderKind::CIMXML(parser, base_iri) => {
                let q = parser.next()?;
                match q {
                    Ok(mut quad) => {
                        quad = fix_cim_quad(quad, base_iri.as_ref());
                        Ok(quad)
                    }
                    Err(e) => Err(e.into()),
                }
            }
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

fn get_or_insert_dt(
    base_rdfnode_type_ref: BaseRDFNodeTypeRef,
    type_map: &mut HashMap<String, BaseRDFNodeType>,
) -> BaseRDFNodeType {
    if let Some(t) = type_map.get(base_rdfnode_type_ref.as_str()) {
        t.clone()
    } else {
        let owned = base_rdfnode_type_ref.clone().to_owned();
        type_map.insert(base_rdfnode_type_ref.as_str().to_string(), owned.into_owned());
        type_map.get(base_rdfnode_type_ref.as_str()).unwrap().clone()
    }
}
