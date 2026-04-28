use crate::errors::TriplestoreError;
use crate::{TriplesToAdd, Triplestore};
use oxrdf::vocab::rdf;
use oxrdf::NamedNode;
use polars::prelude::{DataFrame, PlSmallStr};
use polars_core::prelude::IntoColumn;
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use quick_xml::Reader;
use representation::constants::RDF_PREFIX_IRI;
use representation::dataset::NamedGraph;
use representation::literals::{sniff_text_datatype, SeriesBuilder};
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use std::io::Cursor;

const XML_ROOT: &str = "http://sparql.xyz/facade-x/ns/root";
const DEFAULT_XML_DATA_PREFIX: &str = "http://sparql.xyz/facade-x/data/";
const ROOT_ELEMENT_PROPERTY: &str = "urn:maplib:rootElement";

type SubjectObjectBuilders = (SeriesBuilder, SeriesBuilder);
type ByObjectType = HashMap<String, SubjectObjectBuilders>;
type BySubjectType = HashMap<String, ByObjectType>;
type PredMap = HashMap<String, BySubjectType>;

struct Frame {
    subject: String,
    next_child: usize,
}

impl Triplestore {
    pub fn map_xml(
        &mut self,
        u8s: &mut Vec<u8>,
        named_graph: &NamedGraph,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let mut reader = Reader::from_reader(Cursor::new(u8s.as_slice()));
        reader.config_mut().trim_text(true);

        let mut pred_map: PredMap = HashMap::new();
        let doc_subject = new_iri_subject();

        push_iri_object(
            &mut pred_map,
            rdf::TYPE.as_str(),
            &doc_subject,
            XML_ROOT,
        );

        let mut stack: Vec<Frame> = Vec::new();
        let mut buf = Vec::new();

        loop {
            match reader
                .read_event_into(&mut buf)
                .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
            {
                Event::Eof => break,
                Event::Start(e) => {
                    let subj = open_element(
                        e.name().as_ref(),
                        e.attributes(),
                        &mut stack,
                        &doc_subject,
                        &mut pred_map,
                    )?;
                    stack.push(Frame {
                        subject: subj,
                        next_child: 1,
                    });
                }
                Event::Empty(e) => {
                    let _ = open_element(
                        e.name().as_ref(),
                        e.attributes(),
                        &mut stack,
                        &doc_subject,
                        &mut pred_map,
                    )?;
                }
                Event::End(_) => {
                    stack.pop();
                }
                Event::Text(t) => {
                    let raw = t
                        .decode()
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
                    let s = unescape(&raw)
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
                        .into_owned();
                    if !s.is_empty() {
                        push_text_child(&s, &mut stack, &mut pred_map);
                    }
                }
                Event::CData(t) => {
                    let s = std::str::from_utf8(t.as_ref())
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
                        .to_string();
                    if !s.is_empty() {
                        push_text_child(&s, &mut stack, &mut pred_map);
                    }
                }
                _ => {}
            }
            buf.clear();
        }

        flush_pred_map(self, pred_map, named_graph, transient)
    }
}

fn open_element(
    name: &[u8],
    attrs: quick_xml::events::attributes::Attributes,
    stack: &mut Vec<Frame>,
    doc_subject: &str,
    pred_map: &mut PredMap,
) -> Result<String, TriplestoreError> {
    let subject = new_iri_subject();

    if let Some(parent) = stack.last_mut() {
        let parent_subject = parent.subject.clone();
        let n = parent.next_child;
        parent.next_child += 1;
        let rdfi = rdf_n_property(n);
        push_iri_object(pred_map, rdfi.as_str(), &parent_subject, &subject);
    } else {
        push_iri_object(pred_map, ROOT_ELEMENT_PROPERTY, doc_subject, &subject);
    }

    let type_iri = qname_to_iri(name)?;
    push_iri_object(pred_map, rdf::TYPE.as_str(), &subject, type_iri.as_str());

    for attr in attrs {
        let attr = attr.map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
        let key = attr.key.as_ref();
        if key == b"xmlns" || key.starts_with(b"xmlns:") {
            continue;
        }
        let pred = qname_to_iri(key)?;
        let raw = std::str::from_utf8(&attr.value)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
        let value = unescape(raw)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
            .into_owned();
        push_typed_text(pred_map, pred.as_str(), &subject, &value);
    }

    Ok(subject)
}

fn push_text_child(text: &str, stack: &mut Vec<Frame>, pred_map: &mut PredMap) {
    if text.is_empty() {
        return;
    }
    let Some(frame) = stack.last_mut() else {
        return;
    };
    let n = frame.next_child;
    frame.next_child += 1;
    let subject = frame.subject.clone();
    let rdfi = rdf_n_property(n);
    push_typed_text(pred_map, rdfi.as_str(), &subject, text);
}

fn push_typed_text(pred_map: &mut PredMap, predicate: &str, subject: &str, text: &str) {
    let object_dt = sniff_text_datatype(text);
    let pair = ensure_pair(pred_map, predicate, &BaseRDFNodeType::IRI, &object_dt);
    pair.0.push_string(subject.to_string());
    pair.1
        .push_parsed(text.trim())
        .expect("sniff_text_datatype must agree with push_parsed");
}

fn push_iri_object(pred_map: &mut PredMap, predicate: &str, subject: &str, object: &str) {
    let pair = ensure_pair(
        pred_map,
        predicate,
        &BaseRDFNodeType::IRI,
        &BaseRDFNodeType::IRI,
    );
    pair.0.push_string(subject.to_string());
    pair.1.push_string(object.to_string());
}

fn ensure_pair<'a>(
    pred_map: &'a mut PredMap,
    predicate: &str,
    subject_dt: &BaseRDFNodeType,
    object_dt: &BaseRDFNodeType,
) -> &'a mut SubjectObjectBuilders {
    pred_map
        .entry(predicate.to_string())
        .or_default()
        .entry(subject_dt.to_string())
        .or_default()
        .entry(object_dt.to_string())
        .or_insert_with(|| {
            (
                SeriesBuilder::empty_for_node_type(subject_dt),
                SeriesBuilder::empty_for_node_type(object_dt),
            )
        })
}

fn flush_pred_map(
    triples: &mut Triplestore,
    pred_map: PredMap,
    named_graph: &NamedGraph,
    transient: bool,
) -> Result<(), TriplestoreError> {
    let mut tta_vec = Vec::new();
    for (pred_iri, sub_map) in pred_map {
        let predicate = NamedNode::new_unchecked(pred_iri);
        for (subj_dt_str, obj_map) in sub_map {
            let subject_type = BaseRDFNodeType::from_string(subj_dt_str);
            for (obj_dt_str, (sb, ob)) in obj_map {
                if sb.is_empty() {
                    continue;
                }
                let object_type = BaseRDFNodeType::from_string(obj_dt_str);
                let mut subject_ser = sb.into_series(SUBJECT_COL_NAME);
                subject_ser.rename(PlSmallStr::from_str(SUBJECT_COL_NAME));
                let mut object_ser = ob.into_series(OBJECT_COL_NAME);
                object_ser.rename(PlSmallStr::from_str(OBJECT_COL_NAME));
                let len = subject_ser.len();
                let df = DataFrame::new(
                    len,
                    vec![subject_ser.into_column(), object_ser.into_column()],
                )
                    .unwrap();
                let subject_state = subject_type.default_input_cat_state();
                let object_state = object_type.default_input_cat_state();
                tta_vec.push(TriplesToAdd {
                    df,
                    subject_type: subject_type.clone(),
                    object_type: object_type.clone(),
                    predicate: Some(predicate.clone()),
                    graph: named_graph.clone(),
                    subject_cat_state: subject_state,
                    object_cat_state: object_state,
                    predicate_cat_state: None,
                });
            }
        }
    }
    triples.add_triples_vec(tta_vec, transient)?;
    Ok(())
}

fn new_iri_subject() -> String {
    format!("urn:maplib:{}", uuid::Uuid::new_v4())
}

fn rdf_n_property(n: usize) -> NamedNode {
    NamedNode::new_unchecked(format!("{}_{}", RDF_PREFIX_IRI, n))
}

fn qname_to_iri(name: &[u8]) -> Result<NamedNode, TriplestoreError> {
    let s = std::str::from_utf8(name).map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
    let local = s.rsplit_once(':').map(|(_, l)| l).unwrap_or(s);
    let mut cleaned = String::with_capacity(local.len());
    for (i, c) in local.chars().enumerate() {
        if i == 0 && (c.is_numeric() || c == '_' || c == '-' || c == '.') {
            cleaned.push('_');
        }
        if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
            cleaned.push(c);
        } else {
            cleaned.push('_');
        }
    }
    NamedNode::new(format!("{DEFAULT_XML_DATA_PREFIX}{cleaned}"))
        .map_err(|e| TriplestoreError::XMLError(e.to_string()))
}