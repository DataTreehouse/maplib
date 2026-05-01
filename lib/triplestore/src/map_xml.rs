use crate::errors::TriplestoreError;
use crate::{TriplesToAdd, Triplestore};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{DataFrame, PlSmallStr};
use polars_core::prelude::IntoColumn;
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use quick_xml::Reader;
use representation::constants::RDF_PREFIX_IRI;
use representation::dataset::NamedGraph;
use representation::{BaseRDFNodeType, SeriesBuilder, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use simd_json::prelude::ObjectMut;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use polars::polars_utils::itertools::Itertools;

const XML_ROOT: &str = "http://sparql.xyz/facade-x/ns/root";
const DEFAULT_XML_DATA_PREFIX: &str = "http://sparql.xyz/facade-x/data/";

type SubjectObjectBuilders = (SeriesBuilder, SeriesBuilder);
type ByObjectType = HashMap<BaseRDFNodeType, SubjectObjectBuilders>;
type BySubjectType = HashMap<BaseRDFNodeType, ByObjectType>;
type PredMap = HashMap<String, BySubjectType>;

struct Frame {
    subject: String,
    next_child: usize,
    data_type: Option<Arc<BaseRDFNodeType>>,
    previous_base_prefix: Option<NamedNode>,
    introduced_prefixed_namespaces: Vec<(String, Option<NamedNode>)>,
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
        let mut datatypes_map = HashMap::new();
        let string_type = Arc::new(BaseRDFNodeType::Literal(xsd::STRING.into_owned()));
        datatypes_map.insert(xsd::STRING.as_str().to_string(), string_type.clone());
        let mut prefix_map: HashMap<String, NamedNode> = HashMap::new();
        let mut base_prefix = NamedNode::new_unchecked(DEFAULT_XML_DATA_PREFIX);
        let mut stack: Vec<Frame> = Vec::new();
        let mut buf = Vec::new();
        loop {
            match reader
                .read_event_into(&mut buf)
                .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
            {
                Event::Eof => break,
                Event::Start(e) => {
                    let (subj, text_dt,
                        new_base_prefix,
                        introduced_prefixed_namespaces) = open_element(
                        e.name().as_ref(),
                        e.attributes(),
                        &mut stack,
                        &mut pred_map,
                        &mut prefix_map,
                        &mut datatypes_map,
                        &base_prefix
                    )?;
                    let previous_base_prefix = if let Some(new_base_prefix) = new_base_prefix {
                        let previous_base_prefix = base_prefix;
                        base_prefix = new_base_prefix;
                        Some(previous_base_prefix)
                    } else {
                        None
                    };
                    stack.push(Frame {
                        subject: subj,
                        next_child: 1,
                        data_type: text_dt,
                        previous_base_prefix,
                        introduced_prefixed_namespaces,
                    });
                }
                Event::Empty(e) => {
                    let _ = open_element(
                        e.name().as_ref(),
                        e.attributes(),
                        &mut stack,
                        &mut pred_map,
                        &mut prefix_map,
                        &mut datatypes_map,
                        &base_prefix
                    )?;
                }
                Event::End(_) => {
                    if let Some(Frame {
                        previous_base_prefix,
                        introduced_prefixed_namespaces,
                        ..
                    }) = stack.pop()
                    {
                        for (pre, overridden) in introduced_prefixed_namespaces {
                            if let Some(overridden) = overridden {
                                prefix_map.insert(pre, overridden);
                            } else {
                                prefix_map.remove(&pre);
                            }
                        }
                        if let Some((old)) = previous_base_prefix {
                            base_prefix = old;
                        }
                    }
                }
                Event::Text(t) => {
                    let raw = t
                        .decode()
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
                    let s = unescape(&raw)
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
                        .into_owned();
                    if !s.is_empty() {
                        push_text_child(&s, &mut stack, &mut pred_map, string_type.as_ref())?;
                    }
                }
                Event::CData(t) => {
                    let s = std::str::from_utf8(t.as_ref())
                        .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
                        .to_string();
                    if !s.is_empty() {
                        push_text_child(&s, &mut stack, &mut pred_map, string_type.as_ref())?;
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
    pred_map: &mut PredMap,
    prefix_map: &mut HashMap<String, NamedNode>,
    datatypes_map: &mut HashMap<String, Arc<BaseRDFNodeType>>,
    base_prefix: &NamedNode,
) -> Result<
    (
        String,
        Option<Arc<BaseRDFNodeType>>,
        Option<NamedNode>,
        Vec<(String, Option<NamedNode>)>,
    ),
    TriplestoreError,
> {
    let name = std::str::from_utf8(name).map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
    let subject = new_iri_subject();
    if let Some(parent) = stack.last_mut() {
        let parent_subject = parent.subject.clone();
        let n = parent.next_child;
        parent.next_child += 1;
        let rdfi = rdf_n_property(n);
        push_iri_object(pred_map, rdfi.as_str(), &parent_subject, &subject);
    } else {
        push_iri_object(pred_map, rdf::TYPE.as_str(), &subject, XML_ROOT);
    }
    let mut new_base_prefix = None;
    let mut datatype = None;
    let mut introduced_prefixed_namespaces = Vec::new();
    let mut attrs_vec = Vec::new();
    for attr in attrs {
        let attr = attr.map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
        attrs_vec.push(attr);
    }
    let mut non_xmlns_attrs = Vec::new();
    for attr in &attrs_vec {
        let key = attr.key.as_ref();
        let key =
            std::str::from_utf8(key).map_err(|x| TriplestoreError::XMLError(x.to_string()))?;
        let raw = std::str::from_utf8(&attr.value)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
        let value = unescape(raw)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
            .into_owned();
        if key == "xmlns" {
            let nn = NamedNode::new(value.clone()).map_err(|e| {
                TriplestoreError::XMLError(format!("Error parsing {}: {}", value, e.to_string()))
            })?;
            new_base_prefix = Some(nn);
        } else if key.starts_with("xmlns:") {
            let pre = key.strip_prefix("xmlns:").unwrap();
            let nn = NamedNode::new(value.clone()).map_err(|e| {
                TriplestoreError::XMLError(format!("Error parsing {}: {}", value, e.to_string()))
            })?;

            let previous = prefix_map.insert(pre.to_string(), nn);
            introduced_prefixed_namespaces.push((pre.to_string(), previous));
        } else {
            non_xmlns_attrs.push(attr);
        }
    }
    let use_base_prefix= new_base_prefix.as_ref().unwrap_or(base_prefix);

    for attr in non_xmlns_attrs {
        let key = attr.key.as_ref();
        let key =
            std::str::from_utf8(key).map_err(|x| TriplestoreError::XMLError(x.to_string()))?;
        let raw = std::str::from_utf8(&attr.value)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?;
        let value = unescape(raw)
            .map_err(|e| TriplestoreError::XMLError(e.to_string()))?
            .into_owned();
        if key == "type" {
            let pred = qname_to_iri(&value, prefix_map, use_base_prefix)?;

            if let Some(dt) = datatypes_map.get(value.as_str()) {
                datatype = Some(dt.clone());
            } else {
                let dt = Arc::new(BaseRDFNodeType::Literal(pred.clone()));
                datatypes_map.insert(value.as_str().to_string(), dt.clone());
                datatype = Some(dt);
            }
        } else {
            let pred = qname_to_iri(key, prefix_map, use_base_prefix)?;

            push_typed_text(
                pred_map,
                pred.as_str(),
                &subject,
                &value,
                datatypes_map.get(xsd::STRING.as_str()).unwrap(),
            )?;
        }
    }
    let type_iri = qname_to_iri(
        name,
        prefix_map,
        use_base_prefix,
    )?;
    push_iri_object(pred_map, rdf::TYPE.as_str(), &subject, type_iri.as_str());

    Ok((
        subject,
        datatype,
        new_base_prefix,
        introduced_prefixed_namespaces,
    ))
}

fn push_text_child(
    text: &str,
    stack: &mut Vec<Frame>,
    pred_map: &mut PredMap,
    string_type: &BaseRDFNodeType,
) -> Result<(), TriplestoreError> {
    if text.is_empty() {
        return Ok(());
    }
    let Some(frame) = stack.last_mut() else {
        return Ok(());
    };
    let n = frame.next_child;
    frame.next_child += 1;
    let subject = frame.subject.clone();
    let rdfi = rdf_n_property(n);
    let t = frame
        .data_type
        .as_ref()
        .map(|x| x.as_ref())
        .unwrap_or(string_type);
    push_typed_text(pred_map, rdfi.as_str(), &subject, text, t)?;
    Ok(())
}

fn push_typed_text(
    pred_map: &mut PredMap,
    predicate: &str,
    subject: &str,
    text: &str,
    data_type: &BaseRDFNodeType,
) -> Result<(), TriplestoreError> {
    let pair = ensure_pair(pred_map, predicate, &BaseRDFNodeType::IRI, &data_type);
    match pair.1.parse_literal(text.trim(), None) {
        Ok(()) => {
            pair.0.push_str(subject);
            Ok(())
        }
        Err(e) => Err(TriplestoreError::XMLError(e.to_string())),
    }
}

fn push_iri_object(pred_map: &mut PredMap, predicate: &str, subject: &str, object: &str) {
    let pair = ensure_pair(
        pred_map,
        predicate,
        &BaseRDFNodeType::IRI,
        &BaseRDFNodeType::IRI,
    );
    pair.0.push_str(subject);
    pair.1.push_str(object);
}

fn ensure_pair<'a>(
    pred_map: &'a mut PredMap,
    predicate: &str,
    subject_dt: &BaseRDFNodeType,
    object_dt: &BaseRDFNodeType,
) -> &'a mut SubjectObjectBuilders {
    if !pred_map.contains_key(predicate) {
        pred_map.insert(predicate.to_string(), BySubjectType::new());
    };
    let bst = pred_map.get_mut(predicate).unwrap();

    if !bst.contains_key(subject_dt) {
        bst.insert(subject_dt.clone(), ByObjectType::new());
    }

    let bot = bst.get_mut(subject_dt).unwrap();

    if !bot.contains_key(object_dt) {
        bot.insert(
            object_dt.clone(),
            (
                SeriesBuilder::new(subject_dt),
                SeriesBuilder::new(object_dt),
            ),
        );
    }
    let pair = bot.get_mut(object_dt).unwrap();
    pair
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
        for (subject_type, obj_map) in sub_map {
            for (object_type, (sb, ob)) in obj_map {
                if sb.is_empty() {
                    continue;
                }
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

fn qname_to_iri(
    name: &str,
    prefix_map: &mut HashMap<String, NamedNode>,
    base_prefix: &NamedNode,
) -> Result<NamedNode, TriplestoreError> {
    let nn = if let Some((pre, suf)) = name.split_once(':') {
        if let Some(pre_nn) = prefix_map.get(pre) {
            format!("{}{}", pre_nn.as_str(), suf)
        } else {
            return Err(TriplestoreError::XMLError(format!(
                "Could not find prefix {pre}"
            )));
        }
    } else {
        format!("{}{name}", base_prefix.as_str())
    };
    NamedNode::new(&nn)
        .map_err(|e| TriplestoreError::XMLError(format!("Error parsing IRI {}: {}", nn, e)))
}
