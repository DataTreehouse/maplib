// The following code is adopted and heavily modified from https://github.com/DeciSym/json2rdf/tree/main
// Original copyright statement is below. License can be found in licensing/
// Copyright (c) 2024-2025, DeciSym, LLC
// Licensed under either of:
// - Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
// - BSD 3-Clause License (https://opensource.org/licenses/BSD-3-Clause)
// at your option.

use crate::errors::TriplestoreError;
use crate::{TriplesToAdd, Triplestore};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::PlSmallStr;
use polars_core::prelude::{AnyValue, Column, DataFrame};
use representation::dataset::NamedGraph;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use serde_json::Value;
use std::collections::HashMap;

const BOOLEAN: u8 = 0;
const INT: u8 = 1;
const STRING: u8 = 2;
const FLOAT: u8 = 3;

const BLANK: u8 = 4;
const IRI: u8 = 5;

const JSON_ROOT: &str = "http://sparql.xyz/facade-x/ns/root";
const JSON_ARRAY: &str = "http://sparql.xyz/facade-x/ns/arr";
const JSON_NULL: &str = "http://sparql.xyz/facade-x/ns/null";

const DEFAULT_JSON_KEYS_PREFIX: &str = "http://sparql.xyz/facade-x/data/";

const ROOT_ELEMENT_PROPERTY: &str = "urn:maplib_json:rootElement";

struct TripleTableBuilder<'a> {
    map: HashMap<(u8, u8), TypedSubjectObjectBuilder<'a>>,
}

impl<'a> TripleTableBuilder<'a> {
    pub fn new() -> Self {
        TripleTableBuilder {
            map: HashMap::from_iter([
                ((BLANK, BOOLEAN), TypedSubjectObjectBuilder::new()),
                ((BLANK, INT), TypedSubjectObjectBuilder::new()),
                ((BLANK, STRING), TypedSubjectObjectBuilder::new()),
                ((BLANK, FLOAT), TypedSubjectObjectBuilder::new()),
                ((BLANK, BLANK), TypedSubjectObjectBuilder::new()),
                ((BLANK, IRI), TypedSubjectObjectBuilder::new()),
                ((IRI, IRI), TypedSubjectObjectBuilder::new()),
                ((IRI, STRING), TypedSubjectObjectBuilder::new()),
            ]),
        }
    }

    pub fn push_blank_bool(&mut self, subj: &str, v: bool) {
        if let Some(values) = self.map.get_mut(&(BLANK, BOOLEAN)) {
            values.subjects.push(subj.to_string());
            values.objects.push(AnyValue::Boolean(v));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_blank_int(&mut self, subj: &str, v: i64) {
        if let Some(values) = self.map.get_mut(&(BLANK, INT)) {
            values.subjects.push(subj.to_string());
            values.objects.push(AnyValue::Int64(v));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_blank_float(&mut self, subj: &str, v: f64) {
        if let Some(values) = self.map.get_mut(&(BLANK, FLOAT)) {
            values.subjects.push(subj.to_string());
            values.objects.push(AnyValue::Float64(v));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_blank_string(&mut self, subj: &str, v: String) {
        if let Some(values) = self.map.get_mut(&(BLANK, STRING)) {
            values.subjects.push(subj.to_string());
            values
                .objects
                .push(AnyValue::StringOwned(PlSmallStr::from_string(v)));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_blank_blank(&mut self, subj: &str, v: &str) {
        if let Some(values) = self.map.get_mut(&(BLANK, BLANK)) {
            values.subjects.push(subj.to_string());
            values
                .objects
                .push(AnyValue::StringOwned(PlSmallStr::from_str(v)));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_blank_iri(&mut self, subj: &str, v: &str) {
        if let Some(values) = self.map.get_mut(&(BLANK, IRI)) {
            values.subjects.push(subj.to_string());
            values
                .objects
                .push(AnyValue::StringOwned(PlSmallStr::from_str(v)));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_iri_iri(&mut self, subj: &str, v: &str) {
        if let Some(values) = self.map.get_mut(&(IRI, IRI)) {
            values.subjects.push(subj.to_string());
            values
                .objects
                .push(AnyValue::StringOwned(PlSmallStr::from_str(v)));
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn push_iri_string(&mut self, subj: &str, v: &str) {
        if let Some(values) = self.map.get_mut(&(IRI, STRING)) {
            values.subjects.push(subj.to_string());
            values
                .objects
                .push(AnyValue::StringOwned(PlSmallStr::from_str(v)));
        } else {
            unreachable!("Should never happen")
        }
    }
}

struct TypedSubjectObjectBuilder<'a> {
    subjects: Vec<String>,
    objects: Vec<AnyValue<'a>>,
}

impl<'a> TypedSubjectObjectBuilder<'a> {
    pub fn new() -> Self {
        TypedSubjectObjectBuilder {
            subjects: Vec::new(),
            objects: Vec::new(),
        }
    }
}

fn type_from_u8(u: u8) -> BaseRDFNodeType {
    let t = if u == BOOLEAN {
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
    } else if u == INT {
        BaseRDFNodeType::Literal(xsd::INT.into_owned())
    } else if u == STRING {
        BaseRDFNodeType::Literal(xsd::STRING.into_owned())
    } else if u == FLOAT {
        BaseRDFNodeType::Literal(xsd::FLOAT.into_owned())
    } else if u == BLANK {
        BaseRDFNodeType::BlankNode
    } else if u == IRI {
        BaseRDFNodeType::IRI
    } else {
        unreachable!("Should never happen")
    };
    t
}

impl Triplestore {
    pub fn map_json(
        &mut self,
        u8s: &mut Vec<u8>,
        prefix: Option<&NamedNode>,
        named_graph: &NamedGraph,
        transient: bool,
    ) -> Result<(), TriplestoreError> {
        let prefix = prefix.cloned().unwrap_or(NamedNode::new_unchecked(DEFAULT_JSON_KEYS_PREFIX));
        NamedNode::new(format!("{}a", prefix.as_str()))
            .map_err(|_| TriplestoreError::InvalidPrefixIRI(prefix.as_str().to_string()))?;

        let v: Value = simd_json::serde::from_slice(u8s).unwrap();

        let mut pred_map = HashMap::new();
        let rdf_type = rdf::TYPE.into_owned();
        pred_map.insert(rdf_type.clone(), TripleTableBuilder::new());

        let doc_subject = new_blank_typed_subject(JSON_ROOT, &rdf_type, &mut pred_map);

        let json_arr_property = NamedNode::new_unchecked(JSON_ARRAY);
        pred_map.insert(json_arr_property.clone(), TripleTableBuilder::new());

        let root_elem_property = NamedNode::new_unchecked(ROOT_ELEMENT_PROPERTY);
        pred_map.insert(root_elem_property.clone(), TripleTableBuilder::new());

        process_value(
            &doc_subject,
            None,
            &prefix,
            v,
            &rdf_type,
            &json_arr_property,
            &root_elem_property,
            &mut pred_map,
        );
        let mut triples_to_add = Vec::new();
        for (pred, ttb) in pred_map.into_iter() {
            for (k, t) in ttb.map {
                if !t.subjects.is_empty() {
                    let subjects_col =
                        Column::new(PlSmallStr::from_str(SUBJECT_COL_NAME), t.subjects);
                    let objects_col = Column::new(PlSmallStr::from_str(OBJECT_COL_NAME), t.objects);
                    let (su8, ou8) = k;
                    let subject_type = type_from_u8(su8);
                    let object_type = type_from_u8(ou8);
                    let subject_state = subject_type.default_input_cat_state();
                    let object_state = object_type.default_input_cat_state();
                    let tta = TriplesToAdd {
                        df: DataFrame::new(vec![subjects_col, objects_col]).unwrap(),
                        subject_type: subject_type,
                        object_type: object_type,
                        predicate: Some(pred.clone()),
                        graph: named_graph.clone(),
                        subject_cat_state: subject_state,
                        object_cat_state: object_state,
                        predicate_cat_state: None,
                    };
                    triples_to_add.push(tta);
                }
            }
        }
        self.add_triples_vec(triples_to_add, transient)?;
        Ok(())
    }
}

fn process_value(
    subject: &str,
    property: Option<&NamedNode>,
    prefix: &NamedNode,
    value: Value,
    rdf_type: &NamedNode,
    arr_property: &NamedNode,
    root_elem_property: &NamedNode,
    map: &mut HashMap<NamedNode, TripleTableBuilder>,
) {
    match value {
        Value::Bool(b) => {
            let builder = map.get_mut(property.unwrap_or(root_elem_property)).unwrap();
            builder.push_blank_bool(subject, b);
        }
        Value::Number(num) => {
            let builder = map.get_mut(property.unwrap_or(root_elem_property)).unwrap();
            if let Some(i) = num.as_i64() {
                builder.push_blank_int(subject, i);
            } else if let Some(f) = num.as_f64() {
                builder.push_blank_float(subject, f);
            } else {
                unreachable!("Should never happen")
            }
        }
        Value::String(s) => {
            let builder = map.get_mut(property.unwrap_or(root_elem_property)).unwrap();
            builder.push_blank_string(subject, s);
        }
        Value::Null => {
            let builder = map.get_mut(property.unwrap_or(root_elem_property)).unwrap();
            builder.push_blank_iri(subject, JSON_NULL);
        }
        Value::Object(obj) => {
            let new_subject = if let Some(property) = property {
                let new_subject = new_blank_subject();
                let builder = map.get_mut(property).unwrap();
                builder.push_blank_blank(subject, &new_subject);
                new_subject
            } else {
                subject.to_string()
            };

            for (key, val) in obj {
                let property = new_property(key, prefix, map);
                process_value(
                    &new_subject,
                    Some(&property),
                    prefix,
                    val,
                    rdf_type,
                    arr_property,
                    root_elem_property,
                    map,
                );
            }
        }
        Value::Array(arr) => {
            let array_subject = if let Some(property) = property {
                let array_subject = new_blank_subject();
                let builder = map.get_mut(property).unwrap();
                builder.push_blank_blank(subject, &array_subject);
                array_subject
            } else {
                subject.to_string()
            };

            for value in arr {
                process_value(
                    &array_subject,
                    Some(arr_property),
                    prefix,
                    value,
                    rdf_type,
                    arr_property,
                    root_elem_property,
                    map,
                );
            }
        }
    }
}

fn new_blank_typed_subject(
    t: &str,
    rdf_type: &NamedNode,
    map: &mut HashMap<NamedNode, TripleTableBuilder>,
) -> String {
    let bstr = new_blank_subject();
    map.get_mut(rdf_type).unwrap().push_blank_iri(&bstr, t);
    bstr
}

fn new_blank_subject(
) -> String {
    let bstr = format!("b{}", uuid::Uuid::new_v4());
    bstr
}


fn new_property(
    key: String,
    prefix: &NamedNode,
    map: &mut HashMap<NamedNode, TripleTableBuilder>,
) -> NamedNode {
    let mut keep_chars = Vec::with_capacity(key.len());
    for (i, c) in key.chars().enumerate() {
        if i == 0 {
            if c.is_numeric() || c == '_' || c == '-' || c == '.' {
                keep_chars.push('_');
            }
        }
        if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
            keep_chars.push(c);
        } else {
            keep_chars.push('_');
        }
    }
    let keep_key: String = keep_chars.into_iter().collect();
    let key_nn = NamedNode::new(format!("{}{}", prefix.as_str(), keep_key)).unwrap();
    if !map.contains_key(&key_nn) {
        map.insert(key_nn.clone(), TripleTableBuilder::new());
    }
    key_nn
}
