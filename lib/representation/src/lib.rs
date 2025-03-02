pub mod multitype;
pub mod polars_to_rdf;
pub mod query_context;
pub mod rdf_to_polars;
pub mod solution_mapping;

pub mod errors;
pub mod formatting;
pub mod literals;
pub mod python;
pub mod subtypes;

use crate::multitype::{base_col_name, MULTI_BLANK_DT, MULTI_IRI_DT, MULTI_NONE_DT};
use crate::subtypes::{is_literal_subtype, OWL_REAL};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, NamedNode, NamedNodeRef, NamedOrBlankNode, Subject, Term};
use polars::datatypes::CategoricalOrdering;
use polars::prelude::{DataType, Field, PlSmallStr, TimeUnit};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use spargebra::term::{GroundTerm, TermPattern};
use std::fmt::{Display, Formatter};

pub const VERB_COL_NAME: &str = "verb";
pub const OBJECT_COL_NAME: &str = "object";
pub const SUBJECT_COL_NAME: &str = "subject";
pub const LANG_STRING_VALUE_FIELD: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>";
pub const LANG_STRING_LANG_FIELD: &str = "l";

const RDF_NODE_TYPE_IRI: &str = "IRI";
const RDF_NODE_TYPE_BLANK_NODE: &str = "Blank";
const RDF_NODE_TYPE_NONE: &str = "None";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeTypeRef<'a> {
    IRI,
    BlankNode,
    Literal(NamedNodeRef<'a>),
    None,
    MultiType(Vec<BaseRDFNodeTypeRef<'a>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RDFNodeType {
    IRI,
    BlankNode,
    #[serde(
        deserialize_with = "deserialize_named_node",
        serialize_with = "serialize_named_node"
    )]
    Literal(NamedNode),
    None,
    MultiType(Vec<BaseRDFNodeType>),
}

struct StringVisitor;

impl Visitor<'_> for StringVisitor {
    type Value = NamedNode;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("A valid IRI (without < and >)")
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        NamedNode::new(v).map_err(|x| E::custom(x.to_string()))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        NamedNode::new(v).map_err(|x| E::custom(x.to_string()))
    }
}

fn deserialize_named_node<'de, D>(deserializer: D) -> Result<NamedNode, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_string(StringVisitor)
}

fn serialize_named_node<S>(named_node: &NamedNode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(named_node.as_str())
}

impl RDFNodeType {
    pub fn polars_data_type(&self) -> DataType {
        match self {
            Self::MultiType(types) => {
                let mut fields = Vec::new();
                for t in types {
                    let n = base_col_name(t);
                    if t.is_lang_string() {
                        fields.push(Field::new(
                            PlSmallStr::from_str(LANG_STRING_VALUE_FIELD),
                            DataType::Categorical(None, CategoricalOrdering::Physical),
                        ));
                        fields.push(Field::new(
                            PlSmallStr::from_str(LANG_STRING_LANG_FIELD),
                            DataType::Categorical(None, CategoricalOrdering::Physical),
                        ));
                    } else {
                        fields.push(Field::new(PlSmallStr::from_str(&n), t.polars_data_type()));
                    }
                }
                DataType::Struct(fields)
            }
            Self::IRI | Self::BlankNode | Self::Literal(_) | Self::None => {
                BaseRDFNodeType::from_rdf_node_type(self).polars_data_type()
            }
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum BaseRDFNodeTypeRef<'a> {
    IRI,
    BlankNode,
    Literal(NamedNodeRef<'a>),
    None,
}

impl BaseRDFNodeTypeRef<'_> {
    pub fn is_lang_string(&self) -> bool {
        if let Self::Literal(l) = self {
            l == &rdf::LANG_STRING
        } else {
            false
        }
    }

    pub fn into_owned(self) -> BaseRDFNodeType {
        match self {
            BaseRDFNodeTypeRef::IRI => BaseRDFNodeType::IRI,
            BaseRDFNodeTypeRef::BlankNode => BaseRDFNodeType::BlankNode,
            BaseRDFNodeTypeRef::Literal(l) => BaseRDFNodeType::Literal(l.into_owned()),
            BaseRDFNodeTypeRef::None => BaseRDFNodeType::None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::IRI => MULTI_IRI_DT,
            Self::BlankNode => MULTI_BLANK_DT,
            Self::Literal(l) => l.as_str(),
            Self::None => MULTI_NONE_DT,
        }
    }
}

pub fn get_subject_datatype_ref(s: &Subject) -> BaseRDFNodeTypeRef {
    match s {
        Subject::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Subject::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_term_datatype_ref(t: &Term) -> BaseRDFNodeTypeRef {
    match t {
        Term::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Term::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        Term::Literal(l) => BaseRDFNodeTypeRef::Literal(l.datatype()),
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_ground_term_datatype_ref(t: &GroundTerm) -> BaseRDFNodeTypeRef {
    match t {
        GroundTerm::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        GroundTerm::Literal(l) => BaseRDFNodeTypeRef::Literal(l.datatype()),
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BaseRDFNodeType {
    IRI,
    BlankNode,
    #[serde(
        deserialize_with = "deserialize_named_node",
        serialize_with = "serialize_named_node"
    )]
    Literal(NamedNode),
    None,
}

impl From<BaseRDFNodeType> for RDFNodeType {
    fn from(val: BaseRDFNodeType) -> Self {
        match val {
            BaseRDFNodeType::IRI => Self::IRI,
            BaseRDFNodeType::BlankNode => Self::BlankNode,
            BaseRDFNodeType::Literal(l) => Self::Literal(l),
            BaseRDFNodeType::None => Self::None,
        }
    }
}

impl BaseRDFNodeType {
    pub fn as_ref(&self) -> BaseRDFNodeTypeRef {
        match self {
            Self::IRI => BaseRDFNodeTypeRef::IRI,
            Self::BlankNode => BaseRDFNodeTypeRef::BlankNode,
            Self::Literal(l) => BaseRDFNodeTypeRef::Literal(l.as_ref()),
            Self::None => BaseRDFNodeTypeRef::None,
        }
    }

    pub fn from_rdf_node_type(r: &RDFNodeType) -> Self {
        match r {
            RDFNodeType::IRI => Self::IRI,
            RDFNodeType::BlankNode => Self::BlankNode,
            RDFNodeType::Literal(l) => Self::Literal(l.clone()),
            RDFNodeType::None => Self::None,
            RDFNodeType::MultiType(_) => {
                panic!()
            }
        }
    }
    pub fn is_iri(&self) -> bool {
        self == &Self::IRI
    }
    pub fn is_blank_node(&self) -> bool {
        self == &Self::BlankNode
    }

    pub fn is_lang_string(&self) -> bool {
        self.as_ref().is_lang_string()
    }

    pub fn as_rdf_node_type(&self) -> RDFNodeType {
        match self {
            BaseRDFNodeType::IRI => RDFNodeType::IRI,
            BaseRDFNodeType::BlankNode => RDFNodeType::BlankNode,
            BaseRDFNodeType::Literal(l) => RDFNodeType::Literal(l.clone()),
            BaseRDFNodeType::None => RDFNodeType::None,
        }
    }

    pub fn from_term(term: &Term) -> Self {
        match term {
            Term::NamedNode(_) => BaseRDFNodeType::IRI,
            Term::BlankNode(_) => BaseRDFNodeType::BlankNode,
            Term::Literal(l) => BaseRDFNodeType::Literal(l.datatype().into_owned()),
            #[cfg(feature = "rdf-star")]
            Term::Triple(_) => todo!(),
        }
    }

    pub fn polars_data_type(&self) -> DataType {
        match self {
            BaseRDFNodeType::IRI => DataType::String,
            BaseRDFNodeType::BlankNode => DataType::String,
            BaseRDFNodeType::Literal(l) => match l.as_ref() {
                xsd::STRING => DataType::String,
                xsd::UNSIGNED_INT => DataType::UInt32,
                xsd::UNSIGNED_LONG => DataType::UInt64,
                xsd::INTEGER | xsd::LONG => DataType::Int64,
                xsd::INT => DataType::Int32,
                xsd::DOUBLE | xsd::DECIMAL => DataType::Float64,
                xsd::FLOAT => DataType::Float32,
                xsd::BOOLEAN => DataType::Boolean,
                rdf::LANG_STRING => DataType::Struct(vec![
                    Field::new(LANG_STRING_VALUE_FIELD.into(), DataType::String),
                    Field::new(LANG_STRING_LANG_FIELD.into(), DataType::String),
                ]),
                xsd::DATE_TIME => DataType::Datetime(TimeUnit::Nanoseconds, None),
                xsd::DATE => DataType::Date,
                _ => DataType::String,
            },
            BaseRDFNodeType::None => DataType::Boolean,
        }
    }

    pub fn from_string(s: String) -> Self {
        if s == MULTI_IRI_DT {
            Self::IRI
        } else if s == MULTI_BLANK_DT {
            Self::BlankNode
        } else if s == MULTI_NONE_DT {
            Self::None
        } else {
            if s.starts_with("<") {
                todo!();
            }
            Self::Literal(NamedNode::new_unchecked(s))
        }
    }
}

impl Display for BaseRDFNodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IRI => {
                write!(f, "{RDF_NODE_TYPE_IRI}")
            }
            Self::BlankNode => {
                write!(f, "{RDF_NODE_TYPE_BLANK_NODE}")
            }
            Self::Literal(l) => {
                write!(f, "{}", l)
            }
            Self::None => {
                write!(f, "{RDF_NODE_TYPE_NONE}")
            }
        }
    }
}

impl Display for RDFNodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultiType(types) => {
                let type_strings: Vec<_> = types.iter().map(|x| x.to_string()).collect();
                write!(f, "Multiple({})", type_strings.join(", "))
            }
            Self::IRI | Self::BlankNode | Self::Literal(_) | Self::None => {
                BaseRDFNodeType::from_rdf_node_type(self).fmt(f)
            }
        }
    }
}

impl RDFNodeType {
    pub fn infer_from_term_pattern(tp: &TermPattern) -> Option<RDFNodeType> {
        match tp {
            TermPattern::NamedNode(_) => Some(RDFNodeType::IRI),
            TermPattern::BlankNode(_) => None,
            TermPattern::Literal(l) => Some(RDFNodeType::Literal(l.datatype().into_owned())),
            TermPattern::Variable(_v) => None,
            #[cfg(feature = "rdf-star")]
            TermPattern::Triple(_) => todo!(),
        }
    }

    pub fn is_iri(&self) -> bool {
        self == &Self::IRI
    }
    pub fn is_blank_node(&self) -> bool {
        self == &Self::BlankNode
    }

    pub fn is_lang_string(&self) -> bool {
        if let Self::Literal(l) = self {
            l.as_ref() == rdf::LANG_STRING
        } else {
            false
        }
    }

    pub fn is_lit_type(&self, nnref: NamedNodeRef) -> bool {
        if let Self::Literal(l) = self {
            if l.as_ref() == nnref {
                return true;
            }
        }
        false
    }

    pub fn is_bool(&self) -> bool {
        self.is_lit_type(xsd::BOOLEAN)
    }

    pub fn is_float(&self) -> bool {
        self.is_lit_type(xsd::FLOAT)
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            Self::Literal(l) => literal_is_numeric(l.as_ref()),
            _ => false,
        }
    }
}

pub fn literal_iri_to_namednode(s: &str) -> NamedNode {
    NamedNode::new_unchecked(s)
}

pub fn literal_blanknode_to_blanknode(b: &str) -> BlankNode {
    BlankNode::new_unchecked(b)
}

pub fn owned_term_to_named_or_blank_node(t: Term) -> Option<NamedOrBlankNode> {
    match t {
        Term::NamedNode(nn) => Some(NamedOrBlankNode::NamedNode(nn)),
        Term::BlankNode(bl) => Some(NamedOrBlankNode::BlankNode(bl)),
        _ => None,
    }
}

pub fn owned_term_to_named_node(t: Term) -> Option<NamedNode> {
    match t {
        Term::NamedNode(nn) => Some(nn),
        _ => None,
    }
}

pub fn literal_is_numeric(l: NamedNodeRef) -> bool {
    matches!(l, xsd::FLOAT | xsd::DOUBLE)
        || is_literal_subtype(l, NamedNode::new_unchecked(OWL_REAL).as_ref())
}

pub fn literal_is_boolean(l: NamedNodeRef) -> bool {
    matches!(l, xsd::BOOLEAN)
}

pub fn literal_is_datetime(l: NamedNodeRef) -> bool {
    matches!(l, xsd::DATE_TIME) || matches!(l, xsd::DATE_TIME_STAMP)
}

pub fn literal_is_date(l: NamedNodeRef) -> bool {
    matches!(l, xsd::DATE)
}

pub fn literal_is_string(l: NamedNodeRef) -> bool {
    matches!(l, xsd::STRING)
}
