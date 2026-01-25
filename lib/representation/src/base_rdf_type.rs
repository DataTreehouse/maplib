use crate::cats::literal_is_cat;
use crate::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT, MULTI_NONE_DT};
use crate::rdf_state::RDFNodeState;
use crate::solution_mapping::BaseCatState;
use crate::{
    literal_is_numeric, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, RDF_NODE_TYPE_BLANK_NODE,
    RDF_NODE_TYPE_IRI, RDF_NODE_TYPE_NONE,
};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedNode, NamedNodeRef, Subject, Term};
use polars::datatypes::{DataType, Field, PlSmallStr, TimeUnit, TimeZone};
use spargebra::term::GroundTerm;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum BaseRDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode),
    None,
}

impl BaseRDFNodeType {
    pub fn field_col_name(&self) -> String {
        match &self {
            BaseRDFNodeType::IRI => MULTI_IRI_DT.to_string(),
            BaseRDFNodeType::BlankNode => MULTI_BLANK_DT.to_string(),
            BaseRDFNodeType::Literal(l) => l.to_string(),
            BaseRDFNodeType::None => MULTI_NONE_DT.to_string(),
        }
    }

    pub fn stored_cat(&self) -> bool {
        matches!(
            self.default_stored_cat_state(),
            BaseCatState::CategoricalNative(..)
        )
    }
}

impl BaseRDFNodeType {
    pub fn multi_columns(&self) -> Vec<String> {
        match self {
            BaseRDFNodeType::IRI => vec![MULTI_IRI_DT.to_string()],
            BaseRDFNodeType::BlankNode => vec![MULTI_BLANK_DT.to_string()],
            BaseRDFNodeType::Literal(_) => {
                if self.is_lang_string() {
                    vec![
                        LANG_STRING_VALUE_FIELD.to_string(),
                        LANG_STRING_LANG_FIELD.to_string(),
                    ]
                } else {
                    vec![self.field_col_name().to_string()]
                }
            }
            BaseRDFNodeType::None => {
                vec![MULTI_NONE_DT.to_string()]
            }
        }
    }

    pub fn as_ref(&self) -> BaseRDFNodeTypeRef<'_> {
        match self {
            Self::IRI => BaseRDFNodeTypeRef::IRI,
            Self::BlankNode => BaseRDFNodeTypeRef::BlankNode,
            Self::Literal(l) => BaseRDFNodeTypeRef::Literal(l.as_ref()),
            Self::None => BaseRDFNodeTypeRef::None,
        }
    }

    pub fn is_iri(&self) -> bool {
        matches!(self, BaseRDFNodeType::IRI)
    }
    pub fn is_blank_node(&self) -> bool {
        self == &Self::BlankNode
    }

    pub fn is_lang_string(&self) -> bool {
        self.is_lit_type(rdf::LANG_STRING)
    }
    pub fn is_none(&self) -> bool {
        self == &Self::None
    }

    pub fn is_lit_type(&self, named_node_ref: NamedNodeRef) -> bool {
        if let Self::Literal(l) = self {
            if l.as_ref() == named_node_ref {
                return true;
            }
        }
        false
    }

    pub fn is_numeric(&self) -> bool {
        if let Self::Literal(l) = self {
            literal_is_numeric(l.as_ref())
        } else {
            false
        }
    }

    pub fn into_default_input_rdf_node_state(self) -> RDFNodeState {
        let cat_state = self.default_input_cat_state();
        RDFNodeState::from_bases(self, cat_state)
    }

    pub fn into_default_stored_rdf_node_state(self) -> RDFNodeState {
        let cat_state = self.default_stored_cat_state();
        RDFNodeState::from_bases(self, cat_state)
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

    pub fn default_input_cat_state(&self) -> BaseCatState {
        if self.default_input_polars_data_type() == DataType::String {
            BaseCatState::String
        } else {
            BaseCatState::NonString
        }
    }

    pub fn default_stored_cat_state(&self) -> BaseCatState {
        if matches!(self, BaseRDFNodeType::None) {
            BaseCatState::NonString
        } else if matches!(self, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
            BaseCatState::CategoricalNative(false, None)
        } else if let BaseRDFNodeType::Literal(l) = self {
            if literal_is_cat(l.as_ref()) {
                BaseCatState::CategoricalNative(true, None)
            } else if literal_type(l.as_ref(), &BaseCatState::String, false) == DataType::String {
                BaseCatState::String
            } else {
                BaseCatState::NonString
            }
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn polars_data_type(&self, base_cat_state: &BaseCatState, single_col: bool) -> DataType {
        if matches!(self, BaseRDFNodeType::None) {
            DataType::Boolean
        } else if matches!(self, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
            match base_cat_state {
                BaseCatState::CategoricalNative(_, _) => DataType::UInt32,
                BaseCatState::String => DataType::String,
                BaseCatState::NonString => {
                    unreachable!("Should never happen")
                }
            }
        } else if let BaseRDFNodeType::Literal(l) = self {
            literal_type(l.as_ref(), base_cat_state, single_col)
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn default_input_polars_data_type(&self) -> DataType {
        if matches!(self, BaseRDFNodeType::None) {
            DataType::Boolean
        } else if matches!(self, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
            DataType::String
        } else if let BaseRDFNodeType::Literal(l) = self {
            literal_type(l.as_ref(), &BaseCatState::String, false)
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn default_stored_polars_data_type(&self) -> DataType {
        if matches!(self, BaseRDFNodeType::None) {
            DataType::Boolean
        } else if matches!(self, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
            DataType::UInt32
        } else if let BaseRDFNodeType::Literal(l) = self {
            literal_type(
                l.as_ref(),
                &BaseCatState::CategoricalNative(false, None),
                false,
            )
        } else {
            unreachable!("Should never happen")
        }
    }
}

fn literal_type(
    named_node_ref: NamedNodeRef,
    base_cat_state: &BaseCatState,
    single_col: bool,
) -> DataType {
    match named_node_ref {
        xsd::UNSIGNED_INT => DataType::UInt32,
        xsd::UNSIGNED_LONG => DataType::UInt64,
        xsd::UNSIGNED_SHORT => DataType::UInt16,
        xsd::UNSIGNED_BYTE => DataType::UInt8,
        xsd::INTEGER | xsd::LONG => DataType::Int64,
        xsd::INT => DataType::Int32,
        xsd::SHORT => DataType::Int16,
        xsd::BYTE => DataType::Int8,
        xsd::DOUBLE | xsd::DECIMAL => DataType::Float64,
        xsd::FLOAT => DataType::Float32,
        xsd::BOOLEAN => DataType::Boolean,
        rdf::LANG_STRING => {
            if !single_col {
                DataType::Struct(polars_lang_fields(base_cat_state))
            } else {
                match base_cat_state {
                    BaseCatState::String
                    | BaseCatState::NonString
                    | BaseCatState::CategoricalNative(..) => DataType::String,
                }
            }
        }
        xsd::DATE_TIME => DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        xsd::DATE_TIME_STAMP => DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        xsd::DATE => DataType::Date,
        //TODO: Fix when adding proper list support
        rdf::LIST => DataType::List(Box::new(DataType::Boolean)),
        xsd::STRING | _ => match base_cat_state {
            BaseCatState::CategoricalNative(_, _) => DataType::UInt32,
            BaseCatState::String => DataType::String,
            BaseCatState::NonString => {
                unreachable!("Should never happen")
            }
        },
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
                write!(f, "{l}")
            }
            Self::None => {
                write!(f, "{RDF_NODE_TYPE_NONE}")
            }
        }
    }
}

pub fn polars_lang_fields(base_cat_state: &BaseCatState) -> Vec<Field> {
    let mut fields = vec![];
    match base_cat_state {
        BaseCatState::CategoricalNative(_, _) => {
            fields.push(Field::new(
                PlSmallStr::from_str(LANG_STRING_VALUE_FIELD),
                DataType::UInt32,
            ));
            fields.push(Field::new(
                PlSmallStr::from_str(LANG_STRING_LANG_FIELD),
                DataType::UInt32,
            ));
        }
        BaseCatState::String => {
            fields.push(Field::new(
                PlSmallStr::from_str(LANG_STRING_VALUE_FIELD),
                DataType::String,
            ));
            fields.push(Field::new(
                PlSmallStr::from_str(LANG_STRING_LANG_FIELD),
                DataType::String,
            ));
        }
        BaseCatState::NonString => {}
    }
    if matches!(base_cat_state, BaseCatState::String) {
    } else {
    }
    fields
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

pub fn get_subject_datatype_ref(s: &Subject) -> BaseRDFNodeTypeRef<'_> {
    match s {
        Subject::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Subject::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_term_datatype_ref(t: &Term) -> BaseRDFNodeTypeRef<'_> {
    match t {
        Term::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Term::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        Term::Literal(l) => BaseRDFNodeTypeRef::Literal(l.datatype()),
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_ground_term_datatype_ref(t: &GroundTerm) -> BaseRDFNodeTypeRef<'_> {
    match t {
        GroundTerm::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        GroundTerm::Literal(l) => BaseRDFNodeTypeRef::Literal(l.datatype()),
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}
