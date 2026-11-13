use crate::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT, MULTI_NONE_DT};
use crate::rdf_state::RDFNodeState;
use crate::rdf_to_polars::{default_decimal_type, default_time_unit, default_time_zone};
use crate::solution_mapping::BaseCatState;
use crate::{
    literal_is_numeric, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, RDF_NODE_TYPE_BLANK_NODE,
    RDF_NODE_TYPE_IRI, RDF_NODE_TYPE_NONE,
};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedNode, NamedNodeRef, NamedOrBlankNode, Term};
use polars::datatypes::{DataType, Field, PlSmallStr};
use serde::{Deserialize, Serialize};
use spargebra::term::GroundTerm;
use std::fmt::{Display, Formatter};

/// The type of an RDF node in a column.
///
/// Literals have a datatype and, for xsd:dateTime and xsd:dateTimeStamp, an optional time zone
/// offset in minutes from UTC (e.g. 0 for "Z" and -480 for "-08:00"). None means that the values
/// have no time zone. The values themselves are stored as UTC instants, and values with
/// different offsets are different types, so that the offset of each value is preserved.
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BaseRDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode, Option<i16>),
    None,
}

/// Separates the datatype and the time zone in the field name of a multi type column.
/// A space cannot occur in an IRI.
const TIME_ZONE_SEPARATOR: &str = " ";

impl BaseRDFNodeType {
    pub fn field_col_name(&self) -> String {
        match &self {
            BaseRDFNodeType::IRI => MULTI_IRI_DT.to_string(),
            BaseRDFNodeType::BlankNode => MULTI_BLANK_DT.to_string(),
            BaseRDFNodeType::Literal(l, None) => l.to_string(),
            BaseRDFNodeType::Literal(l, Some(tz)) => format!("{l}{TIME_ZONE_SEPARATOR}{tz}"),
            BaseRDFNodeType::None => MULTI_NONE_DT.to_string(),
        }
    }

    /// The type of a literal value, including the time zone offset of a dateTime.
    pub fn from_literal(l: &oxrdf::Literal) -> Self {
        BaseRDFNodeType::Literal(
            l.datatype().into_owned(),
            literal_time_zone(l.value(), l.datatype()),
        )
    }

    /// A literal type without a time zone.
    pub fn literal(datatype: NamedNode) -> Self {
        BaseRDFNodeType::Literal(datatype, None)
    }

    /// The time zone offset in minutes of a literal type, if any.
    pub fn time_zone_offset(&self) -> Option<i16> {
        if let BaseRDFNodeType::Literal(_, tz) = self {
            *tz
        } else {
            None
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
            BaseRDFNodeType::Literal(..) => {
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
            Self::Literal(l, tz) => BaseRDFNodeTypeRef::Literal(l.as_ref(), *tz),
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
        if let Self::Literal(l, _) = self {
            if l.as_ref() == named_node_ref {
                return true;
            }
        }
        false
    }

    pub fn is_numeric(&self) -> bool {
        if let Self::Literal(l, _) = self {
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
            Term::Literal(l) => BaseRDFNodeType::Literal(
                l.datatype().into_owned(),
                literal_time_zone(l.value(), l.datatype()),
            ),
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
            if let Some((dt, tz)) = s.split_once(TIME_ZONE_SEPARATOR) {
                Self::Literal(NamedNode::new_unchecked(dt), tz.parse().ok())
            } else {
                Self::Literal(NamedNode::new_unchecked(s), None)
            }
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
            BaseCatState::CategoricalNative(None)
        } else if let BaseRDFNodeType::Literal(l, _) = self {
            if literal_type(l.as_ref(), &BaseCatState::String, true) == DataType::String {
                BaseCatState::CategoricalNative(None)
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
                BaseCatState::CategoricalNative(_) => DataType::UInt32,
                BaseCatState::String => DataType::String,
                BaseCatState::NonString => {
                    unreachable!("Should never happen")
                }
            }
        } else if let BaseRDFNodeType::Literal(l, _) = self {
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
        } else if let BaseRDFNodeType::Literal(l, _) = self {
            literal_type(l.as_ref(), &BaseCatState::String, true)
        } else {
            unreachable!("Should never happen")
        }
    }

    pub fn default_stored_polars_data_type(&self) -> DataType {
        if matches!(self, BaseRDFNodeType::None) {
            DataType::Boolean
        } else if matches!(self, BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode) {
            DataType::UInt32
        } else if let BaseRDFNodeType::Literal(l, _) = self {
            literal_type(l.as_ref(), &BaseCatState::CategoricalNative(None), false)
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
        xsd::DOUBLE => DataType::Float64,
        xsd::DECIMAL => default_decimal_type(),
        xsd::FLOAT => DataType::Float32,
        xsd::BOOLEAN => DataType::Boolean,
        rdf::LANG_STRING => {
            if !single_col {
                DataType::Struct(polars_lang_fields(base_cat_state))
            } else {
                match base_cat_state {
                    BaseCatState::String | BaseCatState::NonString => DataType::String,
                    BaseCatState::CategoricalNative(..) => DataType::UInt32,
                }
            }
        }
        // Values are UTC instants, the offset is part of the literal type.
        xsd::DATE_TIME | xsd::DATE_TIME_STAMP => {
            DataType::Datetime(default_time_unit(), Some(default_time_zone()))
        }
        xsd::DATE => DataType::Date,
        //TODO: Fix when adding proper list support
        rdf::LIST => DataType::List(Box::new(DataType::Boolean)),
        xsd::STRING | _ => match base_cat_state {
            BaseCatState::CategoricalNative(_) => DataType::UInt32,
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
            Self::Literal(l, _) => {
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
        BaseCatState::CategoricalNative(_) => {
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
    Literal(NamedNodeRef<'a>, Option<i16>),
    None,
}

impl BaseRDFNodeTypeRef<'_> {
    pub fn is_lang_string(&self) -> bool {
        if let Self::Literal(l, _) = self {
            l == &rdf::LANG_STRING
        } else {
            false
        }
    }

    pub fn from_term<'a>(term: &'a Term) -> BaseRDFNodeTypeRef<'a> {
        match term {
            Term::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
            Term::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
            Term::Literal(l) => BaseRDFNodeTypeRef::Literal(
                l.datatype(),
                literal_time_zone(l.value(), l.datatype()),
            ),
        }
    }

    pub fn into_owned(self) -> BaseRDFNodeType {
        match self {
            BaseRDFNodeTypeRef::IRI => BaseRDFNodeType::IRI,
            BaseRDFNodeTypeRef::BlankNode => BaseRDFNodeType::BlankNode,
            BaseRDFNodeTypeRef::Literal(l, tz) => BaseRDFNodeType::Literal(l.into_owned(), tz),
            BaseRDFNodeTypeRef::None => BaseRDFNodeType::None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::IRI => MULTI_IRI_DT,
            Self::BlankNode => MULTI_BLANK_DT,
            Self::Literal(l, _) => l.as_str(),
            Self::None => MULTI_NONE_DT,
        }
    }
}

pub fn get_subject_datatype_ref(s: &NamedOrBlankNode) -> BaseRDFNodeTypeRef<'_> {
    match s {
        NamedOrBlankNode::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        NamedOrBlankNode::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_term_datatype_ref(t: &Term) -> BaseRDFNodeTypeRef<'_> {
    match t {
        Term::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        Term::BlankNode(_) => BaseRDFNodeTypeRef::BlankNode,
        Term::Literal(l) => {
            BaseRDFNodeTypeRef::Literal(l.datatype(), literal_time_zone(l.value(), l.datatype()))
        }
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

pub fn get_ground_term_datatype_ref(t: &GroundTerm) -> BaseRDFNodeTypeRef<'_> {
    match t {
        GroundTerm::NamedNode(_) => BaseRDFNodeTypeRef::IRI,
        GroundTerm::Literal(l) => {
            BaseRDFNodeTypeRef::Literal(l.datatype(), literal_time_zone(l.value(), l.datatype()))
        }
        #[cfg(feature = "rdf-star")]
        _ => unimplemented!(),
    }
}

/// The time zone offset in minutes of a dateTime literal from its lexical form,
/// or None if it has no time zone or is not a dateTime.
pub fn literal_time_zone(value: &str, datatype: NamedNodeRef) -> Option<i16> {
    if !matches!(datatype, xsd::DATE_TIME | xsd::DATE_TIME_STAMP) {
        return None;
    }
    parse_time_zone_offset(value)
}

/// The offset in minutes of a lexical dateTime: "Z" is 0 and "-08:00" is -480.
pub fn parse_time_zone_offset(value: &str) -> Option<i16> {
    let value = value.trim();
    if value.ends_with('Z') {
        return Some(0);
    }
    // The offset is in the time part, which is after the T.
    let time = value.split_once('T')?.1;
    let offset_start = time.rfind(['+', '-'])?;
    let offset = &time[offset_start..];
    let negative = offset.starts_with('-');
    let (hours, minutes) = offset[1..].split_once(':')?;
    let total = hours.parse::<i16>().ok()? * 60 + minutes.parse::<i16>().ok()?;
    Some(if negative { -total } else { total })
}

/// The lexical form of a time zone offset: 0 is "Z" and -480 is "-08:00".
pub fn format_time_zone_offset(offset: i16) -> String {
    if offset == 0 {
        "Z".to_string()
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let abs = offset.unsigned_abs();
        format!("{sign}{:02}:{:02}", abs / 60, abs % 60)
    }
}
