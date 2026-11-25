use crate::errors::RepresentationError;
use crate::rdf_to_polars::{
    default_decimal_precision, default_decimal_scale, default_time_unit, default_time_zone,
};
use crate::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use memchr::memchr;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedOrBlankNode, Term, Triple};
use polars::prelude::{DataType, NamedFrom, PlSmallStr, Series};
use polars_arrow::array::{Utf8ViewArray, View};
use polars_arrow::bitmap::{Bitmap, MutableBitmap};
use polars_arrow::datatypes::ArrowDataType;
use polars_core::prelude::{
    Int128Chunked, IntoSeries, NewChunkedArray, StringChunked, StructChunked,
};
use std::collections::HashMap;
use std::str::FromStr;

pub type SubjectObjectBuilders = (SeriesBuilder, SeriesBuilder);
pub type ByObjectType = HashMap<BaseRDFNodeType, SubjectObjectBuilders>;
pub type BySubjectType = HashMap<BaseRDFNodeType, ByObjectType>;
pub type PredMap = HashMap<String, BySubjectType>;

/// Builds a string column, as Polars' string builder does, but keeps its bytes in one buffer
/// that grows from empty. Polars' builder reserves at least 8 KiB for its first buffer, and the
/// parsers keep a builder for each predicate and parser thread, so files with many predicates
/// used most of their read memory for these reservations.
#[derive(Default)]
pub struct StringColumnBuilder {
    views: Vec<View>,
    /// The bytes of the values longer than [`View::MAX_INLINE_SIZE`]. The last buffer is being
    /// filled; a new one is started when it would grow past what a view can address.
    buffers: Vec<Vec<u8>>,
    /// Only set once a null is appended.
    validity: Option<MutableBitmap>,
}

impl StringColumnBuilder {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            views: Vec::with_capacity(cap),
            ..Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    pub fn append_value(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let view = if bytes.len() <= View::MAX_INLINE_SIZE as usize {
            View::new_inline(bytes)
        } else {
            let fits = self
                .buffers
                .last()
                .is_some_and(|b| b.len() + bytes.len() <= u32::MAX as usize);
            if !fits {
                self.buffers.push(Vec::new());
            }
            let buffer_idx = self.buffers.len() - 1;
            let buffer = &mut self.buffers[buffer_idx];
            let offset = buffer.len() as u32;
            buffer.extend_from_slice(bytes);
            View::new_from_bytes(bytes, buffer_idx as u32, offset)
        };
        self.views.push(view);
        if let Some(validity) = &mut self.validity {
            validity.push(true);
        }
    }

    pub fn append_null(&mut self) {
        let len = self.views.len();
        self.validity
            .get_or_insert_with(|| {
                let mut validity = MutableBitmap::with_capacity(self.views.capacity());
                validity.extend_constant(len, true);
                validity
            })
            .push(false);
        self.views.push(View::default());
    }

    /// Appends the values of `other`, copying its bytes into this builder's buffer.
    pub fn append(&mut self, other: StringColumnBuilder) {
        if self.is_empty() && self.validity.is_none() {
            *self = other;
            return;
        }
        self.views.reserve(other.views.len());
        match (&mut self.validity, other.validity) {
            (Some(v), Some(o)) => v.extend_from_bitmap(&Bitmap::from(o)),
            (Some(v), None) => v.extend_constant(other.views.len(), true),
            (None, Some(o)) => {
                let mut v = MutableBitmap::with_capacity(self.views.len() + o.len());
                v.extend_constant(self.views.len(), true);
                v.extend_from_bitmap(&Bitmap::from(o));
                self.validity = Some(v);
            }
            (None, None) => {}
        }
        // The validity is already extended above.
        let validity = self.validity.take();
        for view in other.views {
            if view.length <= View::MAX_INLINE_SIZE {
                self.views.push(view);
            } else {
                let buffer = &other.buffers[view.buffer_idx as usize];
                let start = view.offset as usize;
                let bytes = &buffer[start..start + view.length as usize];
                // Safety: the bytes are a value appended to `other` as a str.
                self.append_value(unsafe { std::str::from_utf8_unchecked(bytes) });
            }
        }
        self.validity = validity;
    }

    pub fn finish(self, name: &str) -> StringChunked {
        let mut views = self.views;
        views.shrink_to_fit();
        let buffers: Vec<_> = self
            .buffers
            .into_iter()
            .map(|mut b| {
                b.shrink_to_fit();
                b.into()
            })
            .collect();
        // Safety: the views point into the buffers at values that were appended as strs, and
        // the validity, if any, has a bit for every view.
        let array = unsafe {
            Utf8ViewArray::new_unchecked_unknown_md(
                ArrowDataType::Utf8View,
                views.into(),
                buffers.into(),
                self.validity.map(|v| v.into()),
                None,
            )
        };
        StringChunked::with_chunk(name.into(), array)
    }
}

pub enum SeriesBuilder {
    String(StringColumnBuilder),
    Bool(Vec<Option<bool>>),
    U8(Vec<Option<u8>>),
    U16(Vec<Option<u16>>),
    U32(Vec<Option<u32>>),
    U64(Vec<Option<u64>>),
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Date(Vec<Option<i32>>),
    Datetime(Vec<Option<i64>>),
    Decimal(Vec<Option<i128>>),
    LangString {
        values: StringColumnBuilder,
        langs: StringColumnBuilder,
    },
    /// RDF 1.2 triple terms, which get their ids in the triplestore (see
    /// cats::triple_terms) before they become a column, so they have no into_series.
    TripleTerms(Vec<Option<Triple>>),
}

impl SeriesBuilder {
    pub fn new(dt: &BaseRDFNodeType) -> Self {
        Self::new_with_capacity(dt, 0)
    }

    pub fn new_with_capacity(dt: &BaseRDFNodeType, cap: usize) -> Self {
        match dt {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                SeriesBuilder::String(StringColumnBuilder::with_capacity(cap))
            }
            BaseRDFNodeType::Literal(l, _) => match l.as_ref() {
                xsd::BOOLEAN => SeriesBuilder::Bool(Vec::with_capacity(cap)),
                xsd::UNSIGNED_BYTE => SeriesBuilder::U8(Vec::with_capacity(cap)),
                xsd::UNSIGNED_SHORT => SeriesBuilder::U16(Vec::with_capacity(cap)),
                xsd::UNSIGNED_INT => SeriesBuilder::U32(Vec::with_capacity(cap)),
                xsd::UNSIGNED_LONG => SeriesBuilder::U64(Vec::with_capacity(cap)),
                xsd::BYTE => SeriesBuilder::I8(Vec::with_capacity(cap)),
                xsd::SHORT => SeriesBuilder::I16(Vec::with_capacity(cap)),
                xsd::INT => SeriesBuilder::I32(Vec::with_capacity(cap)),
                xsd::INTEGER | xsd::LONG => SeriesBuilder::I64(Vec::with_capacity(cap)),
                xsd::FLOAT => SeriesBuilder::F32(Vec::with_capacity(cap)),
                xsd::DOUBLE => SeriesBuilder::F64(Vec::with_capacity(cap)),
                xsd::DATE => SeriesBuilder::Date(Vec::with_capacity(cap)),
                xsd::DATE_TIME => SeriesBuilder::Datetime(Vec::with_capacity(cap)),
                xsd::DATE_TIME_STAMP => SeriesBuilder::Datetime(Vec::with_capacity(cap)),
                xsd::DECIMAL => SeriesBuilder::Decimal(Vec::with_capacity(cap)),
                rdf::LANG_STRING => SeriesBuilder::LangString {
                    values: StringColumnBuilder::with_capacity(cap),
                    langs: StringColumnBuilder::with_capacity(cap),
                },
                _ => SeriesBuilder::String(StringColumnBuilder::with_capacity(cap)),
            },
            BaseRDFNodeType::TripleTerm => SeriesBuilder::TripleTerms(Vec::with_capacity(cap)),
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        }
    }

    /// The triple terms of a TripleTerms builder, None where unbound.
    pub fn into_triple_terms(self) -> Vec<Option<Triple>> {
        match self {
            SeriesBuilder::TripleTerms(v) => v,
            _ => panic!("Not a builder of triple terms"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SeriesBuilder::String(v) => v.len(),
            SeriesBuilder::Bool(v) => v.len(),
            SeriesBuilder::U8(v) => v.len(),
            SeriesBuilder::U16(v) => v.len(),
            SeriesBuilder::U32(v) => v.len(),
            SeriesBuilder::U64(v) => v.len(),
            SeriesBuilder::I8(v) => v.len(),
            SeriesBuilder::I16(v) => v.len(),
            SeriesBuilder::I32(v) => v.len(),
            SeriesBuilder::I64(v) => v.len(),
            SeriesBuilder::F32(v) => v.len(),
            SeriesBuilder::F64(v) => v.len(),
            SeriesBuilder::Date(v) => v.len(),
            SeriesBuilder::Datetime(v) => v.len(),
            SeriesBuilder::Decimal(v) => v.len(),
            SeriesBuilder::LangString { values, .. } => values.len(),
            SeriesBuilder::TripleTerms(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends the values of `other`, a builder of the same type.
    pub fn append(&mut self, other: SeriesBuilder) {
        match (self, other) {
            (SeriesBuilder::String(v), SeriesBuilder::String(o)) => v.append(o),
            (SeriesBuilder::Bool(v), SeriesBuilder::Bool(o)) => v.extend(o),
            (SeriesBuilder::U8(v), SeriesBuilder::U8(o)) => v.extend(o),
            (SeriesBuilder::U16(v), SeriesBuilder::U16(o)) => v.extend(o),
            (SeriesBuilder::U32(v), SeriesBuilder::U32(o)) => v.extend(o),
            (SeriesBuilder::U64(v), SeriesBuilder::U64(o)) => v.extend(o),
            (SeriesBuilder::I8(v), SeriesBuilder::I8(o)) => v.extend(o),
            (SeriesBuilder::I16(v), SeriesBuilder::I16(o)) => v.extend(o),
            (SeriesBuilder::I32(v), SeriesBuilder::I32(o)) => v.extend(o),
            (SeriesBuilder::I64(v), SeriesBuilder::I64(o)) => v.extend(o),
            (SeriesBuilder::F32(v), SeriesBuilder::F32(o)) => v.extend(o),
            (SeriesBuilder::F64(v), SeriesBuilder::F64(o)) => v.extend(o),
            (SeriesBuilder::Date(v), SeriesBuilder::Date(o)) => v.extend(o),
            (SeriesBuilder::Datetime(v), SeriesBuilder::Datetime(o)) => v.extend(o),
            (SeriesBuilder::Decimal(v), SeriesBuilder::Decimal(o)) => v.extend(o),
            (SeriesBuilder::TripleTerms(v), SeriesBuilder::TripleTerms(o)) => v.extend(o),
            (
                SeriesBuilder::LangString { values, langs },
                SeriesBuilder::LangString {
                    values: o_values,
                    langs: o_langs,
                },
            ) => {
                values.append(o_values);
                langs.append(o_langs);
            }
            _ => panic!("Should never append builders of different types"),
        }
    }

    pub fn push_str(&mut self, s: &str) {
        if let SeriesBuilder::String(v) = self {
            v.append_value(s);
        } else {
            panic!("Should never be used for non string builder")
        }
    }

    pub fn push_u32(&mut self, u: u32) {
        if let SeriesBuilder::U32(v) = self {
            v.push(Some(u));
        } else {
            panic!("Should never be used for non u32 builder")
        }
    }

    pub fn push_u64(&mut self, u: u64) {
        if let SeriesBuilder::U64(v) = self {
            v.push(Some(u));
        } else {
            panic!("Should never be used for non u64 builder")
        }
    }

    pub fn push_named_or_blank(&mut self, named_or_blank_node: &NamedOrBlankNode) {
        match named_or_blank_node {
            NamedOrBlankNode::NamedNode(v) => {
                self.push_str(v.as_str());
            }
            NamedOrBlankNode::BlankNode(v) => self.push_str(v.as_str()),
        }
    }

    pub fn push_none(&mut self) {
        match self {
            SeriesBuilder::String(b) => {
                b.append_null();
            }
            SeriesBuilder::Bool(v) => {
                v.push(None);
            }
            SeriesBuilder::U8(v) => {
                v.push(None);
            }
            SeriesBuilder::U16(v) => {
                v.push(None);
            }
            SeriesBuilder::U32(v) => {
                v.push(None);
            }
            SeriesBuilder::U64(v) => {
                v.push(None);
            }
            SeriesBuilder::I8(v) => {
                v.push(None);
            }
            SeriesBuilder::I16(v) => {
                v.push(None);
            }
            SeriesBuilder::I32(v) => {
                v.push(None);
            }
            SeriesBuilder::I64(v) => {
                v.push(None);
            }
            SeriesBuilder::F32(v) => {
                v.push(None);
            }
            SeriesBuilder::F64(v) => {
                v.push(None);
            }
            SeriesBuilder::Date(v) => {
                v.push(None);
            }
            SeriesBuilder::Datetime(v) => {
                v.push(None);
            }
            SeriesBuilder::Decimal(v) => {
                v.push(None);
            }
            SeriesBuilder::LangString { values, langs } => {
                values.append_null();
                langs.append_null();
            }
            SeriesBuilder::TripleTerms(v) => v.push(None),
        }
    }

    pub fn parse_term(&mut self, term: &Term) -> Result<(), RepresentationError> {
        match term {
            Term::NamedNode(nn) => {
                self.push_str(nn.as_str());
                Ok(())
            }
            Term::BlankNode(bl) => {
                self.push_str(bl.as_str());
                Ok(())
            }
            // The base direction would be lost, as language strings have no place for it.
            Term::Literal(l) if crate::rdf12::has_base_direction(l) => {
                Err(RepresentationError::Rdf12NotSupported(
                    crate::rdf12::BASE_DIRECTION_NOT_SUPPORTED.to_string(),
                ))
            }
            Term::Literal(l) => self.parse_literal(l.value(), l.language()),
            Term::Triple(t) => {
                if let SeriesBuilder::TripleTerms(v) = self {
                    v.push(Some(t.as_ref().clone()));
                    Ok(())
                } else {
                    Err(RepresentationError::DatatypeError(format!(
                        "the triple term {term} is not a value of this column"
                    )))
                }
            }
        }
    }

    pub fn push_f32(&mut self, value: f32) {
        if let SeriesBuilder::F32(v) = self {
            v.push(Some(value));
        } else {
            unreachable!("Should never be used for non f32 string builder")
        }
    }

    pub fn parse_literal(
        &mut self,
        lex: &str,
        lang: Option<&str>,
    ) -> Result<(), RepresentationError> {
        match self {
            SeriesBuilder::TripleTerms(_) => {
                return Err(RepresentationError::DatatypeError(format!(
                    "the literal {lex} is not a triple term"
                )))
            }
            SeriesBuilder::String(v) => {
                v.append_value(lex);
            }
            SeriesBuilder::Bool(v) => {
                // The xsd:boolean lexical space is {true, false, 1, 0}
                let b = match lex {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    _ => {
                        return Err(RepresentationError::LiteralParseError(format!(
                            "Invalid xsd:boolean {lex}"
                        )))
                    }
                };
                v.push(Some(b));
            }
            SeriesBuilder::U8(v) => {
                let u = u8::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U16(v) => {
                let u = u16::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U32(v) => {
                let u = u32::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U64(v) => {
                let u = u64::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::I8(v) => {
                let i = i8::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I16(v) => {
                let i = i16::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I32(v) => {
                let i = i32::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I64(v) => {
                let i = i64::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::F32(v) => {
                let f = f32::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(f));
            }
            SeriesBuilder::F64(v) => {
                let f = f64::from_str(lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(f));
            }
            SeriesBuilder::Date(v) => {
                let d = parse_xsd_date(lex)?;
                v.push(Some(d));
            }
            SeriesBuilder::Datetime(v) => {
                let i = parse_xsd_datetime_micros(lex)?;
                v.push(Some(i));
            }
            SeriesBuilder::Decimal(v) => {
                let i = parse_xsd_decimal(lex)?;
                v.push(Some(i));
            }
            SeriesBuilder::LangString { values, langs } => {
                if let Some(lang) = lang {
                    values.append_value(lex);
                    langs.append_value(lang);
                } else {
                    return Err(RepresentationError::LiteralParseError(
                        "Lang string missing language tag".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn into_series(self, name: &str) -> Series {
        match self {
            SeriesBuilder::String(v) => v.finish(name).into_series(),
            SeriesBuilder::Bool(v) => Series::new(name.into(), v),
            SeriesBuilder::U8(v) => Series::new(name.into(), v),
            SeriesBuilder::U16(v) => Series::new(name.into(), v),
            SeriesBuilder::U32(v) => Series::new(name.into(), v),
            SeriesBuilder::U64(v) => Series::new(name.into(), v),
            SeriesBuilder::I8(v) => Series::new(name.into(), v),
            SeriesBuilder::I16(v) => Series::new(name.into(), v),
            SeriesBuilder::I32(v) => Series::new(name.into(), v),
            SeriesBuilder::I64(v) => Series::new(name.into(), v),
            SeriesBuilder::F32(v) => Series::new(name.into(), v),
            SeriesBuilder::F64(v) => Series::new(name.into(), v),
            SeriesBuilder::Date(v) => Series::new(name.into(), v).cast(&DataType::Date).unwrap(),
            SeriesBuilder::Datetime(v) => Series::new(name.into(), v)
                .cast(&DataType::Datetime(
                    default_time_unit(),
                    Some(default_time_zone()),
                ))
                .unwrap(),
            SeriesBuilder::Decimal(v) => {
                let i128ch = Int128Chunked::from_iter_options(name.into(), v.into_iter());
                let dec = i128ch
                    .into_decimal(default_decimal_precision(), default_decimal_scale())
                    .unwrap();
                let mut ser = dec.into_series();
                ser.rename(PlSmallStr::from_str(name));
                ser
            }
            // Triple terms get their ids before they become a column.
            SeriesBuilder::TripleTerms(_) => crate::rdf12::unreachable_triple_term(),
            SeriesBuilder::LangString { values, langs } => {
                let len = values.len();
                let val_ser = values.finish(LANG_STRING_VALUE_FIELD).into_series();
                let lang_ser = langs.finish(LANG_STRING_LANG_FIELD).into_series();
                StructChunked::from_series(name.into(), len, [val_ser, lang_ser].iter())
                    .unwrap()
                    .into_series()
            }
        }
    }
}

fn parse_xsd_date(value: &str) -> Result<i32, RepresentationError> {
    // The time zone is not kept for xsd:date values: strip Z, +hh:mm or -hh:mm.
    let use_value = if let Some(v) = value.strip_suffix('Z') {
        v
    } else if let Some(spot) = memchr(b'+', value.as_bytes()) {
        &value[0..spot]
    } else if value.len() > 6
        && value.as_bytes()[value.len() - 6] == b'-'
        && value.as_bytes()[value.len() - 3] == b':'
    {
        &value[0..value.len() - 6]
    } else {
        value
    };
    match NaiveDate::parse_from_str(use_value, "%Y-%m-%d") {
        Ok(parsed) => {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Ok(dur.num_days() as i32)
        }
        Err(x) => Err(RepresentationError::LiteralParseError(x.to_string())),
    }
}

/// Parses an xsd:dateTime lexical form into microseconds since the epoch. Values with a
/// time zone are converted to UTC, values without one are read as UTC wall clock times.
pub fn parse_xsd_datetime_micros(value: &str) -> Result<i64, RepresentationError> {
    if let Ok(dt) = value.parse::<DateTime<Utc>>() {
        Ok(dt.naive_utc().and_utc().timestamp_micros())
    } else if let Ok(dt) = value.parse::<NaiveDateTime>() {
        Ok(dt.and_utc().timestamp_micros())
    } else {
        Err(RepresentationError::LiteralParseError(format!(
            "Could not parse datetime: {}",
            value
        )))
    }
}

/// Parses an xsd:decimal lexical form into the mantissa of a Decimal(precision, scale).
///
/// This works on the digits directly instead of going through rust_decimal, whose 96-bit
/// mantissa (28 digits) cannot hold large values at the default scale and silently
/// shifted the decimal point. Fraction digits beyond the scale are truncated.
fn parse_xsd_decimal(value: &str) -> Result<i128, RepresentationError> {
    let scale = default_decimal_scale();
    let precision = default_decimal_precision();
    let err = || RepresentationError::LiteralParseError(format!("Invalid xsd:decimal: {value}"));
    let trimmed = value.trim();
    let (negative, unsigned) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(err());
    }
    let int_part = int_part.trim_start_matches('0');
    if int_part.len() + scale > precision {
        return Err(RepresentationError::LiteralParseError(format!(
            "xsd:decimal {value} has more than {} integer digits",
            precision - scale
        )));
    }
    let mut mantissa: i128 = 0;
    for b in int_part.bytes() {
        mantissa = mantissa * 10 + (b - b'0') as i128;
    }
    let mut frac_digits = frac_part.bytes();
    for _ in 0..scale {
        let d = frac_digits.next().map(|b| (b - b'0') as i128).unwrap_or(0);
        mantissa = mantissa * 10 + d;
    }
    Ok(if negative { -mantissa } else { mantissa })
}

pub fn ensure_pair<'a>(
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
