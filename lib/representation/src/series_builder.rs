use crate::errors::RepresentationError;
use crate::rdf_to_polars::{
    default_decimal_precision, default_decimal_scale, default_time_unit, default_time_zone,
};
use crate::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use memchr::memchr;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedOrBlankNode, Term};
use polars::prelude::{as_struct, col, DataType, IntoLazy, NamedFrom, PlSmallStr, Series};
use polars_core::frame::DataFrame;
use polars_core::prelude::{Int128Chunked, IntoSeries, NewChunkedArray, StringChunkedBuilder};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

pub type SubjectObjectBuilders = (SeriesBuilder, SeriesBuilder);
pub type ByObjectType = HashMap<BaseRDFNodeType, SubjectObjectBuilders>;
pub type BySubjectType = HashMap<BaseRDFNodeType, ByObjectType>;
pub type PredMap = HashMap<String, BySubjectType>;

pub enum SeriesBuilder {
    String(StringChunkedBuilder, usize),
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
        values: StringChunkedBuilder,
        langs: StringChunkedBuilder,
        len: usize,
    },
}

impl SeriesBuilder {
    pub fn new(dt: &BaseRDFNodeType) -> Self {
        Self::with_capacity(dt, 0)
    }

    pub fn with_capacity(dt: &BaseRDFNodeType, cap: usize) -> Self {
        match dt {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                SeriesBuilder::String(StringChunkedBuilder::new("s".into(), cap), 0)
            }
            BaseRDFNodeType::Literal(l) => match l.as_ref() {
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
                    values: StringChunkedBuilder::new(LANG_STRING_VALUE_FIELD.into(), cap),
                    langs: StringChunkedBuilder::new(LANG_STRING_LANG_FIELD.into(), cap),
                    len: 0,
                },
                _ => SeriesBuilder::String(StringChunkedBuilder::new("s".into(), cap), 0),
            },
            BaseRDFNodeType::None => {
                unreachable!("Should never happen")
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SeriesBuilder::String(_, l) => *l,
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
            SeriesBuilder::LangString { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_str(&mut self, s: &str) {
        if let SeriesBuilder::String(v, l) = self {
            *l = *l + 1;
            v.append_value(s);
        } else {
            panic!("Should never be used for non string builder")
        }
    }

    pub fn push_u32(&mut self, u: u32) {
        if let SeriesBuilder::U32(v) = self {
            v.push(Some(u));
        } else {
            panic!("Should never be used for non string builder")
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
            SeriesBuilder::String(b, len) => {
                b.append_null();
                *len += 1;
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
            SeriesBuilder::LangString { values, langs, len } => {
                *len += 1;
                values.append_null();
                langs.append_null();
            }
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
            Term::Literal(l) => self.parse_literal(l.value(), l.language()),
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
            SeriesBuilder::String(v, l) => {
                *l = *l + 1;
                v.append_value(lex);
            }
            SeriesBuilder::Bool(v) => {
                let b = bool::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(b));
            }
            SeriesBuilder::U8(v) => {
                let u = u8::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U16(v) => {
                let u = u16::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U32(v) => {
                let u = u32::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::U64(v) => {
                let u = u64::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(u));
            }
            SeriesBuilder::I8(v) => {
                let i = i8::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I16(v) => {
                let i = i16::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I32(v) => {
                let i = i32::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::I64(v) => {
                let i = i64::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(i));
            }
            SeriesBuilder::F32(v) => {
                let f = f32::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(f));
            }
            SeriesBuilder::F64(v) => {
                let f = f64::from_str(&lex)
                    .map_err(|x| RepresentationError::LiteralParseError(x.to_string()))?;
                v.push(Some(f));
            }
            SeriesBuilder::Date(v) => {
                let d = parse_xsd_date(&lex)?;
                v.push(Some(d));
            }
            SeriesBuilder::Datetime(v) => {
                let i = parse_xsd_datetime_micros(&lex)?;
                v.push(Some(i));
            }
            SeriesBuilder::Decimal(v) => {
                let i = parse_xsd_decimal(&lex)?;
                v.push(Some(i));
            }
            SeriesBuilder::LangString { values, langs, len } => {
                if let Some(lang) = lang {
                    values.append_value(lex.to_string());
                    langs.append_value(lang.to_string());
                    *len = *len + 1;
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
            SeriesBuilder::String(v, ..) => {
                let s = v.finish();
                let mut s = s.into_series();
                s.rename(name.into());
                s
            }
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
            SeriesBuilder::LangString { values, langs, len } => {
                let mut val_ser = values.finish().into_series();
                val_ser.rename(LANG_STRING_VALUE_FIELD.into());
                let mut lang_ser = langs.finish().into_series();
                lang_ser.rename(LANG_STRING_LANG_FIELD.into());
                let mut df = DataFrame::new(len, vec![val_ser.into(), lang_ser.into()])
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
                let mut ser = df
                    .drop_in_place(OBJECT_COL_NAME)
                    .unwrap()
                    .take_materialized_series();
                ser.rename(PlSmallStr::from_str(name));
                ser
            }
        }
    }
}

fn parse_xsd_date(value: &str) -> Result<i32, RepresentationError> {
    let use_value = if let Some(spot) = memchr(b'+', value.as_bytes()) {
        &value[0..spot]
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

fn parse_xsd_datetime_micros(value: &str) -> Result<i64, RepresentationError> {
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

fn parse_xsd_decimal(value: &str) -> Result<i128, RepresentationError> {
    match Decimal::from_str(value) {
        Ok(mut d) => {
            d.rescale(default_decimal_scale() as u32);
            Ok(d.mantissa())
        }
        Err(x) => Err(RepresentationError::LiteralParseError(x.to_string())),
    }
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
