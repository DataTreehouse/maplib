use crate::rdf_to_polars::{
    default_decimal_precision, default_decimal_scale, default_time_unit, default_time_zone,
};
use crate::{BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use memchr::memchr;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term};
use polars::prelude::{
    as_struct, col, DataType, IntoLazy, NamedFrom, PlSmallStr, Series, TimeUnit, TimeZone,
};
use polars_core::frame::DataFrame;
use polars_core::prelude::{Int128Chunked, IntoSeries, NewChunkedArray};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug)]
pub enum SeriesBuilder {
    String(Vec<Option<String>>),
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
    Datetime {
        values: Vec<Option<i64>>,
        time_unit: TimeUnit,
        time_zone: Option<TimeZone>,
    },
    Decimal {
        values: Vec<Option<i128>>,
        precision: usize,
        scale: usize,
    },
    LangString {
        values: Vec<Option<String>>,
        langs: Vec<Option<String>>,
    },
    Null(usize),
}

impl SeriesBuilder {
    pub fn new(dt: &BaseRDFNodeType) -> Self {
        Self::with_capacity(dt, 0)
    }

    pub fn with_capacity(dt: &BaseRDFNodeType, cap: usize) -> Self {
        match dt {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                SeriesBuilder::String(Vec::with_capacity(cap))
            }
            BaseRDFNodeType::None => SeriesBuilder::Null(0),
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
                xsd::DATE_TIME => SeriesBuilder::Datetime {
                    values: vec![],
                    time_unit: TimeUnit::Nanoseconds,
                    time_zone: None,
                },
                xsd::DATE_TIME_STAMP => SeriesBuilder::Datetime {
                    values: Vec::with_capacity(cap),
                    time_unit: default_time_unit(),
                    time_zone: Some(default_time_zone()),
                },
                xsd::DECIMAL => SeriesBuilder::Decimal {
                    values: Vec::with_capacity(cap),
                    precision: default_decimal_precision(),
                    scale: default_decimal_scale(),
                },
                rdf::LANG_STRING => SeriesBuilder::LangString {
                    values: Vec::with_capacity(cap),
                    langs: Vec::with_capacity(cap),
                },
                _ => SeriesBuilder::String(Vec::with_capacity(cap)),
            },
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
            SeriesBuilder::Datetime { values, .. } => values.len(),
            SeriesBuilder::Decimal { values, .. } => values.len(),
            SeriesBuilder::LangString { values, .. } => values.len(),
            SeriesBuilder::Null(n) => *n,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_str(&mut self, s: &str) {
        match self {
            SeriesBuilder::String(v) => v.push(Some(s.to_string())),
            SeriesBuilder::Bool(v) => v.push(bool::from_str(s).ok()),
            SeriesBuilder::U8(v) => v.push(u8::from_str(s).ok()),
            SeriesBuilder::U16(v) => v.push(u16::from_str(s).ok()),
            SeriesBuilder::U32(v) => v.push(u32::from_str(s).ok()),
            SeriesBuilder::U64(v) => v.push(u64::from_str(s).ok()),
            SeriesBuilder::I8(v) => v.push(i8::from_str(s).ok()),
            SeriesBuilder::I16(v) => v.push(i16::from_str(s).ok()),
            SeriesBuilder::I32(v) => v.push(i32::from_str(s).ok()),
            SeriesBuilder::I64(v) => v.push(i64::from_str(s).ok()),
            SeriesBuilder::F32(v) => v.push(f32::from_str(s).ok()),
            SeriesBuilder::F64(v) => v.push(f64::from_str(s).ok()),
            SeriesBuilder::Date(v) => v.push(parse_xsd_date(s)),
            SeriesBuilder::Datetime { values, .. } => values.push(parse_xsd_datetime_micros(s)),
            SeriesBuilder::Decimal { values, scale, .. } => {
                values.push(parse_xsd_decimal(s, *scale))
            }
            SeriesBuilder::LangString { values, langs } => {
                values.push(Some(s.to_string()));
                langs.push(None);
            }
            SeriesBuilder::Null(n) => *n += 1,
        }
    }

    pub fn push_string(&mut self, s: String) {
        match self {
            SeriesBuilder::String(v) => v.push(Some(s)),
            SeriesBuilder::LangString { values, langs } => {
                values.push(Some(s));
                langs.push(None);
            }
            other => other.push_str(&s),
        }
    }

    pub fn push_lang(&mut self, value: String, lang: String) {
        match self {
            SeriesBuilder::LangString { values, langs } => {
                values.push(Some(value));
                langs.push(Some(lang));
            }
            other => other.push_string(value),
        }
    }

    pub fn push_null(&mut self) {
        match self {
            SeriesBuilder::String(v) => v.push(None),
            SeriesBuilder::Bool(v) => v.push(None),
            SeriesBuilder::U8(v) => v.push(None),
            SeriesBuilder::U16(v) => v.push(None),
            SeriesBuilder::U32(v) => v.push(None),
            SeriesBuilder::U64(v) => v.push(None),
            SeriesBuilder::I8(v) => v.push(None),
            SeriesBuilder::I16(v) => v.push(None),
            SeriesBuilder::I32(v) => v.push(None),
            SeriesBuilder::I64(v) => v.push(None),
            SeriesBuilder::F32(v) => v.push(None),
            SeriesBuilder::F64(v) => v.push(None),
            SeriesBuilder::Date(v) => v.push(None),
            SeriesBuilder::Datetime { values, .. } => values.push(None),
            SeriesBuilder::Decimal { values, .. } => values.push(None),
            SeriesBuilder::LangString { values, langs } => {
                values.push(None);
                langs.push(None);
            }
            SeriesBuilder::Null(n) => *n += 1,
        }
    }

    pub fn push_subject(&mut self, s: NamedOrBlankNode) {
        match s {
            NamedOrBlankNode::NamedNode(nn) => self.push_named_node(nn),
            NamedOrBlankNode::BlankNode(bn) => self.push_blank_node(bn),
        }
    }

    pub fn push_named_node(&mut self, nn: NamedNode) {
        self.push_string(nn.into_string());
    }

    pub fn push_blank_node(&mut self, bn: BlankNode) {
        self.push_string(bn.into_string());
    }

    pub fn push_term(&mut self, t: Term) {
        match t {
            Term::NamedNode(nn) => self.push_named_node(nn),
            Term::BlankNode(bn) => self.push_blank_node(bn),
            Term::Literal(l) => self.push_literal(l),
        }
    }

    pub fn push_literal(&mut self, l: Literal) {
        if matches!(self, SeriesBuilder::LangString { .. }) {
            let lang = l.language().map(|s| s.to_string());
            let (s, _, _) = l.destruct();
            match (self, lang) {
                (SeriesBuilder::LangString { values, langs }, Some(lang)) => {
                    values.push(Some(s));
                    langs.push(Some(lang));
                }
                (other, _) => {
                    other.push_string(s);
                }
            }
        } else {
            let (value, _, _) = l.destruct();
            self.push_str(&value);
        }
    }

    pub fn reparse_as(self, dt: &BaseRDFNodeType) -> SeriesBuilder {
        match self {
            SeriesBuilder::String(values) => {
                let mut new = SeriesBuilder::with_capacity(dt, values.len());
                for v in values {
                    match v {
                        Some(s) => new.push_str(&s),
                        None => new.push_null(),
                    }
                }
                new
            }
            other => other,
        }
    }

    pub fn extend(&mut self, other: SeriesBuilder) {
        match (self, other) {
            (SeriesBuilder::String(a), SeriesBuilder::String(b)) => a.extend(b),
            (SeriesBuilder::Bool(a), SeriesBuilder::Bool(b)) => a.extend(b),
            (SeriesBuilder::U8(a), SeriesBuilder::U8(b)) => a.extend(b),
            (SeriesBuilder::U16(a), SeriesBuilder::U16(b)) => a.extend(b),
            (SeriesBuilder::U32(a), SeriesBuilder::U32(b)) => a.extend(b),
            (SeriesBuilder::U64(a), SeriesBuilder::U64(b)) => a.extend(b),
            (SeriesBuilder::I8(a), SeriesBuilder::I8(b)) => a.extend(b),
            (SeriesBuilder::I16(a), SeriesBuilder::I16(b)) => a.extend(b),
            (SeriesBuilder::I32(a), SeriesBuilder::I32(b)) => a.extend(b),
            (SeriesBuilder::I64(a), SeriesBuilder::I64(b)) => a.extend(b),
            (SeriesBuilder::F32(a), SeriesBuilder::F32(b)) => a.extend(b),
            (SeriesBuilder::F64(a), SeriesBuilder::F64(b)) => a.extend(b),
            (SeriesBuilder::Date(a), SeriesBuilder::Date(b)) => a.extend(b),
            (
                SeriesBuilder::Datetime { values: a, .. },
                SeriesBuilder::Datetime { values: b, .. },
            ) => a.extend(b),
            (
                SeriesBuilder::Decimal { values: a, .. },
                SeriesBuilder::Decimal { values: b, .. },
            ) => a.extend(b),
            (
                SeriesBuilder::LangString {
                    values: av,
                    langs: al,
                },
                SeriesBuilder::LangString {
                    values: bv,
                    langs: bl,
                },
            ) => {
                av.extend(bv);
                al.extend(bl);
            }
            (SeriesBuilder::Null(a), SeriesBuilder::Null(b)) => *a += b,
            _ => panic!("SeriesBuilder::extend called with mismatched variants"),
        }
    }

    pub fn into_series(self, name: &str) -> Series {
        match self {
            SeriesBuilder::String(v) => Series::new(name.into(), v),
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
            SeriesBuilder::Datetime {
                values,
                time_unit,
                time_zone,
            } => Series::new(name.into(), values)
                .cast(&DataType::Datetime(
                    time_unit,
                    Some(time_zone.unwrap_or(default_time_zone())),
                ))
                .unwrap(),
            SeriesBuilder::Decimal {
                values,
                precision,
                scale,
            } => {
                let i128ch = Int128Chunked::from_iter_options(name.into(), values.into_iter());
                let dec = i128ch.into_decimal(precision, scale).unwrap();
                let mut ser = dec.into_series();
                ser.rename(PlSmallStr::from_str(name));
                ser
            }
            SeriesBuilder::LangString { values, langs } => {
                let val_ser = Series::new(LANG_STRING_VALUE_FIELD.into(), values);
                let lang_ser = Series::new(LANG_STRING_LANG_FIELD.into(), langs);
                let len = val_ser.len();
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
            SeriesBuilder::Null(n) => Series::new(name.into(), vec![None::<bool>; n]),
        }
    }
}

fn parse_xsd_date(value: &str) -> Option<i32> {
    let use_value = if let Some(spot) = memchr(b'+', value.as_bytes()) {
        &value[0..spot]
    } else {
        value
    };
    match NaiveDate::parse_from_str(use_value, "%Y-%m-%d") {
        Ok(parsed) => {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Some(dur.num_days() as i32)
        }
        Err(_) => None,
    }
}

fn parse_xsd_datetime_micros(value: &str) -> Option<i64> {
    if let Ok(dt) = value.parse::<DateTime<Utc>>() {
        Some(dt.naive_utc().and_utc().timestamp_micros())
    } else if let Ok(dt) = value.parse::<NaiveDateTime>() {
        Some(dt.and_utc().timestamp_micros())
    } else {
        None
    }
}

fn parse_xsd_decimal(value: &str, scale: usize) -> Option<i128> {
    match Decimal::from_str(value) {
        Ok(mut d) => {
            d.rescale(scale as u32);
            Some(d.mantissa())
        }
        Err(_) => None,
    }
}