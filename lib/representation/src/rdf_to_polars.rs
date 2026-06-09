use crate::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use chrono_tz::Tz;
use memchr::memchr;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Term};
use polars::prelude::{
    as_struct, lit, DataType, Expr, LiteralValue, PlSmallStr, Scalar, TimeUnit, TimeZone,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::warn;

pub fn rdf_term_to_polars_expr(term: &Term) -> Expr {
    match term {
        Term::NamedNode(named_node) => lit(rdf_named_node_to_polars_literal_value(named_node)),
        Term::Literal(l) => rdf_literal_to_polars_expr(l),
        Term::BlankNode(bl) => lit(rdf_blank_node_to_polars_literal_value(bl)),
        #[cfg(feature = "rdf-star")]
        Term::Triple(_) => todo!(),
    }
}

pub fn rdf_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Scalar(Scalar::from(PlSmallStr::from_str(named_node.as_str())))
}

pub fn rdf_owned_named_node_to_polars_literal_value(named_node: NamedNode) -> LiteralValue {
    LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(
        named_node.into_string(),
    )))
}

pub fn rdf_blank_node_to_polars_literal_value(blank_node: &BlankNode) -> LiteralValue {
    LiteralValue::Scalar(Scalar::from(PlSmallStr::from_str(blank_node.as_str())))
}

pub fn rdf_owned_blank_node_to_polars_literal_value(blank_node: BlankNode) -> LiteralValue {
    LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(
        blank_node.into_string(),
    )))
}

//TODO: Sort and check..
pub fn string_rdf_literal(dt: NamedNodeRef) -> bool {
    !matches!(
        dt,
        xsd::BOOLEAN
            | xsd::LONG
            | xsd::INTEGER
            | xsd::INT
            | xsd::FLOAT
            | xsd::DATE_TIME
            | xsd::DATE_TIME_STAMP
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_BYTE
            | xsd::UNSIGNED_LONG
            | xsd::BYTE
            | xsd::DECIMAL
            | xsd::DOUBLE
            | xsd::DURATION
            | xsd::DATE
    )
}

pub fn rdf_literal_to_polars_expr(l: &Literal) -> Expr {
    let dt = l.datatype();
    if dt == rdf::LANG_STRING {
        as_struct(vec![
            lit(l.value()).alias(LANG_STRING_VALUE_FIELD),
            lit(l.language().unwrap()).alias(LANG_STRING_LANG_FIELD),
        ])
    } else {
        lit(rdf_literal_to_polars_literal_value(l, None))
    }
}

pub fn rdf_literal_to_polars_literal_value(
    literal: &Literal,
    override_dt: Option<NamedNodeRef>,
) -> LiteralValue {
    rdf_literal_to_polars_literal_value_impl(
        literal.value(),
        override_dt.unwrap_or(literal.datatype()),
    )
}

pub fn rdf_literal_to_polars_literal_value_impl(
    value: &str,
    datatype: NamedNodeRef,
) -> LiteralValue {
    if datatype == xsd::STRING {
        LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(value.to_string())))
    } else if datatype == rdf::LANG_STRING {
        panic!("Should never be called with lang string")
    } else if datatype == xsd::UNSIGNED_INT {
        if let Ok(u) = u32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedInt {value}");
            LiteralValue::Scalar(Scalar::null(DataType::UInt32))
        }
    } else if datatype == xsd::UNSIGNED_SHORT {
        if let Ok(u) = u16::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedShort {value}");
            LiteralValue::Scalar(Scalar::null(DataType::UInt16))
        }
    } else if datatype == xsd::UNSIGNED_BYTE {
        if let Ok(u) = u8::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedByte {value}");
            LiteralValue::Scalar(Scalar::null(DataType::UInt8))
        }
    } else if datatype == xsd::UNSIGNED_LONG {
        if let Ok(u) = u64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedLong {value}");
            LiteralValue::Scalar(Scalar::null(DataType::UInt64))
        }
    } else if datatype == xsd::BYTE {
        if let Ok(i) = i8::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:byte {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Int8))
        }
    } else if datatype == xsd::SHORT {
        if let Ok(i) = i16::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:short {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Int16))
        }
    } else if datatype == xsd::INTEGER {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:integer {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Int64))
        }
    } else if datatype == xsd::LONG {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:long {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Int64))
        }
    } else if datatype == xsd::INT {
        if let Ok(i) = i32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:int {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Int32))
        }
    } else if datatype == xsd::DOUBLE {
        if let Ok(d) = f64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(d))
        } else {
            warn!("Could not parse xsd:double {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Float64))
        }
    } else if datatype == xsd::FLOAT {
        if let Ok(f) = f32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(f))
        } else {
            warn!("Could not parse xsd:float {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Float32))
        }
    } else if datatype == xsd::BOOLEAN {
        if let Ok(b) = bool::from_str(value) {
            LiteralValue::Scalar(Scalar::from(b))
        } else {
            warn!("Could not parse xsd:boolean {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Boolean))
        }
    } else if datatype == xsd::DATE_TIME {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            let tz = chrono_tz::Tz::from_str(dt.timezone().to_string().as_str())
                .unwrap_or_else(|_| panic!("Can parse timezone {dt}"));
            LiteralValue::Scalar(Scalar::new_datetime(
                dt.naive_utc().and_utc().timestamp_micros(),
                default_time_unit(),
                Some(TimeZone::from_chrono(&tz)),
            ))
        } else {
            let dt_without_tz = value.parse::<NaiveDateTime>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::Scalar(Scalar::new_datetime(
                    dt.and_utc().timestamp_micros(),
                    default_time_unit(),
                    Some(TimeZone::from_chrono(&Tz::UTC)),
                ))
            } else {
                warn!("Could not parse xsd:dateTime {value}");
                LiteralValue::Scalar(Scalar::null(DataType::Datetime(
                    default_time_unit(),
                    Some(TimeZone::from_chrono(&Tz::UTC)),
                )))
            }
        }
    } else if datatype == xsd::DATE_TIME_STAMP {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            let tz = chrono_tz::Tz::from_str(dt.timezone().to_string().as_str())
                .unwrap_or_else(|_| panic!("Can parse timezone {dt}"));
            LiteralValue::Scalar(Scalar::new_datetime(
                dt.naive_utc().and_utc().timestamp_micros(),
                TimeUnit::Microseconds,
                Some(TimeZone::from_chrono(&tz)),
            ))
        } else {
            warn!("Could not parse xsd:dateTimeStamp {value} note that timezone is required");
            LiteralValue::Scalar(Scalar::null(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(default_time_zone()),
            )))
        }
    } else if datatype == xsd::DATE {
        if value.contains("+") {
            warn!("Did not parse xsd:date timezone: {value}");
        }
        let use_value = if let Some(spot) = memchr(b'+', value.as_bytes()) {
            &value[0..spot]
        } else {
            value
        };
        if let Ok(parsed) = NaiveDate::parse_from_str(use_value, "%Y-%m-%d") {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            LiteralValue::Scalar(Scalar::new_date(dur.num_days() as i32))
        } else {
            warn!("Could not parse xsd:date {value}");
            LiteralValue::Scalar(Scalar::null(DataType::Date))
        }
    } else if datatype == xsd::DECIMAL {
        if let Ok(mut d) = Decimal::from_str(value) {
            d.rescale(default_decimal_scale() as u32);
            LiteralValue::Scalar(Scalar::new_decimal(
                d.mantissa(),
                default_decimal_precision(),
                default_decimal_scale(),
            ))
        } else {
            warn!("Could not parse xsd:decimal {value}");
            LiteralValue::Scalar(Scalar::null(default_decimal_type()))
        }
    } else {
        LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(value.to_string())))
    }
}

pub fn default_time_unit() -> TimeUnit {
    TimeUnit::Microseconds
}

pub fn default_time_zone() -> TimeZone {
    TimeZone::UTC
}

pub fn default_decimal_type() -> DataType {
    DataType::Decimal(default_decimal_precision(), default_decimal_scale())
}

pub fn default_decimal_precision() -> usize {
    38
}

pub fn default_decimal_scale() -> usize {
    12
}
