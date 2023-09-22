use chrono::NaiveDate;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode};
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::{LiteralValue, TimeUnit};
use std::str::FromStr;

pub(crate) fn sparql_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Utf8(named_node.as_str().to_string())
}

pub(crate) fn sparql_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();
    let literal_value = if datatype == xsd::STRING {
        LiteralValue::Utf8(value.to_string())
    } else if datatype == xsd::UNSIGNED_INT {
        let u = u32::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt32(u)
    } else if datatype == xsd::UNSIGNED_LONG {
        let u = u64::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt64(u)
    } else if datatype == xsd::INTEGER || datatype == xsd::LONG {
        let i = i64::from_str(value).expect("Integer parsing error");
        LiteralValue::Int64(i)
    } else if datatype == xsd::INT {
        let i = i32::from_str(value).expect("Integer parsing error");
        LiteralValue::Int32(i)
    } else if datatype == xsd::DOUBLE {
        let d = f64::from_str(value).expect("Integer parsing error");
        LiteralValue::Float64(d)
    } else if datatype == xsd::FLOAT {
        let f = f32::from_str(value).expect("Integer parsing error");
        LiteralValue::Float32(f)
    } else if datatype == xsd::BOOLEAN {
        let b = bool::from_str(value).expect("Boolean parsing error");
        LiteralValue::Boolean(b)
    } else if datatype == xsd::DATE_TIME {
        let dt_without_tz = value.parse::<NaiveDateTime>();
        if let Ok(dt) = dt_without_tz {
            LiteralValue::DateTime(dt.timestamp(), TimeUnit::Nanoseconds, None)
        } else {
            let dt_without_tz = value.parse::<DateTime<Utc>>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::DateTime(dt.naive_utc().timestamp(), TimeUnit::Nanoseconds, None)
            } else {
                panic!("Could not parse datetime: {}", value);
            }
        }
    } else if datatype == xsd::DATE {
        let ymd_string: Vec<&str> = value.split('-').collect();
        if ymd_string.len() != 3 {
            todo!("Unsupported date format {}", value)
        }
        let y = i32::from_str(ymd_string.first().unwrap())
            .unwrap_or_else(|_| panic!("Year parsing error {}", ymd_string.first().unwrap()));
        let m = u32::from_str(ymd_string.get(1).unwrap())
            .unwrap_or_else(|_| panic!("Month parsing error {}", ymd_string.get(1).unwrap()));
        let d = u32::from_str(ymd_string.get(2).unwrap())
            .unwrap_or_else(|_| panic!("Day parsing error {}", ymd_string.get(1).unwrap()));
        let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let dt = date.and_hms_opt(0, 0, 0).unwrap();

        LiteralValue::DateTime(dt.timestamp(), TimeUnit::Milliseconds, None)
    } else if datatype == xsd::DECIMAL {
        let d = f64::from_str(value).expect("Decimal parsing error");
        LiteralValue::Float64(d)
    } else {
        todo!("Not implemented!")
    };
    literal_value
}
