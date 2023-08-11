use chrono::{DateTime, NaiveDateTime, Utc};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars_core::datatypes::TimeUnit;
use polars_core::prelude::AnyValue;
use std::str::FromStr;

//This code is copied from Chrontext, which has identical licensing
pub fn sparql_literal_to_any_value(
    value: &String,
    datatype: &Option<NamedNode>,
) -> (AnyValue<'static>, NamedNode) {
    let (anyv, dt) = if let Some(nn) = datatype {
        let datatype = nn.as_ref();
        let literal_value = if datatype == xsd::STRING {
            AnyValue::Utf8Owned(value.into())
        } else if datatype == xsd::UNSIGNED_INT {
            let u = u32::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::UNSIGNED_LONG {
            let u = u64::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::INTEGER {
            let i = i64::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::LONG {
            let i = i64::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::INT {
            let i = i32::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::DOUBLE {
            let d = f64::from_str(value).expect("Integer parsing error");
            AnyValue::from(d)
        } else if datatype == xsd::FLOAT {
            let f = f32::from_str(value).expect("Integer parsing error");
            AnyValue::from(f)
        } else if datatype == xsd::BOOLEAN {
            let b = bool::from_str(value).expect("Boolean parsing error");
            AnyValue::Boolean(b)
        } else if datatype == xsd::DATE_TIME {
            let dt_without_tz = value.parse::<NaiveDateTime>();
            if let Ok(dt) = dt_without_tz {
                AnyValue::Datetime(dt.timestamp_nanos(), TimeUnit::Nanoseconds, &None)
            } else {
                let dt_without_tz = value.parse::<DateTime<Utc>>();
                if let Ok(dt) = dt_without_tz {
                    AnyValue::Datetime(
                        dt.naive_utc().timestamp_nanos(),
                        TimeUnit::Nanoseconds,
                        &None,
                    )
                } else {
                    panic!("Could not parse datetime: {}", value);
                }
            }
        } else if datatype == xsd::DECIMAL {
            let d = f64::from_str(value).expect("Decimal parsing error");
            AnyValue::from(d)
        } else {
            todo!("Not implemented!")
        };
        (literal_value, nn.clone())
    } else {
        (AnyValue::Utf8Owned(value.into()), xsd::STRING.into_owned())
    };
    return (anyv.into_static().unwrap(), dt);
}
