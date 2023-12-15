use crate::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDateTime, Utc};
use oxrdf::vocab::rdf::LANG_STRING;
use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars_core::datatypes::TimeUnit;
use polars_core::prelude::{AnyValue, DataType, Field};
use std::str::FromStr;

//This code is copied and modified from Chrontext, which has identical licensing
pub fn sparql_literal_to_any_value<'a, 'b>(
    value: &'b str,
    language: Option<&str>,
    datatype: &Option<NamedNodeRef<'a>>,
) -> (AnyValue<'static>, NamedNodeRef<'a>) {
    let (anyv, dt) = if let Some(datatype) = datatype {
        let datatype = *datatype;
        let literal_value = if datatype == xsd::STRING {
            AnyValue::Utf8Owned(value.into())
        } else if datatype == xsd::UNSIGNED_INT {
            let u = u32::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::UNSIGNED_LONG || datatype == xsd::NON_NEGATIVE_INTEGER {
            let u = u64::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::INTEGER || datatype == xsd::LONG {
            let i = i64::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::INT {
            let i = i32::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::DOUBLE || datatype == xsd::DECIMAL {
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
                AnyValue::Datetime(
                    dt.timestamp_nanos_opt().unwrap(),
                    TimeUnit::Nanoseconds,
                    &None,
                )
            } else {
                let dt_without_tz = value.parse::<DateTime<Utc>>();
                if let Ok(dt) = dt_without_tz {
                    AnyValue::Datetime(
                        dt.naive_utc().timestamp_nanos_opt().unwrap(),
                        TimeUnit::Nanoseconds,
                        &None,
                    )
                } else {
                    panic!("Could not parse datetime: {}", value);
                }
            }
        } else if datatype == LANG_STRING {
            println!("VALUE: {}", value);
            let val = AnyValue::Utf8(value);
            let lang = AnyValue::Utf8(language.unwrap());
            let polars_fields: Vec<Field> = vec![
                Field::new(LANG_STRING_VALUE_FIELD, DataType::Utf8),
                Field::new(LANG_STRING_LANG_FIELD, DataType::Utf8),
            ];
            let av = AnyValue::StructOwned(Box::new((vec![val, lang], polars_fields)));
            println!("AV {:?}", av);
            av
        } else {
            todo!("Not implemented! {:?}", datatype)
        };
        (literal_value, datatype.clone())
    } else {
        (AnyValue::Utf8Owned(value.into()), xsd::STRING)
    };
    (anyv.into_static().unwrap(), dt)
}
