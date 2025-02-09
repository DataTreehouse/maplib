use crate::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use log::warn;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Term};
use polars::prelude::{as_struct, lit, DataType, Expr, LiteralValue, NamedFrom, Series, TimeUnit};
use std::str::FromStr;

pub fn rdf_term_to_polars_expr(term: &Term) -> Expr {
    match term {
        Term::NamedNode(named_node) => lit(rdf_named_node_to_polars_literal_value(named_node)),
        Term::Literal(l) => {
            let dt = l.datatype();
            if dt == rdf::LANG_STRING {
                as_struct(vec![
                    lit(l.value()).alias(LANG_STRING_VALUE_FIELD),
                    lit(l.language().unwrap()).alias(LANG_STRING_LANG_FIELD),
                ])
            } else {
                lit(rdf_literal_to_polars_literal_value(l))
            }
        }
        Term::BlankNode(bl) => lit(rdf_blank_node_to_polars_literal_value(bl)),
        #[cfg(feature = "rdf-star")]
        Term::Triple(_) => todo!(),
    }
}

pub fn rdf_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::String(named_node.as_str().into())
}

pub fn rdf_owned_named_node_to_polars_literal_value(named_node: NamedNode) -> LiteralValue {
    LiteralValue::String(named_node.into_string().into())
}

pub fn rdf_blank_node_to_polars_literal_value(blank_node: &BlankNode) -> LiteralValue {
    LiteralValue::String(blank_node.as_str().into())
}

pub fn rdf_owned_blank_node_to_polars_literal_value(blank_node: BlankNode) -> LiteralValue {
    LiteralValue::String(blank_node.into_string().into())
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

pub fn rdf_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();

    if datatype == xsd::STRING {
        LiteralValue::String(value.to_string().into())
    } else if datatype == rdf::LANG_STRING {
        panic!("Should never be called with lang string")
    } else if datatype == xsd::UNSIGNED_INT {
        if let Ok(u) = u32::from_str(value) {
            LiteralValue::UInt32(u)
        } else {
            warn!("Could not parse xsd:unsignedInt {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::UNSIGNED_LONG {
        if let Ok(u) = u64::from_str(value) {
            LiteralValue::UInt64(u)
        } else {
            warn!("Could not parse xsd:unsignedLong {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::INTEGER {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Int64(i)
        } else {
            warn!("Could not parse xsd:integer {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::LONG {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Int64(i)
        } else {
            warn!("Could not parse xsd:long {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::INT {
        if let Ok(i) = i32::from_str(value) {
            LiteralValue::Int32(i)
        } else {
            warn!("Could not parse xsd:int {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::DOUBLE {
        if let Ok(d) = f64::from_str(value) {
            LiteralValue::Float64(d)
        } else {
            warn!("Could not parse xsd:double {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::FLOAT {
        if let Ok(f) = f32::from_str(value) {
            LiteralValue::Float32(f)
        } else {
            warn!("Could not parse xsd:float {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::BOOLEAN {
        if let Ok(b) = bool::from_str(value) {
            LiteralValue::Boolean(b)
        } else {
            warn!("Could not parse xsd:boolean {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::DATE_TIME {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            LiteralValue::DateTime(
                dt.naive_utc().and_utc().timestamp_nanos_opt().unwrap(),
                TimeUnit::Nanoseconds,
                Some(dt.timezone().to_string().into()),
            )
        } else {
            let dt_without_tz = value.parse::<NaiveDateTime>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::DateTime(
                    dt.and_utc().timestamp_nanos_opt().unwrap(),
                    TimeUnit::Nanoseconds,
                    None,
                )
            } else {
                warn!("Could not parse xsd:dateTime {}", value);
                LiteralValue::Null
            }
        }
    } else if datatype == xsd::DATE_TIME_STAMP {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            LiteralValue::DateTime(
                dt.naive_utc().and_utc().timestamp_nanos_opt().unwrap(),
                TimeUnit::Nanoseconds,
                Some(dt.timezone().to_string().into()),
            )
        } else {
            warn!(
                "Could not parse xsd:dateTimeStamp {} note that timezone is required",
                value
            );
            LiteralValue::Null
        }
    } else if datatype == xsd::DATE {
        if let Ok(parsed) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

            LiteralValue::Date(dur.num_days() as i32)
        } else {
            warn!("Could not parse xsd:date {}", value);
            LiteralValue::Null
        }
    } else if datatype == xsd::DECIMAL {
        if let Ok(d) = f64::from_str(value) {
            LiteralValue::Float64(d)
        } else {
            warn!("Could not parse xsd:decimal {}", value);
            LiteralValue::Null
        }
    } else {
        LiteralValue::String(value.to_string().into())
    }
}

pub fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let first_non_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null != x)
        .cloned();
    if let Some(first_non_null) = &first_non_null_opt {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<bool>>>(),
            ),
            LiteralValue::String(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::String(u) = x {
                            Some(u.to_string())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<String>>>(),
            ),
            LiteralValue::UInt8(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt8(u) = x {
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u8>>>(),
            ),
            LiteralValue::Int8(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int8(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i8>>>(),
            ),
            LiteralValue::UInt16(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt16(u) = x {
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u16>>>(),
            ),
            LiteralValue::Int16(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int16(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i16>>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u32>>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u64>>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i32>>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i64>>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f32>>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t, tz) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::DateTime(n, _tz_prime, _) = x {
                            //assert_eq!(t, &t_prime);
                            Some(n)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i64>>>(),
            )
            .cast(&DataType::Datetime(*t, tz.clone()))
            .unwrap(),
            LiteralValue::Date(_) => Series::new(
                name.into(),
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Date(t) = x {
                            Some(t)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i32>>>(),
            )
            .cast(&DataType::Date)
            .unwrap(),
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            p => {
                todo!("{p:?}")
            }
        }
    } else {
        Series::new(
            name.into(),
            literal_values
                .iter()
                .map(|_| None)
                .collect::<Vec<Option<bool>>>(),
        )
    }
}
