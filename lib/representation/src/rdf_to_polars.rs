use crate::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use log::warn;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Term};
use polars::prelude::{
    as_struct, lit, AnyValue, DataType, Expr, LiteralValue, NamedFrom, PlSmallStr, Scalar, Series,
    TimeUnit,
};
use std::ops::Deref;
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

pub fn rdf_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();

    if datatype == xsd::STRING {
        LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(value.to_string())))
    } else if datatype == rdf::LANG_STRING {
        panic!("Should never be called with lang string")
    } else if datatype == xsd::UNSIGNED_INT {
        if let Ok(u) = u32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedInt {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::UInt32))
        }
    } else if datatype == xsd::UNSIGNED_LONG {
        if let Ok(u) = u64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(u))
        } else {
            warn!("Could not parse xsd:unsignedLong {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::UInt64))
        }
    } else if datatype == xsd::INTEGER {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:integer {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Int64))
        }
    } else if datatype == xsd::LONG {
        if let Ok(i) = i64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:long {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Int64))
        }
    } else if datatype == xsd::INT {
        if let Ok(i) = i32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(i))
        } else {
            warn!("Could not parse xsd:int {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Int32))
        }
    } else if datatype == xsd::DOUBLE {
        if let Ok(d) = f64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(d))
        } else {
            warn!("Could not parse xsd:double {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Float64))
        }
    } else if datatype == xsd::FLOAT {
        if let Ok(f) = f32::from_str(value) {
            LiteralValue::Scalar(Scalar::from(f))
        } else {
            warn!("Could not parse xsd:float {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Float32))
        }
    } else if datatype == xsd::BOOLEAN {
        if let Ok(b) = bool::from_str(value) {
            LiteralValue::Scalar(Scalar::from(b))
        } else {
            warn!("Could not parse xsd:boolean {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Boolean))
        }
    } else if datatype == xsd::DATE_TIME {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            LiteralValue::Scalar(Scalar::new_datetime(
                dt.naive_utc().and_utc().timestamp_nanos_opt().unwrap(),
                TimeUnit::Nanoseconds,
                Some(dt.timezone().to_string().into()),
            ))
        } else {
            let dt_without_tz = value.parse::<NaiveDateTime>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::Scalar(Scalar::new_datetime(
                    dt.and_utc().timestamp_nanos_opt().unwrap(),
                    TimeUnit::Nanoseconds,
                    None,
                ))
            } else {
                warn!("Could not parse xsd:dateTime {}", value);
                LiteralValue::Scalar(Scalar::null(DataType::Datetime(
                    TimeUnit::Nanoseconds,
                    None,
                )))
            }
        }
    } else if datatype == xsd::DATE_TIME_STAMP {
        let dt_with_tz = value.parse::<DateTime<Utc>>();
        if let Ok(dt) = dt_with_tz {
            LiteralValue::Scalar(Scalar::new_datetime(
                dt.naive_utc().and_utc().timestamp_nanos_opt().unwrap(),
                TimeUnit::Nanoseconds,
                Some(dt.timezone().to_string().into()),
            ))
        } else {
            warn!(
                "Could not parse xsd:dateTimeStamp {} note that timezone is required",
                value
            );
            LiteralValue::Scalar(Scalar::null(DataType::Datetime(
                TimeUnit::Nanoseconds,
                None,
            )))
        }
    } else if datatype == xsd::DATE {
        if let Ok(parsed) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

            LiteralValue::Scalar(Scalar::new_date(dur.num_days() as i32))
        } else {
            warn!("Could not parse xsd:date {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Date))
        }
    } else if datatype == xsd::DECIMAL {
        if let Ok(d) = f64::from_str(value) {
            LiteralValue::Scalar(Scalar::from(d))
        } else {
            warn!("Could not parse xsd:decimal {}", value);
            LiteralValue::Scalar(Scalar::null(DataType::Float64))
        }
    } else {
        LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(value.to_string())))
    }
}

pub fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let first_non_null_opt = literal_values.iter().find(|x| !x.is_null()).cloned();
    if let Some(first_non_null) = &first_non_null_opt {
        if let LiteralValue::Scalar(s) = first_non_null {
            let values = literal_values.into_iter().map(|x| {
                if let LiteralValue::Scalar(s) = x {
                    s.into_value()
                } else {
                    panic!("Should never happen")
                }
            });
            match s.value() {
                AnyValue::Boolean(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Boolean(b) = x {
                                Some(b)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<bool>>>(),
                ),
                AnyValue::String(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::String(u) = x {
                                Some(u.to_string())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<String>>>(),
                ),
                AnyValue::StringOwned(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::StringOwned(u) = x {
                                Some(u.to_string())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<String>>>(),
                ),
                AnyValue::UInt8(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::UInt8(u) = x {
                                Some(u)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<u8>>>(),
                ),
                AnyValue::Int8(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Int8(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i8>>>(),
                ),
                AnyValue::UInt16(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::UInt16(u) = x {
                                Some(u)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<u16>>>(),
                ),
                AnyValue::Int16(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Int16(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i16>>>(),
                ),
                AnyValue::UInt32(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::UInt32(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<u32>>>(),
                ),
                AnyValue::UInt64(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::UInt64(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<u64>>>(),
                ),
                AnyValue::Int32(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Int32(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i32>>>(),
                ),
                AnyValue::Int64(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Int64(i) = x {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i64>>>(),
                ),
                AnyValue::Float32(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Float32(f) = x {
                                Some(f)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<f32>>>(),
                ),
                AnyValue::Float64(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Float64(f) = x {
                                Some(f)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<f64>>>(),
                ),
                AnyValue::Datetime(_, t, tz) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Datetime(n, _tz_prime, _) = x {
                                //assert_eq!(t, &t_prime);
                                Some(n)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i64>>>(),
                )
                .cast(&DataType::Datetime(*t, tz.cloned()))
                .unwrap(),
                AnyValue::DatetimeOwned(_, t, tz) => {
                    let tz = tz.as_ref().map(|x| x.deref().clone());
                    Series::new(
                        name.into(),
                        values
                            .into_iter()
                            .map(|x| {
                                if let AnyValue::DatetimeOwned(n, _tz_prime, _) = x {
                                    //assert_eq!(t, &t_prime);
                                    Some(n)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<Option<i64>>>(),
                    )
                    .cast(&DataType::Datetime(*t, tz))
                    .unwrap()
                }
                AnyValue::Date(_) => Series::new(
                    name.into(),
                    values
                        .into_iter()
                        .map(|x| {
                            if let AnyValue::Date(t) = x {
                                Some(t)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i32>>>(),
                )
                .cast(&DataType::Date)
                .unwrap(),
                AnyValue::Duration(_, _) => {
                    todo!()
                }
                p => {
                    todo!("{p:?}")
                }
            }
        } else {
            todo!()
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
