use chrono::NaiveDate;
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::{LiteralValue, NamedFrom, Series, TimeUnit};
use std::str::FromStr;

pub fn sparql_term_to_polars_literal_value(term: &Term) -> polars::prelude::LiteralValue {
    match term {
        Term::NamedNode(named_node) => sparql_named_node_to_polars_literal_value(named_node),
        Term::Literal(lit) => sparql_literal_to_polars_literal_value(lit),
        Term::BlankNode(bl) => sparql_blank_node_to_polars_literal_value(bl),
        _ => {
            panic!("Not supported")
        }
    }
}

pub fn sparql_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Utf8(named_node.to_string())
}

pub fn sparql_blank_node_to_polars_literal_value(blank_node: &BlankNode) -> LiteralValue {
    LiteralValue::Utf8(blank_node.to_string())
}

pub fn sparql_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
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
        todo!("Not implemented! {:?}", datatype)
    };
    literal_value
}

pub fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let first_non_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null != x)
        .cloned();
    let first_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null == x)
        .cloned();
    if let (Some(first_non_null), None) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            b
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<bool>>(),
            ),
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            u
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<String>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u32>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u64>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i32>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i64>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            f
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<f32>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t, None) =>
            //TODO: Assert time unit lik??
            {
                Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime, None) = x {
                                assert_eq!(t, &t_prime);
                                n
                            } else {
                                panic!("Not possible")
                            }
                        })
                        .collect::<Vec<i64>>(),
                )
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else if let (Some(first_non_null), Some(_)) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
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
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<String>>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
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
                name,
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
                name,
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
                name,
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
                name,
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
                name,
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
            LiteralValue::DateTime(_, t, None) =>
            //TODO: Assert time unit lik??
            {
                Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime, None) = x {
                                assert_eq!(t, &t_prime);
                                Some(n)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<i64>>>(),
                )
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else {
        Series::new(
            name,
            literal_values
                .iter()
                .map(|_| None)
                .collect::<Vec<Option<bool>>>(),
        )
    }
}
