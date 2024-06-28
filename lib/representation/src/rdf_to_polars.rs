use crate::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::NaiveDate;
use log::warn;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
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
    }
}

pub fn rdf_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::String(named_node.as_str().to_string())
}

pub fn rdf_owned_named_node_to_polars_literal_value(named_node: NamedNode) -> LiteralValue {
    LiteralValue::String(named_node.into_string())
}

pub fn rdf_blank_node_to_polars_literal_value(blank_node: &BlankNode) -> LiteralValue {
    LiteralValue::String(blank_node.as_str().to_string())
}

pub fn rdf_owned_blank_node_to_polars_literal_value(blank_node: BlankNode) -> LiteralValue {
    LiteralValue::String(blank_node.into_string())
}

pub fn rdf_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();
    let literal_value = if datatype == xsd::STRING {
        LiteralValue::String(value.to_string())
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
        let dt_without_tz = value.parse::<NaiveDateTime>();
        if let Ok(dt) = dt_without_tz {
            LiteralValue::DateTime(
                dt.and_utc().timestamp_nanos_opt().unwrap(),
                TimeUnit::Nanoseconds,
                None,
            )
        } else {
            let dt_without_tz = value.parse::<DateTime<Utc>>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::DateTime(
                    dt.naive_utc().and_utc().timestamp_nanos_opt().unwrap(),
                    TimeUnit::Nanoseconds,
                    None,
                )
            } else {
                warn!("Could not parse xsd:datetime {}", value);
                LiteralValue::Null
            }
        }
    } else if datatype == xsd::DATE {
        if let Ok(parsed) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let l = LiteralValue::Date(dur.num_days() as i32);
            l
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
        LiteralValue::String(value.to_string())
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
            LiteralValue::String(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::String(u) = x {
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
            LiteralValue::DateTime(_, t, tz) => Series::new(
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
            .cast(&DataType::Datetime(t.clone(), tz.clone()))
            .unwrap(),
            LiteralValue::Date(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Date(t) = x {
                            t
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i32>>(),
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
            LiteralValue::String(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::String(u) = x {
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
