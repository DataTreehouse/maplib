use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use crate::expressions::durations::{cast_to_duration, is_duration_datatype};
use polars::prelude::{
    lit, when, Expr, Field, Int64Chunked, IntoColumn, IntoSeries, LiteralValue, StringChunked,
    StrptimeOptions,
};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::formatting::literal_expr_to_string;
use representation::rdf_to_polars::default_datetime_type;
use representation::series_builder::parse_xsd_datetime_micros;
use representation::solution_mapping::BaseCatState;
use representation::{literal_is_numeric, BaseRDFNodeType};

const INTEGER_PATTERN: &str = r"^[+-]?[0-9]+$";
const DECIMAL_PATTERN: &str = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$";
const DOUBLE_PATTERN: &str = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$";

fn null_of(dt: DataType) -> Expr {
    lit(LiteralValue::untyped_null()).cast(dt)
}

/// Casting to xsd:boolean (SPARQL 1.1 17.5, XPath 19.1.5)
fn cast_to_boolean(c: Expr, src: NamedNodeRef) -> Expr {
    if src == xsd::BOOLEAN {
        c
    } else if src == xsd::STRING {
        let s = c.cast(DataType::String);
        when(s.clone().eq(lit("true")).or(s.clone().eq(lit("1"))))
            .then(lit(true))
            .when(s.clone().eq(lit("false")).or(s.eq(lit("0"))))
            .then(lit(false))
            .otherwise(null_of(DataType::Boolean))
    } else if literal_is_numeric(src) {
        let f = c.cast(DataType::Float64);
        f.clone().neq(lit(0.0)).and(f.is_nan().not())
    } else {
        null_of(DataType::Boolean)
    }
}

/// Casting to numeric types (SPARQL 1.1 17.5, XPath 19.1.2 - 19.1.4).
/// Invalid lexical forms, and NaN or infinite values cast to integers or decimals, are errors (null).
fn cast_to_numeric(c: Expr, src: NamedNodeRef, trg: NamedNodeRef, trg_type: DataType) -> Expr {
    let trg_floating = matches!(trg, xsd::FLOAT | xsd::DOUBLE);
    if src == xsd::STRING {
        let s = c.cast(DataType::String);
        let pattern = if trg_floating {
            DOUBLE_PATTERN
        } else if trg == xsd::DECIMAL {
            DECIMAL_PATTERN
        } else {
            INTEGER_PATTERN
        };
        let valid = s.clone().str().contains(lit(pattern), true);
        let parsed = s
            .clone()
            .str()
            .strip_prefix(lit("+"))
            .cast(trg_type.clone());
        if trg_floating {
            when(s.clone().eq(lit("INF")).or(s.clone().eq(lit("+INF"))))
                .then(lit(f64::INFINITY).cast(trg_type.clone()))
                .when(s.clone().eq(lit("-INF")))
                .then(lit(f64::NEG_INFINITY).cast(trg_type.clone()))
                .when(s.eq(lit("NaN")))
                .then(lit(f64::NAN).cast(trg_type.clone()))
                .when(valid)
                .then(parsed)
                .otherwise(null_of(trg_type))
        } else {
            when(valid).then(parsed).otherwise(null_of(trg_type))
        }
    } else if src == xsd::BOOLEAN {
        c.cast(DataType::Int32).cast(trg_type)
    } else if literal_is_numeric(src) {
        if matches!(src, xsd::FLOAT | xsd::DOUBLE) && !trg_floating {
            when(c.clone().is_finite())
                .then(c.cast(trg_type.clone()))
                .otherwise(null_of(trg_type))
        } else {
            c.cast(trg_type)
        }
    } else {
        null_of(trg_type)
    }
}

/// Casting to xsd:duration, xsd:yearMonthDuration and xsd:dayTimeDuration (XPath 19.1.6):
/// invalid lexical forms are errors (null), and durations are converted to the target type.
fn cast_to_duration_expr(c: Expr, src: NamedNodeRef, trg: NamedNodeRef) -> Expr {
    if !(src == xsd::STRING || is_duration_datatype(src)) {
        return null_of(DataType::String);
    }
    if src == trg && src != xsd::STRING {
        return c;
    }
    let src = src.into_owned();
    let trg = trg.into_owned();
    c.cast(DataType::String).map(
        move |x| {
            let ca: StringChunked = x
                .str()?
                .iter()
                .map(|v| v.and_then(|v| cast_to_duration(v, src.as_ref(), trg.as_ref())))
                .collect();
            Ok(ca.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
    )
}

/// Casting xsd:string to xsd:dateTime (XPath 19.1.7). Values are stored as UTC instants like
/// all other dateTimes: values with a time zone ("Z", "+02:00") are converted to UTC, values
/// without one are kept as wall clock times. Invalid lexical forms are errors (null).
fn cast_string_to_datetime(c: Expr) -> Expr {
    c.cast(DataType::String).map(
        |x| {
            let ca: Int64Chunked = x
                .str()?
                .iter()
                .map(|v| v.and_then(|v| parse_xsd_datetime_micros(v).ok()))
                .collect();
            ca.into_series()
                .cast(&default_datetime_type())
                .map(|s| s.into_column())
        },
        |_, f| Ok(Field::new(f.name().clone(), default_datetime_type())),
    )
}

pub fn cast_literal(
    mut c: Expr,
    src_bt: &BaseRDFNodeType,
    src_bs: &BaseCatState,
    global_cats: LockedCats,
    src: NamedNodeRef,
    trg: NamedNodeRef,
    trg_type: DataType,
) -> Expr {
    // Dictionary-encoded values are decoded to their lexical forms first. Only xsd:string and
    // durations were decoded, so a literal of another dictionary-encoded datatype, such as
    // "5389.96"^^bsbm:USD, was cast from its code: xsd:string() gave "1795". For a
    // language-tagged string, `c` is its value (see xsd_cast_literal).
    c = maybe_decode_expr(c, src_bt, src_bs, global_cats);
    if src_bt.is_lang_string() {
        // A language-tagged string casts to xsd:string, its lexical form, and to nothing else
        // (SPARQL 1.1 17.5): any other cast is an error, which leaves the result unbound.
        return if trg == xsd::STRING {
            c.cast(DataType::String)
        } else {
            null_of(trg_type)
        };
    }
    if trg == xsd::STRING {
        // Same lexical form as STR(), e.g. without padding zeros on decimals
        literal_expr_to_string(c, src_bt)
    } else if trg == xsd::BOOLEAN {
        cast_to_boolean(c, src)
    } else if literal_is_numeric(trg) {
        cast_to_numeric(c, src, trg, trg_type)
    } else if src == xsd::STRING && trg == xsd::DATE_TIME {
        cast_string_to_datetime(c)
    } else if src == xsd::STRING && trg == xsd::DATE {
        c.cast(DataType::String).str().to_date(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::TIME {
        c.cast(DataType::String).str().to_time(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if is_duration_datatype(trg) {
        cast_to_duration_expr(c, src, trg)
    } else {
        c.cast(trg_type)
    }
}
