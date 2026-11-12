use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{lit, when, Expr, LiteralValue, StrptimeOptions};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::formatting::literal_expr_to_string;
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

pub fn cast_literal(
    mut c: Expr,
    src_bt: &BaseRDFNodeType,
    src_bs: &BaseCatState,
    global_cats: LockedCats,
    src: NamedNodeRef,
    trg: NamedNodeRef,
    trg_type: DataType,
) -> Expr {
    // Works around issue where src is string and trg is string.
    // TODO: Handle state comprehensively
    if src == xsd::STRING {
        c = maybe_decode_expr(c, src_bt, src_bs, global_cats);
    }
    if trg == xsd::STRING {
        // Same lexical form as STR(), e.g. without padding zeros on decimals
        literal_expr_to_string(c, src_bt)
    } else if trg == xsd::BOOLEAN {
        cast_to_boolean(c, src)
    } else if literal_is_numeric(trg) {
        cast_to_numeric(c, src, trg, trg_type)
    } else if src == xsd::STRING && trg == xsd::DATE_TIME {
        c.cast(DataType::String).str().to_datetime(
            None,
            None,
            StrptimeOptions {
                format: None,
                strict: true,
                exact: false,
                cache: false,
            },
            lit("raise"),
        )
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
    } else if src == xsd::STRING && trg == xsd::DURATION {
        //Todo handle durations
        c
    } else {
        c.cast(trg_type)
    }
}
