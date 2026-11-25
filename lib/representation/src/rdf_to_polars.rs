use crate::errors::RepresentationError;
use crate::{BaseRDFNodeType, SeriesBuilder, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::rdf;
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Term};
use polars::prelude::{
    as_struct, col, lit, DataFrame, DataType, Expr, IntoLazy, LiteralValue, PlSmallStr, Scalar,
    TimeUnit, TimeZone,
};

pub fn rdf_term_to_polars_expr(term: &Term) -> Result<Expr, RepresentationError> {
    match term {
        Term::NamedNode(named_node) => Ok(lit(rdf_named_node_to_polars_literal_value(named_node))),
        Term::Literal(l) => rdf_literal_to_polars_expr(l),
        Term::BlankNode(bl) => Ok(lit(rdf_blank_node_to_polars_literal_value(bl))),
        Term::Triple(_) => Err(RepresentationError::Rdf12NotSupported(
            crate::rdf12::TRIPLE_TERMS_NOT_SUPPORTED.to_string(),
        )),
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

pub fn rdf_literal_to_polars_expr(l: &Literal) -> Result<Expr, RepresentationError> {
    let dt = l.datatype();
    let l = if dt == rdf::LANG_STRING {
        as_struct(vec![
            lit(l.value()).alias(LANG_STRING_VALUE_FIELD),
            lit(l.language().unwrap()).alias(LANG_STRING_LANG_FIELD),
        ])
    } else {
        lit(rdf_literal_to_polars_literal_value(l, None)?)
    };
    Ok(l)
}

pub fn rdf_literal_to_polars_literal_value(
    literal: &Literal,
    override_dt: Option<NamedNodeRef>,
) -> Result<LiteralValue, RepresentationError> {
    rdf_literal_to_polars_literal_value_impl(
        literal.value(),
        override_dt.unwrap_or(literal.datatype()),
    )
}

pub fn rdf_literal_to_polars_literal_value_impl(
    value: &str,
    datatype: NamedNodeRef,
) -> Result<LiteralValue, RepresentationError> {
    let dt = BaseRDFNodeType::Literal(datatype.into_owned(), None);
    let mut sb = SeriesBuilder::new(&dt);
    sb.parse_literal(value, None)?;
    let ser = sb.into_series("dummy");
    let f = ser.first();
    Ok(LiteralValue::Scalar(f))
}

pub fn default_time_unit() -> TimeUnit {
    TimeUnit::Microseconds
}

pub fn default_time_zone() -> TimeZone {
    TimeZone::UTC
}

/// The internal type of xsd:dateTime values: UTC instants in microseconds. The time zone
/// offset of the literals is part of their RDF type, see BaseRDFNodeType::time_zone_offset.
pub fn default_datetime_type() -> DataType {
    DataType::Datetime(default_time_unit(), Some(default_time_zone()))
}

/// The type a column of this type is cast to so that its datetimes, also inside lists, use
/// the internal representation, or None if it already does. Naive datetimes are read as UTC
/// wall clock times, which is how datetimes without a time zone are stored.
pub fn normalized_datetime_dtype(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::Datetime(tu, tz) => {
            if *tu == default_time_unit() && tz.as_ref() == Some(&default_time_zone()) {
                None
            } else {
                Some(default_datetime_type())
            }
        }
        DataType::List(inner) => {
            normalized_datetime_dtype(inner).map(|t| DataType::List(Box::new(t)))
        }
        _ => None,
    }
}

/// Casts every datetime column of the DataFrame to the internal representation.
pub fn normalize_datetime_columns(df: DataFrame) -> DataFrame {
    let exprs: Vec<_> = df
        .columns()
        .iter()
        .filter_map(|c| {
            normalized_datetime_dtype(c.dtype()).map(|t| col(c.name().as_str()).cast(t))
        })
        .collect();
    if exprs.is_empty() {
        df
    } else {
        df.lazy().with_columns(exprs).collect().unwrap()
    }
}

pub fn default_decimal_type() -> DataType {
    DataType::Decimal(default_decimal_precision(), default_decimal_scale())
}

pub fn default_decimal_precision() -> usize {
    38
}

pub fn default_decimal_scale() -> usize {
    // XSD 1.1 Part 2 5.4: minimally conforming processors support decimals i / 10^k
    // with i < 10^16 and k <= 16, so keep 16 fractional digits instead of truncating.
    16
}
