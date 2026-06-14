use crate::errors::RepresentationError;
use crate::{BaseRDFNodeType, SeriesBuilder, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::rdf;
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, Term};
use polars::prelude::{
    as_struct, lit, DataType, Expr, LiteralValue, PlSmallStr, Scalar, TimeUnit, TimeZone,
};

pub fn rdf_term_to_polars_expr(term: &Term) -> Result<Expr, RepresentationError> {
    match term {
        Term::NamedNode(named_node) => Ok(lit(rdf_named_node_to_polars_literal_value(named_node))),
        Term::Literal(l) => rdf_literal_to_polars_expr(l),
        Term::BlankNode(bl) => Ok(lit(rdf_blank_node_to_polars_literal_value(bl))),
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
    let dt = BaseRDFNodeType::Literal(datatype.into_owned());
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

pub fn default_decimal_type() -> DataType {
    DataType::Decimal(default_decimal_precision(), default_decimal_scale())
}

pub fn default_decimal_precision() -> usize {
    38
}

pub fn default_decimal_scale() -> usize {
    12
}
