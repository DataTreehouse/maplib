use crate::multitype::non_multi_type_string;
use crate::polars_to_rdf::{
    datetime_series_to_strings, XSD_DATETIME_WITH_TZ_FORMAT,
    XSD_DATE_WITHOUT_TZ_FORMAT,
};
use crate::{BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, GetOutput, LazyFrame, LiteralValue};
use std::collections::HashMap;

pub fn format_columns(
    mut lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
) -> LazyFrame {
    for (c, t) in rdf_node_types {
        if matches!(t, RDFNodeType::MultiType(_))
            || t.is_lang_string()
            || t == &RDFNodeType::IRI
            || t == &RDFNodeType::BlankNode
        {
            lf = lf.with_column(expression_to_string(col(c), c, t.clone()));
        }
    }
    lf
}

pub fn base_expression_to_string(
    expr: Expr,
    name: &str,
    base_rdf_node_type: BaseRDFNodeType,
) -> Expr {
    let expr = match base_rdf_node_type {
        BaseRDFNodeType::IRI => lit("<") + expr.cast(DataType::String) + lit(">"),
        BaseRDFNodeType::BlankNode => lit("_:") + expr.cast(DataType::String),
        BaseRDFNodeType::Literal(l) => {
            if l.as_ref() == xsd::DATE_TIME {
                lit("\"")
                    + expr.map(
                        |x| {
                            let dt = x.dtype();
                            let tz = if let DataType::Datetime(_, tz) = dt {
                                tz
                            } else {
                                panic!()
                            };
                            Ok(Some(datetime_series_to_strings(&x, tz)))
                        },
                        GetOutput::from_type(DataType::String),
                    )
                    + lit(format!("\"^^{}", l.to_string()))
            } else if l.as_ref() == xsd::DATE_TIME_STAMP {
                lit("\"")
                    + expr.dt().strftime(XSD_DATETIME_WITH_TZ_FORMAT)
                    + lit(format!("\"^^{}", l.to_string()))
            } else if l.as_ref() == xsd::DATE {
                lit("\"")
                    + expr.dt().strftime(XSD_DATE_WITHOUT_TZ_FORMAT)
                    + lit(format!("\"^^{}", l.to_string()))
            } else if l.as_ref() == rdf::LANG_STRING {
                lit("\"")
                    + expr
                        .clone()
                        .struct_()
                        .field_by_name(LANG_STRING_VALUE_FIELD)
                        .cast(DataType::String)
                    + lit("\"@")
                    + expr
                        .struct_()
                        .field_by_name(LANG_STRING_LANG_FIELD)
                        .cast(DataType::String)
            } else if l.as_ref() == xsd::STRING {
                lit("\"") + expr.cast(DataType::String) + lit("\"")
            } else {
                lit("\"") + expr.cast(DataType::String) + lit(format!("\"^^{}", l.to_string()))
            }
        }
        BaseRDFNodeType::None => lit(LiteralValue::Null).cast(DataType::String),
    };
    expr.alias(name)
}

pub fn expression_to_string(expr: Expr, name: &str, rdf_node_type: RDFNodeType) -> Expr {
    if let RDFNodeType::MultiType(ts) = rdf_node_type {
        let mut exprs = vec![];
        for t in ts {
            if t.is_lang_string() {
                exprs.push(base_expression_to_string(expr.clone(), name, t));
            } else {
                exprs.push(base_expression_to_string(
                    expr.clone()
                        .struct_()
                        .field_by_name(&non_multi_type_string(&t)),
                    name,
                    t,
                ));
            }
        }
        coalesce(&exprs)
    } else {
        base_expression_to_string(
            expr,
            name,
            BaseRDFNodeType::from_rdf_node_type(&rdf_node_type),
        )
    }
}
