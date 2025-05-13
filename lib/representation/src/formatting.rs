use crate::polars_to_rdf::{
    datetime_column_to_strings, XSD_DATETIME_WITH_TZ_FORMAT, XSD_DATE_WITHOUT_TZ_FORMAT,
};
use crate::{
    BaseRDFNodeType, RDFNodeType, IRI_PREFIX_FIELD, IRI_SUFFIX_FIELD, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, Expr, GetOutput, IntoColumn, LazyFrame, LiteralValue};
use std::collections::HashMap;
use oxrdf::NamedNode;

pub fn format_columns(
    mut lf: LazyFrame,
    rdf_node_types: &mut HashMap<String, RDFNodeType>,
) -> LazyFrame {
    let mut iri_cols = vec![];
    let mut iri_multi_cols = vec![];
    for (c, t) in rdf_node_types.iter() {
        if let RDFNodeType::MultiType(types) = t {
            for t in types.iter() {
                if t.is_iri() { 
                    iri_multi_cols.push(c.to_string());
                    break;
                }
            }
            lf = lf.with_column(expression_to_string(col(c), c, t.clone()));
        } else if  t.is_lang_string()
            || t == &RDFNodeType::BlankNode
        {
            lf = lf.with_column(expression_to_string(col(c), c, t.clone()));
        } else if t.is_iri() {
            lf = lf.with_column(base_expression_to_string(
                col(c),
                c,
                BaseRDFNodeType::from_rdf_node_type(t),
            ));
            iri_cols.push(c.to_string());
        }
    }
    for c in iri_cols {
        rdf_node_types.insert(c, RDFNodeType::IRI(None));
    }
    for c in  iri_multi_cols {
        let mut types = vec![];
        let existing = rdf_node_types.remove(&c).unwrap();
        if let RDFNodeType::MultiType(existing) = existing {
            for t in existing {
                if t.is_iri() {
                    types.push(BaseRDFNodeType::IRI(None));
                } else {
                    types.push(t);
                }
            }
        } else {
            unreachable!("Should never happen");
        }
        rdf_node_types.insert(c, RDFNodeType::MultiType(types));
    }
    lf
}

pub fn iri_col_to_string_no_brackets(expr:Expr, nn:Option<&NamedNode>) -> Expr {
    if let Some(nn) = nn {
        let prefix = lit(nn.as_str());
        let suffix = expr
            .struct_()
            .field_by_name(IRI_SUFFIX_FIELD)
            .cast(DataType::String);
        prefix + suffix
        
    } else {
        let prefix = expr
            .clone()
            .struct_()
            .field_by_name(IRI_PREFIX_FIELD)
            .cast(DataType::String);
        let suffix = expr
            .struct_()
            .field_by_name(IRI_SUFFIX_FIELD)
            .cast(DataType::String);
        prefix + suffix
    }
}

pub fn base_expression_to_string(
    expr: Expr,
    name: &str,
    base_rdf_node_type: BaseRDFNodeType,
) -> Expr {
    let expr = match base_rdf_node_type {
        BaseRDFNodeType::IRI(nn) => {
            let iri = iri_col_to_string_no_brackets(expr, nn.as_ref());
            lit("<") + iri + lit(">")
        }
        BaseRDFNodeType::BlankNode => lit("_:") + expr.cast(DataType::String),
        BaseRDFNodeType::Literal(l) if l.as_ref() == xsd::DATE_TIME => {
            lit("\"")
                + expr.map(
                    |x| {
                        let dt = x.dtype();
                        let tz = if let DataType::Datetime(_, tz) = dt {
                            tz
                        } else {
                            panic!()
                        };
                        Ok(Some(datetime_column_to_strings(&x, tz).into_column()))
                    },
                    GetOutput::from_type(DataType::String),
                )
                + lit(format!("\"^^{}", l))
        }
        BaseRDFNodeType::Literal(l) if l.as_ref() == xsd::DATE_TIME_STAMP => {
            lit("\"") + expr.dt().strftime(XSD_DATETIME_WITH_TZ_FORMAT) + lit(format!("\"^^{}", l))
        }
        BaseRDFNodeType::Literal(l) if l.as_ref() == xsd::DATE => {
            lit("\"") + expr.dt().strftime(XSD_DATE_WITHOUT_TZ_FORMAT) + lit(format!("\"^^{}", l))
        }
        BaseRDFNodeType::Literal(l) if l.as_ref() == rdf::LANG_STRING => {
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
        }
        BaseRDFNodeType::Literal(l) if l.as_ref() == xsd::STRING => {
            lit("\"") + expr.cast(DataType::String) + lit("\"")
        }
        // Fallback
        BaseRDFNodeType::Literal(l) => {
            lit("\"") + expr.cast(DataType::String) + lit(format!("\"^^{}", l))
        }
        BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
    };
    expr.alias(name)
}

pub fn expression_to_string(expr: Expr, name: &str, rdf_node_type: RDFNodeType) -> Expr {
    if let RDFNodeType::MultiType(ts) = rdf_node_type {
        let mut exprs = vec![];
        for t in ts {
            if t.is_multifield() {
                exprs.push(base_expression_to_string(expr.clone(), name, t));
            } else {
                exprs.push(base_expression_to_string(
                    expr.clone().struct_().field_by_name(&t.base_col()),
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
