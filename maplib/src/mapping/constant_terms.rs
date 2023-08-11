use crate::ast::{ConstantLiteral, ConstantTerm, PType};
use crate::constants::{BLANK_NODE_IRI, NONE_IRI};
use crate::mapping::errors::MappingError;
use crate::mapping::RDFNodeType;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{concat_list, lit, Expr, LiteralValue, SpecialEq};
use polars_core::datatypes::DataType;
use polars_core::prelude::{AnyValue, IntoSeries, ListChunked, Series};
use representation::literals::sparql_literal_to_any_value;
use std::ops::Deref;

pub fn constant_to_expr(
    constant_term: &ConstantTerm,
    ptype_opt: &Option<PType>,
) -> Result<(Expr, PType, RDFNodeType, Option<String>), MappingError> {
    let (expr, ptype, rdf_node_type, language_tag) = match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::IRI(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.as_str().to_string())),
                PType::BasicType(xsd::ANY_URI.into_owned(), "xsd:anyURI".to_string()),
                RDFNodeType::IRI,
                None,
            ),
            ConstantLiteral::BlankNode(bn) => (
                Expr::Literal(LiteralValue::Utf8(bn.as_str().to_string())),
                PType::BasicType(
                    NamedNode::new_unchecked(BLANK_NODE_IRI),
                    BLANK_NODE_IRI.to_string(),
                ),
                RDFNodeType::BlankNode,
                None,
            ),
            ConstantLiteral::Literal(lit) => {
                let (mut any, dt) = sparql_literal_to_any_value(&lit.value, &lit.data_type_iri);
                let mut value_series = Series::new_empty("literal", &DataType::Utf8);
                //Workaround for owned utf 8..
                if let AnyValue::Utf8Owned(s) = any {
                    any = AnyValue::Utf8(&s);
                    value_series = value_series.extend_constant(any, 1).unwrap();
                } else {
                    value_series = value_series.extend_constant(any, 1).unwrap();
                }
                let language_tag = if let Some(tag) = &lit.language {
                    Some(tag.clone())
                } else {
                    None
                };
                (
                    Expr::Literal(LiteralValue::Series(SpecialEq::new(value_series))),
                    PType::BasicType(
                        lit.data_type_iri.as_ref().unwrap().clone(),
                        lit.data_type_iri.as_ref().unwrap().to_string(),
                    ),
                    RDFNodeType::Literal(dt),
                    language_tag,
                )
            }
            ConstantLiteral::None => (
                Expr::Literal(LiteralValue::Null),
                PType::BasicType(NamedNode::new_unchecked(NONE_IRI), NONE_IRI.to_string()),
                RDFNodeType::None,
                None,
            ),
        },
        ConstantTerm::ConstantList(inner) => {
            let mut expressions = vec![];
            let mut last_ptype = None;
            let mut last_rdf_node_type = None;
            for ct in inner {
                let (constant_expr, actual_ptype, rdf_node_type, language_tag) =
                    constant_to_expr(ct, ptype_opt)?;
                if language_tag.is_some() {
                    todo!()
                }
                if last_ptype.is_none() {
                    last_ptype = Some(actual_ptype);
                } else if last_ptype.as_ref().unwrap() != &actual_ptype {
                    return Err(MappingError::ConstantListHasInconsistentPType(
                        constant_term.clone(),
                        last_ptype.as_ref().unwrap().clone(),
                        actual_ptype.clone(),
                    ));
                }
                last_rdf_node_type = Some(rdf_node_type);
                expressions.push(constant_expr);
            }
            let out_ptype = PType::ListType(Box::new(last_ptype.unwrap()));
            let out_rdf_node_type = last_rdf_node_type.as_ref().unwrap().clone();

            if let RDFNodeType::Literal(_lit) = last_rdf_node_type.as_ref().unwrap() {
                let mut all_series = vec![];
                for ex in &expressions {
                    if let Expr::Literal(inner) = ex {
                        if let LiteralValue::Series(series) = inner {
                            all_series.push(series.deref().clone())
                        } else {
                            panic!("Should never happen");
                        }
                    } else {
                        panic!("Should also never happen");
                    }
                }
                let mut first = all_series.remove(0);
                for s in &all_series {
                    first.append(s).unwrap();
                }
                let out_series = ListChunked::from_iter([first]).into_series();
                (lit(out_series), out_ptype, out_rdf_node_type, None)
            } else {
                (
                    concat_list(expressions).expect("Concat OK"),
                    out_ptype,
                    out_rdf_node_type,
                    None,
                )
            }
        }
    };
    if let Some(ptype_in) = ptype_opt {
        if ptype_in != &ptype {
            return Err(MappingError::ConstantDoesNotMatchDataType(
                constant_term.clone(),
                ptype_in.clone(),
                ptype.clone(),
            ));
        }
    }
    Ok((expr, ptype, rdf_node_type, language_tag))
}
