use crate::ast::{ConstantLiteral, ConstantTerm, PType};
use crate::constants::{BLANK_NODE_IRI, NONE_IRI, OTTR_IRI};
use crate::mapping::errors::MappingError;
use crate::mapping::RDFNodeType;

use oxrdf::NamedNode;
use polars::prelude::{concat_list, lit, Expr, LiteralValue, SpecialEq};
use polars_core::datatypes::DataType;
use polars_core::prelude::{AnyValue, IntoSeries, ListChunked, Series};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use representation::literals::sparql_literal_to_any_value;
use std::ops::Deref;

const BLANK_NODE_SERIES_NAME: &str = "blank_node_series";

pub fn constant_to_expr(
    constant_term: &ConstantTerm,
    ptype_opt: &Option<PType>,
) -> Result<(Expr, PType, RDFNodeType, Option<String>), MappingError> {
    let (expr, ptype, rdf_node_type, language_tag) = match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::Iri(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.as_str().to_string())),
                PType::Basic(NamedNode::new_unchecked(OTTR_IRI), "ottr:IRI".to_string()),
                RDFNodeType::IRI,
                None,
            ),
            ConstantLiteral::BlankNode(_) => {
                panic!("Should never happen")
            }
            ConstantLiteral::Literal(lit) => {
                let dt = if let Some(nn) = &lit.data_type_iri {
                    Some(nn.as_ref())
                } else {
                    None
                };
                let (mut any, dt) = sparql_literal_to_any_value(&lit.value, &dt);
                //Workaround for owned utf 8..
                let value_series = if let AnyValue::Utf8Owned(s) = any {
                    any = AnyValue::Utf8(&s);
                    let mut value_series = Series::new_empty("literal", &DataType::Utf8);
                    value_series = value_series.extend_constant(any, 1).unwrap();
                    value_series
                } else {
                    Series::from_any_values("literal", &[any], false).unwrap()
                };
                let language_tag = lit.language.as_ref().cloned();
                (
                    Expr::Literal(LiteralValue::Series(SpecialEq::new(value_series))),
                    PType::Basic(
                        lit.data_type_iri.as_ref().unwrap().clone(),
                        lit.data_type_iri.as_ref().unwrap().to_string(),
                    ),
                    RDFNodeType::Literal(dt.into()),
                    language_tag,
                )
            }
            ConstantLiteral::None => (
                Expr::Literal(LiteralValue::Null),
                PType::Basic(NamedNode::new_unchecked(NONE_IRI), NONE_IRI.to_string()),
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
            let out_ptype = PType::List(Box::new(last_ptype.unwrap()));
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
    if let Some(ptype_inferred) = ptype_opt {
        if ptype_inferred != &ptype {
            return Err(MappingError::ConstantDoesNotMatchDataType(
                constant_term.clone(),
                ptype_inferred.clone(),
                ptype,
            ));
        }
    }
    Ok((expr, ptype, rdf_node_type, language_tag))
}

pub fn constant_blank_node_to_series(
    layer: usize,
    pattern_num: usize,
    blank_node_counter: usize,
    constant_term: &ConstantTerm,
    n_rows: usize,
) -> Result<(Series, PType, RDFNodeType), MappingError> {
    Ok(match constant_term {
        ConstantTerm::Constant(ConstantLiteral::BlankNode(bl)) => {
            let any_value_vec: Vec<_> = (blank_node_counter..(blank_node_counter + n_rows))
                .into_par_iter()
                .map(|i| {
                    AnyValue::Utf8Owned(
                        format!("_:{}_l{}_p{}_r{}", bl.as_str(), layer, pattern_num, i).into(),
                    )
                })
                .collect();

            (
                Series::from_any_values_and_dtype(
                    BLANK_NODE_SERIES_NAME,
                    any_value_vec.as_slice(),
                    &DataType::Utf8,
                    false,
                )
                .unwrap(),
                PType::Basic(
                    NamedNode::new_unchecked(BLANK_NODE_IRI),
                    BLANK_NODE_IRI.to_string(),
                ),
                RDFNodeType::BlankNode,
            )
        }
        ConstantTerm::ConstantList(_) => {
            todo!("Not yet implemented support for lists of blank nodes")
        }
        _ => {
            panic!("Should never happen")
        }
    })
}
