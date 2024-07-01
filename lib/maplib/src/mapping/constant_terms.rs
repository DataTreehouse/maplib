use crate::mapping::errors::MappingError;
use crate::mapping::{MappingColumnType, RDFNodeType};
use templates::ast::{ConstantTerm, ConstantTermOrList, PType};

use oxrdf::{Literal, Term};
use polars::prelude::{
    concat_list, lit, AnyValue, DataType, Expr, IntoSeries, ListChunked, LiteralValue, Series,
};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use representation::rdf_to_polars::{
    polars_literal_values_to_series, rdf_named_node_to_polars_literal_value,
    rdf_term_to_polars_expr,
};
use representation::BaseRDFNodeType;
use std::ops::Deref;

const BLANK_NODE_SERIES_NAME: &str = "blank_node_series";

pub fn constant_to_expr(
    constant_term: &ConstantTermOrList,
    ptype_opt: &Option<PType>,
) -> Result<(Expr, PType, MappingColumnType), MappingError> {
    let (expr, ptype, rdf_node_type) = match constant_term {
        ConstantTermOrList::ConstantTerm(c) => match c {
            ConstantTerm::Iri(iri) => {
                let polars_literal = rdf_named_node_to_polars_literal_value(iri);
                (
                    Expr::Literal(polars_literal),
                    PType::Basic(BaseRDFNodeType::IRI, Some("ottr:IRI".to_string())),
                    MappingColumnType::Flat(RDFNodeType::IRI),
                )
            }
            ConstantTerm::BlankNode(_) => {
                panic!("Should never happen")
            }
            ConstantTerm::Literal(lit) => {
                let dt = lit.data_type_iri.as_ref().map(|nn| nn.as_ref());
                let language = lit.language.as_deref();
                let rdf_lit = if let Some(language) = language {
                    Literal::new_language_tagged_literal(&lit.value, language).unwrap()
                } else if let Some(dt) = dt {
                    Literal::new_typed_literal(&lit.value, dt)
                } else {
                    Literal::new_simple_literal(&lit.value)
                };
                let the_dt = rdf_lit.datatype().into_owned();
                let expr = rdf_term_to_polars_expr(&Term::Literal(rdf_lit));
                (
                    expr,
                    PType::Basic(BaseRDFNodeType::Literal(the_dt.clone()), None),
                    MappingColumnType::Flat(RDFNodeType::Literal(the_dt)),
                )
            }
            ConstantTerm::None => (
                Expr::Literal(LiteralValue::Null),
                PType::Basic(BaseRDFNodeType::None, None),
                MappingColumnType::Flat(RDFNodeType::None),
            ),
        },
        ConstantTermOrList::ConstantList(inner) => {
            let mut expressions = vec![];
            let mut last_ptype = None;
            let mut last_mapping_col_type = None;
            for ct in inner {
                let (constant_expr, actual_ptype, mapping_col_type) =
                    constant_to_expr(ct, ptype_opt)?;
                if last_ptype.is_none() {
                    last_ptype = Some(actual_ptype);
                } else if last_ptype.as_ref().unwrap() != &actual_ptype {
                    return Err(MappingError::ConstantListHasInconsistentPType(
                        constant_term.clone(),
                        last_ptype.as_ref().unwrap().clone(),
                        actual_ptype.clone(),
                    ));
                }
                last_mapping_col_type = Some(mapping_col_type);
                expressions.push(constant_expr);
            }
            let out_ptype = PType::List(Box::new(last_ptype.unwrap()));
            let out_rdf_node_type = MappingColumnType::Nested(Box::new(
                last_mapping_col_type.as_ref().unwrap().clone(),
            ));

            if let MappingColumnType::Flat(RDFNodeType::Literal(_lit)) =
                last_mapping_col_type.as_ref().unwrap()
            {
                let mut all_series = vec![];
                for ex in &expressions {
                    if let Expr::Literal(inner) = ex {
                        if let LiteralValue::Series(series) = inner {
                            all_series.push(series.deref().clone())
                        } else {
                            all_series.push(polars_literal_values_to_series(
                                vec![inner.clone()],
                                "dummy",
                            ))
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
                (lit(out_series), out_ptype, out_rdf_node_type)
            } else {
                (
                    concat_list(expressions).expect("Concat OK"),
                    out_ptype,
                    out_rdf_node_type,
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
    Ok((expr, ptype, rdf_node_type))
}

pub fn constant_blank_node_to_series(
    layer: usize,
    pattern_num: usize,
    blank_node_counter: usize,
    constant_term: &ConstantTermOrList,
    n_rows: usize,
) -> Result<(Series, PType, RDFNodeType), MappingError> {
    Ok(match constant_term {
        ConstantTermOrList::ConstantTerm(ConstantTerm::BlankNode(bl)) => {
            let any_value_vec: Vec<_> = (blank_node_counter..(blank_node_counter + n_rows))
                .into_par_iter()
                .map(|i| {
                    AnyValue::StringOwned(
                        format!("{}_l{}_p{}_r{}", bl.as_str(), layer, pattern_num, i).into(),
                    )
                })
                .collect();

            (
                Series::from_any_values_and_dtype(
                    BLANK_NODE_SERIES_NAME,
                    any_value_vec.as_slice(),
                    &DataType::String,
                    false,
                )
                .unwrap(),
                PType::Basic(BaseRDFNodeType::BlankNode, None),
                RDFNodeType::BlankNode,
            )
        }
        ConstantTermOrList::ConstantList(_) => {
            todo!("Not yet implemented support for lists of blank nodes")
        }
        _ => {
            panic!("Should never happen")
        }
    })
}
