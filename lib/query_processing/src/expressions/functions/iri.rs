use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{coalesce, col, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::MULTI_IRI_DT;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
pub fn iri(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let dt = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let (c, s) = if !dt.is_multi() {
        let b = dt.get_base_type().unwrap();
        let bs = dt.get_base_state().unwrap();
        let (c, s) = match b {
            BaseRDFNodeType::IRI => (col(first_context.as_str()), bs.clone()),
            BaseRDFNodeType::Literal(l) => {
                if l.as_ref() == xsd::STRING {
                    (
                        maybe_decode_expr(col(first_context.as_str()), b, bs, global_cats),
                        BaseCatState::String,
                    )
                } else {
                    (
                        lit(LiteralValue::untyped_null()).cast(DataType::String),
                        BaseCatState::String,
                    )
                }
            }
            BaseRDFNodeType::BlankNode | BaseRDFNodeType::None => (
                lit(LiteralValue::untyped_null()).cast(DataType::String),
                BaseCatState::String,
            ),
        };
        (c, s)
    } else {
        let mut iri_col = None;
        let mut string_col = None;
        let mut iri_state = None;

        for (t, s) in &dt.map {
            match t {
                BaseRDFNodeType::Literal(l) => {
                    if l.as_ref() == xsd::STRING {
                        string_col = Some(maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(t.field_col_name().as_str()),
                            t,
                            s,
                            global_cats.clone(),
                        ));
                    }
                }
                _ => {}
            }
        }
        for (t, s) in &dt.map {
            match t {
                BaseRDFNodeType::IRI => {
                    if string_col.is_some() {
                        iri_col = Some(maybe_decode_expr(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(MULTI_IRI_DT),
                            t,
                            s,
                            global_cats.clone(),
                        ));
                        iri_state = Some(BaseCatState::String);
                    } else {
                        iri_col = Some(
                            col(first_context.as_str())
                                .struct_()
                                .field_by_name(MULTI_IRI_DT),
                        );
                        iri_state = Some(s.clone());
                    }
                }
                _ => {}
            }
        }
        if iri_state.is_none() {
            iri_state = Some(BaseCatState::String);
        }
        let e = if iri_col.is_some() && string_col.is_none() {
            iri_col.unwrap()
        } else if iri_col.is_none() && string_col.is_some() {
            string_col.unwrap()
        } else if iri_col.is_some() && string_col.is_some() {
            coalesce(&[
                iri_col.unwrap().cast(DataType::String),
                string_col.unwrap().cast(DataType::String),
            ])
        } else {
            lit(LiteralValue::untyped_null()).cast(DataType::String)
        };
        (e, iri_state.unwrap())
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(c.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeState::from_bases(BaseRDFNodeType::IRI, s),
    );
    Ok(solution_mappings)
}
