use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::error::PolarsError;
use polars::prelude::{col, lit, IntoColumn, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, SeriesBuilder};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
use tracing::warn;

pub fn str_dt(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 2 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "2".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let first_type = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
    let use_base = if first_type.is_lit_type(xsd::STRING) {
        Some((
            first_type.get_base_state().unwrap(),
            col(first_context.as_str()),
        ))
    } else if first_type.is_multi() {
        if let Some(base_state) = first_type.map.get(&base_string) {
            Some((
                base_state,
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(&base_string.field_col_name()),
            ))
        } else {
            None
        }
    } else {
        None
    };
    if let Some((use_base_state, use_base_col)) = use_base {
        let dt_arg = args.get(1).unwrap();
        let dt_iri = if let Expression::NamedNode(nn) = &dt_arg {
            nn.clone()
        } else {
            return Err(QueryProcessingError::BadArgument(format!(
                "STRDT second argument should be IRI"
            )));
        };
        let out_dt = BaseRDFNodeType::Literal(dt_iri.clone());
        let out_dt_cloned = out_dt.clone();
        let out_polars_type = out_dt.default_input_polars_data_type();
        let outer_context_cloned = outer_context.clone();
        let decoded_col =
            maybe_decode_expr(use_base_col, &base_string, use_base_state, global_cats);
        let expr = decoded_col.map(
            move |x| {
                let mut builder = SeriesBuilder::new(&out_dt_cloned.clone());
                for v in x.str()?.iter() {
                    if let Some(v) = v {
                        match builder.parse_literal(v, None) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Could not parse literal {}", e);
                                return Err(PolarsError::InvalidOperation(
                                    "Error parsing literal to target type in STRDT function".into(),
                                ));
                            }
                        }
                    } else {
                        builder.push_none();
                    }
                }
                let series = builder.into_series(outer_context_cloned.clone().as_str());
                Ok(series.into_column())
            },
            move |_, f| {
                let mut field = f.clone();
                field.dtype = out_polars_type.clone();
                Ok(field)
            },
        );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(expr.alias(outer_context.as_str()));
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            out_dt.into_default_input_rdf_node_state(),
        );
    } else {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(
                    BaseRDFNodeType::None
                        .into_default_input_rdf_node_state()
                        .polars_data_type(),
                )
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        );
    }
    Ok(solution_mappings)
}
