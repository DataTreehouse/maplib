use crate::errors::QueryProcessingError;
use crate::udf::UdfRegistry;
use oxrdf::NamedNode;
use polars::prelude::{col, lit, IntoLazy, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::algebra::Expression;
use std::collections::{HashMap, HashSet};

pub fn udf(
    iri: &NamedNode,
    mut solution_mappings: SolutionMappings,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
    registry: &UdfRegistry,
) -> Result<SolutionMappings, QueryProcessingError> {
    if let Some(f) = registry.get_function(iri) {
        let input_types = f.input_types().cloned();
        let output_type = f.output_type().clone();
        if let Some(input_types) = &input_types {
            if input_types.len() != args.len() {
                return Err(QueryProcessingError::UDFError(format!(
                    "UDF '{}' expects {} args, got {}",
                    iri,
                    input_types.len(),
                    args.len()
                )));
            }
        }
        let mut arg_exprs = vec![];
        for i in 0..args_contexts.len() {
            let cname = args_contexts.get(&i).unwrap().as_str();
            let t = solution_mappings.rdf_node_types.get(cname).unwrap();
            // Detect ambiguous type
            if t.is_multi() && input_types.is_none() {
                let types: Vec<_> = t.map.keys().map(|x| x.to_string()).collect();
                return Err(QueryProcessingError::UDFError(format!("Please specify types for inputs, argument {i} has multiple possible types at evaluation: {}", types.join(", "))));
            }

            // Detect given type argument not in input
            let specified_type = if let Some(input_types) = &input_types {
                let use_t = input_types.get(i).unwrap();
                if !t.map.contains_key(use_t) {
                    arg_exprs.push(
                        lit(LiteralValue::untyped_null())
                            .cast(use_t.default_input_polars_data_type()),
                    );
                    continue;
                }
                Some(use_t)
            } else {
                None
            };

            let (base_t, base_s) = if let Some(specified_type) = specified_type {
                let base_s = t.map.get(&specified_type).unwrap();
                (specified_type, base_s)
            } else {
                (t.get_base_type().unwrap(), t.get_base_state().unwrap())
            };

            let mut arg_expr = if t.is_multi() {
                col(cname).struct_().field_by_name(&base_t.field_col_name())
            } else {
                col(cname)
            };

            arg_expr = maybe_decode_expr(arg_expr, base_t, base_s, global_cats.clone());
            arg_exprs.push(arg_expr.alias(format!("{i}")));
        }

        let mut df = solution_mappings.mappings.collect().unwrap();

        let input_df = df.clone().lazy().select(arg_exprs).collect().unwrap();

        let colnames: HashSet<_> = input_df
            .columns()
            .iter()
            .map(|x| x.name().to_string())
            .collect();
        let result_df = f.clone().call(&iri, input_df)?;
        let mut maybe_output = None;
        for c in result_df.columns() {
            if !colnames.contains(c.name().as_str()) || result_df.columns().len() == 1 {
                if c.dtype() != &output_type.clone().default_input_polars_data_type() {
                    return Err(QueryProcessingError::UDFError(format!(
                        "Output type:{} of UDF {} did not have the expected type: {}",
                        c.dtype(),
                        iri,
                        output_type.default_input_polars_data_type(),
                    )));
                }
                maybe_output = Some(c.clone());
            }
        }
        if let Some(mut output) = maybe_output {
            if output.len() != df.height() {
                return Err(QueryProcessingError::UDFError(format!(
                    "Output from {} should have {} rows, got {}",
                    iri,
                    df.height(),
                    output.len()
                )));
            }
            output.rename(outer_context.as_str().into());
            df.with_column(output)
                .map_err(|x| QueryProcessingError::UDFError(x.to_string()))?;
        }
        solution_mappings.mappings = df.lazy();
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            output_type.clone().into_default_input_rdf_node_state(),
        );
        Ok(solution_mappings)
    } else {
        Err(QueryProcessingError::CustomFunctionNotFound(
            iri.to_string(),
        ))
    }
}
