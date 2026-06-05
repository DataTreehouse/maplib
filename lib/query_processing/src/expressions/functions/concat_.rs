use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::prelude::{col, concat_str, lit, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn concat_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() < 2 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            ">1".to_string(),
        ));
    }
    let SolutionMappings {
        mappings,
        rdf_node_types: datatypes,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let mut cols = Vec::with_capacity(args.len());
    let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned());
    for i in 0..args.len() {
        let c = args_contexts.get(&i).unwrap().as_str();
        let t = datatypes.get(c).unwrap();
        if let Some(s) = t.map.get(&base_string) {
            if t.is_multi() {
                cols.push(maybe_decode_expr(
                    col(c)
                        .struct_()
                        .field_by_name(&base_string.field_col_name()),
                    &base_string,
                    s,
                    global_cats.clone(),
                ))
            } else {
                cols.push(maybe_decode_expr(
                    col(c),
                    &base_string,
                    s,
                    global_cats.clone(),
                ))
            }
        } else {
            cols.push(lit(LiteralValue::untyped_null()).cast(DataType::String))
        }
    }
    let new_mappings =
        mappings.with_column(concat_str(cols, "", true).alias(outer_context.as_str()));
    solution_mappings = SolutionMappings::new(new_mappings, datatypes, height_upper_bound);
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::STRING.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
