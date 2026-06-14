use crate::errors::QueryProcessingError;
use crate::expressions::functions::eval_uuid_namespace::eval_uuid_namespace;
use crate::expressions::functions::str_function;
use polars::datatypes::{DataType, Field, PlSmallStr};
use polars::prelude::{as_struct, by_name, IntoColumn, StringChunkedBuilder};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn uuid_v5(
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
    let tmp_column = uuid::Uuid::new_v4().to_string();
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_row_index(PlSmallStr::from_str(&tmp_column), None);
    let a1_str = args_contexts.get(&0).unwrap().as_str();
    let a2_str = args_contexts.get(&1).unwrap().as_str();
    let t1 = solution_mappings.rdf_node_types.get(a1_str).unwrap();
    let t2 = solution_mappings.rdf_node_types.get(a2_str).unwrap();
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        as_struct(vec![
            str_function(a1_str, t1, global_cats.clone()).alias("arg1"),
            str_function(a2_str, t2, global_cats.clone()).alias("arg2"),
        ])
        .map(
            |c| {
                let struct_col = c.struct_()?;
                let arg1_col = struct_col.field_by_name("arg1")?;
                let arg2_col = struct_col.field_by_name("arg2")?;
                let arg1_iter = arg1_col.str()?.iter();
                let arg2_iter = arg2_col.str()?.iter();
                let mut uuids_builder =
                    StringChunkedBuilder::new(PlSmallStr::from_str("uuids"), struct_col.len());

                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    if let (Some(arg1), Some(arg2)) = (arg1, arg2) {
                        let use_uuid = eval_uuid_namespace(arg1)?;
                        uuids_builder.append_value(format!(
                            "urn:uuid:{}",
                            uuid::Uuid::new_v5(&use_uuid, arg2.as_bytes())
                        ));
                    } else {
                        uuids_builder.append_null();
                    }
                }

                let s = uuids_builder.finish();
                Ok(s.into_column())
            },
            |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
        )
        .alias(outer_context.as_str()),
    );
    solution_mappings.mappings =
        solution_mappings
            .mappings
            .drop(by_name([&tmp_column], true, false));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
