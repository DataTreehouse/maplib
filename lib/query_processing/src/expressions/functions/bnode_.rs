use crate::errors::QueryProcessingError;
use crate::expressions::functions::str_lang::simple_string_expr;
use polars::prelude::{concat_str, int_range, len, lit, DataType, IntoColumn, StringChunked};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;
use uuid::Uuid;

/// BNODE() gives a fresh blank node for each solution and each call.
/// BNODE(str) gives the same blank node for the same simple literal within a solution.
pub fn bnode_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() > 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "0 or 1".to_string(),
        ));
    }
    let row = int_range(lit(0), len(), 1, DataType::Int64).cast(DataType::String);
    let expr = if let Some(first_context) = args_contexts.get(&0) {
        let t = solution_mappings
            .rdf_node_types
            .get(first_context.as_str())
            .unwrap();
        let s = simple_string_expr(first_context.as_str(), t, global_cats);
        // The label must not depend on the call, only on the solution and the string.
        // The string is hex encoded so that the label is a valid blank node label.
        let hexed = s.map(
            |x| {
                let encoded: StringChunked = x
                    .str()?
                    .iter()
                    .map(|s| s.map(|s| hex::encode(s.as_bytes())))
                    .collect();
                Ok(encoded.into_column())
            },
            |_, f| Ok(f.clone()),
        );
        concat_str(
            [lit(format!("bn{}r", *BNODE_PREFIX)), row, lit("s"), hexed],
            "",
            false,
        )
    } else {
        let call = Uuid::new_v4().simple().to_string();
        concat_str([lit(format!("bn{call}r")), row], "", false)
    };
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeState::from_bases(BaseRDFNodeType::BlankNode, BaseCatState::String),
    );
    Ok(solution_mappings)
}

static BNODE_PREFIX: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| Uuid::new_v4().simple().to_string());
