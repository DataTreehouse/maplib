use crate::errors::QueryProcessingError;
use crate::expressions::functions::str_function::str_function;
use md5::Digest;
use oxrdf::vocab::xsd;
use polars::prelude::{IntoColumn, StringChunked};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use sha1::Sha1;
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn sha1_(
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
    let mut expr = str_function(
        first_context.as_str(),
        solution_mappings
            .rdf_node_types
            .get(first_context.as_str())
            .unwrap(),
        global_cats,
    );
    expr = expr.map(
        move |x| {
            let mut encoded = Vec::with_capacity(x.len());

            for s in x.str()?.iter() {
                if let Some(s) = s {
                    let mut hasher = Sha1::new();
                    hasher.update(s.as_bytes());
                    let out = hasher.finalize();
                    encoded.push(Some(hex::encode(out.as_slice())));
                } else {
                    encoded.push(None);
                }
            }
            let column = StringChunked::from_iter(encoded).into_column();
            Ok(column)
        },
        |_, f| Ok(f.clone()),
    );
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::STRING.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
