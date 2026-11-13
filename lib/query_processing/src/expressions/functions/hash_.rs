use crate::errors::QueryProcessingError;
use crate::expressions::functions::str_function::str_function;
use md5::Md5;
use oxrdf::vocab::xsd;
use polars::prelude::{IntoColumn, StringChunked};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::BaseRDFNodeType;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub enum HashAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

fn hex_digest<D: Digest>(s: &str) -> String {
    hex::encode(D::digest(s.as_bytes()).as_slice())
}

/// MD5, SHA1, SHA256, SHA384 and SHA512: the lowercase hex digest of the UTF-8 bytes of the
/// string, as an xsd:string (SPARQL 1.1 section 17.4.6).
pub fn hash_(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
    algorithm: HashAlgorithm,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 1 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "1".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let expr = str_function(
        first_context.as_str(),
        solution_mappings
            .rdf_node_types
            .get(first_context.as_str())
            .unwrap(),
        global_cats,
    )
    .map(
        move |x| {
            let hash = match algorithm {
                HashAlgorithm::Md5 => hex_digest::<Md5>,
                HashAlgorithm::Sha1 => hex_digest::<Sha1>,
                HashAlgorithm::Sha256 => hex_digest::<Sha256>,
                HashAlgorithm::Sha384 => hex_digest::<Sha384>,
                HashAlgorithm::Sha512 => hex_digest::<Sha512>,
            };
            let encoded: StringChunked = x.str()?.iter().map(|s| s.map(hash)).collect();
            Ok(encoded.into_column())
        },
        |_, f| Ok(f.clone()),
    );
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}
