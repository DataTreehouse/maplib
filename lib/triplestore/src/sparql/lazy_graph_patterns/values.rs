use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::pushdowns::Pushdowns;
use oxrdf::{Term, Variable};
use polars::prelude::{as_struct, col, DataFrame, IntoLazy, JoinType, PlSmallStr};
use polars_core::prelude::IntoColumn;
use query_processing::graph_patterns::{join, values_pattern};
use representation::multitype::{base_col_name};
use representation::polars_to_rdf::particular_opt_term_vec_to_series;
use representation::query_context::Context;
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::{
    get_ground_term_datatype_ref, BaseRDFNodeTypeRef, RDFNodeType, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use spargebra::term::GroundTerm;
use std::collections::HashMap;
use std::iter;

impl Triplestore {
    pub(crate) fn lazy_values(
        &mut self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
        _context: &Context,
        _pushdowns: Pushdowns,
    ) -> Result<SolutionMappings, SparqlError> {
        let sm = values_pattern(variables, bindings);
        if let Some(mut mappings) = solution_mappings {
            mappings = join(mappings, sm, JoinType::Inner)?;
            Ok(mappings)
        } else {
            Ok(sm)
        }
    }
}
