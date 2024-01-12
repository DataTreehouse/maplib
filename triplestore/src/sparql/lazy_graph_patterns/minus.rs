use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;
use polars::export::ahash::HashSet;
use polars::prelude::{col, Expr, JoinArgs, JoinType};
use polars_core::datatypes::DataType;
use query_processing::graph_patterns::minus;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{is_string_col, SolutionMappings};
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub(crate) fn lazy_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing minus graph pattern");
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings.clone(), &left_context)?;

        let right_solution_mappings =
            self.lazy_graph_pattern(right, solution_mappings, &right_context)?;

        Ok(minus(left_solution_mappings, right_solution_mappings)?)
    }
}
