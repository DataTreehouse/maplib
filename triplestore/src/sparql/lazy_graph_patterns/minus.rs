use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use representation::solution_mapping::{is_string_col, SolutionMappings};
use log::debug;
use polars::export::ahash::HashSet;
use polars::prelude::{col, Expr, JoinArgs, JoinType};
use polars_core::datatypes::DataType;
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
        let mut left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings.clone(), &left_context)?;

        let right_solution_mappings =
            self.lazy_graph_pattern(right, solution_mappings, &right_context)?;

        let SolutionMappings {
            mappings: mut right_mappings,
            rdf_node_types: right_datatypes,
        } = right_solution_mappings;

        let right_column_set: HashSet<_> = right_datatypes.keys().collect();
        let left_column_set: HashSet<_> = left_solution_mappings.rdf_node_types.keys().collect();

        let mut join_on: Vec<_> = left_column_set
            .intersection(&right_column_set)
            .cloned()
            .collect();
        join_on.sort();

        if join_on.is_empty() {
            Ok(left_solution_mappings)
        } else {
            let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();
            for c in join_on {
                if is_string_col(left_solution_mappings.rdf_node_types.get(c).unwrap()) {
                    right_mappings =
                        right_mappings.with_column(col(c).cast(DataType::Categorical(None)));
                    left_solution_mappings.mappings = left_solution_mappings
                        .mappings
                        .with_column(col(c).cast(DataType::Categorical(None)));
                }
            }
            let all_false = [false].repeat(join_on_cols.len());
            right_mappings = right_mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.sort_by_exprs(
                join_on_cols.as_slice(),
                all_false.as_slice(),
                false,
                false,
            );
            left_solution_mappings.mappings = left_solution_mappings.mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinArgs::new(JoinType::Anti),
            );
            Ok(left_solution_mappings)
        }
    }
}
