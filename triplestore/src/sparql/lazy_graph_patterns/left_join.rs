use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;

use polars::prelude::JoinType;
use query_processing::expressions::drop_inner_contexts;
use query_processing::graph_patterns::{filter, join};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    pub fn lazy_left_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing left join graph pattern");
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);

        let left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings, &left_context, parameters)?;

        let mut right_solution_mappings = self.lazy_graph_pattern(
            right,
            Some(left_solution_mappings.clone()),
            &right_context,
            parameters,
        )?;

        if let Some(expr) = expression {
            right_solution_mappings = self.lazy_expression(
                expr,
                right_solution_mappings,
                &expression_context,
                parameters,
            )?;
            right_solution_mappings = filter(right_solution_mappings, &expression_context)?;
            right_solution_mappings =
                drop_inner_contexts(right_solution_mappings, &vec![&expression_context]);
        }
        let left_solution_mappings = join(
            left_solution_mappings,
            right_solution_mappings,
            JoinType::Left,
        )?;

        Ok(left_solution_mappings)
    }
}
