use super::Triplestore;
use crate::sparql::errors::SparqlError;

use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::OrderExpression;
use std::collections::HashMap;

impl Triplestore {
    pub fn lazy_order_expression(
        &self,
        oexpr: &OrderExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<(SolutionMappings, bool, Context), SparqlError> {
        match oexpr {
            OrderExpression::Asc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, &inner_context, parameters)?,
                    true,
                    inner_context,
                ))
            }
            OrderExpression::Desc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, &inner_context, parameters)?,
                    false,
                    inner_context,
                ))
            }
        }
    }
}
