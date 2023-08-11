use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use spargebra::algebra::OrderExpression;

impl Triplestore {
    pub fn lazy_order_expression(
        &self,
        oexpr: &OrderExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> Result<(SolutionMappings, bool, Context), SparqlError> {
        match oexpr {
            OrderExpression::Asc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, &inner_context)?,
                    true,
                    inner_context,
                ))
            }
            OrderExpression::Desc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                Ok((
                    self.lazy_expression(expr, solution_mappings, &inner_context)?,
                    false,
                    inner_context,
                ))
            }
        }
    }
}
