use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use log::debug;
use oxrdf::Variable;
use query_processing::graph_patterns::extend;
use spargebra::algebra::{Expression, GraphPattern};

impl Triplestore {
    pub(crate) fn lazy_extend(
        &self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing extend graph pattern");
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let expression_context = context.extension_with(PathEntry::ExtendExpression);

        let mut output_solution_mappings =
            self.lazy_graph_pattern(inner, input_solution_mappings, &inner_context)?;

        output_solution_mappings =
            self.lazy_expression(expression, output_solution_mappings, &expression_context)?;
        Ok(extend(output_solution_mappings, &expression_context, variable)?)
    }
}
