use super::Triplestore;
use crate::sparql::errors::SparqlError;
use log::debug;

use query_processing::graph_patterns::filter;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing filter graph pattern");
        let inner_context = context.extension_with(PathEntry::FilterInner);
        let expression_context = context.extension_with(PathEntry::FilterExpression);

        let mut output_solution_mappings =
            self.lazy_graph_pattern(inner, input_solution_mappings, &inner_context, parameters)?;
        output_solution_mappings = self.lazy_expression(
            expression,
            output_solution_mappings,
            &expression_context,
            parameters,
        )?;
        Ok(filter(output_solution_mappings, &expression_context)?)
    }
}
