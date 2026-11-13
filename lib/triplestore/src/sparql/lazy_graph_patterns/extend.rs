use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::expressions::contains_graph_pattern;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use query_processing::graph_patterns::{extend, join};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::{HashMap, HashSet};

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(crate) fn lazy_extend(
        &self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing extend graph pattern");
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let expression_context = context.extension_with(PathEntry::ExtendExpression);
        pushdowns.remove_variable(variable);
        let expression_pushdowns = if contains_graph_pattern(expression) {
            Some(pushdowns.clone())
        } else {
            None
        };
        // The expression is evaluated in the scope of the inner pattern (the group of the BIND),
        // so it must not see variables that only the incoming solution mappings bind. When it
        // uses such a variable, the group is evaluated on its own and joined afterwards.
        let (input_solution_mappings, join_afterwards) = if let Some(sm) = input_solution_mappings {
            if uses_outer_variables(expression, inner, &sm) {
                (None, Some(sm))
            } else {
                (Some(sm), None)
            }
        } else {
            (None, None)
        };
        let mut output_solution_mappings = self.lazy_graph_pattern(
            inner,
            input_solution_mappings,
            &inner_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        output_solution_mappings = self.lazy_expression(
            expression,
            output_solution_mappings,
            &expression_context,
            parameters,
            expression_pushdowns.as_ref(),
            query_settings,
            dataset,
        )?;
        let mut sm = extend(
            output_solution_mappings,
            &expression_context,
            variable,
            self.global_cats.clone(),
        )?;
        if let Some(outer) = join_afterwards {
            sm = join(
                outer,
                sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(sm)
    }
}

/// Whether the expression uses a variable that the inner pattern does not bind, but the
/// solution mappings do.
fn uses_outer_variables(
    expression: &Expression,
    inner: &GraphPattern,
    solution_mappings: &SolutionMappings,
) -> bool {
    let mut used = HashSet::new();
    find_all_used_variables_in_expression(expression, &mut used, true, true);
    let mut in_scope = HashSet::new();
    inner.on_in_scope_variable(|v| {
        in_scope.insert(v.clone());
    });
    used.iter()
        .any(|v| !in_scope.contains(v) && solution_mappings.rdf_node_types.contains_key(v.as_str()))
}
