use super::{QuerySettings, Triplestore};
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::lateral::lateral_key_columns;
use polars::prelude::{by_name, col};
use query_processing::pushdowns::Pushdowns;
use query_processing::window::{rank_one, window_function, WindowColumns};
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::OrderExpression;
use spargebra::window::{WindowCall, WindowFunction};
use std::collections::HashMap;

/// The arguments of a window call, evaluated into columns.
struct WindowInputs {
    arg: Option<Context>,
    partition_by: Vec<Context>,
    order_by: Vec<Context>,
    ascending: Vec<bool>,
}

impl WindowInputs {
    fn drop(self, mut sm: SolutionMappings) -> SolutionMappings {
        let mut names: Vec<String> = self.arg.iter().map(|c| c.as_str().to_string()).collect();
        names.extend(self.partition_by.iter().map(|c| c.as_str().to_string()));
        names.extend(self.order_by.iter().map(|c| c.as_str().to_string()));
        for n in &names {
            sm.rdf_node_types.remove(n);
        }
        sm.mappings = sm.mappings.drop(by_name(names, true, false));
        sm
    }
}

impl Triplestore {
    /// Evaluates a window function call into the column of the context, see
    /// `query_processing::window`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_window(
        &self,
        window: &WindowCall,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let (sm, inputs) = self.window_inputs(
            window,
            solution_mappings,
            context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        let sm = self.window_of_inputs(window, sm, &inputs, context)?;
        Ok(inputs.drop(sm))
    }

    /// For `FILTER(?r = 1)` over `BIND(RANK() OVER (...) AS ?r)` (or DENSE_RANK): the solutions
    /// with rank 1, with the column of the context bound to 1, and whether the filter is already
    /// applied (it is not when `rank_one` does not apply, then the column is the rank).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_window_rank_one(
        &self,
        window: &WindowCall,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<(SolutionMappings, bool), SparqlError> {
        let (sm, inputs) = self.window_inputs(
            window,
            solution_mappings,
            context,
            parameters,
            None,
            query_settings,
            dataset,
        )?;
        let (sm, filtered) = if inputs.order_by.len() == 1 {
            match rank_one(
                sm,
                &inputs.partition_by,
                &inputs.order_by[0],
                inputs.ascending[0],
                context,
                self.global_cats.clone(),
            ) {
                Ok(sm) => (sm, true),
                Err(sm) => (self.window_of_inputs(window, sm, &inputs, context)?, false),
            }
        } else {
            (self.window_of_inputs(window, sm, &inputs, context)?, false)
        };
        Ok((inputs.drop(sm), filtered))
    }

    fn window_of_inputs(
        &self,
        window: &WindowCall,
        sm: SolutionMappings,
        inputs: &WindowInputs,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        Ok(window_function(
            sm,
            &window.function,
            WindowColumns {
                arg: inputs.arg.as_ref(),
                partition_by: &inputs.partition_by,
                order_by: &inputs.order_by,
                ascending: inputs.ascending.clone(),
            },
            window.frame,
            context,
            self.global_cats.clone(),
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    fn window_inputs(
        &self,
        window: &WindowCall,
        solution_mappings: SolutionMappings,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Option<&Pushdowns>,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<(SolutionMappings, WindowInputs), SparqlError> {
        let mut sm = solution_mappings;
        let mut i = 0u16;
        let mut next_context = || {
            i += 1;
            context.extension_with(PathEntry::FunctionCall(i - 1))
        };
        let arg = if let Some(arg) = &window.arg {
            let c = next_context();
            sm = self.lazy_expression(
                arg,
                sm,
                &c,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            )?;
            Some(c)
        } else {
            None
        };
        let mut partition_by = vec![];
        for e in &window.partition_by {
            let c = next_context();
            sm = self.lazy_expression(e, sm, &c, parameters, pushdowns, query_settings, dataset)?;
            partition_by.push(c);
        }
        // Inside LATERAL, the right side is evaluated for all left solutions at once, with a key
        // column per left solution: the window is computed for each of them on its own.
        for key in lateral_key_columns(parameters) {
            if let Some(t) = sm.rdf_node_types.get(&key).cloned() {
                let c = next_context();
                sm.mappings = sm.mappings.with_column(col(&key).alias(c.as_str()));
                sm.rdf_node_types.insert(c.as_str().to_string(), t);
                partition_by.push(c);
            }
        }
        let mut order_by = vec![];
        let mut ascending = vec![];
        for o in &window.order_by {
            let (e, asc) = match o {
                OrderExpression::Asc(e) => (e, true),
                OrderExpression::Desc(e) => (e, false),
            };
            let c = next_context();
            sm = self.lazy_expression(e, sm, &c, parameters, pushdowns, query_settings, dataset)?;
            order_by.push(c);
            ascending.push(asc);
        }
        Ok((
            sm,
            WindowInputs {
                arg,
                partition_by,
                order_by,
                ascending,
            },
        ))
    }
}

/// Whether the window call is a RANK or DENSE_RANK, for which rank 1 can be found without
/// ranking.
pub(crate) fn is_rank_window(window: &WindowCall) -> bool {
    matches!(
        window.function,
        WindowFunction::Rank | WindowFunction::DenseRank
    ) && window.frame.is_none()
        && window.order_by.len() == 1
}
