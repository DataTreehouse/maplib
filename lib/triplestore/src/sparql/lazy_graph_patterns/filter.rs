use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::lazy_graph_patterns::geo_filter::{
    binds_variable_in_triple, conjunction, conjuncts, split_for_spatial_join,
};
use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::expressions::{contains_graph_pattern, effective_boolean_value};
use query_processing::graph_patterns::{filter, join};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    pub(crate) fn lazy_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing filter graph pattern");
        let inner_context = context.extension_with(PathEntry::FilterInner);
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        let expression_pushdowns = if contains_graph_pattern(expression) {
            Some(pushdowns.clone())
        } else {
            None
        };

        // GeoSPARQL conjuncts that the geo index can answer are taken out of the filter.
        let mut input_solution_mappings = input_solution_mappings;
        let mut residual: Option<Expression> = None;
        let mut geo_joined: Option<SolutionMappings> = None;
        let mut used_geo_index = false;
        if let Some(geo_index) = &self.geo_index {
            let mut remaining = conjuncts(expression);

            // Conjuncts comparing a variable with a constant geometry restrict the variable
            // before the inner pattern is evaluated.
            let candidates =
                geo::filter_candidates(geo_index, &remaining, self.global_cats.clone())?;
            let mut used = vec![];
            for c in candidates {
                if !binds_variable_in_triple(inner, c.variable.as_str()) {
                    continue;
                }
                used.push(c.conjunct);
                let mappings = c.mappings.as_lazy();
                input_solution_mappings = Some(if let Some(sm) = input_solution_mappings {
                    join(
                        sm,
                        mappings,
                        JoinType::Inner,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?
                } else {
                    mappings
                });
            }
            if !used.is_empty() {
                used_geo_index = true;
                remaining = remaining
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !used.contains(i))
                    .map(|(_, c)| c)
                    .collect();
            }

            // A conjunct relating geometry variables from two independent parts of the inner
            // pattern is evaluated as a spatial join of the two parts.
            if input_solution_mappings.is_none() {
                let mut join_conjunct = None;
                for (i, c) in remaining.iter().enumerate() {
                    if let Some((a, b)) = geo::spatial_join_variables(c) {
                        if let Some(sides) = split_for_spatial_join(inner, a.as_str(), b.as_str()) {
                            join_conjunct = Some((i, sides));
                            break;
                        }
                    }
                }
                if let Some((i, (left, right))) = join_conjunct {
                    let left_sm = self.lazy_graph_pattern(
                        &left,
                        None,
                        &inner_context.extension_with(PathEntry::JoinLeftSide),
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        dataset,
                    )?;
                    let right_sm = self.lazy_graph_pattern(
                        &right,
                        None,
                        &inner_context.extension_with(PathEntry::JoinRightSide),
                        parameters,
                        pushdowns.clone(),
                        query_settings,
                        dataset,
                    )?;
                    match geo::spatial_join(
                        geo_index,
                        remaining[i],
                        left_sm,
                        right_sm,
                        self.global_cats.clone(),
                    )? {
                        Ok(joined) => {
                            geo_joined = Some(joined);
                            remaining.remove(i);
                        }
                        // The sides are already evaluated, so they are joined as the inner
                        // pattern would be, and the conjunct stays in the filter.
                        Err((left_sm, right_sm)) => {
                            geo_joined = Some(join(
                                left_sm,
                                right_sm,
                                JoinType::Inner,
                                self.global_cats.clone(),
                                query_settings.max_rows,
                            )?);
                        }
                    }
                    used_geo_index = true;
                }
            }
            if used_geo_index {
                residual = conjunction(remaining);
            }
        }
        let expression = if used_geo_index {
            match &residual {
                Some(residual) => residual,
                None => {
                    // The geo index answered the whole filter.
                    return if let Some(joined) = geo_joined {
                        Ok(joined)
                    } else {
                        self.lazy_graph_pattern(
                            inner,
                            input_solution_mappings,
                            &inner_context,
                            parameters,
                            pushdowns,
                            query_settings,
                            dataset,
                        )
                    };
                }
            }
        } else {
            expression
        };

        let mut output_solution_mappings = if let Some(joined) = geo_joined {
            joined
        } else {
            self.lazy_graph_pattern(
                inner,
                input_solution_mappings,
                &inner_context,
                parameters,
                pushdowns,
                query_settings,
                dataset,
            )?
        };
        output_solution_mappings = self.lazy_expression(
            expression,
            output_solution_mappings,
            &expression_context,
            parameters,
            expression_pushdowns.as_ref(),
            query_settings,
            dataset,
        )?;
        output_solution_mappings = effective_boolean_value(
            output_solution_mappings,
            &expression_context,
            self.global_cats.clone(),
        );
        output_solution_mappings = filter(output_solution_mappings, &expression_context)?;
        Ok(output_solution_mappings)
    }
}
