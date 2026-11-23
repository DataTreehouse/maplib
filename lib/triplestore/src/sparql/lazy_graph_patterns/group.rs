use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use oxrdf::vocab::xsd;
use crate::sparql::lazy_graph_patterns::lateral::{lateral_key_columns, lateral_seed};
use polars::prelude::{
    coalesce, col, cols, lit, when, DataType, IntoLazy, JoinArgs, JoinType, LiteralValue,
};
use polars_core::prelude::UniqueKeepStrategy;
use polars_core::df;
use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::{group_by, join, prepare_group_by};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use representation::BaseRDFNodeType;
use spargebra::algebra::{AggregateExpression, AggregateFunction, GraphPattern};
use std::collections::HashMap;
use uuid::Uuid;

impl Triplestore {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(crate) fn lazy_group(
        &self,
        inner: &GraphPattern,
        variables: &[Variable],
        aggregates: &[(Variable, AggregateExpression)],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing group graph pattern");
        // Inside the right side of a LATERAL, each substituted binding is grouped on its own.
        let lateral_keys = lateral_key_columns(parameters);
        if lateral_keys.is_empty() && variables.len() <= 1 {
            // Counts over one triple pattern, also per predicate (GROUP BY its predicate variable).
            let counted = match self.count_from_tables(inner, variables, aggregates, dataset)? {
                Some(c) => Some(c),
                None if variables.is_empty() => {
                    self.count_joins_from_codes(inner, aggregates, dataset)?
                }
                None => None,
            };
            if let Some(grouped) = counted {
                return Ok(if let Some(solution_mappings) = solution_mappings {
                    join(
                        solution_mappings,
                        grouped,
                        JoinType::Inner,
                        self.global_cats.clone(),
                        query_settings.max_rows,
                    )?
                } else {
                    grouped
                });
            }
        }
        let inner_context = context.extension_with(PathEntry::GroupInner);
        pushdowns.limit_to_variables(variables);
        pushdowns.add_graph_pattern_pushdowns(inner);
        let output_solution_mappings = self.lazy_graph_pattern(
            inner,
            None,
            &inner_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        let mut output_solution_mappings = output_solution_mappings;
        // A GROUP BY variable the inner pattern does not bind is unbound in every solution,
        // so it groups as a single unbound key (SPARQL 1.1, 18.5.1).
        for v in variables {
            if !output_solution_mappings
                .rdf_node_types
                .contains_key(v.as_str())
            {
                output_solution_mappings.mappings =
                    output_solution_mappings.mappings.with_column(
                        lit(LiteralValue::untyped_null())
                            .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                            .alias(v.as_str()),
                    );
                output_solution_mappings.rdf_node_types.insert(
                    v.as_str().to_string(),
                    BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                );
            }
        }
        let lateral_keys: Vec<_> = lateral_keys
            .into_iter()
            .filter(|k| output_solution_mappings.rdf_node_types.contains_key(k))
            .collect();
        let group_variables: Vec<Variable> = variables
            .iter()
            .cloned()
            .chain(lateral_keys.iter().map(|k| Variable::new_unchecked(k.clone())))
            .collect();
        let (mut output_solution_mappings, by, dummy_varname) =
            prepare_group_by(output_solution_mappings, &group_variables);
        let mut aggregate_expressions = vec![];
        let mut new_rdf_node_types = HashMap::new();
        for v in &group_variables {
            let maybe_t = output_solution_mappings.rdf_node_types.get(v.as_str());
            if let Some(t) = maybe_t {
                new_rdf_node_types.insert(v.as_str().to_string(), t.clone());
            } else {
                return Err(SparqlError::GroupByWithUndefinedVariable(
                    v.clone(),
                    Box::new(inner.clone()),
                ));
            }
        }
        let mut aggregate_contexts = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let AggregateReturn {
                solution_mappings: aggregate_solution_mappings,
                expr,
                context: c,
                rdf_node_type,
            } = self.sparql_aggregate_expression_as_lazy_column_and_expression(
                v,
                a,
                output_solution_mappings,
                &aggregate_context,
                parameters,
                query_settings,
                dataset,
            )?;
            output_solution_mappings = aggregate_solution_mappings;
            new_rdf_node_types.insert(v.as_str().to_string(), rdf_node_type);
            aggregate_expressions.push(expr);
            if let Some(c) = c {
                aggregate_contexts.push(c);
            }
        }
        output_solution_mappings.rdf_node_types = new_rdf_node_types;
        let mut grouped = group_by(
            output_solution_mappings,
            aggregate_expressions,
            by,
            dummy_varname,
        )?;

        let dummy_col_name = Uuid::new_v4().to_string();

        let count_cols: Vec<_> = aggregates
            .iter()
            .filter(|(_, agg)| match agg {
                AggregateExpression::CountSolutions { .. } => true,
                AggregateExpression::FunctionCall { name, .. } => {
                    matches!(name, AggregateFunction::Count)
                }
            })
            .map(|(v, _)| v.as_str().to_string())
            .collect();
        let sum_avg_cols: Vec<_> = aggregates
            .iter()
            .filter(|(_, agg)| {
                matches!(agg, AggregateExpression::FunctionCall { name, .. }
                    if matches!(name, AggregateFunction::Sum | AggregateFunction::Avg))
            })
            .map(|(v, _)| v.as_str().to_string())
            .collect();

        // Without GROUP BY, there is always exactly one group, even if there are no solutions.
        if variables.is_empty() && !aggregates.is_empty() {
            let present_col_name = Uuid::new_v4().to_string();
            let lf_lhs = grouped
                .mappings
                .with_column(lit(0u32).alias(&dummy_col_name))
                .with_column(lit(true).alias(&present_col_name));

            let mut lf = if lateral_keys.is_empty() {
                let lf_rhs = df!(&dummy_col_name => [0u32]).unwrap().lazy();
                lf_rhs.left_join(lf_lhs, &*dummy_col_name, &*dummy_col_name)
            } else {
                // One group for every substituted binding.
                let seed = lateral_seed(parameters).unwrap();
                let lf_rhs = seed
                    .mappings
                    .clone()
                    .lazy()
                    .select(lateral_keys.iter().map(col).collect::<Vec<_>>())
                    .unique(None, UniqueKeepStrategy::Any)
                    .with_column(lit(0u32).alias(&dummy_col_name));
                let mut on: Vec<_> = lateral_keys.iter().map(col).collect();
                on.push(col(&*dummy_col_name));
                lf_rhs
                    .join(lf_lhs, on.clone(), on, JoinArgs::new(JoinType::Left))
                    .unwrap()
            };
            for c in count_cols {
                // COUNT over an empty group is 0, COUNT results are Int64 (xsd:integer).
                lf = lf.with_column(coalesce(&[
                    col(c),
                    col(&*dummy_col_name).cast(DataType::Int64),
                ]));
            }
            // SUM and AVG over an empty group are 0 (xsd:integer), but a group with values
            // keeps its result, which is unbound on a type error.
            let empty = col(&*present_col_name).is_null();
            for c in sum_avg_cols {
                let state = grouped.rdf_node_types.get(&c).unwrap();
                let numeric_type = state
                    .get_base_type()
                    .filter(|t| !state.is_multi() && t.is_numeric())
                    .cloned();
                if let Some(t) = numeric_type {
                    let dtype = t.polars_data_type(state.get_base_state().unwrap(), true);
                    lf = lf.with_column(
                        when(empty.clone())
                            .then(lit(0).cast(dtype))
                            .otherwise(col(&c))
                            .alias(&c),
                    );
                } else if state.is_none() {
                    // No values, so the result is the integer 0.
                    lf = lf.with_column(lit(0i64).alias(&c));
                    grouped.rdf_node_types.insert(
                        c.clone(),
                        BaseRDFNodeType::literal(xsd::INTEGER.into_owned())
                            .into_default_input_rdf_node_state(),
                    );
                }
            }
            lf = lf.drop(cols([&dummy_col_name, &present_col_name]));
            grouped.mappings = lf;
        }

        for a in aggregate_contexts {
            grouped.rdf_node_types.remove(a.as_str());
        }
        let solution_mappings = if let Some(solution_mappings) = solution_mappings {
            join(
                solution_mappings,
                grouped,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?
        } else {
            grouped
        };
        Ok(solution_mappings)
    }
}
