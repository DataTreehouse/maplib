use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use oxrdf::Variable;
use polars::prelude::IntoLazy;
use polars::prelude::JoinType;
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::{join, union, values_pattern};
use query_processing::pushdowns::Pushdowns;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use spargebra::term::GroundTerm;
use std::collections::HashMap;

impl Triplestore {
    pub(crate) fn lazy_values(
        &self,
        solution_mappings: Option<SolutionMappings>,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
        _context: &Context,
        _pushdowns: Pushdowns,
        query_settings: &QuerySettings,
    ) -> Result<SolutionMappings, SparqlError> {
        let Some(solution_mappings) = solution_mappings else {
            // Without anything to join with, UNDEF is simply an unbound value.
            let sm = values_pattern(variables, bindings)
                .map_err(|x| SparqlError::ValuesError(x.to_string()))?;
            let cats = self.global_cats.read()?;
            return Ok(cats.encode_solution_mappings(sm).as_lazy());
        };
        // UNDEF in a row means that the variable is unconstrained by that row.
        // The rows are partitioned by which variables they bind, and each partition is
        // joined only on the variables that it binds.
        let mut partitions: Vec<(Vec<bool>, Vec<Vec<Option<GroundTerm>>>)> = vec![];
        for row in bindings {
            let pattern: Vec<bool> = row.iter().map(|x| x.is_some()).collect();
            let bound_row: Vec<_> = row.iter().filter(|x| x.is_some()).cloned().collect();
            if let Some((_, rows)) = partitions.iter_mut().find(|(p, _)| p == &pattern) {
                rows.push(bound_row);
            } else {
                partitions.push((pattern, vec![bound_row]));
            }
        }
        if partitions.is_empty() {
            partitions.push((vec![true; variables.len()], vec![]));
        }
        let mut sms = vec![];
        for (pattern, rows) in partitions {
            let bound_variables: Vec<Variable> = variables
                .iter()
                .zip(pattern.iter())
                .filter(|(_, b)| **b)
                .map(|(v, _)| v.clone())
                .collect();
            let sm = if bound_variables.is_empty() {
                SolutionMappings::new(
                    DataFrame::empty_with_height(rows.len()).lazy(),
                    HashMap::new(),
                    rows.len(),
                )
            } else {
                let sm = values_pattern(&bound_variables, &rows)
                    .map_err(|x| SparqlError::ValuesError(x.to_string()))?;
                // No disk based storage of local cats if they are created
                let cats = self.global_cats.read()?;
                cats.encode_solution_mappings(sm).as_lazy()
            };
            sms.push(join(
                solution_mappings.clone(),
                sm,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?);
        }
        Ok(union(sms, false, self.global_cats.clone())?)
    }
}
