use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::Variable;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::JoinType;
use query_processing::graph_patterns::{join, project};
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::GraphPattern;
use crate::sparql::lazy_graph_patterns::lateral::{
    is_lateral_key, lateral_seed, project_seed, with_seed,
};
use std::borrow::Cow;
use std::collections::HashMap;

impl Triplestore {
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_project(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        mut pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing project graph pattern");
        let inner_context = context.extension_with(PathEntry::ProjectInner);
        pushdowns.limit_to_variables(variables);
        pushdowns.add_graph_pattern_pushdowns(inner);
        // Inside the right side of a LATERAL, only the projected substituted variables are
        // substituted in the sub-select, and the key of the substitution is kept.
        let inner_parameters;
        let mut parameters = parameters;
        let mut variables = Cow::Borrowed(variables);
        if let Some(seed) = lateral_seed(parameters) {
            let seed = project_seed(seed, &variables);
            if let Some(seed) = &seed {
                let mut keys: Vec<_> = seed
                    .rdf_node_types
                    .keys()
                    .filter(|c| is_lateral_key(c))
                    .cloned()
                    .collect();
                keys.sort();
                variables
                    .to_mut()
                    .extend(keys.into_iter().map(Variable::new_unchecked));
            }
            inner_parameters = with_seed(parameters, seed);
            parameters = inner_parameters.as_ref();
        }
        let mut project_solution_mappings = self.lazy_graph_pattern(
            inner,
            None,
            &inner_context,
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;
        project_solution_mappings = project(
            project_solution_mappings,
            &variables,
            query_settings.strict_project,
        )?;
        let solution_mappings = if let Some(solution_mappings) = solution_mappings {
            join(
                solution_mappings,
                project_solution_mappings,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?
        } else {
            project_solution_mappings
        };

        Ok(solution_mappings)
    }
}
