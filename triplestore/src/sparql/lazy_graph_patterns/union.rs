use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::debug;
use polars::prelude::diag_concat_lf;
use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub(crate) fn lazy_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing union graph pattern");
        let left_context = context.extension_with(PathEntry::UnionLeftSide);
        let right_context = context.extension_with(PathEntry::UnionRightSide);

        let SolutionMappings {
            mappings: left_mappings,
            columns: mut left_columns,
            rdf_node_types: mut left_datatypes,
        } = self.lazy_graph_pattern(&left, solution_mappings.clone(), &left_context)?;

        let SolutionMappings {
            mappings: right_mappings,
            columns: right_columns,
            rdf_node_types: mut right_datatypes,
        } = self.lazy_graph_pattern(right, solution_mappings, &right_context)?;

        let output_mappings = diag_concat_lf(vec![left_mappings, right_mappings], true, true)
            .expect("Concat problem");
        left_columns.extend(right_columns);
        for (v, dt) in right_datatypes.drain() {
            if let Some(left_dt) = left_datatypes.get(&v) {
                assert_eq!(&dt, left_dt);
            } else {
                left_datatypes.insert(v, dt);
            }
        }
        Ok(SolutionMappings::new(
            output_mappings,
            left_columns,
            left_datatypes,
        ))
    }
}
