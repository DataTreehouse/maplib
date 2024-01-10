use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::multitype::create_compatible_solution_mappings;
use crate::sparql::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use log::debug;
use polars::prelude::{concat_lf_diagonal, UnionArgs};
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
            rdf_node_types: left_datatypes,
        } = self.lazy_graph_pattern(left, solution_mappings.clone(), &left_context)?;

        let SolutionMappings {
            mappings: right_mappings,
            rdf_node_types: right_datatypes,
        } = self.lazy_graph_pattern(right, solution_mappings, &right_context)?;

        let (left_mappings, mut left_datatypes, right_mappings, mut right_datatypes) =
            create_compatible_solution_mappings(
                left_mappings,
                left_datatypes,
                right_mappings,
                right_datatypes,
            );
        for (k, v) in right_datatypes.drain() {
            left_datatypes.entry(k).or_insert(v);
        }

        let output_mappings =
            concat_lf_diagonal(vec![left_mappings, right_mappings], UnionArgs::default())
                .expect("Concat problem");
        Ok(SolutionMappings::new(
            output_mappings,
            left_datatypes,
        ))
    }
}
