use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::multitype::{
    clean_up_after_sort_workaround, helper_cols_sort_workaround_polars_object_series_bug,
};
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use log::debug;
use polars::prelude::{col, Expr};
use spargebra::algebra::{GraphPattern, OrderExpression};

impl Triplestore {
    pub(crate) fn lazy_order_by(
        &self,
        inner: &GraphPattern,
        expression: &Vec<OrderExpression>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing order by graph pattern");
        let mut output_solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::OrderByInner),
        )?;

        let SolutionMappings {
            mappings,
            columns,
            rdf_node_types,
        } = output_solution_mappings;

        let (mappings, original_map) = helper_cols_sort_workaround_polars_object_series_bug(
            mappings,
            &rdf_node_types,
            &expression,
        );

        output_solution_mappings = SolutionMappings {
            mappings,
            columns,
            rdf_node_types,
        };

        let order_expression_contexts: Vec<Context> = (0..expression.len())
            .map(|i| context.extension_with(PathEntry::OrderByExpression(i as u16)))
            .collect();
        let mut asc_ordering = vec![];
        let mut inner_contexts = vec![];
        for i in 0..expression.len() {
            let (ordering_solution_mappings, reverse, inner_context) = self.lazy_order_expression(
                expression.get(i).unwrap(),
                output_solution_mappings,
                order_expression_contexts.get(i).unwrap(),
            )?;
            output_solution_mappings = ordering_solution_mappings;
            inner_contexts.push(inner_context);
            asc_ordering.push(reverse);
        }
        let SolutionMappings {
            mut mappings,
            columns,
            rdf_node_types: datatypes,
        } = output_solution_mappings;

        mappings = mappings.sort_by_exprs(
            inner_contexts
                .iter()
                .map(|c| col(c.as_str()))
                .collect::<Vec<Expr>>(),
            asc_ordering.iter().map(|asc| !asc).collect::<Vec<bool>>(),
            true,
            false,
        );
        mappings = mappings.drop_columns(
            inner_contexts
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<&str>>(),
        );

        mappings = clean_up_after_sort_workaround(mappings, original_map);

        Ok(SolutionMappings::new(mappings, columns, datatypes))
    }
}
