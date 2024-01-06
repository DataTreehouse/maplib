use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::ordering::{decide_order, Order};
use crate::sparql::multitype::{create_join_compatible_solution_mappings, join_workaround};
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::{is_string_col, SolutionMappings};
use log::debug;
use polars::prelude::JoinArgs;
use polars::prelude::{col, Expr, JoinType};
use polars_core::datatypes::DataType;

use spargebra::algebra::GraphPattern;

impl Triplestore {
    pub fn lazy_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let (left_solution_mappings, right_solution_mappings) = match decide_order(left, right) {
            Order::Left => {
                let left_solution_mappings =
                    self.lazy_graph_pattern(left, solution_mappings, &left_context)?;
                let right_solution_mappings = self.lazy_graph_pattern(
                    right,
                    Some(left_solution_mappings.clone()),
                    &right_context,
                )?;
                (left_solution_mappings, right_solution_mappings)
            }
            Order::Right => {
                let right_solution_mappings =
                    self.lazy_graph_pattern(right, solution_mappings, &right_context)?;
                let left_solution_mappings = self.lazy_graph_pattern(
                    left,
                    Some(right_solution_mappings.clone()),
                    &left_context,
                )?;
                (left_solution_mappings, right_solution_mappings)
            }
        };

        let SolutionMappings {
            mappings: right_mappings,
            columns: mut right_columns,
            rdf_node_types: right_datatypes,
        } = right_solution_mappings;

        let mut join_on: Vec<_> = left_solution_mappings
            .columns
            .intersection(&right_columns)
            .cloned()
            .collect();
        join_on.sort();

        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();

        let SolutionMappings {
            mappings: left_mappings,
            columns: left_columns,
            rdf_node_types: left_datatypes,
        } = left_solution_mappings;

        let (mut left_mappings, mut left_datatypes, mut right_mappings, right_datatypes) =
            create_join_compatible_solution_mappings(
                left_mappings,
                left_datatypes,
                right_mappings,
                right_datatypes,
                true,
            );

        if join_on.is_empty() {
            left_mappings = left_mappings.join(
                right_mappings,
                join_on_cols.as_slice(),
                join_on_cols.as_slice(),
                JoinArgs::new(JoinType::Cross),
            )
        } else {
            for c in join_on {
                let dt = right_datatypes.get(&c).unwrap();
                if is_string_col(dt) {
                    right_mappings =
                        right_mappings.with_column(col(&c).cast(DataType::Categorical(None)));
                    left_mappings =
                        left_mappings.with_column(col(&c).cast(DataType::Categorical(None)));
                }
            }

            left_mappings = join_workaround(
                left_mappings,
                &left_datatypes,
                right_mappings,
                &right_datatypes,
                JoinArgs::new(JoinType::Inner),
            );
        }

        for (k, v) in &right_datatypes {
            if !left_datatypes.contains_key(k) {
                left_datatypes.insert(k.clone(), v.clone());
            }
        }

        let mut left_solution_mappings = SolutionMappings {
            mappings: left_mappings,
            columns: left_columns,
            rdf_node_types: left_datatypes,
        };

        for c in right_columns.drain() {
            left_solution_mappings.columns.insert(c);
        }

        Ok(left_solution_mappings)
    }
}
