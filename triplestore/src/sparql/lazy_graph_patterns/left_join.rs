use super::Triplestore;
use crate::sparql::errors::SparqlError;
use representation::multitype::{create_join_compatible_solution_mappings, join_workaround};
use crate::sparql::query_context::{Context, PathEntry};
use representation::solution_mapping::{is_string_col, SolutionMappings};
use log::debug;
use polars::export::ahash::HashSet;
use polars::prelude::{col, Expr, JoinArgs, JoinType};
use polars_core::datatypes::DataType;

use spargebra::algebra::{Expression, GraphPattern};

impl Triplestore {
    pub fn lazy_left_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing left join graph pattern");
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let expression_context = context.extension_with(PathEntry::LeftJoinExpression);

        let left_solution_mappings =
            self.lazy_graph_pattern(left, solution_mappings, &left_context)?;

        let mut right_solution_mappings =
            self.lazy_graph_pattern(right, Some(left_solution_mappings.clone()), &right_context)?;

        if let Some(expr) = expression {
            right_solution_mappings =
                self.lazy_expression(expr, right_solution_mappings, &expression_context)?;
            right_solution_mappings.mappings = right_solution_mappings
                .mappings
                .filter(col(expression_context.as_str()))
                .drop_columns([&expression_context.as_str()]);
        }
        let SolutionMappings {
            mappings: right_mappings,
            rdf_node_types: right_datatypes,
        } = right_solution_mappings;

        let SolutionMappings {
            mappings: left_mappings,
            rdf_node_types: left_datatypes,
        } = left_solution_mappings;

        let mut join_on:Vec < _ > =
            {
                let right_column_set: HashSet<_> = right_datatypes.keys().collect();
                let left_column_set: HashSet<_> = left_datatypes.keys().collect();

                left_column_set
                    .intersection(&right_column_set)
                    .map(|x|(*x).clone())
                    .collect()
            };
        join_on.sort();

        let (mut left_mappings, mut left_datatypes, mut right_mappings, right_datatypes) =
            create_join_compatible_solution_mappings(
                left_mappings,
                left_datatypes,
                right_mappings,
                right_datatypes,
                false,
            );

        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();

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
                    right_mappings = right_mappings
                        .with_column(col(&c).cast(DataType::Categorical(None)).alias(&c));
                    left_mappings = left_mappings
                        .with_column(col(&c).cast(DataType::Categorical(None)).alias(&c));
                }
            }
            left_mappings = join_workaround(
                left_mappings,
                &left_datatypes,
                right_mappings,
                &right_datatypes,
                JoinArgs {
                    how: JoinType::Left,
                    validation: Default::default(),
                    suffix: None,
                    slice: None,
                },
            );
        }

        for (k, v) in &right_datatypes {
            if !left_datatypes.contains_key(k) {
                left_datatypes.insert(k.clone(), v.clone());
            }
        }

        let left_solution_mappings = SolutionMappings {
            mappings: left_mappings,
            rdf_node_types: left_datatypes,
        };

        Ok(left_solution_mappings)
    }
}
