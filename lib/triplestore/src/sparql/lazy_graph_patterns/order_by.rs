use utils::polars::CollectAuto;
use super::Triplestore;
use crate::sparql::errors::SparqlError;
use tracing::{instrument, trace};

use crate::sparql::QuerySettings;
use polars::prelude::by_name;
use crate::sparql::paging::KeysetSpec;
use oxrdf::vocab::xsd;
use oxrdf::Variable;
use polars::prelude::{col, lit, IntoLazy};
use polars_core::frame::DataFrame;
use query_processing::graph_patterns::{
    keyset_order_by, order_by, ranks_are_global, union, values_pattern_terms, KeysetBound,
    KeysetMarker,
};
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::Expression;
use query_processing::pushdowns::Pushdowns;
use representation::dataset::QueryGraph;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{EagerSolutionMappings, SolutionMappings};
use spargebra::algebra::{GraphPattern, OrderExpression};
use std::collections::HashMap;

const KEYSET_MARKER: &str = "__maplib_keyset_marker";

impl Triplestore {
    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lazy_order_by(
        &self,
        inner: &GraphPattern,
        expression: &[OrderExpression],
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        trace!("Processing order by graph pattern");
        // A page after the first of a paged query, see the paging module.
        let (keyset, expression) = match expression
            .first()
            .and_then(KeysetSpec::from_order_expression)
        {
            Some(spec) => (Some(spec?), &expression[1..]),
            None => (None, expression),
        };
        let mut output_solution_mappings = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::OrderByInner),
            parameters,
            pushdowns,
            query_settings,
            dataset,
        )?;

        // The cursor row: compared as constants when its sort keys can be computed on their own
        // (marker_row), else added to the solutions and ranked with them.
        let mut marker_row = None;
        if let Some(keyset) = &keyset {
            let (variables, marker) = self.keyset_marker(expression, keyset)?;
            match self.align_keyset_marker(&output_solution_mappings, &variables, &marker)? {
                Some(aligned) => marker_row = Some(aligned),
                None => {
                    output_solution_mappings =
                        self.add_keyset_marker_row(output_solution_mappings, marker)?
                }
            }
        }

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
                parameters,
                query_settings,
                dataset,
            )?;
            output_solution_mappings = ordering_solution_mappings;
            inner_contexts.push(inner_context);
            asc_ordering.push(reverse);
        }
        let sort_columns: Vec<_> = inner_contexts
            .iter()
            .map(|x| x.as_str().to_string())
            .collect();
        output_solution_mappings = if let Some(keyset) = &keyset {
            let marker = match marker_row {
                Some(mut row) => {
                    for (i, oe) in expression.iter().enumerate() {
                        let (r, _, _) = self.lazy_order_expression(
                            oe,
                            row,
                            order_expression_contexts.get(i).unwrap(),
                            parameters,
                            query_settings,
                            dataset,
                        )?;
                        row = r;
                    }
                    let df = row
                        .mappings
                        .select(sort_columns.iter().map(col).collect::<Vec<_>>())
                        .collect()
                        .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
                    KeysetMarker::Row(df)
                }
                None => KeysetMarker::Column(KEYSET_MARKER.to_string()),
            };
            keyset_order_by(
                output_solution_mappings,
                &sort_columns,
                asc_ordering,
                self.global_cats.clone(),
                &KeysetBound {
                    marker,
                    // Results that may have duplicate rows also get the rows equal to the
                    // cursor, of which the paging skips those already returned.
                    include_equal: !keyset.strict,
                },
            )?
        } else {
            order_by(
                output_solution_mappings,
                &sort_columns,
                asc_ordering,
                self.global_cats.clone(),
            )?
        };
        output_solution_mappings.mappings =
            output_solution_mappings
                .mappings
                .drop(by_name(sort_columns, true, false));
        Ok(output_solution_mappings)
    }

    /// The row of the cursor of a paged query, where the sort keys (the variables of the ORDER
    /// BY) have the values of the last solution of the previous page, with those variables.
    fn keyset_marker(
        &self,
        expression: &[OrderExpression],
        keyset: &KeysetSpec,
    ) -> Result<(Vec<Variable>, SolutionMappings), SparqlError> {
        let mut all_variables = vec![];
        let mut variables = vec![];
        let mut row = vec![];
        if expression.len() != keyset.keys.len() {
            return Err(SparqlError::PagingError(
                "the cursor does not match the ORDER BY".to_string(),
            ));
        }
        for (oe, k) in expression.iter().zip(&keyset.keys) {
            let (OrderExpression::Asc(Expression::Variable(v))
            | OrderExpression::Desc(Expression::Variable(v))) = oe
            else {
                return Err(SparqlError::PagingError(
                    "paged queries are ordered by variables".to_string(),
                ));
            };
            all_variables.push(v.clone());
            if let Some(k) = k {
                variables.push(v.clone());
                row.push(Some(k.clone()));
            }
        }
        let marker = if variables.is_empty() {
            SolutionMappings::new(DataFrame::empty_with_height(1).lazy(), HashMap::new(), 1)
        } else {
            let sm = values_pattern_terms(&variables, &[row])
                .map_err(|x| SparqlError::ValuesError(x.to_string()))?;
            let cats = self.global_cats.read()?;
            cats.encode_solution_mappings(sm).as_lazy()
        };
        Ok((all_variables, marker))
    }

    /// The cursor row with the column types of the solutions, when its sort keys can be
    /// computed on their own (see [`ranks_are_global`]): every value is in the global
    /// dictionaries, which are in memory, and its type is among those of its column. None
    /// otherwise.
    fn align_keyset_marker(
        &self,
        solution_mappings: &SolutionMappings,
        variables: &[Variable],
        marker: &SolutionMappings,
    ) -> Result<Option<SolutionMappings>, SparqlError> {
        let columns: Vec<String> = variables.iter().map(|v| v.as_str().to_string()).collect();
        if !ranks_are_global(solution_mappings, &columns, &self.global_cats) {
            return Ok(None);
        }
        let mut types = HashMap::new();
        for c in &columns {
            let data_type = solution_mappings.rdf_node_types.get(c).unwrap();
            if let Some(marker_type) = marker.rdf_node_types.get(c) {
                for (t, s) in &marker_type.map {
                    let global = !matches!(
                        s,
                        BaseCatState::CategoricalNative(Some(_)) | BaseCatState::String
                    );
                    if !global || !data_type.map.contains_key(t) {
                        return Ok(None);
                    }
                }
            }
            types.insert(c.clone(), data_type.clone());
        }
        // A union with no rows of the solutions gives the marker row their column layout (the
        // fields of columns with several types).
        let schema = solution_mappings
            .mappings
            .clone()
            .select(columns.iter().map(col).collect::<Vec<_>>())
            .collect_schema()
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        let empty = SolutionMappings::new(
            DataFrame::empty_with_schema(&schema).lazy(),
            types.clone(),
            0,
        );
        let aligned = union(vec![empty, marker.clone()], false, self.global_cats.clone())?;
        for (c, t) in &types {
            match aligned.rdf_node_types.get(c) {
                Some(a) if a.types_equal(t) => {}
                _ => return Ok(None),
            }
        }
        let df = aligned
            .mappings
            .collect_auto()
            .map_err(|e| SparqlError::QueryExecutionError(e.to_string()))?;
        Ok(Some(SolutionMappings::new(df.lazy(), aligned.rdf_node_types, 1)))
    }

    /// Adds the cursor row to the solutions, marked by a column that is true only for it.
    fn add_keyset_marker_row(
        &self,
        solution_mappings: SolutionMappings,
        mut marker_row: SolutionMappings,
    ) -> Result<SolutionMappings, SparqlError> {
        let marker_type = RDFNodeState::from_bases(
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None),
            BaseCatState::NonString,
        );
        marker_row.mappings = marker_row
            .mappings
            .with_column(lit(true).alias(KEYSET_MARKER));
        marker_row
            .rdf_node_types
            .insert(KEYSET_MARKER.to_string(), marker_type.clone());
        let mut solution_mappings = solution_mappings;
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(lit(false).alias(KEYSET_MARKER));
        solution_mappings
            .rdf_node_types
            .insert(KEYSET_MARKER.to_string(), marker_type);
        Ok(union(
            vec![solution_mappings, marker_row],
            false,
            self.global_cats.clone(),
        )?)
    }
}
