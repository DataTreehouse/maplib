use super::{MapOptions, Model};
use crate::errors::MaplibError;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{NamedNode, NamedOrBlankNode};
use polars::datatypes::{AnyValue, DataType, PlSmallStr};
use polars::frame::DataFrame;
use polars::prelude::{
    as_struct, by_name, col, lit, when, Expr, IdxSize, IntoColumn, IntoLazy, LazyFrame,
    LiteralValue, NamedFrom, Series,
};
use representation::dataset::NamedGraph;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, SeriesBuilder, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use shacl::ValidationReport;
use std::collections::HashMap;
use templates::MappingColumnType;
use tracing::debug;
use triplestore::sparql::QuerySettings;
use uuid::Uuid;

const SHACL_RESULT_TEMPLATE: &str =
    "https://datatreehouse.github.io/maplib/vocab#ShaclResultTemplate";
const SHACL_REPORT_TEMPLATE: &str =
    "https://datatreehouse.github.io/maplib/vocab#ShaclReportTemplate";

const SHACL_COUNTS_TEMPLATE: &str =
    "https://datatreehouse.github.io/maplib/vocab#ShaclCountsTemplate";
const SHACL_DOC: &str = r#"
@prefix maplib: <https://datatreehouse.github.io/maplib/vocab#>.
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix sh:    <http://www.w3.org/ns/shacl#> .

maplib:ShaclReportTemplate [
    ?report,
    ?result,
    xsd:boolean ?conforms,
    ] :: {
ottr:Triple(?report, rdf:type, sh:ValidationReport),
ottr:Triple(?report, sh:conforms, ?conforms),
cross | ottr:Triple(?report, sh:result, ++?result),
} .

maplib:ShaclResultTemplate [
    ?result,
    ?source_shape,
    ?focus_node,
    ? ?value,
    ottr:IRI ?source_constraint_component,
    ? ?source_constraint,
    ? ?message,
    ? ottr:IRI ?details,
    ?result_severity,
    ? ?result_path,
    ? ?conforms,
    ? List<ottr:IRI> ?details,
    ] :: {
ottr:Triple(?result, a, sh:ValidationResult),
ottr:Triple(?result, sh:sourceShape, ?source_shape),
ottr:Triple(?result, sh:value, ?value),
ottr:Triple(?result, sh:sourceConstraintComponent, ?source_constraint_component),
ottr:Triple(?result, sh:sourceConstraint, ?source_constraint),
ottr:Triple(?result, sh:focusNode, ?focus_node),
ottr:Triple(?result, sh:resultSeverity, ?result_severity),
ottr:Triple(?result, sh:resultPath, ?result_path),
ottr:Triple(?result, maplib:resultConforms, ?conforms),
cross | ottr:Triple(?result, sh:resultMessage, ++?message),
cross | ottr:Triple(?result, maplib:details, ++?details),
} .

maplib:ShaclCountsTemplate [
    ottr:IRI ?report,
    ottr:IRI ?target_count_iri,
    ?shape,
    xsd:unsignedInt ?target_count,
    ] :: {
ottr:Triple(?report, maplib:targetShapeCount, ?target_count_iri),
ottr:Triple(?target_count_iri, maplib:targetShape, ?shape),
ottr:Triple(?target_count_iri, maplib:targetCount, ?target_count),
} .
"#;

// The nodes of complex paths (blank nodes reached through these predicates from a
// sh:resultPath), whose triples are copied from the shapes graph into the report graph.
const RESULT_PATH_NODES: &str = "(rdf:first|rdf:rest|sh:inversePath|sh:alternativePath|sh:zeroOrMorePath|sh:oneOrMorePath|sh:zeroOrOnePath)*";

/// The pattern, evaluated in `graph` from a query whose source graph is `source`.
fn pattern_in_graph(graph: &NamedGraph, source: &NamedGraph, pattern: &str) -> Option<String> {
    if graph == source {
        Some(pattern.to_string())
    } else if let NamedGraph::NamedGraph(nn) = graph {
        Some(format!("GRAPH {nn} {{ {pattern} }}"))
    } else {
        None
    }
}

/// A query from `source` that copies the structure of the complex sh:resultPath values of the
/// report graph from the shapes graph, or None if one of the graphs cannot be named in it.
fn result_paths_query(
    shapes_graph: &NamedGraph,
    report_graph: &NamedGraph,
    source: &NamedGraph,
) -> Option<String> {
    let report = pattern_in_graph(
        report_graph,
        source,
        "?result sh:resultPath ?path . FILTER(isBlank(?path))",
    )?;
    let shapes = pattern_in_graph(
        shapes_graph,
        source,
        &format!("?path {RESULT_PATH_NODES} ?node . FILTER(isBlank(?node)) ?node ?p ?o ."),
    )?;
    Some(format!(
        "PREFIX sh: <http://www.w3.org/ns/shacl#>\n\
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
         CONSTRUCT {{ ?node ?p ?o }} WHERE {{ {report} {shapes} }}"
    ))
}

impl Model {
    /// A complex sh:resultPath is a blank node of the shapes graph, as in sh:path. The report
    /// graph gets a copy of its triples, so that the path can be read from the report.
    pub(crate) fn copy_result_paths(
        &mut self,
        shapes_graph: &NamedGraph,
        report_graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        if shapes_graph == report_graph {
            return Ok(());
        }
        let Some((query, source)) = result_paths_query(shapes_graph, report_graph, report_graph)
            .map(|q| (q, report_graph))
            .or_else(|| {
                result_paths_query(shapes_graph, report_graph, shapes_graph)
                    .map(|q| (q, shapes_graph))
            })
        else {
            return Ok(());
        };
        let query_settings = QuerySettings {
            include_graphs: vec![],
            max_rows: None,
            strict_project: false,
            federation: None,
            evaluation_hook: None,
        };
        self.triplestore.insert(
            &query,
            None,
            &query_settings,
            source,
            report_graph,
            None,
            false,
        )?;
        Ok(())
    }

    pub fn map_validation_result_to_report_graph(
        &mut self,
        report: &mut ValidationReport,
        report_graph: &NamedGraph,
        conforms_col: bool,
    ) -> Result<(), MaplibError> {
        self.add_templates_from_string(SHACL_DOC)
            .expect("Template should be correct");
        let map_options = MapOptions {
            graph: report_graph.clone(),
            validate_iris: false,
        };
        let mut result_cols = vec![];
        let mut offset = 0;
        let report_uri = create_report_uri();
        if let Some(SolutionMappings {
            mappings,
            rdf_node_types,
            ..
        }) = report.concatenated_results(self.triplestore.global_cats.clone())?
        {
            debug!("Started creating results input");
            let (df, column_types) = create_results_input(
                mappings.clone().lazy(),
                &rdf_node_types,
                offset,
                &report_uri,
                conforms_col,
            );
            debug!("Finished creating results input");
            offset += df.height();
            let result_col = df.column("result").unwrap().clone();
            result_cols.push(result_col);
            self.expand(
                SHACL_RESULT_TEMPLATE,
                Some(df),
                Some(column_types),
                map_options.clone(),
            )?;
        }

        if !report.shape_targets.is_empty() {
            let (targets_df, targets_types) = create_targets_input(report, &report_uri);

            self.expand(
                SHACL_COUNTS_TEMPLATE,
                Some(targets_df),
                Some(targets_types),
                map_options.clone(),
            )?;
        }

        if result_cols.is_empty() {
            result_cols.push(
                Series::new_empty(PlSmallStr::from_str("result"), &DataType::String).into_column(),
            );
        }
        for mut result_col in result_cols {
            let result_ser = result_col.into_materialized_series();
            let report_df = DataFrame::new(
                1,
                vec![
                    Series::from_any_values_and_dtype(
                        PlSmallStr::from_str("report"),
                        &[AnyValue::StringOwned(report_uri.as_str().into())],
                        &DataType::String,
                        true,
                    )
                    .unwrap()
                    .into_column(),
                    Series::from_any_values_and_dtype(
                        PlSmallStr::from_str("result"),
                        &[AnyValue::List(result_ser.clone())],
                        &DataType::List(Box::new(DataType::String)),
                        true,
                    )
                    .unwrap()
                    .into_column(),
                    Series::new(PlSmallStr::from_str("conforms"), [report.conforms]).into_column(),
                ],
            )
            .unwrap();
            let mut report_types = HashMap::new();
            report_types.insert(
                "result".to_string(),
                MappingColumnType::Nested(Box::new(MappingColumnType::Flat(
                    BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                ))),
            );
            report_types.insert(
                "report".to_string(),
                MappingColumnType::Flat(BaseRDFNodeType::IRI.into_default_input_rdf_node_state()),
            );
            report_types.insert(
                "conforms".to_string(),
                MappingColumnType::Flat(
                    BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned(), None)
                        .into_default_input_rdf_node_state(),
                ),
            );
            self.expand(
                SHACL_REPORT_TEMPLATE,
                Some(report_df),
                Some(report_types),
                map_options.clone(),
            )?;
        }
        if let Some(SolutionMappings {
            mappings,
            rdf_node_types,
            ..
        }) = report.concatenated_details(self.triplestore.global_cats.clone())?
        {
            debug!("Started creating details input");
            let (details_df, details_types) = create_results_input(
                mappings.clone().lazy(),
                &rdf_node_types,
                offset,
                &report_uri,
                conforms_col,
            );
            debug!("Finished creating details input");

            self.expand(
                SHACL_RESULT_TEMPLATE,
                Some(details_df),
                Some(details_types),
                map_options,
            )?;
        }
        Ok(())
    }
}

const MESSAGE_COL: &str = "message";

/// The type of the elements of the message column of SHACL validation results:
/// language tagged strings, strings and none, as non-categorical strings.
pub fn shacl_message_element_state() -> RDFNodeState {
    RDFNodeState::default_from_types(vec![
        BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned(), None),
        BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None),
        BaseRDFNodeType::None,
    ])
}

/// Formats the elements of the message column of SHACL validation results:
/// "value"@lang for language tagged strings, and the value for strings.
pub fn format_shacl_message_list(expr: Expr) -> Expr {
    let element = col("");
    let lang = element.clone().struct_().field_by_name(LANG_STRING_LANG_FIELD);
    let lang_value = element.clone().struct_().field_by_name(LANG_STRING_VALUE_FIELD);
    let string_value = element.struct_().field_by_name(
        &BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None).field_col_name(),
    );
    expr.list().eval(
        when(lang.clone().is_not_null())
            .then(lit("\"") + lang_value + lit("\"@") + lang)
            .otherwise(string_value),
    )
}

fn create_results_input(
    mut mappings: LazyFrame,
    types: &HashMap<String, RDFNodeState>,
    offset: usize,
    report_uri: &NamedNode,
    conforms_col: bool,
) -> (DataFrame, HashMap<String, MappingColumnType>) {
    let mut rdf_node_types = types.clone();
    if !conforms_col {
        rdf_node_types.remove("conforms");
        mappings = mappings.drop(by_name(["conforms"], false, false));
    }
    if rdf_node_types.contains_key("id") {
        mappings = mappings
            .with_column(
                (lit(create_id_uri_prefix(report_uri).as_str()) + col("id").cast(DataType::String))
                    .alias("result"),
            )
            .drop(by_name(["id"], true, false));
        rdf_node_types.remove("id").unwrap();
    } else {
        mappings = mappings.with_row_index("result", Some(offset as IdxSize));
        mappings = mappings.with_column(
            (lit(create_tr_uri_prefix(report_uri).as_str()) + col("result").cast(DataType::String))
                .alias("result"),
        );
    }
    rdf_node_types.insert(
        "result".to_string(),
        BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
    );

    if !rdf_node_types.contains_key("result_path") {
        mappings = mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(DataType::String)
                .alias("result_path"),
        );
        rdf_node_types.insert(
            "result_path".to_string(),
            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
        );
    }

    // Only results of SPARQL-based constraints have a sh:sourceConstraint.
    if !rdf_node_types.contains_key("source_constraint") {
        mappings = mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(DataType::String)
                .alias("source_constraint"),
        );
        rdf_node_types.insert(
            "source_constraint".to_string(),
            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
        );
    }

    let mut column_types = HashMap::new();
    for (k, v) in rdf_node_types {
        column_types.insert(k, MappingColumnType::Flat(v));
    }

    // A result has a list of messages, as a shape may have several sh:message values
    if column_types.contains_key(MESSAGE_COL) {
        column_types.insert(
            MESSAGE_COL.to_string(),
            MappingColumnType::Nested(Box::new(MappingColumnType::Flat(
                shacl_message_element_state(),
            ))),
        );
    }

    if column_types.contains_key("details") {
        mappings =
            mappings.with_column(col("details").list().eval(
                lit(create_id_uri_prefix(report_uri).as_str()) + col("").cast(DataType::String),
            ));
    } else {
        mappings = mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(DataType::List(Box::new(DataType::String)))
                .alias("details"),
        )
    }
    column_types.insert(
        "details".to_string(),
        MappingColumnType::Nested(Box::new(MappingColumnType::Flat(
            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
        ))),
    );

    (mappings.collect().unwrap(), column_types)
}

fn create_targets_input(
    report: &ValidationReport,
    report_uri: &NamedNode,
) -> (DataFrame, HashMap<String, MappingColumnType>) {
    let mut shape_blanks_builder = SeriesBuilder::new(&BaseRDFNodeType::BlankNode);
    let mut shape_iris_builder = SeriesBuilder::new(&BaseRDFNodeType::IRI);

    let counts_dt = BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None);
    let mut counts_builder = SeriesBuilder::new(&counts_dt);

    for p in &report.shape_targets {
        match &p.shape_node {
            NamedOrBlankNode::NamedNode(nn) => {
                shape_iris_builder.push_str(nn.as_str());
                shape_blanks_builder.push_none();
            }
            NamedOrBlankNode::BlankNode(bl) => {
                shape_blanks_builder.push_str(bl.as_str());
                shape_iris_builder.push_none();
            }
        }
        counts_builder.push_u32(p.count as u32);
    }
    let mut targets_df = DataFrame::new(
        report.shape_targets.len(),
        vec![
            shape_blanks_builder
                .into_series("blank_shapes")
                .into_column(),
            shape_iris_builder.into_series("iri_shapes").into_column(),
            counts_builder.into_series("target_count").into_column(),
        ],
    )
    .unwrap();
    targets_df = targets_df
        .lazy()
        .group_by([col("blank_shapes"), col("iri_shapes")])
        .agg([col("target_count").sum()])
        .with_column(lit(report_uri.as_str()).alias("report"))
        .with_column(
            as_struct(vec![
                col("blank_shapes").alias(BaseRDFNodeType::BlankNode.field_col_name()),
                col("iri_shapes").alias(BaseRDFNodeType::IRI.field_col_name()),
            ])
            .alias("shape"),
        )
        .select([col("report"), col("shape"), col("target_count")])
        .collect()
        .unwrap();

    let mut target_count_iri_builder = SeriesBuilder::new(&BaseRDFNodeType::IRI);
    for _ in 0..targets_df.height() {
        target_count_iri_builder.push_str(&format!("urn:maplib:tc_{}", Uuid::new_v4()));
    }
    targets_df
        .with_column(
            target_count_iri_builder
                .into_series("target_count_iri")
                .into_column(),
        )
        .unwrap();

    let shapes_dt = RDFNodeState::from_map(HashMap::from_iter([
        (
            BaseRDFNodeType::IRI,
            BaseRDFNodeType::IRI.default_input_cat_state(),
        ),
        (
            BaseRDFNodeType::BlankNode,
            BaseRDFNodeType::BlankNode.default_input_cat_state(),
        ),
    ]));
    let types = HashMap::from_iter([
        (
            "report".to_string(),
            MappingColumnType::Flat(BaseRDFNodeType::IRI.into_default_input_rdf_node_state()),
        ),
        (
            "target_count_iri".to_string(),
            MappingColumnType::Flat(BaseRDFNodeType::IRI.into_default_input_rdf_node_state()),
        ),
        (
            "target_count".to_string(),
            MappingColumnType::Flat(counts_dt.into_default_input_rdf_node_state()),
        ),
        ("shape".to_string(), MappingColumnType::Flat(shapes_dt)),
    ]);

    (targets_df, types)
}

fn create_report_uri() -> NamedNode {
    NamedNode::new_unchecked(format!("urn:maplib:report_{}", Uuid::new_v4()))
}

fn create_id_uri_prefix(report_uri: &NamedNode) -> NamedNode {
    NamedNode::new_unchecked(format!("{}_r_", report_uri.as_str()))
}

fn create_tr_uri_prefix(report_uri: &NamedNode) -> NamedNode {
    NamedNode::new_unchecked(format!("{}_tr_", report_uri.as_str()))
}
