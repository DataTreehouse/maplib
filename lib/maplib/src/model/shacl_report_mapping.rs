use super::{MapOptions, Model};
use crate::errors::MaplibError;
use oxrdf::vocab::xsd;
use polars::datatypes::{AnyValue, DataType, PlSmallStr};
use polars::frame::DataFrame;
use polars::prelude::{
    by_name, col, lit, IdxSize, IntoColumn, IntoLazy, LazyFrame, LiteralValue, NamedFrom, Series,
};
use representation::dataset::NamedGraph;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use shacl::ValidationReport;
use std::collections::HashMap;
use templates::MappingColumnType;
use tracing::debug;
use uuid::Uuid;

const SHACL_RESULT_TEMPLATE: &str = "https://github.com/DataTreehouse/maplib#ShaclResultTemplate";
const SHACL_REPORT_TEMPLATE: &str = "https://github.com/DataTreehouse/maplib#ShaclReportTemplate";
const SHACL_DOC: &str = r#"
@prefix maplib: <https://github.com/DataTreehouse/maplib#>.
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
ottr:Triple(?result, sh:focusNode, ?focus_node),
ottr:Triple(?result, sh:resultMessage, ?message),
ottr:Triple(?result, sh:resultSeverity, ?result_severity),
ottr:Triple(?result, sh:resultPath, ?result_path),
ottr:Triple(?result, maplib:resultConforms, ?conforms),
cross | ottr:Triple(?result, maplib:details, ++?details),
} .
"#;

impl Model {
    pub fn map_validation_result_to_report_graph(
        &mut self,
        report: &mut ValidationReport,
        report_graph: &NamedGraph,
        include_details: bool,
    ) -> Result<(), MaplibError> {
        self.add_templates_from_string(SHACL_DOC)
            .expect("Template should be correct");
        let map_options = MapOptions {
            graph: report_graph.clone(),
            validate_iris: false,
        };
        let mut result_cols = vec![];
        let mut offset = 0;
        let uuid = Uuid::new_v4().to_string();
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
                &uuid,
                include_details,
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
                        &[AnyValue::StringOwned("urn:maplib:report".into())],
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
                    BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
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
                &uuid,
                include_details,
            );
            debug!("Finished creating details input");

            self.expand(
                SHACL_RESULT_TEMPLATE,
                Some(details_df),
                Some(details_types),
                map_options,
            )?;
        }
        report.results = None;
        Ok(())
    }
}

fn create_results_input(
    mut mappings: LazyFrame,
    types: &HashMap<String, RDFNodeState>,
    offset: usize,
    uuid: &str,
    include_details: bool,
) -> (DataFrame, HashMap<String, MappingColumnType>) {
    let mut rdf_node_types = types.clone();
    if !include_details {
        rdf_node_types.remove("conforms");
        mappings = mappings.drop(by_name(["conforms"], false, false));
    }
    if rdf_node_types.contains_key("id") {
        mappings = mappings
            .with_column(
                (lit(format!("urn:maplib:r{}_", uuid)) + col("id").cast(DataType::String))
                    .alias("result"),
            )
            .drop(by_name(["id"], true, false));
        rdf_node_types.remove("id").unwrap();
    } else {
        mappings = mappings.with_row_index("result", Some(offset as IdxSize));
        mappings = mappings.with_column(
            (lit(format!("urn:maplib:tr{}_", uuid)) + col("result").cast(DataType::String))
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

    let mut column_types = HashMap::new();
    for (k, v) in rdf_node_types {
        column_types.insert(k, MappingColumnType::Flat(v));
    }

    if column_types.contains_key("details") {
        mappings = mappings.with_column(
            col("details")
                .list()
                .eval(lit(format!("urn:maplib:r{}_", uuid)) + col("").cast(DataType::String)),
        );
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
