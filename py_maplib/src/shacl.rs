use crate::{fix_cats_and_multicolumns, Mapping};
use maplib::mapping::{
    ExpandOptions as RustExpandOptions, Mapping as RustMapping, MappingColumnType,
};
use oxrdf::vocab::xsd;
use polars::datatypes::{AnyValue, DataType};
use polars::frame::DataFrame;
use polars::prelude::{
    col, lit, IntoLazy, ListNameSpaceExtension, LiteralValue, NamedFrom, Series,
};
use pydf_io::to_python::df_to_py_df;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use shacl::ValidationReport as RustValidationReport;
use std::collections::HashMap;

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
    ?value,
    ottr:IRI ?source_constraint_component,
    ?message,
    ? ottr:IRI ?details,
    ?result_severity,
    ?result_path,
    ?details,
    ] :: {
ottr:Triple(?result, a, sh:ValidationResult),
ottr:Triple(?result, sh:sourceShape, ?source_shape),
ottr:Triple(?result, sh:value, ?value),
ottr:Triple(?result, sh:sourceConstraintComponent, ?source_constraint_component),
ottr:Triple(?result, sh:focusNode, ?focus_node),
ottr:Triple(?result, sh:message, ?message),
ottr:Triple(?result, sh:resultSeverity, ?result_severity),
ottr:Triple(?result, sh:resultPath, ?result_path),
cross | ottr:Triple(?result, maplib:details, ++?details),
} .
"#;

#[derive(Debug, Clone)]
#[pyclass]
pub struct ValidationReport {
    inner: RustValidationReport,
}

impl ValidationReport {
    pub fn new(inner: RustValidationReport) -> ValidationReport {
        ValidationReport { inner }
    }
}

#[pymethods]
impl ValidationReport {
    #[getter]
    pub fn conforms(&self) -> bool {
        self.inner.conforms
    }

    pub fn results(
        &self,
        multi_as_strings: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let report = if let Some(mut df) = self.inner.df.clone() {
            (df, _) = fix_cats_and_multicolumns(
                df,
                self.inner.rdf_node_types.as_ref().unwrap().clone(),
                multi_as_strings.unwrap_or(true),
            );
            Some(df_to_py_df(df, HashMap::new(), py)?)
        } else {
            None
        };
        Ok(report)
    }

    pub fn details(
        &self,
        multi_as_strings: Option<bool>,
        py: Python<'_>,
    ) -> PyResult<Option<PyObject>> {
        let details = if let Some(EagerSolutionMappings {
            mut mappings,
            rdf_node_types,
        }) = self.inner.details.clone()
        {
            (mappings, _) = fix_cats_and_multicolumns(
                mappings,
                rdf_node_types,
                multi_as_strings.unwrap_or(true),
            );
            Some(df_to_py_df(mappings, HashMap::new(), py)?)
        } else {
            None
        };
        Ok(details)
    }

    pub fn graph(&self, include_details: Option<bool>) -> PyResult<Option<Mapping>> {
        let mut new_mapping = RustMapping::from_str(SHACL_DOC, None).unwrap();
        if let Some(df) = &self.inner.df {
            let (df, column_types) =
                create_results_input(df, self.inner.rdf_node_types.as_ref().unwrap());
            let result_ser = df.column("result").unwrap().clone();

            let report_df = DataFrame::new(vec![
                Series::from_any_values_and_dtype(
                    "report",
                    &[AnyValue::StringOwned("report".into())],
                    &DataType::String,
                    true,
                )
                .unwrap(),
                Series::from_any_values_and_dtype(
                    "result",
                    &[AnyValue::List(result_ser)],
                    &DataType::List(Box::new(DataType::String)),
                    true,
                )
                .unwrap(),
                Series::new("conforms", [self.inner.conforms]),
            ])
            .unwrap();
            let mut report_types = HashMap::new();
            report_types.insert(
                "result".to_string(),
                MappingColumnType::Nested(Box::new(MappingColumnType::Flat(
                    RDFNodeType::BlankNode,
                ))),
            );
            report_types.insert(
                "report".to_string(),
                MappingColumnType::Flat(RDFNodeType::BlankNode),
            );
            report_types.insert(
                "conforms".to_string(),
                MappingColumnType::Flat(RDFNodeType::Literal(xsd::BOOLEAN.into_owned())),
            );

            new_mapping
                .expand(
                    SHACL_RESULT_TEMPLATE,
                    Some(df),
                    Some(column_types),
                    RustExpandOptions {
                        unique_subsets: Some(vec![vec!["result".to_string()]]),
                    },
                )
                .unwrap();
            new_mapping
                .expand(
                    SHACL_REPORT_TEMPLATE,
                    Some(report_df),
                    Some(report_types),
                    RustExpandOptions {
                        unique_subsets: Some(vec![
                            vec!["report".to_string()],
                            vec!["result".to_string()],
                        ]),
                    },
                )
                .unwrap();
            if include_details.unwrap_or(false) {
                if let Some(EagerSolutionMappings {
                    mappings: details_df,
                    rdf_node_types: details_types,
                }) = &self.inner.details
                {
                    let (details_df, details_types) =
                        create_results_input(details_df, details_types);
                    new_mapping
                        .expand(
                            SHACL_RESULT_TEMPLATE,
                            Some(details_df),
                            Some(details_types),
                            RustExpandOptions {
                                unique_subsets: Some(vec![vec!["result".to_string()]]),
                            },
                        )
                        .unwrap();
                }
            }
            Ok(Some(Mapping::from_inner_mapping(new_mapping)))
        } else {
            Ok(None)
        }
    }
}

fn create_results_input(
    df: &DataFrame,
    types: &HashMap<String, RDFNodeType>,
) -> (DataFrame, HashMap<String, MappingColumnType>) {
    let mut lf = df.clone().lazy();
    let mut rdf_node_types = types.clone();

    if rdf_node_types.contains_key("id") {
        lf = lf
            .with_column((lit("r_") + col("id").cast(DataType::String)).alias("result"))
            .drop(["id"]);
        rdf_node_types.remove("id").unwrap();
    } else {
        lf = lf.with_row_index("result", None);
        lf = lf.with_column((lit("tr_") + col("result").cast(DataType::String)).alias("result"));
    }
    rdf_node_types.insert("result".to_string(), RDFNodeType::BlankNode);

    if !rdf_node_types.contains_key("result_path") {
        lf = lf.with_column(
            lit(LiteralValue::Null)
                .cast(DataType::String)
                .alias("result_path"),
        );
        rdf_node_types.insert("result_path".to_string(), RDFNodeType::IRI);
    }

    let mut column_types = HashMap::new();
    for (k, v) in rdf_node_types {
        column_types.insert(k, MappingColumnType::Flat(v));
    }

    if column_types.contains_key("details") {
        lf = lf.with_column(
            col("details")
                .list()
                .eval(lit("r_") + col("").cast(DataType::String), true),
        );
    } else {
        lf = lf.with_column(
            lit(LiteralValue::Null)
                .cast(DataType::List(Box::new(DataType::String)))
                .alias("details"),
        )
    }
    column_types.insert(
        "details".to_string(),
        MappingColumnType::Nested(Box::new(MappingColumnType::Flat(RDFNodeType::BlankNode))),
    );

    (lf.collect().unwrap(), column_types)
}
