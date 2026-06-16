use crate::error::PyMaplibError;
use crate::py_model::PyModel;
use crate::shacl::PyValidationReport;
use crate::{
    map_types, parse_named_node, parse_optional_named_node, resolve_format, resolve_normal_format,
    NamedGraph, PyIndexingOptions, PyTemplate, StringOrPathBuf, TemplateType,
    VirtualizedPythonDatabase,
};
use crate::{
    DEFAULT_DEBUG_NO_RESULTS, DEFAULT_INCLUDE_TRANSIENT, DEFAULT_MAP_TO_TRANSIENT,
    DEFAULT_STREAMING, DEFAULT_TRIPLES_BATCH_SIZE,
};

use chrono::Utc;
use cimxml_export::export::FullModelDetails;
use datalog::inference::InferenceResult;
use maplib::errors::MaplibError;
use maplib::model::{MapOptions, Model as InnerModel};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::frame::DataFrame;
use pyo3::prelude::*;
use representation::df_to_python::fix_cats_and_multicolumns;
use representation::polars_to_rdf::XSD_DATETIME_WITH_TZ_FORMAT;
use representation::python::PyIRI;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeState;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use tracing::{info, warn};
use triplestore::sparql::{InsertResult, QueryResult, UpdateResult};
use triplestore::triples_read::ExtendedRdfFormat;
use triplestore::IndexingOptions;

pub(crate) fn add_template_mutex(
    inner: &mut MutexGuard<InnerModel>,
    template: TemplateType,
) -> PyResult<()> {
    match template {
        TemplateType::TemplateString(s) => {
            inner
                .add_templates_from_string(&s)
                .map_err(PyMaplibError::from)?;
        }
        TemplateType::TemplatePyTemplate(t) => {
            inner
                .add_template(t.into_inner())
                .map_err(PyMaplibError::from)?;
        }
        TemplateType::TemplateIRI(_) => {
            return Err(PyMaplibError::FunctionArgumentError(
                "Adding IRIs templates is not supported".to_string(),
            )
            .into())
        }
    }
    Ok(())
}

pub(crate) fn size_mutex(
    inner: &mut MutexGuard<InnerModel>,
    graph: Option<String>,
) -> PyResult<usize> {
    let graph = parse_optional_named_node(graph)?;
    let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    Ok(inner.graph_size(&graph))
}

pub(crate) fn add_virtualization_mutex(
    inner: &mut MutexGuard<InnerModel>,
    virtualized_database: VirtualizedPythonDatabase,
    resources: HashMap<String, PyTemplate>,
) -> PyResult<()> {
    let mut mapped_resources = HashMap::new();
    for (r, t) in resources {
        mapped_resources.insert(r, t.template);
    }
    inner
        .add_virtualization(virtualized_database, mapped_resources)
        .map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn add_prefixes_mutex(
    inner: &mut MutexGuard<InnerModel>,
    prefixes: HashMap<String, NamedNode>,
) -> PyResult<()> {
    inner.prefixes.extend(prefixes.into_iter());
    Ok(())
}

pub(crate) fn truncate_graph_mutex(
    inner: &mut MutexGuard<InnerModel>,
    graph: &NamedGraph,
) -> PyResult<()> {
    inner.triplestore.truncate(graph);
    Ok(())
}

pub(crate) fn detach_graph_mutex(
    model: &mut MutexGuard<InnerModel>,
    graph: &NamedGraph,
    preserve_name: bool,
) -> PyResult<PyModel> {
    let sprout = model
        .detach_graph(graph, preserve_name)
        .map_err(PyMaplibError::from)?;
    let m = PyModel {
        inner: Mutex::new(sprout),
    };
    Ok(m)
}

pub(crate) fn map_mutex(
    inner: &mut MutexGuard<InnerModel>,
    template: TemplateType,
    df: Option<DataFrame>,
    graph: Option<String>,
    types: Option<HashMap<String, RDFNodeState>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<Py<PyAny>>> {
    let template = match template {
        TemplateType::TemplateIRI(i) => i.into_inner().to_string(),
        TemplateType::TemplateString(s) => {
            if let Ok(nn) = NamedNode::new(&s) {
                nn.as_str().to_string()
            } else if s.len() < 200 {
                s
            } else {
                let s = inner
                    .add_templates_from_string(&s)
                    .map_err(PyMaplibError::from)?;
                let s = if let Some(s) = s {
                    s
                } else {
                    return Err(PyMaplibError::FunctionArgumentError(
                        "Template stOTTR document contained no templates".to_string(),
                    )
                    .into());
                };
                s.as_str().to_string()
            }
        }
        TemplateType::TemplatePyTemplate(t) => {
            let t_string = t.template.signature.iri.as_str().to_string();
            inner
                .add_template(t.into_inner())
                .map_err(PyMaplibError::from)?;
            t_string
        }
    };

    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let options = MapOptions::from_args(named_graph, validate_iris);
    let types = map_types(types);

    if let Some(df) = df {
        if df.height().gt(&0) {
            let _report = inner
                .expand(&template, Some(df), types, options)
                .map_err(PyMaplibError::from)?;
        } else {
            warn!("Template expansion of {template} with empty DataFrame");
        }
    } else {
        let _report = inner
            .expand(&template, None, None, options)
            .map_err(PyMaplibError::from)?;
    }

    Ok(None)
}

pub(crate) fn map_json_mutex(
    inner: &mut MutexGuard<InnerModel>,
    string_or_path: StringOrPathBuf,
    graph: Option<String>,
    transient: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());

    match string_or_path {
        StringOrPathBuf::String(string) => {
            let is_json_string = string.is_empty() || string.contains("{") || string.contains("[");

            if is_json_string {
                inner
                    .map_json_string(
                        string,
                        &named_graph,
                        transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                    )
                    .map_err(PyMaplibError::from)?;
            } else {
                let p = PathBuf::from(string.as_str());
                inner
                    .map_json_path(
                        p.as_ref(),
                        &named_graph,
                        transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                    )
                    .map_err(PyMaplibError::from)?;
            }
        }
        StringOrPathBuf::PathBuf(path) => {
            inner
                .map_json_path(
                    path.as_ref(),
                    &named_graph,
                    transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                )
                .map_err(PyMaplibError::from)?;
        }
    }
    Ok(())
}

pub(crate) fn map_xml_mutex(
    inner: &mut MutexGuard<InnerModel>,
    string_or_path: StringOrPathBuf,
    graph: Option<String>,
    transient: Option<bool>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());

    match string_or_path {
        StringOrPathBuf::String(string) => {
            let is_xml_string = string.trim_start().starts_with('<');

            if is_xml_string {
                inner
                    .map_xml_string(
                        string,
                        &named_graph,
                        transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                    )
                    .map_err(PyMaplibError::from)?;
            } else {
                let p = PathBuf::from(string);
                inner
                    .map_xml_path(
                        p.as_ref(),
                        &named_graph,
                        transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                    )
                    .map_err(PyMaplibError::from)?;
            }
        }
        StringOrPathBuf::PathBuf(path) => {
            inner
                .map_xml_path(
                    path.as_ref(),
                    &named_graph,
                    transient.unwrap_or(DEFAULT_MAP_TO_TRANSIENT),
                )
                .map_err(PyMaplibError::from)?;
        }
    }

    Ok(())
}

pub(crate) fn map_triples_mutex(
    inner: &mut MutexGuard<InnerModel>,
    df: DataFrame,
    predicate: Option<String>,
    graph: Option<String>,
    types: Option<HashMap<String, RDFNodeState>>,
    validate_iris: Option<bool>,
) -> PyResult<Option<Py<PyAny>>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let options = MapOptions::from_args(named_graph, validate_iris);
    let types = map_types(types);
    let predicate = if let Some(predicate) = predicate {
        Some(NamedNode::new(predicate).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
    } else {
        None
    };
    inner
        .expand_triples(df, types, predicate, options)
        .map_err(PyMaplibError::from)?;
    Ok(None)
}

pub(crate) fn map_default_mutex(
    inner: &mut MutexGuard<InnerModel>,
    df: DataFrame,
    primary_key_column: String,
    dry_run: Option<bool>,
    graph: NamedGraph,
    types: Option<HashMap<String, RDFNodeState>>,
    validate_iris: Option<bool>,
) -> PyResult<String> {
    let options = MapOptions::from_args(graph, validate_iris);
    let dry_run = dry_run.unwrap_or(false);
    let types = map_types(types);
    let tmpl = inner
        .expand_default(df, primary_key_column, vec![], dry_run, types, options)
        .map_err(PyMaplibError::from)?;
    if dry_run {
        info!("Produced template:\n\n {tmpl}");
    }
    Ok(format!("{tmpl}"))
}

pub fn map_df_mutex(
    inner: &mut MutexGuard<InnerModel>,
    df: DataFrame,
    graph: NamedGraph,
) -> PyResult<()> {
    inner.map_df(&df, &graph).map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn query_mutex(
    inner: &mut MutexGuard<InnerModel>,
    query: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    graph: Option<NamedGraph>,
    streaming: Option<bool>,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<QueryResult> {
    let res = inner
        .query(
            &query,
            mapped_parameters.as_ref(),
            graph.as_ref(),
            streaming.unwrap_or(DEFAULT_STREAMING),
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;
    Ok(res)
}

pub(crate) fn update_mutex(
    inner: &mut MutexGuard<InnerModel>,
    update: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    graph: Option<String>,
    streaming: Option<bool>,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<UpdateResult> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = if let Some(graph) = graph {
        Some(NamedGraph::NamedGraph(graph))
    } else {
        None
    };
    let res = inner
        .update(
            &update,
            mapped_parameters.as_ref(),
            named_graph.as_ref(),
            streaming.unwrap_or(DEFAULT_STREAMING),
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;
    Ok(res)
}

pub(crate) fn create_index_mutex(
    inner: &mut MutexGuard<InnerModel>,
    options: Option<PyIndexingOptions>,
    all: Option<bool>,
    graph: Option<String>,
) -> PyResult<()> {
    let named_graph = if all.unwrap_or(true) {
        None
    } else {
        let graph = parse_optional_named_node(graph)?;
        let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
        Some(named_graph)
    };
    let options = if let Some(options) = options {
        options.inner
    } else {
        IndexingOptions::default()
    };
    inner
        .create_index(options, named_graph.as_ref())
        .map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn validate_mutex(
    inner: &mut MutexGuard<InnerModel>,
    shape_graph: Option<String>,
    data_graph: Option<String>,
    report_graph: Option<String>,
    inferences_graph: Option<String>,
    include_details: Option<bool>,
    include_conforms: Option<bool>,
    streaming: Option<bool>,
    max_shape_constraint_results: Option<usize>,
    only_shapes: Option<Vec<String>>,
    deactivate_shapes: Option<Vec<String>>,
    dry_run: Option<bool>,
    max_rows: Option<usize>,
    serial: Option<bool>,
    max_iterations: Option<usize>,
    debug_rules: Option<bool>,
) -> PyResult<PyValidationReport> {
    let data_graph = parse_optional_named_node(data_graph)?;
    let data_graph = NamedGraph::from_maybe_named_node(data_graph.as_ref());
    let shape_graph = parse_optional_named_node(shape_graph)?;
    let shape_graph = NamedGraph::from_maybe_named_node(shape_graph.as_ref());
    let report_graph = parse_optional_named_node(report_graph)?;
    let report_graph = if let Some(report_graph) = report_graph {
        Some(NamedGraph::NamedGraph(report_graph))
    } else {
        None
    };

    let inferences_graph = parse_optional_named_node(inferences_graph)?;
    let inferences_graph = if let Some(inferences_graph) = inferences_graph {
        Some(NamedGraph::NamedGraph(inferences_graph))
    } else {
        None
    };
    if only_shapes.is_some() && deactivate_shapes.is_some() {
        return Err(PyMaplibError::FunctionArgumentError(
            "only_shapes and deactivate_shapes cannot both be set".to_string(),
        )
        .into());
    }
    let only_shapes = if let Some(only_shapes) = only_shapes {
        let only_shapes: Result<Vec<_>, _> = only_shapes
            .into_iter()
            .map(|x| NamedNode::new(x).map_err(|x| PyMaplibError::from(MaplibError::from(x))))
            .collect();
        Some(only_shapes?)
    } else {
        None
    };

    let deactivate_shapes = if let Some(deactivate_shapes) = deactivate_shapes {
        let deactivate_shapes: Result<Vec<_>, _> = deactivate_shapes
            .into_iter()
            .map(|x| NamedNode::new(x).map_err(|x| PyMaplibError::from(MaplibError::from(x))))
            .collect();
        deactivate_shapes?
    } else {
        vec![]
    };

    let report = inner
        .validate(
            &data_graph,
            &shape_graph,
            report_graph.as_ref(),
            inferences_graph.as_ref(),
            include_details.unwrap_or(false),
            include_conforms.unwrap_or(false),
            streaming.unwrap_or(false),
            max_shape_constraint_results,
            false, //TODO: Needs more work before can be exposed
            max_rows,
            only_shapes,
            deactivate_shapes,
            dry_run.unwrap_or(false),
            serial.unwrap_or(false),
            max_iterations,
            debug_rules.unwrap_or(false),
        )
        .map_err(PyMaplibError::from)?;
    inner.latest_report_graph = report_graph.clone();
    let py_report =
        PyValidationReport::new(report, inner.triplestore.global_cats.clone(), report_graph);
    Ok(py_report)
}

pub(crate) fn insert_mutex(
    inner: &mut MutexGuard<InnerModel>,
    query: String,
    mapped_parameters: Option<HashMap<String, EagerSolutionMappings>>,
    transient: Option<bool>,
    streaming: Option<bool>,
    source_graph: NamedGraph,
    target_graph: NamedGraph,
    include_transient: Option<bool>,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> PyResult<InsertResult> {
    let insert_result = inner
        .insert(
            &query,
            mapped_parameters.as_ref(),
            &source_graph,
            include_transient.unwrap_or(DEFAULT_INCLUDE_TRANSIENT),
            &target_graph,
            transient.unwrap_or(false),
            streaming.unwrap_or(DEFAULT_STREAMING),
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::from)?;

    Ok(insert_result)
}

pub(crate) fn read_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    format: Option<String>,
    base_iri: Option<String>,
    transient: Option<bool>,
    parallel: Option<bool>,
    checked: Option<bool>,
    graph: Option<String>,
    replace_graph: Option<bool>,
    triples_batch_size: Option<usize>,
    known_contexts: HashMap<String, String>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let path = Path::new(&file_path);
    let format = if let Some(format) = format {
        Some(resolve_format(&format).map_err(PyMaplibError::from)?)
    } else {
        None
    };
    inner
        .read_triples(
            path,
            format,
            base_iri,
            transient.unwrap_or(false),
            parallel,
            checked.unwrap_or(true),
            &named_graph,
            replace_graph.unwrap_or(false),
            triples_batch_size.unwrap_or(DEFAULT_TRIPLES_BATCH_SIZE),
            known_contexts,
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn read_template_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
) -> PyResult<()> {
    let path = Path::new(&file_path);
    inner.read_template(path).map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn reads_mutex(
    inner: &mut MutexGuard<InnerModel>,
    s: &str,
    format: &str,
    base_iri: Option<String>,
    transient: Option<bool>,
    parallel: Option<bool>,
    checked: Option<bool>,
    graph: Option<String>,
    replace_graph: Option<bool>,
    triples_batch_size: Option<usize>,
    known_contexts: HashMap<String, String>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let format = resolve_format(format).map_err(PyMaplibError::from)?;
    if format == ExtendedRdfFormat::HDT {
        return Err(PyMaplibError::FunctionArgumentError(
            "HDT is a binary format, use read() instead of reads()".to_string(),
        )
        .into());
    }
    inner
        .reads(
            s,
            format,
            base_iri,
            transient.unwrap_or(false),
            parallel,
            checked.unwrap_or(true),
            &named_graph,
            replace_graph.unwrap_or(false),
            triples_batch_size.unwrap_or(DEFAULT_TRIPLES_BATCH_SIZE),
            known_contexts,
        )
        .map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn write_triples_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    format: Option<String>,
    graph: Option<String>,
    prefixes: Option<HashMap<String, NamedNode>>,
) -> PyResult<()> {
    let format = if let Some(format) = format {
        resolve_format(&format).map_err(PyMaplibError::from)?
    } else {
        ExtendedRdfFormat::Normal(RdfFormat::NTriples)
    };
    if format == ExtendedRdfFormat::CIMXML {
        return Err(PyMaplibError::FunctionArgumentError(
            "Use write_cim_xml to write CIM XML".to_string(),
        )
        .into());
    }
    let path_buf = PathBuf::from(file_path);
    let mut actual_file = File::create(path_buf.as_path())
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    match format {
        ExtendedRdfFormat::Normal(format) => {
            inner
                .write_triples(&mut actual_file, &named_graph, format, prefixes.as_ref())
                .unwrap();
        }
        ExtendedRdfFormat::HDT => {
            inner
                .write_hdt(&mut actual_file, &named_graph)
                .map_err(PyMaplibError::from)?;
        }
        ExtendedRdfFormat::CIMXML => unreachable!("rejected above"),
    }
    Ok(())
}

pub(crate) fn write_cim_xml_mutex(
    inner: &mut MutexGuard<InnerModel>,
    file_path: String,
    profile_graph: String,
    model_iri: Option<String>,
    version: Option<String>,
    description: Option<String>,
    created: Option<String>,
    scenario_time: Option<String>,
    modeling_authority_set: Option<String>,
    mut prefixes: HashMap<String, NamedNode>,
    graph: Option<String>,
) -> PyResult<()> {
    if !prefixes.contains_key("cim") {
        prefixes.insert(
            "cim".to_string(),
            NamedNode::new_unchecked("http://iec.ch/TC57/CIM100#"),
        );
    }
    let model_iri = parse_optional_named_node(model_iri)?.unwrap_or(NamedNode::new_unchecked(
        format!("urn:uuid:{}", uuid::Uuid::new_v4()),
    ));
    let version = version.map(oxrdf::Literal::new_simple_literal);
    let description = description.map(oxrdf::Literal::new_simple_literal);
    let profile_graph = parse_named_node(profile_graph)?;
    let profile_graph = NamedGraph::NamedGraph(profile_graph);
    let created = oxrdf::Literal::new_typed_literal(
        created.unwrap_or(Utc::now().format(XSD_DATETIME_WITH_TZ_FORMAT).to_string()),
        xsd::DATE_TIME,
    );
    let scenario_time = oxrdf::Literal::new_typed_literal(
        scenario_time.unwrap_or(Utc::now().format(XSD_DATETIME_WITH_TZ_FORMAT).to_string()),
        xsd::DATE_TIME,
    );
    let modeling_authority_set = modeling_authority_set.map(oxrdf::Literal::new_simple_literal);
    let graph = parse_optional_named_node(graph)?;
    let graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let path_buf = PathBuf::from(file_path);
    let mut actual_file = File::create(path_buf.as_path())
        .map_err(|x| PyMaplibError::from(MaplibError::FileCreateIOError(x)))?;
    let fullmodel_details = FullModelDetails::new(
        model_iri,
        description,
        version,
        created,
        scenario_time,
        modeling_authority_set,
    )
    .map_err(|x| PyMaplibError::from(MaplibError::CIMXMLError(x)))?;
    inner
        .write_cim_xml(
            &mut actual_file,
            fullmodel_details,
            prefixes,
            &graph,
            &profile_graph,
        )
        .unwrap();
    Ok(())
}

pub(crate) fn writes_mutex(
    inner: &mut MutexGuard<InnerModel>,
    format: Option<String>,
    graph: Option<String>,
    prefixes: Option<HashMap<String, NamedNode>>,
) -> PyResult<String> {
    let format = if let Some(format) = format {
        if format.eq_ignore_ascii_case("hdt") {
            return Err(PyMaplibError::FunctionArgumentError(
                "HDT is a binary format, use write() instead of writes()".to_string(),
            )
            .into());
        }
        resolve_normal_format(&format).map_err(PyMaplibError::from)?
    } else {
        RdfFormat::NTriples
    };
    let mut out = vec![];
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    inner
        .write_triples(&mut out, &named_graph, format, prefixes.as_ref())
        .unwrap();
    Ok(String::from_utf8(out).unwrap())
}

pub(crate) fn write_native_parquet_mutex(
    inner: &mut MutexGuard<InnerModel>,
    folder_path: String,
    graph: Option<String>,
) -> PyResult<()> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    inner
        .write_native_parquet(&folder_path, &named_graph)
        .map_err(PyMaplibError::from)?;
    Ok(())
}

pub(crate) fn get_predicate_iris_mutex(
    inner: &mut MutexGuard<InnerModel>,
    graph: Option<String>,
    include_transient: Option<bool>,
) -> PyResult<Vec<PyIRI>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let nns = inner
        .get_predicate_iris(&named_graph, include_transient.unwrap_or(false))
        .map_err(PyMaplibError::from)?;
    Ok(nns.into_iter().map(PyIRI::from).collect())
}

pub(crate) fn get_predicate_mutex(
    inner: &mut MutexGuard<InnerModel>,
    iri: PyIRI,
    graph: Option<String>,
    include_transient: bool,
) -> PyResult<Vec<EagerSolutionMappings>> {
    let graph = parse_optional_named_node(graph)?;
    let named_graph = NamedGraph::from_maybe_named_node(graph.as_ref());
    let eager_sms = inner
        .get_predicate(&iri.into_inner(), &named_graph, include_transient)
        .map_err(PyMaplibError::from)?;
    let global_cats = &inner.triplestore.global_cats;
    let mut out_eager_sms = Vec::with_capacity(eager_sms.len());
    for EagerSolutionMappings {
        mut mappings,
        mut rdf_node_types,
    } in eager_sms
    {
        (mappings, rdf_node_types) =
            fix_cats_and_multicolumns(mappings, rdf_node_types, true, global_cats.clone());
        out_eager_sms.push(EagerSolutionMappings::new(mappings, rdf_node_types));
    }
    Ok(out_eager_sms)
}

pub(crate) fn infer_mutex(
    inner: &mut MutexGuard<InnerModel>,
    rulesets: Vec<String>,
    max_iterations: Option<usize>,
    max_results: Option<usize>,
    graph: Option<&NamedGraph>,
    include_transient: bool,
    max_rows: Option<usize>,
    debug: Option<bool>,
) -> Result<InferenceResult, PyMaplibError> {
    inner
        .infer(
            rulesets,
            max_iterations,
            max_results,
            graph,
            include_transient,
            max_rows,
            debug.unwrap_or(DEFAULT_DEBUG_NO_RESULTS),
        )
        .map_err(PyMaplibError::MaplibError)
}
