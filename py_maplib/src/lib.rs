extern crate core;
mod error;
mod mutexes;
mod py_indexing_options;
mod py_model;
mod shacl;

use crate::error::*;
use crate::py_indexing_options::PyIndexingOptions;
use crate::py_model::PyModel;
use crate::shacl::PyValidationReport;

use polars::frame::DataFrame;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{filter, prelude::*};

use maplib::errors::MaplibError;

use pyo3::prelude::*;
use representation::result::{QueryResult, QueryResultKind as SparqlQueryResult};
use std::collections::HashMap;
use std::path::PathBuf;

//The below snippet controlling alloc-library is from https://github.com/pola-rs/polars/blob/main/py-polars/src/lib.rs
//And has a MIT license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deapub pub l
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#[cfg(target_os = "linux")]
use tikv_jemallocator::Jemalloc;

use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::{col, lit, IntoLazy};
use pyo3::types::{PyList, PyString};
use pyo3::IntoPyObjectExt;
use representation::python::{
    PyBlankNode, PyIRI, PyLiteral, PyPrefix, PyRDFType, PySolutionMappings, PyVariable,
    PyXSDDuration,
};
use representation::solution_mapping::EagerSolutionMappings;

use datalog::python::PyInferenceResult;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;
use pyo3::exceptions::PyTypeError;
use representation::cats::LockedCats;
use representation::dataset::NamedGraph;
use representation::debug::DebugOutputs;
use representation::df_to_python::{df_to_py_df, fix_cats_and_multicolumns};
use representation::python_df_to_rust::polars_df_to_rust_df;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::{
    BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME,
};
use templates::python::owl::PyOWL;
use templates::python::rdf::PyRDF;
use templates::python::rdfs::PyRDFS;
use templates::python::xsd::PyXSD;
use templates::python::{py_triple, PyArgument, PyInstance, PyParameter, PyTemplate};
use templates::MappingColumnType;
use triplestore::triples_read::ExtendedRdfFormat;
use triplestore::NewTriples;

use virtualization::python::VirtualizedPythonDatabase;
use virtualized_query::python::{
    PyAggregateExpression, PyExpression, PyOrderExpression, PyVirtualizedQuery,
};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DEFAULT_STREAMING: bool = false;
const DEFAULT_FTS: bool = false;
const DEFAULT_INCLUDE_TRANSIENT: bool = true;
const DEFAULT_MAP_TO_TRANSIENT: bool = false;
const DEFAULT_DEBUG_NO_RESULTS: bool = false;
const DEFAULT_TRIPLES_BATCH_SIZE: usize = 10_000_000;

type ParametersType<'a> = HashMap<String, Bound<'a, PySolutionMappings>>;

// pymethods non-critical functions

pub(crate) enum TemplateType {
    TemplateIRI(PyIRI),
    TemplateString(String),
    TemplatePyTemplate(PyTemplate),
}

impl TryFrom<Bound<'_, PyAny>> for TemplateType {
    type Error = PyMaplibError;

    fn try_from(value: Bound<'_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(i) = value.extract::<PyIRI>() {
            Ok(Self::TemplateIRI(i))
        } else if let Ok(s) = value.extract::<String>() {
            Ok(Self::TemplateString(s))
        } else if let Ok(t) = value.extract::<PyTemplate>() {
            Ok(Self::TemplatePyTemplate(t))
        } else {
            Err(PyMaplibError::FunctionArgumentError(
                "Expected template to be Template or stOTTR document string".to_string(),
            ))
        }
    }
}

#[derive(Clone, FromPyObject)]
enum StringOrPathBuf {
    String(String),
    PathBuf(PathBuf),
}

#[pymodule]
#[pyo3(name = "maplib")]
fn _maplib(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // The python log writer is too slow, needs a logging object cache similar to pyo3-log
    // let make_writer = tracing_pyo3_logger::PythonLogMakeWriter::new(py)?;
    // let pyo3_log_layer = tracing_subscriber::fmt::layer()
    //     .with_writer(make_writer)
    //     .with_target(false)
    //     .with_level(false)
    //     .without_time()
    //     .with_filter(tracing_subscriber::filter::LevelFilter::INFO);

    // let subscriber = tracing_subscriber::registry().with(pyo3_log_layer);
    // subscriber.init();

    let fmt = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(filter::LevelFilter::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    fmt.init();

    m.add_class::<PyIndexingOptions>()?;
    m.add_class::<PyModel>()?;
    m.add_class::<PyValidationReport>()?;
    m.add_class::<PySolutionMappings>()?;
    m.add_class::<PyInferenceResult>()?;
    m.add_class::<PyRDFType>()?;
    m.add_class::<PyPrefix>()?;
    m.add_class::<PyVariable>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyBlankNode>()?;
    m.add_class::<PyIRI>()?;
    m.add_class::<PyXSD>()?;
    m.add_class::<PyRDF>()?;
    m.add_class::<PyRDFS>()?;
    m.add_class::<PyOWL>()?;
    m.add_class::<PyParameter>()?;
    m.add_class::<PyArgument>()?;
    m.add_class::<PyTemplate>()?;
    m.add_class::<PyInstance>()?;
    m.add_function(wrap_pyfunction!(py_triple, m)?)?;
    m.add("MaplibException", py.get_type::<MaplibException>())?;
    m.add(
        "FunctionArgumentException",
        py.get_type::<FunctionArgumentException>(),
    )?;
    m.add_class::<VirtualizedPythonDatabase>()?;
    //m.add_class::<PyFlightClient>()?;

    let child = PyModule::new(m.py(), "vq")?;
    child.add_class::<PyVirtualizedQuery>()?;
    child.add_class::<PyExpression>()?;
    child.add_class::<PyOrderExpression>()?;
    child.add_class::<PyAggregateExpression>()?;
    child.add_class::<PyXSDDuration>()?;
    m.add_submodule(&child)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("chrontext.vq", child)?;
    Ok(())
}

fn query_to_result(
    res: QueryResult,
    native_dataframe: bool,
    include_details: bool,
    return_json: bool,
    global_cats: LockedCats,
    py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    if return_json {
        let json = res.kind.json(global_cats.clone());
        return Ok(PyString::new(py, &json).into());
    }
    let QueryResult {
        kind,
        debug,
        pushdown_paths,
    } = res;
    match kind {
        SparqlQueryResult::Select(EagerSolutionMappings {
            mut mappings,
            mut rdf_node_types,
        }) => {
            (mappings, rdf_node_types) =
                fix_cats_and_multicolumns(mappings, rdf_node_types, native_dataframe, global_cats);
            let pydf = df_to_py_df(
                mappings,
                rdf_node_types,
                debug,
                Some(pushdown_paths),
                include_details,
                py,
            )?;
            Ok(pydf)
        }
        SparqlQueryResult::Construct(dfs) => {
            let mut query_results = vec![];
            for (
                EagerSolutionMappings {
                    mut mappings,
                    mut rdf_node_types,
                },
                predicate,
            ) in dfs
            {
                if let Some(predicate) = predicate {
                    mappings = mappings
                        .lazy()
                        .with_column(
                            lit(rdf_named_node_to_polars_literal_value(&predicate))
                                .alias(PREDICATE_COL_NAME),
                        )
                        .select([
                            col(SUBJECT_COL_NAME),
                            col(PREDICATE_COL_NAME),
                            col(OBJECT_COL_NAME),
                        ])
                        .collect()
                        .unwrap();
                    rdf_node_types.insert(
                        PREDICATE_COL_NAME.to_string(),
                        BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                    );
                }
                (mappings, rdf_node_types) = fix_cats_and_multicolumns(
                    mappings,
                    rdf_node_types,
                    native_dataframe,
                    global_cats.clone(),
                );
                let pydf = df_to_py_df(
                    mappings,
                    rdf_node_types,
                    debug.clone(),
                    Some(pushdown_paths.clone()),
                    include_details,
                    py,
                )?;
                query_results.push(pydf);
            }
            PyList::new(py, query_results)?.into_py_any(py)
        }
    }
}

//Allowing complex type as it is not used anywhere else.
#[allow(clippy::type_complexity)]
pub(crate) fn map_parameters(
    parameters: Option<HashMap<String, Bound<PySolutionMappings>>>,
    py: Python,
) -> PyResult<Option<HashMap<String, EagerSolutionMappings>>> {
    if let Some(parameters) = parameters {
        let mut mapped_parameters = HashMap::new();
        for (k, pysm) in parameters {
            mapped_parameters.insert(k, pysm.borrow().to_inner(py)?);
        }

        Ok(Some(mapped_parameters))
    } else {
        Ok(None)
    }
}

fn resolve_normal_format(format: &str) -> Result<RdfFormat, PyMaplibError> {
    match format.to_lowercase().as_str() {
        "json-ld" | "json" | "jsonld" => Ok(RdfFormat::JsonLd {
            profile: Default::default(),
        }),
        "ntriples" => Ok(RdfFormat::NTriples),
        "turtle" => Ok(RdfFormat::Turtle),
        "rdf/xml" | "xml" | "rdfxml" => Ok(RdfFormat::RdfXml),
        _ => Err(PyMaplibError::FunctionArgumentError(format!(
            "Unknown format: {}",
            format
        ))),
    }
}

fn resolve_format(format: &str) -> Result<ExtendedRdfFormat, PyMaplibError> {
    match format.to_lowercase().as_str() {
        "cim" | "cim/xml" | "cimxml" => Ok(ExtendedRdfFormat::CIMXML),
        "hdt" => Ok(ExtendedRdfFormat::HDT),
        _ => match resolve_normal_format(format) {
            Ok(o) => Ok(ExtendedRdfFormat::Normal(o)),
            Err(e) => Err(e),
        },
    }
}

fn parse_named_node(s: String) -> PyResult<NamedNode> {
    Ok(NamedNode::new(s).map_err(|x| PyMaplibError::from(MaplibError::from(x)))?)
}

fn parse_optional_named_node(s: Option<String>) -> PyResult<Option<NamedNode>> {
    let nn = if let Some(s) = s {
        Some(parse_named_node(s)?)
    } else {
        None
    };
    Ok(nn)
}

fn map_types(
    types: Option<HashMap<String, RDFNodeState>>,
) -> Option<HashMap<String, MappingColumnType>> {
    if let Some(types) = types {
        let mut new_types = HashMap::new();
        for (k, v) in types {
            new_types.insert(k, MappingColumnType::Flat(v));
        }
        Some(new_types)
    } else {
        None
    }
}

fn new_triples_to_dict(
    new_triples: Vec<NewTriples>,
    solution_mappings: bool,
    global_cats: LockedCats,
    py: Python<'_>,
) -> PyResult<HashMap<String, Py<PyAny>>> {
    let mut map = HashMap::new();
    //TODO: Handle case where same predicate occurs multiple times
    for NewTriples {
        df,
        graph: _,
        predicate,
        subject_type,
        object_type,
    } in new_triples
    {
        if let Some(mut df) = df {
            let mut types = HashMap::new();
            types.insert(
                SUBJECT_COL_NAME.to_string(),
                subject_type.into_default_stored_rdf_node_state(),
            );
            types.insert(
                OBJECT_COL_NAME.to_string(),
                object_type.into_default_stored_rdf_node_state(),
            );
            (df, types) =
                fix_cats_and_multicolumns(df, types, solution_mappings, global_cats.clone());
            let py_sm = df_to_py_df(df, types, None, None, solution_mappings, py)?;
            map.insert(predicate.as_str().to_string(), py_sm);
        }
    }
    Ok(map)
}

fn print_debug_if_exists(debug_outputs: Option<&DebugOutputs>) {
    if let Some(debug_outputs) = debug_outputs {
        print!("{}", debug_outputs);
    }
}

fn create_prefix_map(
    prefixes: Option<Bound<'_, PyAny>>,
) -> PyResult<Option<HashMap<String, NamedNode>>> {
    if let Some(prefixes) = prefixes {
        let mut use_prefixes = HashMap::new();
        if let Ok(prefixes) = prefixes.extract::<HashMap<String, String>>() {
            for (k, v) in prefixes {
                let nn = NamedNode::new(v).map_err(|x| {
                    PyMaplibError::FunctionArgumentError(format!(
                        "Error parsing prefix {}:{}",
                        k, x
                    ))
                })?;
                use_prefixes.insert(k, nn);
            }
        } else if let Ok(prefixes) = prefixes.extract::<HashMap<String, PyIRI>>() {
            for (k, v) in prefixes {
                use_prefixes.insert(k, v.into_inner());
            }
        } else if let Ok(prefixes) = prefixes.extract::<HashMap<String, PyPrefix>>() {
            for (k, v) in prefixes {
                use_prefixes.insert(k, v.iri.clone());
            }
        } else {
            return Err(PyMaplibError::FunctionArgumentError(
                "Prefixes should be Dict[str,str]".to_string(),
            )
            .into());
        };
        Ok(Some(use_prefixes))
    } else {
        Ok(None)
    }
}

pub fn data_to_mappings_types(
    data: &Bound<'_, PyAny>,
    py: Python<'_>,
) -> PyResult<(DataFrame, Option<HashMap<String, RDFNodeState>>)> {
    if let Ok(sm) = data.extract::<PySolutionMappings>() {
        let EagerSolutionMappings {
            mappings,
            rdf_node_types,
        } = sm.to_inner(py)?;
        Ok((mappings, Some(rdf_node_types)))
    } else {
        let df = polars_df_to_rust_df(data)?;
        Ok((df, None))
    }
}

fn resolve_rdf_node_type(obj: &Bound<'_, PyAny>) -> PyResult<BaseRDFNodeType> {
    if let Ok(rdf_type) = obj.extract::<PyRDFType>() {
        let state = rdf_type
            .flat
            .as_ref()
            .ok_or_else(|| PyMaplibError::UDFError("RDFType is not flat".to_string()))?;
        if state.map.len() != 1 {
            return Err(PyTypeError::new_err(
                "`RDFType` should only contain one mapping",
            ));
        }
        return Ok(state.map.keys().next().unwrap().clone());
    }
    if let Ok(iri) = obj.extract::<PyIRI>() {
        return Ok(BaseRDFNodeType::Literal(iri.into_inner()));
    }
    if let Ok(s) = obj.extract::<String>() {
        if s == "IRI" || s == "iri" {
            return Ok(BaseRDFNodeType::IRI);
        }
        if s == "BlankNode" || s == "blanknode" {
            return Ok(BaseRDFNodeType::BlankNode);
        }
        let nn = NamedNode::new(&s).map_err(|e| {
            PyMaplibError::FunctionArgumentError(format!("Invalid IRI '{}': {}", s, e))
        })?;
        return Ok(BaseRDFNodeType::Literal(nn));
    }
    Err(PyTypeError::new_err("Expected RDFType or IRI"))
}
