//! Streaming mapping: `m.map(template, data, stream=True)` (and `map_triples` / `map_default`)
//! returns a `ModelStream`, a lazy pipeline of steps (`update`, `insert`, `infer`, `write`) that
//! runs when `collect()` is called. Each batch of the mapping input is mapped into a fresh
//! in-memory model that has the templates, prefixes and UDFs of the parent model. The steps run on
//! that batch model in the order they were added, the `write` steps append the batch to their
//! files, and the batch model is then dropped. Memory use therefore depends on the batch size, not
//! on the total size of the input. The parent model's triples are not changed.
//!
//! `m.map_json(documents, stream=True)` and `m.map_xml(documents, stream=True)` work the same way,
//! where a batch is `documents_per_batch` documents of an iterable of documents.

use crate::error::PyMaplibError;
use crate::mutexes::{
    infer_mutex, insert_mutex, map_default_batch_mutex, map_default_first_batch_mutex,
    map_json_mutex, map_resolved_mutex, map_triples_mutex, map_xml_mutex, update_mutex,
};
use crate::py_model::PyModel;
use crate::{
    classify_document_data, classify_mapping_data, create_prefix_map, document_uuid_namespace,
    for_each_document, for_each_mapping_batch, map_parameters, parse_include_graphs,
    parse_optional_named_node, print_debug_if_exists, resolve_format, DocumentData, MappingData,
    ParametersType, StringOrPathBuf,
};
use maplib::model::Model as InnerModel;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::frame::DataFrame;
use pyo3::prelude::*;
use representation::dataset::NamedGraph;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeState;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use templates::ast::Template;
use tracing::instrument;
use triplestore::triples_read::ExtendedRdfFormat;
use triplestore::{StorageOptions, Triplestore};

/// How each batch of the input is mapped.
pub(crate) enum StreamSource {
    Map {
        template_iri: String,
        graph: Option<String>,
        validate_iris: Option<bool>,
    },
    MapTriples {
        predicate: Option<String>,
        graph: Option<String>,
        validate_iris: Option<bool>,
    },
    MapDefault {
        primary_key_column: String,
        graph: NamedGraph,
        validate_iris: Option<bool>,
    },
    /// map_json / map_xml
    Documents {
        kind: DocumentKind,
        graph: Option<String>,
        uuid_namespace: Option<String>,
        documents_per_batch: usize,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum DocumentKind {
    Json,
    Xml,
}

impl DocumentKind {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            DocumentKind::Json => "JSON",
            DocumentKind::Xml => "XML",
        }
    }

    pub(crate) fn map_mutex(
        &self,
        inner: &mut std::sync::MutexGuard<InnerModel>,
        doc: StringOrPathBuf,
        graph: Option<String>,
        uuid_namespace: Option<String>,
    ) -> PyResult<()> {
        match self {
            DocumentKind::Json => map_json_mutex(inner, doc, graph, uuid_namespace),
            DocumentKind::Xml => map_xml_mutex(inner, doc, graph, uuid_namespace),
        }
    }
}

/// The input of one batch: a DataFrame, or a number of documents with their index in the input.
enum BatchInput {
    Frame(DataFrame, Option<HashMap<String, RDFNodeState>>),
    Documents(Vec<(usize, StringOrPathBuf)>),
}

#[derive(Clone)]
enum StreamStep {
    Update {
        update: String,
        parameters: Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    },
    Insert {
        query: String,
        parameters: Option<HashMap<String, EagerSolutionMappings>>,
        source_graph: NamedGraph,
        target_graph: NamedGraph,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    },
    Infer {
        rulesets: Vec<String>,
        graph: Option<NamedGraph>,
        max_iterations: Option<usize>,
        max_results: Option<usize>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    },
    Write {
        file_path: PathBuf,
        format: RdfFormat,
        /// None writes all graphs for N-Quads.
        graph: Option<NamedGraph>,
        prefixes: Option<HashMap<String, NamedNode>>,
    },
}

#[pyclass(name = "ModelStream", frozen)]
pub struct PyModelStream {
    model: Py<PyModel>,
    data: Py<PyAny>,
    source: Arc<StreamSource>,
    steps: Vec<StreamStep>,
}

impl PyModelStream {
    pub(crate) fn new(model: Py<PyModel>, data: Py<PyAny>, source: StreamSource) -> Self {
        PyModelStream {
            model,
            data,
            source: Arc::new(source),
            steps: vec![],
        }
    }

    fn with_step(&self, py: Python<'_>, step: StreamStep) -> PyModelStream {
        let mut steps = self.steps.clone();
        steps.push(step);
        PyModelStream {
            model: self.model.clone_ref(py),
            data: self.data.clone_ref(py),
            source: self.source.clone(),
            steps,
        }
    }
}

#[pymethods]
impl PyModelStream {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
            update,
            parameters=None,
            graph=None,
            include_graphs=None,
            max_rows=None,
            debug=None,
    ))]
    fn update(
        &self,
        py: Python<'_>,
        update: String,
        parameters: Option<ParametersType>,
        graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<PyModelStream> {
        parse_optional_named_node(graph.clone())?;
        let parameters = map_parameters(parameters, py)?;
        Ok(self.with_step(
            py,
            StreamStep::Update {
                update,
                parameters,
                graph,
                include_graphs,
                max_rows,
                debug,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
            query,
            parameters=None,
            source_graph=None,
            target_graph=None,
            include_graphs=None,
            max_rows=None,
            debug=None,
    ))]
    fn insert(
        &self,
        py: Python<'_>,
        query: String,
        parameters: Option<ParametersType>,
        source_graph: Option<String>,
        target_graph: Option<String>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<PyModelStream> {
        let parameters = map_parameters(parameters, py)?;
        let source_graph = parse_optional_named_node(source_graph)?;
        let source_graph = NamedGraph::from_maybe_named_node(source_graph.as_ref());
        let target_graph = parse_optional_named_node(target_graph)?;
        let target_graph = NamedGraph::from_maybe_named_node(target_graph.as_ref());
        Ok(self.with_step(
            py,
            StreamStep::Insert {
                query,
                parameters,
                source_graph,
                target_graph,
                include_graphs,
                max_rows,
                debug,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        rulesets,
        graph=None,
        max_iterations=100_000,
        max_results=10_000_000,
        include_graphs=None,
        max_rows=100_000_000,
        debug=None))]
    fn infer(
        &self,
        py: Python<'_>,
        rulesets: Py<PyAny>,
        graph: Option<String>,
        max_iterations: Option<usize>,
        max_results: Option<usize>,
        include_graphs: Option<Vec<String>>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    ) -> PyResult<PyModelStream> {
        let rulesets = if let Ok(s) = rulesets.extract::<String>(py) {
            vec![s]
        } else if let Ok(ss) = rulesets.extract::<Vec<String>>(py) {
            ss
        } else {
            return Err(PyMaplibError::FunctionArgumentError(
                "ruleset should be str or List[str]".to_string(),
            )
            .into());
        };
        let graph = parse_optional_named_node(graph)?;
        let graph = graph.map(|graph| NamedGraph::from_maybe_named_node(Some(&graph)));
        Ok(self.with_step(
            py,
            StreamStep::Infer {
                rulesets,
                graph,
                max_iterations,
                max_results,
                include_graphs: parse_include_graphs(include_graphs, true)?,
                max_rows,
                debug,
            },
        ))
    }

    #[pyo3(signature = (file_path, format=None, graph=None, prefixes=None))]
    fn write(
        &self,
        py: Python<'_>,
        file_path: &Bound<'_, PyAny>,
        format: Option<String>,
        graph: Option<String>,
        prefixes: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyModelStream> {
        let file_path = PathBuf::from(file_path.str()?.to_string());
        let format = if let Some(format) = format {
            resolve_format(&format)?
        } else {
            ExtendedRdfFormat::Normal(RdfFormat::NTriples)
        };
        let format = match format {
            ExtendedRdfFormat::Normal(
                f @ (RdfFormat::NTriples | RdfFormat::NQuads | RdfFormat::Turtle),
            ) => f,
            _ => {
                return Err(PyMaplibError::FunctionArgumentError(
                    "Streams can only be written as \"ntriples\", \"nquads\" or \"turtle\""
                        .to_string(),
                )
                .into())
            }
        };
        let graph = parse_optional_named_node(graph)?;
        let graph = if format == RdfFormat::NQuads && graph.is_none() {
            None
        } else {
            Some(NamedGraph::from_maybe_named_node(graph.as_ref()))
        };
        let prefixes = create_prefix_map(prefixes)?;
        Ok(self.with_step(
            py,
            StreamStep::Write {
                file_path,
                format,
                graph,
                prefixes,
            },
        ))
    }

    /// Runs the stream: maps each batch of the input into a fresh model, runs the steps on it and
    /// appends its triples to the files of the write steps.
    #[instrument(skip_all)]
    fn collect(&self, py: Python<'_>) -> PyResult<()> {
        let model = self.model.bind(py).get();
        let carrier = py.detach(|| {
            let mut inner = model.inner.lock().unwrap();
            empty_model_like(&mut inner, false)
        })?;
        let mut state = BatchState {
            carrier,
            default_template: None,
        };
        let mut writers = open_writers(&self.steps, &state.carrier.prefixes)?;
        let source = self.source.as_ref();
        let steps = &self.steps;

        if let StreamSource::Documents {
            kind,
            documents_per_batch,
            ..
        } = source
        {
            let mut run = |py: Python<'_>, docs: Vec<(usize, StringOrPathBuf)>| -> PyResult<()> {
                py.detach(|| {
                    run_batch(
                        &mut state,
                        source,
                        steps,
                        &mut writers,
                        BatchInput::Documents(docs),
                    )
                })
            };
            match classify_document_data(self.data.bind(py), kind.name())? {
                DocumentData::Single(doc) => run(py, vec![(0, doc)])?,
                DocumentData::Many(data) => {
                    let mut pending = Vec::with_capacity(*documents_per_batch);
                    for_each_document(&data, kind.name(), &mut |py, i, doc| {
                        pending.push((i, doc));
                        if pending.len() >= *documents_per_batch {
                            run(py, std::mem::take(&mut pending))?;
                        }
                        Ok(())
                    })?;
                    if !pending.is_empty() {
                        run(py, pending)?;
                    }
                }
            }
        } else {
            let mut run = |py: Python<'_>,
                           df: DataFrame,
                           types: Option<HashMap<String, RDFNodeState>>|
             -> PyResult<()> {
                if df.height() == 0 {
                    return Ok(());
                }
                py.detach(|| {
                    run_batch(
                        &mut state,
                        source,
                        steps,
                        &mut writers,
                        BatchInput::Frame(df, types),
                    )
                })
            };
            match classify_mapping_data(self.data.bind(py), py)? {
                MappingData::Eager(df, types) => run(py, df, types)?,
                MappingData::Batched(data) => for_each_mapping_batch(&data, py, &mut run)?,
            }
        }

        for w in writers.iter_mut().flatten() {
            w.flush().map_err(|x| {
                PyMaplibError::from(maplib::errors::MaplibError::FileCreateIOError(x))
            })?;
        }
        if let StreamSource::MapDefault { .. } = source {
            if state.default_template.is_none() {
                return Err(PyMaplibError::FunctionArgumentError(
                    "map_default received no batches, so no template could be created".to_string(),
                )
                .into());
            }
        }
        Ok(())
    }
}

/// What is carried from one batch model to the next.
struct BatchState {
    /// An empty model with the templates, prefixes, UDFs and blank node counters for the next batch.
    carrier: InnerModel,
    default_template: Option<Template>,
}

/// An empty in-memory model with the templates, prefixes, settings, UDFs and blank node counters of
/// the given model. The templates are moved out of the given model when take_templates is set.
fn empty_model_like(model: &mut InnerModel, take_templates: bool) -> PyResult<InnerModel> {
    let mut triplestore = Triplestore::new(StorageOptions::memory(), Some(model.indexing.clone()))
        .map_err(|x| PyMaplibError::from(maplib::errors::MaplibError::TriplestoreError(x)))?;
    triplestore.udf_registry = model.triplestore.udf_registry.clone();
    triplestore.set_blank_node_scope(model.triplestore.blank_node_scope());
    let template_dataset = if take_templates {
        std::mem::take(&mut model.template_dataset)
    } else {
        model.template_dataset.clone()
    };
    Ok(InnerModel {
        blank_node_counter: model.blank_node_counter,
        template_dataset,
        triplestore,
        indexing: model.indexing.clone(),
        prefixes: model.prefixes.clone(),
        chrontext_settings: model.chrontext_settings.clone(),
    })
}

/// Creates (truncates) the files of the write steps, one entry per step (None for other steps).
/// Turtle prefixes are declared once here, the batches are then written without declarations.
fn open_writers(
    steps: &[StreamStep],
    model_prefixes: &HashMap<String, NamedNode>,
) -> PyResult<Vec<Option<BufWriter<File>>>> {
    let mut writers = vec![];
    for step in steps {
        if let StreamStep::Write {
            file_path,
            format,
            prefixes,
            ..
        } = step
        {
            let file = File::create(file_path).map_err(|x| {
                PyMaplibError::from(maplib::errors::MaplibError::FileCreateIOError(x))
            })?;
            let mut w = BufWriter::new(file);
            if *format == RdfFormat::Turtle {
                let prefixes = merge_prefixes(model_prefixes, prefixes.as_ref());
                let mut sorted: Vec<_> = prefixes.iter().collect();
                sorted.sort();
                let write_err =
                    |x| PyMaplibError::from(maplib::errors::MaplibError::FileCreateIOError(x));
                for (k, v) in &sorted {
                    writeln!(w, "@prefix {}: {} .", k, v).map_err(write_err)?;
                }
                if !sorted.is_empty() {
                    writeln!(w).map_err(write_err)?;
                }
            }
            writers.push(Some(w));
        } else {
            writers.push(None);
        }
    }
    Ok(writers)
}

fn merge_prefixes(
    model_prefixes: &HashMap<String, NamedNode>,
    prefixes: Option<&HashMap<String, NamedNode>>,
) -> HashMap<String, NamedNode> {
    let mut use_prefixes = model_prefixes.clone();
    if let Some(prefixes) = prefixes {
        use_prefixes.extend(prefixes.clone());
    }
    use_prefixes
}

fn run_batch(
    state: &mut BatchState,
    source: &StreamSource,
    steps: &[StreamStep],
    writers: &mut [Option<BufWriter<File>>],
    input: BatchInput,
) -> PyResult<()> {
    let batch_model = Mutex::new(empty_model_like(&mut state.carrier, true)?);
    let mut inner = batch_model.lock().unwrap();
    let res = run_batch_steps(
        &mut inner,
        &mut state.default_template,
        source,
        steps,
        writers,
        input,
    );
    // Hand the templates and blank node counters on to the next batch, also after an error.
    state.carrier = empty_model_like(&mut inner, true)?;
    res
}

fn run_batch_steps(
    inner: &mut std::sync::MutexGuard<InnerModel>,
    default_template: &mut Option<Template>,
    source: &StreamSource,
    steps: &[StreamStep],
    writers: &mut [Option<BufWriter<File>>],
    input: BatchInput,
) -> PyResult<()> {
    map_batch(inner, default_template, source, input)?;
    run_steps(inner, steps, writers)
}

/// Maps the input of one batch into the batch model.
fn map_batch(
    inner: &mut std::sync::MutexGuard<InnerModel>,
    default_template: &mut Option<Template>,
    source: &StreamSource,
    input: BatchInput,
) -> PyResult<()> {
    let (df, types) = match input {
        BatchInput::Frame(df, types) => (df, types),
        BatchInput::Documents(docs) => {
            let StreamSource::Documents {
                kind,
                graph,
                uuid_namespace,
                ..
            } = source
            else {
                unreachable!("documents are only read by document streams")
            };
            for (i, doc) in docs {
                kind.map_mutex(
                    inner,
                    doc,
                    graph.clone(),
                    document_uuid_namespace(uuid_namespace, i),
                )?;
            }
            return Ok(());
        }
    };
    match source {
        StreamSource::Map {
            template_iri,
            graph,
            validate_iris,
        } => {
            map_resolved_mutex(
                inner,
                template_iri,
                Some(df),
                graph.clone(),
                types,
                *validate_iris,
            )?;
        }
        StreamSource::MapTriples {
            predicate,
            graph,
            validate_iris,
        } => {
            map_triples_mutex(
                inner,
                df,
                predicate.clone(),
                graph.clone(),
                types,
                *validate_iris,
            )?;
        }
        StreamSource::MapDefault {
            primary_key_column,
            graph,
            validate_iris,
        } => {
            if let Some(template) = default_template.as_ref() {
                map_default_batch_mutex(
                    inner,
                    template,
                    df,
                    primary_key_column,
                    graph.clone(),
                    types,
                    *validate_iris,
                )?;
            } else {
                *default_template = Some(map_default_first_batch_mutex(
                    inner,
                    df,
                    primary_key_column,
                    graph.clone(),
                    types,
                    *validate_iris,
                )?);
            }
        }
        StreamSource::Documents { .. } => {
            unreachable!("document streams only read documents")
        }
    }
    Ok(())
}

/// Runs the steps of the stream on the batch model.
fn run_steps(
    inner: &mut std::sync::MutexGuard<InnerModel>,
    steps: &[StreamStep],
    writers: &mut [Option<BufWriter<File>>],
) -> PyResult<()> {
    for (step, writer) in steps.iter().zip(writers.iter_mut()) {
        match step {
            StreamStep::Update {
                update,
                parameters,
                graph,
                include_graphs,
                max_rows,
                debug,
            } => {
                let res = update_mutex(
                    inner,
                    update.clone(),
                    parameters.clone(),
                    graph.clone(),
                    include_graphs.clone(),
                    *max_rows,
                    *debug,
                )?;
                print_debug_if_exists(res.debug.as_ref());
            }
            StreamStep::Insert {
                query,
                parameters,
                source_graph,
                target_graph,
                include_graphs,
                max_rows,
                debug,
            } => {
                let res = insert_mutex(
                    inner,
                    query.clone(),
                    parameters.clone(),
                    source_graph.clone(),
                    target_graph.clone(),
                    include_graphs.clone(),
                    *max_rows,
                    *debug,
                )?;
                print_debug_if_exists(res.debug.as_ref());
            }
            StreamStep::Infer {
                rulesets,
                graph,
                max_iterations,
                max_results,
                include_graphs,
                max_rows,
                debug,
            } => {
                infer_mutex(
                    inner,
                    rulesets.clone(),
                    *max_iterations,
                    *max_results,
                    graph.as_ref(),
                    include_graphs.clone(),
                    *max_rows,
                    *debug,
                )?;
            }
            StreamStep::Write {
                format,
                graph,
                prefixes,
                ..
            } => {
                let w = writer.as_mut().expect("write steps have writers");
                write_batch(inner, w, *format, graph.as_ref(), prefixes.as_ref())?;
            }
        }
    }
    Ok(())
}

fn write_batch(
    inner: &mut InnerModel,
    w: &mut BufWriter<File>,
    format: RdfFormat,
    graph: Option<&NamedGraph>,
    prefixes: Option<&HashMap<String, NamedNode>>,
) -> PyResult<()> {
    match format {
        RdfFormat::NQuads => inner
            .write_quads(w, graph, format, prefixes)
            .map_err(PyMaplibError::from)?,
        RdfFormat::Turtle => {
            let use_prefixes = merge_prefixes(&inner.prefixes, prefixes);
            inner
                .triplestore
                .write_pretty_turtle_without_prefix_declarations(
                    w,
                    graph.expect("turtle writes have a graph"),
                    &use_prefixes,
                )
                .map_err(|x| {
                    PyMaplibError::from(maplib::errors::MaplibError::TriplestoreError(x))
                })?
        }
        _ => inner
            .write_triples(
                w,
                graph.expect("triple formats have a graph"),
                format,
                prefixes,
            )
            .map_err(PyMaplibError::from)?,
    }
    Ok(())
}
