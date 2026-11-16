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
//!
//! With `in_place=True` on the map call, each batch is instead mapped into the parent model and the
//! steps run on it, one batch at a time under its write lock, which is released between batches.
//! The per-batch graphs are then removed by `drop_graphs` steps, see
//! `docs/model_stream_iterations.md`.

use crate::error::PyMaplibError;
use crate::mutexes::{
    infer_mutex, insert_mutex, map_default_batch_mutex, map_default_first_batch_mutex, map_json_mutex, map_resolved_mutex, map_triples_mutex,
    map_xml_mutex, update_mutex,
};
use crate::py_io::WriteTarget;
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
use std::io::{BufWriter, Write};
use std::sync::Arc;
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
    /// map_default: the template is made from the first batch, with all parameters optional.
    MapDefault {
        primary_key_column: String,
        graph: Option<String>,
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
        inner: &mut InnerModel,
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

type StreamWriter = BufWriter<Box<dyn Write + Send>>;

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
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug: Option<bool>,
    },
    Write {
        target: WriteTarget,
        format: RdfFormat,
        /// None writes all graphs for N-Quads.
        graph: Option<NamedGraph>,
        prefixes: Option<HashMap<String, NamedNode>>,
    },
    DropGraphs {
        graphs: Vec<NamedGraph>,
    },
}

#[pyclass(name = "ModelStream", frozen)]
pub struct PyModelStream {
    model: Py<PyModel>,
    data: Py<PyAny>,
    source: Arc<StreamSource>,
    steps: Vec<StreamStep>,
    /// Run each batch on the parent model instead of a fresh batch model.
    in_place: bool,
    /// With in_place, compact the model and collect unused dictionary entries every N batches.
    compact_every: Option<usize>,
}

impl PyModelStream {
    pub(crate) fn new(
        model: Py<PyModel>,
        data: Py<PyAny>,
        source: StreamSource,
        in_place: bool,
        compact_every: Option<usize>,
    ) -> PyResult<Self> {
        if compact_every == Some(0) {
            return Err(PyMaplibError::FunctionArgumentError(
                "compact_every should be at least 1".to_string(),
            )
            .into());
        }
        if compact_every.is_some() && !in_place {
            return Err(PyMaplibError::FunctionArgumentError(
                "compact_every requires in_place=True".to_string(),
            )
            .into());
        }
        Ok(PyModelStream {
            model,
            data,
            source: Arc::new(source),
            steps: vec![],
            in_place,
            compact_every,
        })
    }

    fn with_step(&self, py: Python<'_>, step: StreamStep) -> PyModelStream {
        let mut steps = self.steps.clone();
        steps.push(step);
        PyModelStream {
            model: self.model.clone_ref(py),
            data: self.data.clone_ref(py),
            source: self.source.clone(),
            steps,
            in_place: self.in_place,
            compact_every: self.compact_every,
        }
    }

    /// Checks the steps before anything runs: with in_place, the graphs that drop_graphs steps
    /// drop must be empty at the start, and write steps may only write graphs that a later
    /// drop_graphs step drops, or the transient graph.
    fn validate(&self, inner: &InnerModel) -> PyResult<()> {
        if !self.in_place {
            return Ok(());
        }
        for step in &self.steps {
            if let StreamStep::DropGraphs { graphs } = step {
                for g in graphs {
                    let n = inner.triplestore.graph_height(g);
                    if n > 0 {
                        return Err(PyMaplibError::FunctionArgumentError(format!(
                            "The graph {} is dropped by a drop_graphs step of this in-place stream, \
                             but already has {} triples. Drop it first with Model.drop_graphs if \
                             these are leftovers of an earlier run.",
                            g, n
                        ))
                        .into());
                    }
                }
            }
        }
        let transient = NamedGraph::transient();
        for (i, step) in self.steps.iter().enumerate() {
            if let StreamStep::Write { graph, .. } = step {
                let Some(graph) = graph else {
                    return Err(PyMaplibError::FunctionArgumentError(
                        "A write step of an in-place stream must name the graph to write, \
                         since writing all graphs would write the persistent graphs every batch"
                            .to_string(),
                    )
                    .into());
                };
                let dropped_later = self.steps[i + 1..].iter().any(|s| {
                    matches!(s, StreamStep::DropGraphs { graphs } if graphs.contains(graph))
                });
                if graph != &transient && !dropped_later {
                    return Err(PyMaplibError::FunctionArgumentError(format!(
                        "A write step of an in-place stream writes the graph {}, which no later \
                         drop_graphs step drops. It would be written again every batch. Write a \
                         per-batch graph that a later drop_graphs step drops, or the transient \
                         graph.",
                        graph
                    ))
                    .into());
                }
            }
        }
        Ok(())
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
        include_graphs=None,
        max_rows=100_000_000,
        debug=None))]
    fn infer(
        &self,
        py: Python<'_>,
        rulesets: Py<PyAny>,
        graph: Option<String>,
        max_iterations: Option<usize>,
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
        let target = WriteTarget::extract(file_path)?;
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
                target,
                format,
                graph,
                prefixes,
            },
        ))
    }

    /// Drops the graphs from the batch model (None is the default graph). With in_place, these are
    /// the per-batch graphs, removed from the model at the end of every batch.
    fn drop_graphs(
        &self,
        py: Python<'_>,
        graphs: Vec<Option<String>>,
    ) -> PyResult<PyModelStream> {
        let graphs = parse_graphs(graphs)?;
        Ok(self.with_step(py, StreamStep::DropGraphs { graphs }))
    }

    /// Runs the stream: maps each batch of the input into a fresh model (or, with in_place, into
    /// the model itself), runs the steps on it and appends its triples to the files of the write
    /// steps.
    #[instrument(skip_all)]
    fn collect(&self, py: Python<'_>) -> PyResult<()> {
        let model = self.model.bind(py).get();
        let (carrier, prefixes) = py.detach(|| -> PyResult<_> {
            let mut inner = model.inner.write().unwrap_or_else(|e| e.into_inner());
            self.validate(&inner)?;
            let carrier = if self.in_place {
                None
            } else {
                Some(empty_model_like(&mut inner, false)?)
            };
            Ok((carrier, inner.prefixes.clone()))
        })?;
        let mut state = BatchState {
            carrier,
            default_template: None,
            batches: 0,
        };
        let mut writers = open_writers(&self.steps, &prefixes)?;
        let res = self.run_batches(py, &mut state, &mut writers);
        // Hand the blank node counters back to the model, also after an error, so that later
        // streams and mappings do not reuse blank node labels already written by this one. With
        // in_place, the counters already live on the model.
        if let Some(carrier) = &state.carrier {
            py.detach(|| {
                let mut inner = model.inner.write().unwrap_or_else(|e| e.into_inner());
                inner.blank_node_counter = inner.blank_node_counter.max(carrier.blank_node_counter);
                let scope = inner
                    .triplestore
                    .blank_node_scope()
                    .max(carrier.triplestore.blank_node_scope());
                inner.triplestore.set_blank_node_scope(scope);
            });
        }
        res?;
        for w in writers.iter_mut().flatten() {
            w.flush().map_err(|x| {
                PyMaplibError::from(maplib::errors::MaplibError::FileCreateIOError(x))
            })?;
        }
        Ok(())
    }
}

impl PyModelStream {
    fn run_batches(
        &self,
        py: Python<'_>,
        state: &mut BatchState,
        writers: &mut [Option<StreamWriter>],
    ) -> PyResult<()> {
        let source = self.source.as_ref();
        let model = self.model.bind(py).get();

        if let StreamSource::Documents {
            kind,
            documents_per_batch,
            ..
        } = source
        {
            let mut run = |py: Python<'_>, docs: Vec<(usize, StringOrPathBuf)>| -> PyResult<()> {
                py.detach(|| {
                    self.run_batch(
                        model,
                        state,
                        writers,
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
                    self.run_batch(
                        model,
                        state,
                        writers,
                        BatchInput::Frame(df, types),
                    )
                })
            };
            match classify_mapping_data(self.data.bind(py), py)? {
                MappingData::Eager(df, types) => run(py, df, types)?,
                MappingData::Batched(data) => for_each_mapping_batch(&data, py, &mut run)?,
            }
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

    /// Runs one batch, on a fresh batch model, or with in_place on the model under its write lock.
    fn run_batch(
        &self,
        model: &PyModel,
        state: &mut BatchState,
        writers: &mut [Option<StreamWriter>],
        input: BatchInput,
    ) -> PyResult<()> {
        let source = self.source.as_ref();
        let steps = &self.steps;
        state.batches += 1;
        let Some(carrier) = state.carrier.as_mut() else {
            let mut inner = model.inner.write().unwrap_or_else(|e| e.into_inner());
            run_batch_steps(
                &mut inner,
                &mut state.default_template,
                source,
                steps,
                writers,
                input,
            )?;
            if let Some(n) = self.compact_every {
                if state.batches % n == 0 {
                    compact_and_collect(&mut inner)?;
                }
            }
            return Ok(());
        };
        let mut inner = empty_model_like(carrier, true)?;
        let res = run_batch_steps(
            &mut inner,
            &mut state.default_template,
            source,
            steps,
            writers,
            input,
        );
        // Hand the templates and blank node counters on to the next batch, also after an error.
        state.carrier = Some(empty_model_like(&mut inner, true)?);
        res
    }
}

/// Compacts the model and removes the dictionary entries that no graph refers to.
fn compact_and_collect(inner: &mut InnerModel) -> PyResult<()> {
    inner.compact().map_err(PyMaplibError::from)?;
    if inner.triplestore.storage_folder.is_none() {
        inner.triplestore.garbage_collect().map_err(|x| {
            PyMaplibError::from(maplib::errors::MaplibError::TriplestoreError(x))
        })?;
    }
    Ok(())
}

/// Parses graph names, where None is the default graph.
pub(crate) fn parse_graphs(graphs: Vec<Option<String>>) -> PyResult<Vec<NamedGraph>> {
    graphs
        .into_iter()
        .map(|g| {
            let g = parse_optional_named_node(g)?;
            Ok(NamedGraph::from_maybe_named_node(g.as_ref()))
        })
        .collect()
}

/// What is carried from one batch to the next.
struct BatchState {
    /// Without in_place: an empty model with the templates, prefixes, UDFs and blank node counters
    /// for the next batch.
    carrier: Option<InnerModel>,
    /// The template made by map_default from the first batch.
    default_template: Option<Template>,
    /// The number of batches started.
    batches: usize,
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
) -> PyResult<Vec<Option<StreamWriter>>> {
    let mut writers = vec![];
    for step in steps {
        if let StreamStep::Write {
            target,
            format,
            prefixes,
            ..
        } = step
        {
            let mut w = BufWriter::new(target.open()?);
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

fn run_batch_steps(
    inner: &mut InnerModel,
    default_template: &mut Option<Template>,
    source: &StreamSource,
    steps: &[StreamStep],
    writers: &mut [Option<StreamWriter>],
    input: BatchInput,
) -> PyResult<()> {
    if let Err(e) = map_batch(inner, default_template, source, input) {
        run_remaining_drops(inner, steps, 0);
        return Err(e);
    }
    run_steps(inner, steps, writers)
}

/// After a failed step, runs the drop_graphs steps from index `from` on, so that a failing batch
/// leaves no per-batch graphs behind. Errors from these are ignored in favour of the first one.
fn run_remaining_drops(inner: &mut InnerModel, steps: &[StreamStep], from: usize) {
    for step in &steps[from..] {
        if let StreamStep::DropGraphs { graphs } = step {
            let _ = inner.triplestore.drop_graphs(graphs);
        }
    }
}

/// Maps the input of one batch into the batch model.
fn map_batch(
    inner: &mut InnerModel,
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
    inner: &mut InnerModel,
    steps: &[StreamStep],
    writers: &mut [Option<StreamWriter>],
) -> PyResult<()> {
    for (i, (step, writer)) in steps.iter().zip(writers.iter_mut()).enumerate() {
        if let Err(e) = run_step(inner, step, writer) {
            run_remaining_drops(inner, steps, i + 1);
            return Err(e);
        }
    }
    Ok(())
}

fn run_step(
    inner: &mut InnerModel,
    step: &StreamStep,
    writer: &mut Option<StreamWriter>,
) -> PyResult<()> {
    {
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
                    false,
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
                include_graphs,
                max_rows,
                debug,
            } => {
                infer_mutex(
                    inner,
                    rulesets.clone(),
                    *max_iterations,
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
            StreamStep::DropGraphs { graphs } => {
                inner.triplestore.drop_graphs(graphs).map_err(|x| {
                    PyMaplibError::from(maplib::errors::MaplibError::TriplestoreError(x))
                })?;
            }
        }
    }
    Ok(())
}

fn write_batch(
    inner: &mut InnerModel,
    w: &mut StreamWriter,
    format: RdfFormat,
    graph: Option<&NamedGraph>,
    prefixes: Option<&HashMap<String, NamedNode>>,
) -> PyResult<()> {
    match format {
        RdfFormat::NQuads => inner
            .write_quads(w, graph.map(std::slice::from_ref), format, prefixes)
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
