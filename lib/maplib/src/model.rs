mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;
pub mod shacl_report_mapping;

use crate::errors::MaplibError;
use crate::model::errors::MappingError;
use cimxml_export::export::{cim_xml_write, FullModelDetails};
use cimxml_import::{cim_geometries_to_geosparql, CimPositionPoint};
use datalog::inference::{infer, InferenceResult};
use datalog::reasoning::{reason, ReasoningProfile, ReasoningResult};
use datalog::parser::parse_datalog_ruleset;
use external_sparql::federation::FederationSettings;
use file_io::remote::{RemoteLocation, RemoteOptions};
use oxrdf::{NamedNode, Term};
use oxrdfio::RdfFormat;
use polars::prelude::DataFrame;
use pyo3::{Py, PyAny};
use representation::solution_mapping::EagerSolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use shacl::{validate, ValidationReport};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use templates::as_rdf::templates_to_triples;
use templates::ast::{ConstantTermOrList, PType, Template};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use templates::MappingColumnType;
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::paging::{PageCursor, QueryPage};
use triplestore::sparql::{InsertResult, QuerySettings, UpdateResult};
use triplestore::{IndexingOptions, NewTriples, StorageOptions, Triplestore};

use chrontext::engine::{ChrontextSettings, Engine};
use datalog::ast::DatalogRuleset;
use external_sparql::{SparqlEndpoint, SparqlMethod};
use query_processing::bindings::maybe_replace_bindings;
use representation::constants::{
    LEGACY_CHRONTEXT_PREFIX_IRI, MAPLIB_PREFIX, MAPLIB_PREFIX_IRI, OTTR_TRIPLE,
};
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::{df_as_result, QuerySolutions};
use representation::prefixes::get_default_prefixes;
use representation::result::{QueryResult, QueryResultKind};
use spargebra::term::GroundTerm;
use tracing::instrument;
use triplestore::errors::TriplestoreError;
use triplestore::query_cache::{cache_plan, CachePlan, PlanContext, QueryCache, Touched};
use triplestore::triples_read::ExtendedRdfFormat;
use virtualization::insights_hub::{
    insights_hub_resource_template, InsightsHubAssetModel, INSIGHTS_HUB_NAMESPACE,
    INSIGHTS_HUB_RESOURCE,
};
use virtualization::osipi::{
    osipi_resource_template, read_asset_framework, read_pi_points, OSIPIAssetModel, OSIPIDatabase,
    OSIPIPointModel, OSIPISettings, PIPointSelection, OSIPI_NAMESPACE, OSIPI_RESOURCE,
};
use virtualization::python::VirtualizedPythonDatabase;
use virtualization::{Virtualization, VirtualizedDatabase};

/// CGMES 3.0 and CGMES 2.4.15 namespaces.
const CIM_NAMESPACES: [&str; 2] = [
    "http://iec.ch/TC57/CIM100#",
    "http://iec.ch/TC57/2013/CIM-schema-cim16#",
];

pub struct Model {
    pub blank_node_counter: usize,
    pub template_dataset: TemplateDataset,
    pub triplestore: Triplestore,
    pub indexing: IndexingOptions,
    pub prefixes: HashMap<String, NamedNode>,
    pub chrontext_settings: Option<ChrontextSettings>,
}

impl Model {
    pub fn list_udfs(&self) -> Vec<NamedNode> {
        self.triplestore.udf_registry.list_udfs()
    }

    pub fn add_graph(
        &mut self,
        other_triplestore: &Triplestore,
        source_graph: NamedGraph,
        target_graph: NamedGraph,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .add_graph(other_triplestore, source_graph, target_graph)
            .map_err(MaplibError::TriplestoreError)
    }

    /// Copies the triples of `source_graph` of `source` (this model when None) into
    /// `target_graph`. Blank nodes from another model get new labels, so that they stay
    /// distinct from the blank nodes of this model.
    pub fn add_triples(
        &mut self,
        source: Option<&Triplestore>,
        source_graph: &NamedGraph,
        target_graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .copy_triples(source, source_graph, target_graph, true)
            .map_err(MaplibError::TriplestoreError)
    }
}

#[derive(Clone, Default)]
pub struct MapOptions {
    pub graph: NamedGraph,
    pub validate_iris: bool,
}

impl MapOptions {
    pub fn from_args(graph: NamedGraph, validate_iris: Option<bool>) -> Self {
        MapOptions {
            graph,
            validate_iris: validate_iris.unwrap_or(true),
        }
    }
}

#[derive(Debug)]
struct OTTRTripleInstance {
    df: DataFrame,
    dynamic_columns: HashMap<String, MappingColumnType>,
    static_columns: HashMap<String, StaticColumn>,
}

#[derive(Clone, Debug)]
struct StaticColumn {
    constant_term: ConstantTermOrList,
    ptype: Option<PType>,
}

impl Model {
    #[instrument(skip_all)]
    pub fn new(
        template_dataset: Option<&TemplateDataset>,
        storage: StorageOptions,
        indexing: Option<IndexingOptions>,
        prefixes: Option<HashMap<String, NamedNode>>,
    ) -> Result<Model, MaplibError> {
        let indexing = indexing.unwrap_or_default();
        let template_dataset = if let Some(template_dataset) = template_dataset {
            template_dataset.clone()
        } else {
            TemplateDataset::new_empty()
        };
        let mut use_prefixes = get_default_prefixes();
        if let Some(prefixes) = prefixes {
            use_prefixes.extend(prefixes)
        };
        Ok(Model {
            blank_node_counter: 0,
            template_dataset,
            triplestore: Triplestore::new(storage, Some(indexing.clone()))
                .map_err(MaplibError::TriplestoreError)?,
            indexing,
            prefixes: use_prefixes,
            chrontext_settings: None,
        })
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        recursive: bool,
        storage: StorageOptions,
    ) -> Result<Model, MaplibError> {
        let dataset = TemplateDataset::from_folder(path, recursive, Some(&get_default_prefixes()))
            .map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage, None, None)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        storage: StorageOptions,
    ) -> Result<Model, MaplibError> {
        let dataset = TemplateDataset::from_file(path, Some(&get_default_prefixes()))
            .map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage, None, None)
    }

    pub fn from_str(s: &str, storage: StorageOptions) -> Result<Model, MaplibError> {
        let doc = document_from_str(s, Some(&get_default_prefixes()))?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage, None, None)
    }

    pub fn from_strs(ss: Vec<&str>, storage: StorageOptions) -> Result<Model, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s, Some(&get_default_prefixes()))?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::from_documents(docs).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage, None, None)
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), MaplibError> {
        self.template_dataset
            .add_template(template)
            .map_err(MaplibError::TemplateError)?;
        Ok(())
    }

    pub fn graph_size(&self, named_graph: &NamedGraph) -> usize {
        self.triplestore.graph_size(named_graph)
    }

    #[instrument(skip_all)]
    pub fn add_templates_from_string(&mut self, s: &str) -> Result<Option<NamedNode>, MaplibError> {
        let doc = document_from_str(s, Some(&self.prefixes)).map_err(MaplibError::TemplateError)?;
        let mut dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        let return_template_iri = if !dataset.templates.is_empty() {
            Some(dataset.templates.first().unwrap().signature.iri.clone())
        } else {
            None
        };
        self.template_dataset
            .prefix_map
            .extend(dataset.prefix_map.drain());
        for (_, f) in dataset.functions.drain() {
            self.template_dataset.add_function(f);
        }
        for t in dataset.templates {
            self.add_template(t)?
        }

        Ok(return_template_iri)
    }

    #[instrument(skip_all)]
    pub fn add_virtualization(
        &mut self,
        db: VirtualizedPythonDatabase,
        resources: HashMap<String, Template>,
    ) -> Result<(), MaplibError> {
        // Functions defined with def are inlined into the templates of the resources, and the
        // function calls are checked, as chrontext evaluates them without the function library.
        let mut inlined = HashMap::new();
        for (r, t) in resources {
            let t = templates::functions::inline_template_functions(
                &t,
                &self.template_dataset.functions,
            )
            .map_err(|e| {
                MaplibError::TemplateError(
                    templates::dataset::errors::TemplateError::FunctionError(e),
                )
            })?;
            inlined.insert(r, t);
        }
        let virtualized_database = Arc::new(VirtualizedDatabase::VirtualizedPythonDatabase(db));
        let virtualization = Virtualization { resources: inlined };
        // Queries over the newly virtualized predicates now read the virtualized database.
        self.triplestore.query_cache.versions.record(Touched::All);
        // Each call adds a virtualized database, and the virtualized databases are used together.
        // A resource that was given before is moved to the new virtualized database.
        self.chrontext_settings
            .get_or_insert_with(ChrontextSettings::default)
            .add_virtualization(virtualized_database, virtualization);
        Ok(())
    }

    /// Reads the element hierarchy, element templates and attributes of the PI Asset Framework
    /// through the PI Web API into `graph`, and virtualizes the time series of the attributes,
    /// so that their data points can be queried in SPARQL. The PI virtualization is added next to
    /// the other virtualizations, replacing an earlier PI virtualization.
    #[instrument(skip_all)]
    pub fn map_osipi(
        &mut self,
        settings: OSIPISettings,
        graph: &NamedGraph,
    ) -> Result<OSIPIAssetModel, MaplibError> {
        let asset_model = read_asset_framework(&settings)?;
        self.add_osipi_virtualization(settings, &asset_model.ntriples, graph)?;
        Ok(asset_model)
    }

    /// Reads the PI Points (tags) of the PI Data Archive through the data servers of the PI Web
    /// API into `graph`, without the Asset Framework, and virtualizes their recorded values, so
    /// that their data points can be queried in SPARQL. The virtualization is added to any
    /// existing ones; time series from map_osipi through the same PI Web API remain queryable.
    #[instrument(skip_all)]
    pub fn map_pi_points(
        &mut self,
        settings: OSIPISettings,
        selection: PIPointSelection,
        graph: &NamedGraph,
    ) -> Result<OSIPIPointModel, MaplibError> {
        let point_model = read_pi_points(&settings, &selection)?;
        self.add_osipi_virtualization(settings, &point_model.ntriples, graph)?;
        Ok(point_model)
    }

    fn add_osipi_virtualization(
        &mut self,
        settings: OSIPISettings,
        ntriples: &str,
        graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        let template = osipi_resource_template()?;
        let db = OSIPIDatabase::new(settings)?;
        self.reads(
            ntriples,
            ExtendedRdfFormat::Normal(RdfFormat::NTriples),
            None,
            None,
            false,
            false,
            graph,
            false,
            10_000_000,
            HashMap::new(),
        )?;
        for (k, v) in [
            ("osipi", OSIPI_NAMESPACE),
            (MAPLIB_PREFIX, MAPLIB_PREFIX_IRI),
            // Deprecated: the former namespace, kept so that existing queries still parse.
            ("chrontext", LEGACY_CHRONTEXT_PREFIX_IRI),
        ] {
            self.prefixes
                .insert(k.to_string(), NamedNode::new_unchecked(v));
        }
        self.triplestore.query_cache.versions.record(Touched::All);
        self.chrontext_settings
            .get_or_insert_with(ChrontextSettings::default)
            .add_virtualization(
                Arc::new(VirtualizedDatabase::OSIPIDatabase(db)),
                Virtualization {
                    resources: HashMap::from([(OSIPI_RESOURCE.to_string(), template)]),
                },
            );
        Ok(())
    }

    /// Adds the asset model of Siemens Insights Hub, read with
    /// `virtualization::insights_hub::read_insights_hub`, to `graph`, and virtualizes the time
    /// series of its variables, so that their data points can be queried in SPARQL. The
    /// virtualization is added to any existing ones.
    #[instrument(skip_all)]
    pub fn add_insights_hub(
        &mut self,
        asset_model: InsightsHubAssetModel,
        graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        let template = insights_hub_resource_template()?;
        self.reads(
            &asset_model.ntriples,
            ExtendedRdfFormat::Normal(RdfFormat::NTriples),
            None,
            None,
            false,
            false,
            graph,
            false,
            10_000_000,
            HashMap::new(),
        )?;
        self.add_prefixes(HashMap::from([
            (
                "insights_hub".to_string(),
                NamedNode::new_unchecked(INSIGHTS_HUB_NAMESPACE),
            ),
            (
                MAPLIB_PREFIX.to_string(),
                NamedNode::new_unchecked(MAPLIB_PREFIX_IRI),
            ),
            // Deprecated: the former namespace, kept so that existing queries still parse.
            (
                "chrontext".to_string(),
                NamedNode::new_unchecked(LEGACY_CHRONTEXT_PREFIX_IRI),
            ),
        ]));
        self.add_virtualization(
            asset_model.database,
            HashMap::from([(INSIGHTS_HUB_RESOURCE.to_string(), template)]),
        )
    }

    #[instrument(skip_all)]
    pub fn map_json_path(
        &mut self,
        path: &Path,
        graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), MaplibError> {
        let mut u8s =
            fs::read(path).map_err(|x| TriplestoreError::ReadJSONFileError(x.to_string()))?;
        let use_uuid_namespace = if let Some(uuid_namespace) = uuid_namespace {
            uuid_namespace
        } else {
            String::from(path.to_string_lossy())
        };
        self.triplestore
            .map_json(&mut u8s, Some(use_uuid_namespace), graph)
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn map_json_string(
        &mut self,
        mut p: String,
        graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), MaplibError> {
        //Safety: we are never reading this vec back to a string
        let u8s = unsafe { p.as_mut_vec() };
        self.triplestore
            .map_json(u8s, uuid_namespace, graph)
            .map_err(MaplibError::TriplestoreError)
    }

    /// Converts tabular data to RDF using CSV on the Web (CSVW) metadata.
    ///
    /// `source` and `metadata` are local paths or IRIs. `iri_mappings` maps IRI prefixes to
    /// local directories: documents with IRIs starting with such a prefix are read from that
    /// directory, and local paths below such a directory get IRIs with that prefix.
    /// Returns the SPARQL Update that was used, and the warnings.
    #[instrument(skip_all)]
    pub fn map_csvw(
        &mut self,
        source: &str,
        metadata: Option<&str>,
        graph: &NamedGraph,
        minimal: bool,
        iri_mappings: &[(String, std::path::PathBuf)],
    ) -> Result<csvw::CsvwResult, MaplibError> {
        let mut loader = csvw::loader::Loader::new();
        for (prefix, dir) in iri_mappings {
            loader.add_mapping(prefix.clone(), dir.clone());
        }
        let to_iri = |s: &str| -> Result<String, MaplibError> {
            if csvw::jsonld::is_absolute_iri(s) && !Path::new(s).exists() {
                return Ok(s.to_string());
            }
            let p = std::path::absolute(Path::new(s))
                .map_err(|x| csvw::errors::CsvwError::ReadError(s.to_string(), x.to_string()))?;
            for (prefix, dir) in iri_mappings {
                if let Ok(dir) = std::path::absolute(dir) {
                    if let Ok(rest) = p.strip_prefix(&dir) {
                        let rest = rest.to_string_lossy().replace('\\', "/");
                        return Ok(format!("{prefix}{rest}"));
                    }
                }
            }
            Ok(csvw::loader::file_iri(&p)?)
        };
        let source_iri = to_iri(source)?;
        let metadata_iri = match metadata {
            Some(m) => Some(to_iri(m)?),
            None => None,
        };
        let options = csvw::CsvwOptions {
            minimal,
            metadata: metadata_iri,
            graph: graph.clone(),
        };
        // The generated SPARQL update nests expressions deeply (percent-decoding is about 20
        // nested REPLACEs), which overflows the main thread's stack in a debug build.
        let res = on_big_stack(|| {
            csvw::csvw_to_rdf(&mut self.triplestore, &source_iri, &loader, &options)
        })?;
        Ok(res)
    }

    /// Maps the results of the logical table queries of an R2RML mapping to RDF, see
    /// <https://www.w3.org/TR/r2rml/>. `tables` holds the result of each query in
    /// `mapping.queries()`. The term maps are evaluated column-wise, and each predicate,
    /// object map and graph is added as one batch through the ottr:Triple expansion.
    /// Returns the warnings about data errors that were ignored.
    #[instrument(skip_all)]
    pub fn map_r2rml(
        &mut self,
        mapping: &r2rml::R2RMLMapping,
        tables: &HashMap<String, DataFrame>,
        options: &r2rml::R2RMLOptions,
    ) -> Result<Vec<String>, MaplibError> {
        let t0 = std::time::Instant::now();
        let (batches, warnings) = r2rml::execute(mapping, tables, options)?;
        tracing::debug!(target: "r2rml_timing", "r2rml_timing: evaluate={}", t0.elapsed().as_secs_f64());
        let t0 = std::time::Instant::now();
        // The batches have flat columns of one RDF type each, which is all the ottr:Triple
        // expansion would check, so they are added to the triplestore directly, in one call so
        // that they are encoded and indexed together.
        let flat = |types: &mut HashMap<String, RDFNodeState>, c: &str| {
            types
                .remove(c)
                .and_then(|t| t.map.into_iter().next())
                .ok_or_else(|| MaplibError::R2RMLError(r2rml::R2RMLError::MappingError(format!("no type for the {c} column"))))
        };
        let mut to_add = Vec::with_capacity(batches.len());
        for mut b in batches {
            let (subject_type, subject_cat_state) = flat(&mut b.types, representation::SUBJECT_COL_NAME)?;
            let (object_type, object_cat_state) = flat(&mut b.types, representation::OBJECT_COL_NAME)?;
            let predicate_cat_state = if b.predicate.is_none() {
                Some(flat(&mut b.types, representation::PREDICATE_COL_NAME)?.1)
            } else {
                None
            };
            to_add.push(triplestore::TriplesToAdd {
                df: b.df,
                subject_type,
                object_type,
                predicate: b.predicate,
                graph: b.graph,
                subject_cat_state,
                object_cat_state,
                predicate_cat_state,
            });
        }
        self.triplestore.add_triples_vec(to_add)?;
        tracing::debug!(target: "r2rml_timing", "r2rml_timing: expand={}", t0.elapsed().as_secs_f64());
        Ok(warnings)
    }

    #[instrument(skip_all)]
    pub fn map_df(
        &mut self,
        df: &DataFrame,
        graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .map_df(df, graph, uuid_namespace)
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn map_xml_path(
        &mut self,
        path: &Path,
        graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), MaplibError> {
        let mut u8s = fs::read(path).map_err(|x| TriplestoreError::XMLError(x.to_string()))?;
        let use_uuid_namespace = if let Some(uuid_namespace) = uuid_namespace {
            uuid_namespace
        } else {
            String::from(path.to_string_lossy())
        };
        self.triplestore
            .map_xml(&mut u8s, graph, Some(use_uuid_namespace))
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn map_xml_string(
        &mut self,
        mut p: String,
        graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), MaplibError> {
        //Safety: we are never reading this vec back to a string
        let u8s = unsafe { p.as_mut_vec() };
        self.triplestore
            .map_xml(u8s, graph, uuid_namespace)
            .map_err(MaplibError::TriplestoreError)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn read_triples(
        &mut self,
        p: &Path,
        rdf_format: Option<ExtendedRdfFormat>,
        base_iri: Option<String>,
        parallel: Option<bool>,
        checked: bool,
        strict: bool,
        graph: &NamedGraph,
        replace_graph: bool,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(graph)
        }
        self.triplestore
            .read_triples_from_path(
                p,
                rdf_format,
                base_iri,
                parallel,
                checked,
                strict,
                graph,
                &self.prefixes,
                triples_batch_size,
                known_contexts,
            )
            .map_err(MaplibError::TriplestoreError)
    }

    /// Add GeoSPARQL geometries for the cim:Location resources in the graph, built from their
    /// cim:PositionPoint coordinates and the CRS of their cim:CoordinateSystem.
    #[instrument(skip_all)]
    pub fn map_cim_geometries(&mut self, graph: &NamedGraph) -> Result<(), MaplibError> {
        let mut points = vec![];
        for cim in CIM_NAMESPACES {
            let query = format!(
                r#"PREFIX cim: <{cim}>
SELECT ?loc ?crs ?seq ?x ?y ?z WHERE {{
    ?pp cim:PositionPoint.Location ?loc .
    ?pp cim:PositionPoint.xPosition ?xv .
    ?pp cim:PositionPoint.yPosition ?yv .
    OPTIONAL {{ ?pp cim:PositionPoint.sequenceNumber ?seqv }}
    OPTIONAL {{ ?pp cim:PositionPoint.zPosition ?zv }}
    OPTIONAL {{ ?loc cim:Location.CoordinateSystem ?cs . ?cs cim:CoordinateSystem.crsUrn ?crsv }}
    FILTER(isIRI(?loc))
    BIND(STR(?xv) AS ?x)
    BIND(STR(?yv) AS ?y)
    BIND(STR(?zv) AS ?z)
    BIND(STR(?seqv) AS ?seq)
    BIND(STR(?crsv) AS ?crs)
}}"#
            );
            let res = self.query(&query, None, Some(graph), vec![], None, false, None)?;
            let QueryResultKind::Select(sm) = res.kind else {
                unreachable!("Should never happen")
            };
            let QuerySolutions {
                variables,
                solutions,
            } = df_as_result(&sm, self.triplestore.global_cats.clone());
            let idx = |name: &str| variables.iter().position(|v| v.as_str() == name);
            let (loc_i, crs_i, seq_i, x_i, y_i, z_i) = (
                idx("loc"),
                idx("crs"),
                idx("seq"),
                idx("x"),
                idx("y"),
                idx("z"),
            );
            let get = |row: &Vec<Option<Term>>, i: Option<usize>| -> Option<String> {
                match i.and_then(|i| row.get(i)).and_then(|t| t.as_ref()) {
                    Some(Term::Literal(l)) => Some(l.value().to_string()),
                    Some(Term::NamedNode(nn)) => Some(nn.as_str().to_string()),
                    _ => None,
                }
            };
            for row in &solutions {
                let Some(Some(Term::NamedNode(location))) = loc_i.and_then(|i| row.get(i)) else {
                    continue;
                };
                let (Some(x), Some(y)) = (get(row, x_i), get(row, y_i)) else {
                    continue;
                };
                let sequence_number = match get(row, seq_i) {
                    Some(s) => Some(s.trim().parse::<i64>().map_err(|_| {
                        MaplibError::CIMGeometryError(format!(
                            "Invalid sequence number {s} for position point of {location}"
                        ))
                    })?),
                    None => None,
                };
                points.push(CimPositionPoint {
                    location: location.clone(),
                    crs_urn: get(row, crs_i),
                    sequence_number,
                    x,
                    y,
                    z: get(row, z_i),
                });
            }
        }
        if points.is_empty() {
            return Ok(());
        }
        let triples = cim_geometries_to_geosparql(points).map_err(MaplibError::CIMGeometryError)?;
        let mut nt = String::new();
        for t in triples {
            nt.push_str(&t.to_string());
            nt.push_str(" .\n");
        }
        self.reads(
            &nt,
            ExtendedRdfFormat::Normal(RdfFormat::NTriples),
            None,
            Some(false),
            true,
            true,
            graph,
            false,
            10_000_000,
            HashMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_template(&mut self, p: &Path) -> Result<(), MaplibError> {
        let mut dataset = TemplateDataset::from_file(p, Some(&self.prefixes))
            .map_err(MaplibError::TemplateError)?;
        self.template_dataset
            .prefix_map
            .extend(dataset.prefix_map.drain());
        for (_, f) in dataset.functions.drain() {
            self.template_dataset.add_function(f);
        }
        for t in dataset.templates {
            self.add_template(t)?
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reads(
        &mut self,
        s: &str,
        rdf_format: ExtendedRdfFormat,
        base_iri: Option<String>,
        parallel: Option<bool>,
        checked: bool,
        strict: bool,
        graph: &NamedGraph,
        replace_graph: bool,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(graph)
        }
        self.triplestore
            .read_triples_from_string(
                s,
                rdf_format,
                base_iri,
                parallel,
                checked,
                strict,
                graph,
                &self.prefixes,
                triples_batch_size,
                known_contexts,
            )
            .map_err(MaplibError::TriplestoreError)
    }

    /// Reads triples from bytes in memory, in any format including HDT.
    #[allow(clippy::too_many_arguments)]
    pub fn read_bytes(
        &mut self,
        data: &[u8],
        rdf_format: ExtendedRdfFormat,
        base_iri: Option<String>,
        parallel: Option<bool>,
        checked: bool,
        strict: bool,
        graph: &NamedGraph,
        replace_graph: bool,
        triples_batch_size: usize,
        known_contexts: HashMap<String, String>,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(graph)
        }
        self.triplestore
            .read_triples(
                data,
                rdf_format,
                base_iri,
                parallel,
                checked,
                strict,
                graph,
                &self.prefixes,
                triples_batch_size,
                known_contexts,
            )
            .map_err(MaplibError::TriplestoreError)
    }

    /// The templates held by this model, excluding the built-in `ottr:Triple` primitive that
    /// `TemplateDataset` injects as the base case every template expands to.
    pub fn get_templates(&self) -> Vec<&Template> {
        self.template_dataset
            .templates
            .iter()
            .filter(|t| t.signature.iri.as_str() != OTTR_TRIPLE)
            .collect()
    }

    /// Materialize the model's OTTR templates into `graph` as RDF using the flattened maplib
    /// template vocabulary (prefix `mtpl`). Triples are added alongside any existing content of
    /// the graph.
    #[instrument(skip_all)]
    pub fn templates_to_graph(&mut self, graph: &NamedGraph) -> Result<(), MaplibError> {
        let triples = templates_to_triples(&self.template_dataset.templates);
        self.triplestore
            .add_triples(triples, graph)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn query(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
        bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> Result<QueryResult, MaplibError> {
        self.query_with_federation(
            query,
            parameters,
            graph,
            include_graphs,
            max_rows,
            debug_no_results,
            bindings,
            None,
        )
    }

    /// As `query`, with federation settings (SERVICE) that replace those of the triplestore,
    /// e.g. for queries to a served model.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn query_with_federation(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
        bindings: Option<&HashMap<String, GroundTerm>>,
        federation: Option<Arc<FederationSettings>>,
    ) -> Result<QueryResult, MaplibError> {
        let query_settings = QuerySettings {
            include_graphs,
            max_rows,
            strict_project: false,
            federation,
            evaluation_hook: None,
        };
        on_big_stack(|| {
            if let Some(chrontext_settings) = &self.chrontext_settings {
                let engine = Engine::new(
                    chrontext_settings,
                    &self.triplestore,
                    parameters,
                    query_settings,
                    graph.cloned(),
                    debug_no_results,
                );
                let qr = engine
                    .query_blocking(query, Some(&self.prefixes), bindings)
                    .map_err(MaplibError::ChrontextError)?;
                Ok(qr)
            } else {
                self.triplestore
                    .query(
                        query,
                        parameters,
                        &query_settings,
                        graph,
                        Some(&self.prefixes),
                        debug_no_results,
                        bindings,
                    )
                    .map_err(|x| x.into())
            }
        })
    }

    /// The query result cache of this model, shared by everything that queries it.
    pub fn query_cache(&self) -> Arc<QueryCache> {
        self.triplestore.query_cache.clone()
    }

    /// Adds prefixes, recording a change for the query cache if any prefix is new or changed.
    pub fn add_prefixes(&mut self, prefixes: HashMap<String, NamedNode>) {
        let changed = prefixes
            .iter()
            .any(|(k, v)| self.prefixes.get(k) != Some(v));
        if changed {
            self.prefixes.extend(prefixes);
            self.triplestore
                .query_cache
                .versions
                .record(Touched::Prefixes);
        }
    }

    /// What a query reads, so its result can be cached, or why it can not be. Errors parsing
    /// the query give a bypass: the query itself reports them.
    pub fn cache_plan(
        &self,
        query: &str,
        graph: Option<&NamedGraph>,
        include_graphs: &[NamedGraph],
        bindings: Option<&HashMap<String, GroundTerm>>,
    ) -> CachePlan {
        // Parsing and planning recurse like the query itself.
        on_big_stack(|| {
            let Ok(parsed) = spargebra::Query::parse(query, None, Some(&self.prefixes)) else {
                return CachePlan::Bypass("error");
            };
            let Ok(parsed) = maybe_replace_bindings(parsed, bindings) else {
                return CachePlan::Bypass("error");
            };
            let virtualized_iris = self.chrontext_settings.as_ref().map(|s| {
                let mut nns = vec![];
                for b in &s.backends {
                    for t in b.virtualization.resources.values() {
                        t.find_named_nodes(&mut nns);
                    }
                }
                nns.into_iter().collect::<HashSet<_>>()
            });
            let udfs = self.triplestore.udf_registry.list_udfs();
            cache_plan(
                &parsed,
                &PlanContext {
                    graph,
                    include_graphs,
                    virtualized_iris: virtualized_iris.as_ref(),
                    udfs: &udfs,
                },
            )
        })
    }

    /// A page of at most `page_size` solutions of a SELECT query, after the cursor `after`
    /// (None for the first page), with federation settings as in `query_with_federation`. See
    /// [`triplestore::sparql::paging`].
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn query_page(
        &self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        bindings: Option<&HashMap<String, GroundTerm>>,
        page_size: usize,
        after: Option<&PageCursor>,
        federation: Option<Arc<FederationSettings>>,
    ) -> Result<QueryPage, MaplibError> {
        if self.chrontext_settings.is_some() {
            return Err(MaplibError::SparqlError(SparqlError::PagingError(
                "queries of models with virtualized databases can not be paged yet".to_string(),
            )));
        }
        let query_settings = QuerySettings {
            include_graphs,
            max_rows,
            strict_project: false,
            federation,
            evaluation_hook: None,
        };
        on_big_stack(|| {
            self.triplestore
                .query_page(
                    query,
                    parameters,
                    &query_settings,
                    graph,
                    Some(&self.prefixes),
                    bindings,
                    page_size,
                    after,
                )
                .map_err(|x| x.into())
        })
    }

    #[instrument(skip_all)]
    pub fn query_external(
        &self,
        query: &str,
        endpoint: &str,
        method: SparqlMethod,
        bindings: Option<&HashMap<String, GroundTerm>>,
        headers: Option<&Vec<(String, String)>>,
    ) -> Result<QueryResult, MaplibError> {
        let endpoint = SparqlEndpoint::new(endpoint, method);
        let r = endpoint.query_blocking(query, Some(&self.prefixes), bindings, headers)?;
        Ok(r)
    }

    #[instrument(skip_all)]
    pub fn insert(
        &mut self,
        query: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        source_graph: &NamedGraph,
        include_graphs: Vec<NamedGraph>,
        target_graph: &NamedGraph,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<InsertResult, MaplibError> {
        let query_settings = QuerySettings {
            include_graphs,
            max_rows,
            strict_project: true,
            federation: None,
            evaluation_hook: None,
        };
        on_big_stack(|| {
            self.triplestore
                .insert(
                    query,
                    parameters,
                    &query_settings,
                    source_graph,
                    target_graph,
                    Some(&self.prefixes),
                    debug_no_results,
                )
                .map_err(|x| x.into())
        })
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, MaplibError> {
        self.update_with_federation(
            update,
            parameters,
            graph,
            include_graphs,
            max_rows,
            debug_no_results,
            None,
        )
    }

    /// As `update`, with federation settings (SERVICE and LOAD) that replace those of the
    /// triplestore, e.g. for updates sent to a served model.
    #[allow(clippy::too_many_arguments)]
    pub fn update_with_federation(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
        federation: Option<Arc<FederationSettings>>,
    ) -> Result<UpdateResult, MaplibError> {
        self.update_with_federation_keep_triples(
            update,
            parameters,
            graph,
            include_graphs,
            max_rows,
            debug_no_results,
            federation,
            false,
        )
    }

    /// As `update`, and with `keep_triples` the changes of the result also hold the triples that
    /// were inserted and deleted, not only their counts.
    #[allow(clippy::too_many_arguments)]
    pub fn update_keep_triples(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
        keep_triples: bool,
    ) -> Result<UpdateResult, MaplibError> {
        self.update_with_federation_keep_triples(
            update,
            parameters,
            graph,
            include_graphs,
            max_rows,
            debug_no_results,
            None,
            keep_triples,
        )
    }

    /// As `update_with_federation`, and with `keep_triples` the changes of the result also hold
    /// the triples that were inserted and deleted.
    #[allow(clippy::too_many_arguments)]
    pub fn update_with_federation_keep_triples(
        &mut self,
        update: &str,
        parameters: Option<&HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
        federation: Option<Arc<FederationSettings>>,
        keep_triples: bool,
    ) -> Result<UpdateResult, MaplibError> {
        let query_settings = QuerySettings {
            include_graphs,
            max_rows,
            strict_project: true,
            federation,
            evaluation_hook: None,
        };
        on_big_stack(|| {
            self.triplestore
                .update_keep_triples(
                    update,
                    parameters,
                    &query_settings,
                    graph,
                    Some(&self.prefixes),
                    debug_no_results,
                    keep_triples,
                )
                .map_err(|x| x.into())
        })
    }

    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        target_graph: &NamedGraph,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let new_triples = self
            .triplestore
            .insert_construct_result(sms, target_graph)?;
        Ok(new_triples)
    }

    pub fn write_triples<W: Write>(
        &self,
        buffer: &mut W,
        graph: &NamedGraph,
        rdf_format: RdfFormat,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<(), MaplibError> {
        let mut use_prefixes = self.prefixes.clone();
        if let Some(prefixes) = prefixes {
            use_prefixes.extend(prefixes.clone());
        }
        self.triplestore
            .write_triples(buffer, rdf_format, graph, &use_prefixes)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    /// Writes the graphs as a JSON-LD dataset: the triples of the default graph (if it is one of
    /// them) at the top level, and a graph object for each of the other graphs. Graphs that do
    /// not exist are left out.
    pub fn write_jsonld<W: Write>(
        &self,
        buffer: &mut W,
        graphs: &[NamedGraph],
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<(), MaplibError> {
        let mut use_prefixes = self.prefixes.clone();
        if let Some(prefixes) = prefixes {
            use_prefixes.extend(prefixes.clone());
        }
        let default = NamedGraph::DefaultGraph;
        let top = graphs.contains(&default).then_some(&default);
        let named: Vec<_> = graphs.iter().filter(|g| **g != default).cloned().collect();
        self.triplestore
            .write_pretty_jsonld(buffer, top, &named, &use_prefixes)
            .map_err(MaplibError::TriplestoreError)
    }

    /// Writes a dataset format (TriG or N-Quads): the given graphs that exist, or all graphs if
    /// graphs is None.
    pub fn write_quads<W: Write>(
        &self,
        buffer: &mut W,
        graphs: Option<&[NamedGraph]>,
        rdf_format: RdfFormat,
        prefixes: Option<&HashMap<String, NamedNode>>,
    ) -> Result<(), MaplibError> {
        let mut use_prefixes = self.prefixes.clone();
        if let Some(prefixes) = prefixes {
            use_prefixes.extend(prefixes.clone());
        }
        self.triplestore
            .write_quads(buffer, rdf_format, graphs, &use_prefixes)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    pub fn write_hdt<W: Write>(
        &self,
        buffer: &mut W,
        graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .write_hdt(buffer, graph)
            .map_err(MaplibError::TriplestoreError)
    }

    pub fn write_cim_xml<W: Write>(
        &mut self,
        buffer: &mut W,
        fullmodel_details: FullModelDetails,
        prefixes: HashMap<String, NamedNode>,
        graph: &NamedGraph,
        profile_graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        cim_xml_write(
            buffer,
            &mut self.triplestore,
            graph,
            profile_graph,
            prefixes,
            fullmodel_details,
        )
        .map_err(MaplibError::CIMXMLError)
    }

    fn resolve_template(&self, s: &str) -> Result<&Template, MaplibError> {
        if let Some(t) = self.template_dataset.get(s) {
            return Ok(t);
        }
        if let Some((prefix, local)) = s.split_once(':') {
            // Prefixes of the template documents take precedence over those of the model
            let nn = self
                .template_dataset
                .prefix_map
                .get(prefix)
                .or_else(|| self.prefixes.get(prefix));
            if let Some(nn) = nn {
                let possible_template_name = nn.as_str().to_string() + local;
                if let Some(t) = self.template_dataset.get(&possible_template_name) {
                    return Ok(t);
                } else {
                    return Err(MappingError::NoTemplateForTemplateNameFromPrefix(
                        possible_template_name,
                    )
                    .into());
                }
            }
        }
        Err(MappingError::TemplateNotFound(s.to_string()).into())
    }

    pub fn validate(
        &mut self,
        data_graph: &NamedGraph,
        shape_graph: &NamedGraph,
        report_graph: Option<&NamedGraph>,
        inferences_graph: Option<&NamedGraph>,
        include_details: bool,
        include_conforms: bool,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        only_shapes: Option<Vec<NamedNode>>,
        deactivate_shapes: Vec<NamedNode>,
        dry_run: bool,
        serial: bool,
        max_iterations: Option<usize>,
        debug_rules: bool,
    ) -> Result<ValidationReport, MaplibError> {
        let mut res = on_big_stack(|| {
            validate(
                &mut self.triplestore,
                data_graph,
                shape_graph,
                inferences_graph,
                include_details,
                include_conforms,
                // The size of the report is bounded by the memory limit instead.
                None,
                include_graphs,
                max_rows,
                only_shapes,
                deactivate_shapes,
                dry_run,
                Some(self.prefixes.clone()),
                serial,
                max_iterations,
                debug_rules,
            )
        })
        .map_err(MaplibError::ShaclError)?;
        if let Some(report_graph) = report_graph {
            self.map_validation_result_to_report_graph(
                &mut res,
                report_graph,
                include_details || include_conforms,
            )?;
            self.copy_result_paths(shape_graph, report_graph)?;
        }
        Ok(res)
    }

    fn truncate_graph(&mut self, graph: &NamedGraph) {
        self.triplestore.truncate(graph);
    }

    pub fn get_predicate_iris(
        &self,
        graph: &NamedGraph,
        include_graphs: &[NamedGraph],
    ) -> Result<Vec<NamedNode>, MaplibError> {
        self.triplestore
            .get_predicate_iris(graph, include_graphs)
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn get_predicate(
        &self,
        predicate: &NamedNode,
        graph: &NamedGraph,
        include_graphs: &[NamedGraph],
    ) -> Result<Vec<EagerSolutionMappings>, MaplibError> {
        let sms = self
            .triplestore
            .get_predicate_eager_solution_mappings(predicate, graph, include_graphs)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(sms)
    }

    #[instrument(skip_all)]
    pub fn create_index(&mut self, indexing: IndexingOptions) -> Result<(), MaplibError> {
        self.triplestore
            .create_index(indexing.clone())
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    pub fn infer(
        &mut self,
        rulesets: Vec<String>,
        max_iterations: Option<usize>,
        graph: Option<&NamedGraph>,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<InferenceResult, MaplibError> {
        if rulesets.is_empty() {
            return Err(MaplibError::MissingDatalogRuleset);
        }
        let mut ruleset: Option<DatalogRuleset> = None;
        for r in rulesets {
            let new_ruleset = parse_datalog_ruleset(&r, None, Some(&self.prefixes))
                .map_err(|x| MaplibError::DatalogSyntaxError(x.to_string()))?;
            if let Some(orig_ruleset) = &mut ruleset {
                orig_ruleset.extend(new_ruleset);
            } else {
                ruleset = Some(new_ruleset);
            }
        }

        let res = on_big_stack(|| {
            infer(
                &mut self.triplestore,
                graph,
                ruleset.as_ref().unwrap(),
                max_iterations,
                // The number of inferred triples is bounded by the memory limit instead.
                None,
                include_graphs,
                max_rows,
                debug_no_results,
            )
        });
        res.map_err(MaplibError::DatalogError)
    }

    /// Reasons over the graph with a built-in profile ("owl-rl", "rdfs-plus" or "rdfs"), putting
    /// the inferred triples in the target graph.
    pub fn reason(
        &mut self,
        profile: &str,
        graph: &NamedGraph,
        target_graph: &NamedGraph,
        include_graphs: Vec<NamedGraph>,
        max_rows: Option<usize>,
        max_violations: usize,
    ) -> Result<ReasoningResult, MaplibError> {
        let profile = ReasoningProfile::parse(profile).map_err(MaplibError::DatalogError)?;
        reason(
            &mut self.triplestore,
            profile,
            graph,
            target_graph,
            include_graphs,
            max_rows,
            max_violations,
        )
        .map_err(MaplibError::DatalogError)
    }

    pub fn infer_rdfs(&mut self, graph: &NamedGraph) -> Result<usize, MaplibError> {
        let res = self.triplestore.interesting_rdfs_rules(graph)?;
        Ok(res)
    }

    pub fn detach_graph(
        &mut self,
        graph: &NamedGraph,
        preserve_name: bool,
    ) -> Result<Model, MaplibError> {
        let triplestore = self.triplestore.detach_graph(graph, preserve_name)?;
        Ok(Model {
            blank_node_counter: self.blank_node_counter,
            template_dataset: self.template_dataset.clone(),
            triplestore,
            indexing: self.indexing.clone(),
            prefixes: self.prefixes.clone(),
            chrontext_settings: self.chrontext_settings.clone(),
        })
    }

    pub fn serialize_triples(
        &mut self,
        path: &Path,
        raw_db: bool,
        single_file: bool,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .serialize_triples(path, raw_db, single_file)?;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<(), MaplibError> {
        self.triplestore.compact()?;
        Ok(())
    }

    /// Serializes the model to object storage, see `Triplestore::serialize_triples_remote`.
    pub fn serialize_triples_remote(
        &mut self,
        location: &RemoteLocation,
        raw_db: bool,
        single_file: bool,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .serialize_triples_remote(location, raw_db, single_file, None)?;
        Ok(())
    }

    /// Deserializes a model from object storage, see `Triplestore::deserialize_triples_remote`.
    pub fn deserialize_triples_remote(
        url: &str,
        options: &RemoteOptions,
        storage: StorageOptions,
    ) -> Result<Self, MaplibError> {
        let triplestore = Triplestore::deserialize_triples_remote(url, options, storage)?;
        Ok(Self::from_triplestore(triplestore))
    }

    pub fn deserialize_triples(path: &Path, storage: StorageOptions) -> Result<Self, MaplibError> {
        let triplestore = Triplestore::deserialize_triples(path, storage)?;
        Ok(Self::from_triplestore(triplestore))
    }

    fn from_triplestore(triplestore: Triplestore) -> Self {
        Model {
            blank_node_counter: 0,
            template_dataset: Default::default(),
            triplestore,
            indexing: Default::default(),
            prefixes: Default::default(),
            chrontext_settings: None,
        }
    }

    pub fn add_udf(
        &mut self,
        iri: NamedNode,
        f: Py<PyAny>,
        output_type: BaseRDFNodeType,
        input_types: Option<Vec<BaseRDFNodeType>>,
    ) {
        self.triplestore.add_udf(iri, f, output_type, input_types);
    }
}

/// Queries are processed recursively, so long chains of UNION or || and deeply nested
/// expressions can need far more stack than a thread has (8 MB on the main thread).
/// The work is moved to a larger stack on the same thread, so Python signal checks still work.
pub fn on_big_stack<R>(f: impl FnOnce() -> R) -> R {
    const RED_ZONE: usize = 64 * 1024 * 1024;
    const STACK_SIZE: usize = 1024 * 1024 * 1024;
    stacker::maybe_grow(RED_ZONE, STACK_SIZE, f)
}
