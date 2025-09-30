mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;

use crate::errors::MaplibError;
use crate::mapping::errors::MappingError;
use cimxml::export::{cim_xml_write, FullModelDetails};
use datalog::inference::infer;
use datalog::parser::parse_datalog_ruleset;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::DataFrame;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeState;
use shacl::{validate, ValidationReport};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use templates::ast::{ConstantTermOrList, PType, Template};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use templates::MappingColumnType;
use triplestore::errors::TriplestoreError;
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::QueryResult;
use triplestore::{IndexingOptions, NewTriples, Triplestore};

use datalog::ast::DatalogRuleset;
use tracing::instrument;

pub struct Model {
    pub template_dataset: TemplateDataset,
    pub base_triplestore: Triplestore,
    pub triplestores_map: HashMap<NamedNode, Triplestore>,
    pub blank_node_counter: usize,
    pub default_template_counter: usize,
    pub indexing: IndexingOptions,
}

#[derive(Clone, Default)]
pub struct MapOptions {
    pub graph: Option<NamedNode>,
    pub validate_iris: bool,
}

impl MapOptions {
    pub fn from_args(graph: Option<NamedNode>, validate_iris: Option<bool>) -> Self {
        MapOptions {
            graph,
            validate_iris: validate_iris.unwrap_or(true),
        }
    }
}

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
        storage_folder: Option<String>,
        indexing: Option<IndexingOptions>,
    ) -> Result<Model, MaplibError> {
        let use_disk = storage_folder.is_some();
        let indexing = if use_disk {
            if let Some(indexing) = indexing {
                indexing
            } else {
                IndexingOptions {
                    object_sort_all: false,
                    object_sort_some: None,
                    fts_path: None,
                }
            }
        } else {
            indexing.unwrap_or_default()
        };
        let template_dataset = if let Some(template_dataset) = template_dataset {
            template_dataset.clone()
        } else {
            TemplateDataset::new_empty()?
        };
        Ok(Model {
            template_dataset,
            base_triplestore: Triplestore::new(storage_folder, Some(indexing.clone()))
                .map_err(MaplibError::TriplestoreError)?,
            triplestores_map: Default::default(),
            blank_node_counter: 0,
            default_template_counter: 0,
            indexing,
        })
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        recursive: bool,
        storage_folder: Option<String>,
    ) -> Result<Model, MaplibError> {
        let dataset =
            TemplateDataset::from_folder(path, recursive).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        storage_folder: Option<String>,
    ) -> Result<Model, MaplibError> {
        let dataset = TemplateDataset::from_file(path).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None)
    }

    pub fn from_str(s: &str, storage_folder: Option<String>) -> Result<Model, MaplibError> {
        let doc = document_from_str(s)?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None)
    }

    pub fn from_strs(ss: Vec<&str>, storage_folder: Option<String>) -> Result<Model, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s)?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::from_documents(docs).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None)
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), MaplibError> {
        self.template_dataset
            .add_template(template)
            .map_err(MaplibError::TemplateError)?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn add_templates_from_string(&mut self, s: &str) -> Result<Option<NamedNode>, MaplibError> {
        let doc = document_from_str(s).map_err(MaplibError::TemplateError)?;
        let mut dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        let return_template_iri = if !dataset.templates.is_empty() {
            Some(
                dataset
                    .templates
                    .first()
                    .unwrap()
                    .signature
                    .template_name
                    .clone(),
            )
        } else {
            None
        };
        self.template_dataset
            .prefix_map
            .extend(dataset.prefix_map.drain());
        for t in dataset.templates {
            self.add_template(t)?
        }

        Ok(return_template_iri)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn read_triples(
        &mut self,
        p: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        parallel: Option<bool>,
        checked: bool,
        graph: Option<NamedNode>,
        replace_graph: bool,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .read_triples_from_path(p, rdf_format, base_iri, transient, parallel, checked)
            .map_err(MaplibError::TriplestoreError)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reads(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: Option<bool>,
        checked: bool,
        graph: Option<NamedNode>,
        replace_graph: bool,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .read_triples_from_string(s, rdf_format, base_iri, transient, parallel, checked)
            .map_err(MaplibError::TriplestoreError)
    }

    pub fn get_triplestore(&mut self, graph: &Option<NamedNode>) -> &mut Triplestore {
        if let Some(graph) = graph {
            if !self.triplestores_map.contains_key(graph) {
                self.triplestores_map.insert(
                    graph.clone(),
                    Triplestore::new(None, Some(self.indexing.clone())).unwrap(),
                );
            }
            self.triplestores_map.get_mut(graph).unwrap()
        } else {
            &mut self.base_triplestore
        }
    }

    #[instrument(skip_all)]
    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<NamedNode>,
        streaming: bool,
        include_transient: bool,
    ) -> Result<QueryResult, MaplibError> {
        let use_triplestore = self.get_triplestore(&graph);
        use_triplestore
            .query(query, parameters, streaming, include_transient)
            .map_err(|x| x.into())
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<NamedNode>,
        streaming: bool,
        include_transient: bool,
    ) -> Result<(), MaplibError> {
        let use_triplestore = self.get_triplestore(&graph);
        use_triplestore
            .update(update, parameters, streaming, include_transient)
            .map_err(|x| x.into())
    }

    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        transient: bool,
        target_graph: Option<NamedNode>,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let use_triplestore = self.get_triplestore(&target_graph);
        let new_triples = use_triplestore.insert_construct_result(sms, transient)?;
        Ok(new_triples)
    }

    pub fn write_triples<W: Write>(
        &mut self,
        buffer: &mut W,
        graph: Option<NamedNode>,
        rdf_format: RdfFormat,
    ) -> Result<(), MaplibError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .write_triples(buffer, rdf_format)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    pub fn write_cim_xml<W: Write>(
        &mut self,
        buffer: &mut W,
        fullmodel_details: FullModelDetails,
        prefixes: HashMap<String, NamedNode>,
        graph: Option<NamedNode>,
        profile_graph: NamedNode,
    ) -> Result<(), MaplibError> {
        let mut profile_triplestore = self.triplestores_map.remove(&profile_graph).unwrap();
        let triplestore = self.get_triplestore(&graph);
        let res = cim_xml_write(
            buffer,
            triplestore,
            &mut profile_triplestore,
            prefixes,
            fullmodel_details,
        )
        .map_err(MaplibError::CIMXMLError);
        self.triplestores_map
            .insert(profile_graph, profile_triplestore);
        res
    }

    pub fn write_native_parquet(
        &mut self,
        path: &str,
        graph: Option<NamedNode>,
    ) -> Result<(), MaplibError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .write_native_parquet(Path::new(path))
            .map_err(MaplibError::TriplestoreError)
    }

    fn resolve_template(&self, s: &str) -> Result<&Template, MaplibError> {
        if let Some(t) = self.template_dataset.get(s) {
            return Ok(t);
        } else {
            let mut split_colon = s.split(':');
            let prefix_maybe = split_colon.next();
            if let Some(prefix) = prefix_maybe {
                if let Some(nn) = self.template_dataset.prefix_map.get(prefix) {
                    let possible_template_name = nn.as_str().to_string()
                        + split_colon.collect::<Vec<&str>>().join(":").as_str();
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
        }
        Err(MappingError::TemplateNotFound(s.to_string()).into())
    }

    pub fn validate(
        &mut self,
        shape_graph: &NamedNode,
        include_details: bool,
        include_conforms: bool,
        streaming: bool,
        max_shape_constraint_results: Option<usize>,
        include_transient: bool,
        only_shapes: Option<Vec<NamedNode>>,
        deactivate_shapes: Vec<NamedNode>,
        dry_run: bool,
    ) -> Result<ValidationReport, MaplibError> {
        let (shape_graph, mut shape_triplestore) = if let Some((shape_graph, shape_triplestore)) =
            self.triplestores_map.remove_entry(shape_graph)
        {
            (shape_graph, shape_triplestore)
        } else {
            return Err(
                TriplestoreError::GraphDoesNotExist(shape_graph.as_str().to_string()).into(),
            );
        };
        let res = validate(
            &mut self.base_triplestore,
            &mut shape_triplestore,
            include_details,
            include_conforms,
            streaming,
            max_shape_constraint_results,
            include_transient,
            only_shapes,
            deactivate_shapes,
            dry_run,
        );
        self.triplestores_map.insert(shape_graph, shape_triplestore);
        res.map_err(|x| x.into())
    }

    fn truncate_graph(&mut self, graph: &Option<NamedNode>) {
        self.get_triplestore(graph).truncate();
    }

    pub fn get_predicate_iris(
        &mut self,
        graph: &Option<NamedNode>,
        include_transient: bool,
    ) -> Result<Vec<NamedNode>, MaplibError> {
        let triplestore = self.get_triplestore(graph);
        Ok(triplestore.get_predicate_iris(include_transient))
    }

    #[instrument(skip_all)]
    pub fn get_predicate(
        &mut self,
        predicate: &NamedNode,
        graph: Option<NamedNode>,
        include_transient: bool,
    ) -> Result<Vec<EagerSolutionMappings>, MaplibError> {
        let triplestore = self.get_triplestore(&graph);
        let sms = triplestore
            .get_predicate_eager_solution_mappings(predicate, include_transient)
            .map_err(|x| MaplibError::SparqlError(x))?;
        Ok(sms)
    }

    #[instrument(skip_all)]
    pub fn create_index(
        &mut self,
        indexing: IndexingOptions,
        all: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MaplibError> {
        if all {
            for t in self.triplestores_map.values_mut() {
                t.create_index(indexing.clone())
                    .map_err(MaplibError::TriplestoreError)?;
            }
            self.base_triplestore
                .create_index(indexing.clone())
                .map_err(MaplibError::TriplestoreError)?;
            self.indexing = indexing;
        } else {
            let triplestore = self.get_triplestore(&graph);
            triplestore
                .create_index(indexing)
                .map_err(MaplibError::TriplestoreError)?;
        }
        Ok(())
    }

    pub fn infer(
        &mut self,
        rulesets: Vec<String>,
        max_iterations: Option<usize>,
    ) -> Result<Option<HashMap<NamedNode, EagerSolutionMappings>>, MaplibError> {
        if rulesets.is_empty() {
            return Err(MaplibError::MissingDatalogRuleset);
        }
        let mut ruleset: Option<DatalogRuleset> = None;
        for r in rulesets {
            let new_ruleset = parse_datalog_ruleset(&r, None)
                .map_err(|x| MaplibError::DatalogSyntaxError(x.to_string()))?;
            if let Some(orig_ruleset) = &mut ruleset {
                orig_ruleset.extend(new_ruleset);
            } else {
                ruleset = Some(new_ruleset);
            }
        }

        let res = infer(
            &mut self.base_triplestore,
            ruleset.as_ref().unwrap(),
            max_iterations,
        );
        Ok(res.map_err(|x| MaplibError::DatalogError(x))?)
    }
}
