mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;

use crate::errors::MaplibError;
use crate::mapping::errors::MappingError;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::DataFrame;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use shacl::errors::ShaclError;
use shacl::{validate, ValidationReport};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use templates::ast::{ConstantTermOrList, PType, Template};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use templates::MappingColumnType;
use triplestore::errors::TriplestoreError;
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::QueryResult;
use triplestore::{IndexingOptions, NewTriples, Triplestore};

pub struct Mapping {
    pub template_dataset: TemplateDataset,
    pub base_triplestore: Triplestore,
    pub triplestores_map: HashMap<NamedNode, Triplestore>,
    pub blank_node_counter: usize,
    pub default_template_counter: usize,
    pub indexing: IndexingOptions,
}

#[derive(Clone, Default)]
pub struct ExpandOptions {
    pub graph: Option<NamedNode>,
    pub validate_iris: bool,
    pub delay_index: bool,
}

impl ExpandOptions {
    pub fn from_args(
        graph: Option<NamedNode>,
        validate_iris: Option<bool>,
        delay_index: Option<bool>,
    ) -> Self {
        ExpandOptions {
            graph: graph,
            validate_iris: validate_iris.unwrap_or(true),
            delay_index: delay_index.unwrap_or(true),
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

#[derive(Debug, PartialEq)]
pub struct MappingReport {}

impl Mapping {
    pub fn new(
        template_dataset: &TemplateDataset,
        storage_folder: Option<String>,
        indexing: Option<IndexingOptions>,
    ) -> Result<Mapping, MaplibError> {
        #[allow(clippy::match_single_binding)]
        match env_logger::try_init() {
            _ => {}
        };

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
        Ok(Mapping {
            template_dataset: template_dataset.clone(),
            base_triplestore: Triplestore::new(storage_folder, Some(indexing.clone()))
                .map_err(MappingError::TriplestoreError)?,
            triplestores_map: Default::default(),
            blank_node_counter: 0,
            default_template_counter: 0,
            indexing,
        })
    }

    pub fn index_unindexed(&mut self, graph: &Option<NamedNode>) -> Result<(), TriplestoreError> {
        let t = self.get_triplestore(graph);
        t.index_unindexed()
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        recursive: bool,
        storage_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset =
            TemplateDataset::from_folder(path, recursive).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, storage_folder, None)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        storage_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset = TemplateDataset::from_file(path).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, storage_folder, None)
    }

    pub fn from_str(s: &str, storage_folder: Option<String>) -> Result<Mapping, MaplibError> {
        let doc = document_from_str(s)?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, storage_folder, None)
    }

    pub fn from_strs(
        ss: Vec<&str>,
        storage_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s)?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::from_documents(docs).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, storage_folder, None)
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), MaplibError> {
        self.template_dataset
            .add_template(template)
            .map_err(MaplibError::TemplateError)?;
        Ok(())
    }

    pub fn add_templates_from_string(&mut self, s: &str) -> Result<Option<NamedNode>, MaplibError> {
        let doc = document_from_str(s).map_err(|x| MaplibError::TemplateError(x))?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        let return_template_iri = if !dataset.templates.is_empty() {
            Some(
                dataset
                    .templates
                    .get(0)
                    .unwrap()
                    .signature
                    .template_name
                    .clone(),
            )
        } else {
            None
        };
        for t in dataset.templates {
            self.add_template(t)?
        }
        Ok(return_template_iri)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_triples(
        &mut self,
        p: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
        graph: Option<NamedNode>,
        replace_graph: bool,
    ) -> Result<(), MappingError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .read_triples_from_path(p, rdf_format, base_iri, transient, parallel, checked)
            .map_err(MappingError::TriplestoreError)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_triples_string(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
        graph: Option<NamedNode>,
        replace_graph: bool,
    ) -> Result<(), MappingError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .read_triples_from_string(s, rdf_format, base_iri, transient, parallel, checked)
            .map_err(MappingError::TriplestoreError)
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

    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<NamedNode>,
        streaming: bool,
    ) -> Result<QueryResult, SparqlError> {
        let use_triplestore = self.get_triplestore(&graph);
        use_triplestore.query(query, parameters, streaming)
    }

    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        transient: bool,
        target_graph: Option<NamedNode>,
        delay_index: bool,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let use_triplestore = self.get_triplestore(&target_graph);
        let new_triples = use_triplestore.insert_construct_result(sms, transient, delay_index)?;
        Ok(new_triples)
    }

    pub fn write_triples<W: Write>(
        &mut self,
        buffer: &mut W,
        graph: Option<NamedNode>,
        rdf_format: RdfFormat,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore.write_triples(buffer, rdf_format).unwrap();
        Ok(())
    }

    pub fn write_native_parquet(
        &mut self,
        path: &str,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .write_native_parquet(Path::new(path))
            .map_err(MappingError::TriplestoreError)
    }

    fn resolve_template(&self, s: &str) -> Result<&Template, MappingError> {
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
                        ));
                    }
                }
            }
        }
        Err(MappingError::TemplateNotFound(s.to_string()))
    }

    pub fn validate(
        &mut self,
        shape_graph: &NamedNode,
        include_details: bool,
        include_conforms: bool,
        streaming: bool,
        max_shape_results: Option<usize>,
        folder_path: Option<&PathBuf>,
    ) -> Result<ValidationReport, ShaclError> {
        let (shape_graph, mut shape_triplestore) =
            self.triplestores_map.remove_entry(shape_graph).unwrap();
        match validate(
            &mut self.base_triplestore,
            &mut shape_triplestore,
            include_details,
            include_conforms,
            streaming,
            max_shape_results,
            folder_path,
        ) {
            Ok(vr) => {
                self.triplestores_map.insert(shape_graph, shape_triplestore);
                Ok(vr)
            }
            Err(e) => {
                self.triplestores_map.insert(shape_graph, shape_triplestore);
                Err(e)
            }
        }
    }

    fn truncate_graph(&mut self, graph: &Option<NamedNode>) {
        self.get_triplestore(graph).truncate();
    }

    pub fn get_predicate_iris(
        &mut self,
        graph: &Option<NamedNode>,
        include_transient: bool,
    ) -> Result<Vec<NamedNode>, SparqlError> {
        let triplestore = self.get_triplestore(graph);
        Ok(triplestore.get_predicate_iris(include_transient))
    }

    pub fn get_predicate(
        &mut self,
        predicate: &NamedNode,
        graph: Option<NamedNode>,
        include_transient: bool,
    ) -> Result<Vec<EagerSolutionMappings>, SparqlError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore.get_predicate_eager_solution_mappings(predicate, include_transient)
    }

    pub fn create_index(
        &mut self,
        indexing: IndexingOptions,
        all: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        if all {
            for t in self.triplestores_map.values_mut() {
                t.create_index(indexing.clone())
                    .map_err(MappingError::TriplestoreError)?;
            }
            self.base_triplestore
                .create_index(indexing.clone())
                .map_err(MappingError::TriplestoreError)?;
            self.indexing = indexing;
        } else {
            let triplestore = self.get_triplestore(&graph);
            triplestore
                .create_index(indexing)
                .map_err(MappingError::TriplestoreError)?;
        }
        Ok(())
    }
}
