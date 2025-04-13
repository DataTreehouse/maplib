mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;

use crate::errors::MaplibError;
use crate::mapping::errors::MappingError;
use cimxml::cim_xml_write;
use datalog::ast::DatalogRuleset;
use datalog::inference::infer;
use datalog::parser::parse_datalog_ruleset;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::DataFrame;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
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
    pub ruleset: Option<DatalogRuleset>,
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
            graph,
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
                .map_err(MaplibError::TriplestoreError)?,
            triplestores_map: Default::default(),
            blank_node_counter: 0,
            default_template_counter: 0,
            indexing,
            ruleset: None,
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
        let doc = document_from_str(s).map_err(MaplibError::TemplateError)?;
        let dataset =
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
        cim_prefix: NamedNode,
        profile_graph: NamedNode,
        fullmodel_details: HashMap<String, String>,
        graph: Option<NamedNode>,
    ) -> Result<(), MaplibError> {
        let mut profile_triplestore = self.triplestores_map.remove(&profile_graph).unwrap();
        let triplestore = self.get_triplestore(&graph);
        let res = cim_xml_write(
            buffer,
            triplestore,
            &mut profile_triplestore,
            &cim_prefix,
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
        max_shape_results: Option<usize>,
        folder_path: Option<&PathBuf>,
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
            max_shape_results,
            folder_path,
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

    pub fn get_predicate(
        &mut self,
        predicate: &NamedNode,
        graph: Option<NamedNode>,
        include_transient: bool,
    ) -> Result<Vec<EagerSolutionMappings>, MaplibError> {
        let triplestore = self.get_triplestore(&graph);
        triplestore
            .index_unindexed()
            .map_err(SparqlError::IndexingError)?;
        triplestore
            .get_predicate_eager_solution_mappings(predicate, include_transient)
            .map_err(|x| x.into())
    }

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

    pub fn add_ruleset(&mut self, datalog_ruleset: &str) -> Result<(), MaplibError> {
        let ruleset = parse_datalog_ruleset(datalog_ruleset, None)
            .map_err(|x| MaplibError::DatalogSyntaxError(x.to_string()))?;
        println!("Display debug {:?}", ruleset);

        if let Some(existing_ruleset) = &mut self.ruleset {
            existing_ruleset.extend(ruleset)
        } else {
            self.ruleset = Some(ruleset);
        }
        Ok(())
    }

    pub fn drop_ruleset(&mut self) {
        self.ruleset = None;
    }

    pub fn infer(
        &mut self,
        insert: bool,
    ) -> Result<Option<HashMap<NamedNode, EagerSolutionMappings>>, MaplibError> {
        if let Some(ruleset) = self.ruleset.take() {
            let res = infer(&mut self.base_triplestore, &ruleset, insert);
            match res {
                Ok(o) => Ok(o),
                Err(e) => {
                    // Put it back.. this is done to be able to make a mutable borrow of the base triplestore
                    self.ruleset = Some(ruleset);
                    Err(MaplibError::DatalogError(e))
                }
            }
        } else {
            todo!("Make an error!!")
        }
    }
}
