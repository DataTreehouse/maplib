mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;

use crate::errors::MaplibError;
use crate::model::errors::MappingError;
use cimxml::export::{cim_xml_write, FullModelDetails};
use datalog::inference::{infer, InferenceResult};
use datalog::parser::parse_datalog_ruleset;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::prelude::DataFrame;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeState;
use shacl::{infer_shacl, validate, ShaclInferenceResult, ValidationReport};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use templates::ast::{ConstantTermOrList, PType, Template};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use templates::MappingColumnType;
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::{QueryResult, QuerySettings, UpdateResult};
use triplestore::{IndexingOptions, NewTriples, Triplestore};

use datalog::ast::DatalogRuleset;
use representation::constants::{FX_PREFIX, FX_PREFIX_IRI, XYZ_PREFIX, XYZ_PREFIX_IRI};
use representation::dataset::NamedGraph;
use representation::prefixes::get_default_prefixes;
use tracing::instrument;
use triplestore::errors::TriplestoreError;

pub struct Model {
    pub template_dataset: TemplateDataset,
    pub triplestore: Triplestore,
    pub blank_node_counter: usize,
    pub default_template_counter: usize,
    pub indexing: IndexingOptions,
    pub prefixes: HashMap<String, NamedNode>,
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
        prefixes: Option<HashMap<String, NamedNode>>,
    ) -> Result<Model, MaplibError> {
        let use_disk = storage_folder.is_some();
        let indexing = if use_disk {
            if let Some(indexing) = indexing {
                indexing
            } else {
                IndexingOptions::default()
            }
        } else {
            indexing.unwrap_or_default()
        };
        let template_dataset = if let Some(template_dataset) = template_dataset {
            template_dataset.clone()
        } else {
            TemplateDataset::new_empty()?
        };
        let mut use_prefixes = get_default_prefixes();
        if let Some(prefixes) = prefixes {
            use_prefixes.extend(prefixes)
        };
        Ok(Model {
            template_dataset,
            triplestore: Triplestore::new(storage_folder, Some(indexing.clone()))
                .map_err(MaplibError::TriplestoreError)?,
            blank_node_counter: 0,
            default_template_counter: 0,
            indexing,
            prefixes: use_prefixes,
        })
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        recursive: bool,
        storage_folder: Option<String>,
    ) -> Result<Model, MaplibError> {
        let dataset = TemplateDataset::from_folder(path, recursive, Some(&get_default_prefixes()))
            .map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None, None)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        storage_folder: Option<String>,
    ) -> Result<Model, MaplibError> {
        let dataset = TemplateDataset::from_file(path, Some(&get_default_prefixes()))
            .map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None, None)
    }

    pub fn from_str(s: &str, storage_folder: Option<String>) -> Result<Model, MaplibError> {
        let doc = document_from_str(s, Some(&get_default_prefixes()))?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None, None)
    }

    pub fn from_strs(ss: Vec<&str>, storage_folder: Option<String>) -> Result<Model, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s, Some(&get_default_prefixes()))?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::from_documents(docs).map_err(MaplibError::TemplateError)?;
        Model::new(Some(&dataset), storage_folder, None, None)
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), MaplibError> {
        self.template_dataset
            .add_template(template)
            .map_err(MaplibError::TemplateError)?;
        Ok(())
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
        for t in dataset.templates {
            self.add_template(t)?
        }

        Ok(return_template_iri)
    }

    #[instrument(skip_all)]
    pub fn map_json_path(
        &mut self,
        path: &Path,
        prefix: Option<&NamedNode>,
        graph: &NamedGraph,
        transient: bool,
    ) -> Result<(), MaplibError> {
        self.add_json_prefixes();
        let mut u8s =
            fs::read(path).map_err(|x| TriplestoreError::ReadJSONFileError(x.to_string()))?;

        self.triplestore
            .map_json(&mut u8s, prefix, graph, transient)
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn map_json_string(
        &mut self,
        mut p: String,
        prefix: Option<&NamedNode>,
        graph: &NamedGraph,
        transient: bool,
    ) -> Result<(), MaplibError> {
        self.add_json_prefixes();
        //Safety: we are never reading this vec back to a string
        let u8s = unsafe { p.as_mut_vec() };
        self.triplestore
            .map_json(u8s, prefix, graph, transient)
            .map_err(MaplibError::TriplestoreError)
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
        graph: &NamedGraph,
        replace_graph: bool,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        self.triplestore
            .read_triples_from_path(
                p,
                rdf_format,
                base_iri,
                transient,
                parallel,
                checked,
                graph,
                &self.prefixes,
            )
            .map_err(MaplibError::TriplestoreError)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_template(&mut self, p: &Path) -> Result<(), MaplibError> {
        let mut dataset = TemplateDataset::from_file(p, Some(&self.prefixes))
            .map_err(MaplibError::TemplateError)?;
        self.template_dataset
            .prefix_map
            .extend(dataset.prefix_map.drain());
        for t in dataset.templates {
            self.add_template(t)?
        }
        Ok(())
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
        graph: &NamedGraph,
        replace_graph: bool,
    ) -> Result<(), MaplibError> {
        if replace_graph {
            self.truncate_graph(&graph)
        }
        self.triplestore
            .read_triples_from_string(
                s,
                rdf_format,
                base_iri,
                transient,
                parallel,
                checked,
                graph,
                &self.prefixes,
            )
            .map_err(MaplibError::TriplestoreError)
    }

    #[instrument(skip_all)]
    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        streaming: bool,
        include_transient: bool,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<QueryResult, MaplibError> {
        let query_settings = QuerySettings {
            include_transient,
            max_rows,
            strict_project: false,
        };
        self.triplestore
            .query(
                query,
                parameters,
                streaming,
                &query_settings,
                graph,
                Some(&self.prefixes),
                debug_no_results,
            )
            .map_err(|x| x.into())
    }

    pub fn update(
        &mut self,
        update: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<&NamedGraph>,
        streaming: bool,
        include_transient: bool,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<UpdateResult, MaplibError> {
        let query_settings = QuerySettings {
            include_transient,
            max_rows,
            strict_project: false,
        };
        self.triplestore
            .update(
                update,
                parameters,
                streaming,
                &query_settings,
                graph,
                Some(&self.prefixes),
                debug_no_results,
            )
            .map_err(|x| x.into())
    }

    pub fn insert_construct_result(
        &mut self,
        sms: Vec<(EagerSolutionMappings, Option<NamedNode>)>,
        transient: bool,
        target_graph: &NamedGraph,
    ) -> Result<Vec<NewTriples>, SparqlError> {
        let new_triples =
            self.triplestore
                .insert_construct_result(sms, transient, &target_graph)?;
        Ok(new_triples)
    }

    pub fn write_triples<W: Write>(
        &mut self,
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

    pub fn write_cim_xml<W: Write>(
        &mut self,
        buffer: &mut W,
        fullmodel_details: FullModelDetails,
        prefixes: HashMap<String, NamedNode>,
        graph: &NamedGraph,
        profile_graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        let res = cim_xml_write(
            buffer,
            &mut self.triplestore,
            graph,
            profile_graph,
            prefixes,
            fullmodel_details,
        )
        .map_err(MaplibError::CIMXMLError);
        res
    }

    pub fn write_native_parquet(
        &mut self,
        path: &str,
        graph: &NamedGraph,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .write_native_parquet(Path::new(path), graph)
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
        data_graph: &NamedGraph,
        shape_graph: &NamedGraph,
        include_details: bool,
        include_conforms: bool,
        streaming: bool,
        max_shape_constraint_results: Option<usize>,
        include_transient: bool,
        max_rows: Option<usize>,
        only_shapes: Option<Vec<NamedNode>>,
        deactivate_shapes: Vec<NamedNode>,
        dry_run: bool,
        serial: bool,
    ) -> Result<ValidationReport, MaplibError> {
        let res = validate(
            &mut self.triplestore,
            data_graph,
            shape_graph,
            include_details,
            include_conforms,
            streaming,
            max_shape_constraint_results,
            include_transient,
            max_rows,
            only_shapes,
            deactivate_shapes,
            dry_run,
            Some(self.prefixes.clone()),
            serial,
        );
        res.map_err(|x| x.into())
    }

    fn truncate_graph(&mut self, graph: &NamedGraph) {
        self.triplestore.truncate(graph);
    }

    pub fn get_predicate_iris(
        &mut self,
        graph: &NamedGraph,
        include_transient: bool,
    ) -> Result<Vec<NamedNode>, MaplibError> {
        Ok(self
            .triplestore
            .get_predicate_iris(include_transient, graph)
            .map_err(|x| MaplibError::TriplestoreError(x))?)
    }

    #[instrument(skip_all)]
    pub fn get_predicate(
        &mut self,
        predicate: &NamedNode,
        graph: &NamedGraph,
        include_transient: bool,
    ) -> Result<Vec<EagerSolutionMappings>, MaplibError> {
        let sms = self
            .triplestore
            .get_predicate_eager_solution_mappings(predicate, include_transient, graph)
            .map_err(|x| MaplibError::TriplestoreError(x))?;
        Ok(sms)
    }

    #[instrument(skip_all)]
    pub fn create_index(
        &mut self,
        indexing: IndexingOptions,
        graph: Option<&NamedGraph>,
    ) -> Result<(), MaplibError> {
        self.triplestore
            .create_index(indexing.clone(), graph)
            .map_err(MaplibError::TriplestoreError)?;
        Ok(())
    }

    pub fn infer(
        &mut self,
        rulesets: Vec<String>,
        max_iterations: Option<usize>,
        max_results: Option<usize>,
        graph: Option<&NamedGraph>,
        include_transient: bool,
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

        let res = infer(
            &mut self.triplestore,
            graph,
            ruleset.as_ref().unwrap(),
            max_iterations,
            max_results,
            include_transient,
            max_rows,
            debug_no_results,
        );
        Ok(res.map_err(|x| MaplibError::DatalogError(x))?)
    }

    pub fn infer_shacl(
        &mut self,
        data_graph: &NamedGraph,
        shape_graph: &NamedGraph,
        streaming: bool,
        include_transient: bool,
        max_iterations: Option<usize>,
        max_results: Option<usize>,
        max_rows: Option<usize>,
        debug_no_results: bool,
    ) -> Result<ShaclInferenceResult, MaplibError> {
        let res = infer_shacl(
            &mut self.triplestore,
            data_graph,
            shape_graph,
            streaming,
            max_iterations,
            max_results,
            include_transient,
            max_rows,
            debug_no_results,
        );
        Ok(res.map_err(|x| MaplibError::ShaclError(x))?)
    }

    pub fn detach_graph(
        &mut self,
        graph: &NamedGraph,
        preserve_name: bool,
    ) -> Result<Model, MaplibError> {
        let triplestore = self.triplestore.detach_graph(graph, preserve_name)?;
        Ok(Model {
            template_dataset: self.template_dataset.clone(),
            triplestore,
            blank_node_counter: self.blank_node_counter,
            default_template_counter: self.default_template_counter,
            indexing: self.indexing.clone(),
            prefixes: self.prefixes.clone(),
        })
    }

    fn add_json_prefixes(&mut self) {
        self.prefixes.insert(
            XYZ_PREFIX.to_string(),
            NamedNode::new_unchecked(XYZ_PREFIX_IRI),
        );
        self.prefixes.insert(
            FX_PREFIX.to_string(),
            NamedNode::new_unchecked(FX_PREFIX_IRI),
        );
    }
}
