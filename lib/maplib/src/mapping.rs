mod constant_terms;
pub mod default;
pub mod errors;
pub mod expansion;
mod validation_inference;

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
use std::path::Path;
use templates::ast::{ConstantTermOrList, PType, Template};
use templates::dataset::TemplateDataset;
use templates::document::document_from_str;
use templates::MappingColumnType;
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::QueryResult;
use triplestore::Triplestore;

pub struct Mapping {
    pub template_dataset: TemplateDataset,
    pub base_triplestore: Triplestore,
    pub triplestores_map: HashMap<NamedNode, Triplestore>,
    use_caching: bool,
    pub blank_node_counter: usize,
}

#[derive(Clone, Default)]
pub struct ExpandOptions {
    pub unique_subsets: Option<Vec<Vec<String>>>,
}

struct OTTRTripleInstance {
    df: DataFrame,
    dynamic_columns: HashMap<String, MappingColumnType>,
    static_columns: HashMap<String, StaticColumn>,
    has_unique_subset: bool,
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
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        #[allow(clippy::match_single_binding)]
        match env_logger::try_init() {
            _ => {}
        };

        let use_caching = caching_folder.is_some();
        Ok(Mapping {
            template_dataset: template_dataset.clone(),
            base_triplestore: Triplestore::new(caching_folder)
                .map_err(MappingError::TriplestoreError)?,
            triplestores_map: Default::default(),
            use_caching,
            blank_node_counter: 0,
        })
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset = TemplateDataset::from_folder(path).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset = TemplateDataset::from_file(path).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_str(s: &str, caching_folder: Option<String>) -> Result<Mapping, MaplibError> {
        let doc = document_from_str(s)?;
        let dataset =
            TemplateDataset::from_documents(vec![doc]).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_strs(
        ss: Vec<&str>,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s)?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::from_documents(docs).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn add_template(&mut self, template: Template) -> Result<(), MaplibError> {
        self.template_dataset
            .add_template(template)
            .map_err(|x| MaplibError::TemplateError(x))?;
        Ok(())
    }

    pub fn read_triples(
        &mut self,
        p: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
        deduplicate: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
        triplestore
            .read_triples_from_path(
                p,
                rdf_format,
                base_iri,
                transient,
                parallel,
                checked,
                deduplicate,
            )
            .map_err(MappingError::TriplestoreError)
    }

    pub fn read_triples_string(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        parallel: bool,
        checked: bool,
        deduplicate: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
        triplestore
            .read_triples_from_string(
                s,
                rdf_format,
                base_iri,
                transient,
                parallel,
                checked,
                deduplicate,
            )
            .map_err(MappingError::TriplestoreError)
    }

    pub fn get_triplestore(&mut self, graph: Option<NamedNode>) -> &mut Triplestore {
        if let Some(graph) = graph {
            if !self.triplestores_map.contains_key(&graph) {
                self.triplestores_map
                    .insert(graph.clone(), Triplestore::new(None).unwrap());
            }
            self.triplestores_map.get_mut(&graph).unwrap()
        } else {
            &mut self.base_triplestore
        }
    }

    pub fn query(
        &mut self,
        query: &str,
        parameters: &Option<HashMap<String, EagerSolutionMappings>>,
        graph: Option<NamedNode>,
    ) -> Result<QueryResult, SparqlError> {
        let use_triplestore = self.get_triplestore(graph);
        use_triplestore.query(query, parameters)
    }

    pub fn insert_construct_result(
        &mut self,
        dfs: Vec<(DataFrame, HashMap<String, RDFNodeType>)>,
        transient: bool,
        target_graph: Option<NamedNode>,
    ) -> Result<(), SparqlError> {
        let use_triplestore = self.get_triplestore(target_graph);
        use_triplestore.insert_construct_result(dfs, transient)
    }

    pub fn write_n_triples(
        &mut self,
        buffer: &mut dyn Write,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
        triplestore.write_n_triples_all_dfs(buffer, 1024).unwrap();
        Ok(())
    }

    pub fn write_native_parquet(
        &mut self,
        path: &str,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
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
    ) -> Result<ValidationReport, ShaclError> {
        let (shape_graph, mut shape_triplestore) =
            self.triplestores_map.remove_entry(&shape_graph).unwrap();
        match {
            validate(
                &mut self.base_triplestore,
                &mut shape_triplestore,
                include_details,
                include_conforms,
            )
        } {
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
}
