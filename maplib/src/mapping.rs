mod constant_terms;
pub mod default;
pub mod errors;
mod validation_inference;

use crate::ast::{
    ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Signature, StottrTerm,
    StottrVariable, Template,
};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_str;
use crate::errors::MaplibError;
use crate::mapping::constant_terms::{constant_blank_node_to_series, constant_to_expr};
use crate::mapping::errors::MappingError;
use crate::templates::TemplateDataset;
use log::debug;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use polars::lazy::prelude::{col, Expr};
use polars::prelude::{lit, DataFrame, IntoLazy, NamedFrom, Series};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use shacl::errors::ShaclError;
use shacl::{validate, ValidationReport};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use triplestore::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use triplestore::sparql::errors::SparqlError;
use triplestore::sparql::QueryResult;
use triplestore::{TriplesToAdd, Triplestore};
use uuid::Uuid;

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
    dynamic_columns: HashMap<String, PrimitiveColumn>,
    static_columns: HashMap<String, StaticColumn>,
    has_unique_subset: bool,
}

#[derive(Clone, Debug)]
struct StaticColumn {
    constant_term: ConstantTerm,
    ptype: Option<PType>,
}

#[derive(Clone, Debug)]
pub struct PrimitiveColumn {
    pub rdf_node_type: RDFNodeType,
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
        let dataset = TemplateDataset::new(vec![doc]).map_err(MaplibError::TemplateError)?;
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
        let dataset = TemplateDataset::new(docs).map_err(MaplibError::TemplateError)?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn read_triples(
        &mut self,
        p: &Path,
        rdf_format: Option<RdfFormat>,
        base_iri: Option<String>,
        transient: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
        triplestore
            .read_triples_from_path(p, rdf_format, base_iri, transient)
            .map_err(MappingError::TriplestoreError)
    }

    pub fn read_triples_string(
        &mut self,
        s: &str,
        rdf_format: RdfFormat,
        base_iri: Option<String>,
        transient: bool,
        graph: Option<NamedNode>,
    ) -> Result<(), MappingError> {
        let triplestore = self.get_triplestore(graph);
        triplestore
            .read_triples_from_string(s, rdf_format, base_iri, transient)
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

    pub fn expand(
        &mut self,
        template: &str,
        df: Option<DataFrame>,
        options: ExpandOptions,
    ) -> Result<MappingReport, MappingError> {
        let now = Instant::now();
        let target_template = self.resolve_template(template)?.clone();
        let target_template_name = target_template.signature.template_name.as_str().to_string();

        let columns = self.validate_infer_dataframe_columns(&target_template.signature, &df)?;
        let ExpandOptions {
            unique_subsets: unique_subsets_opt,
        } = options;
        let unique_subsets = if let Some(unique_subsets) = unique_subsets_opt {
            unique_subsets
        } else {
            vec![]
        };
        let call_uuid = Uuid::new_v4().to_string();

        if self.use_caching && df.is_some() {
            let df = df.unwrap();
            let n_50_mb = (df.estimated_size() / 50_000_000) + 1;
            let chunk_size = df.height() / n_50_mb;
            let mut offset = 0i64;
            loop {
                let to_row = min(df.height(), offset as usize + chunk_size);
                let df_slice = df.slice_par(offset, to_row);
                offset += chunk_size as i64;
                let (result_vec, new_blank_node_counter) = self._expand(
                    0,
                    0,
                    self.blank_node_counter,
                    &target_template_name,
                    Some(df_slice),
                    &target_template.signature,
                    columns.clone(),
                    HashMap::new(),
                    unique_subsets.clone(),
                )?;
                self.process_results(result_vec, &call_uuid, new_blank_node_counter)?;
                debug!("Finished processing {} rows", to_row);
                if offset >= df.height() as i64 {
                    break;
                }
            }
        } else {
            let (result_vec, new_blank_node_counter) = self._expand(
                0,
                0,
                self.blank_node_counter,
                &target_template_name,
                df,
                &target_template.signature,
                columns,
                HashMap::new(),
                unique_subsets,
            )?;
            self.process_results(result_vec, &call_uuid, new_blank_node_counter)?;
            debug!("Expansion took {} seconds", now.elapsed().as_secs_f32());
        }
        Ok(MappingReport {})
    }

    pub fn validate(
        &mut self,
        shape_graph: &NamedNode,
        include_details: bool,
    ) -> Result<ValidationReport, ShaclError> {
        let (shape_graph, mut shape_triplestore) =
            self.triplestores_map.remove_entry(&shape_graph).unwrap();
        match {
            validate(
                &mut self.base_triplestore,
                &mut shape_triplestore,
                include_details,
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

    fn _expand(
        &self,
        layer: usize,
        pattern_num: usize,
        mut blank_node_counter: usize,
        name: &str,
        df: Option<DataFrame>,
        calling_signature: &Signature,
        dynamic_columns: HashMap<String, PrimitiveColumn>,
        static_columns: HashMap<String, StaticColumn>,
        unique_subsets: Vec<Vec<String>>,
    ) -> Result<(Vec<OTTRTripleInstance>, usize), MappingError> {
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                if let Some(df) = df {
                    Ok((
                        vec![OTTRTripleInstance {
                            df,
                            dynamic_columns,
                            static_columns,
                            has_unique_subset: !unique_subsets.is_empty(),
                        }],
                        blank_node_counter,
                    ))
                } else {
                    panic!("Reached OTTR_TRIPLE basis rule without df, this should never happen")
                }
            } else {
                let mut expand_params_vec = vec![];
                let colnames: HashSet<_> = if let Some(df) = &df {
                    df.get_column_names()
                        .iter()
                        .map(|x| x.to_string())
                        .collect()
                } else {
                    Default::default()
                };
                for i in &template.pattern_list {
                    let mut instance_series = vec![];
                    let vs = get_variable_names(i);
                    if let Some(df) = &df {
                        for v in vs {
                            if colnames.contains(v) {
                                instance_series.push(df.column(v).unwrap().clone());
                            }
                        }
                    }
                    expand_params_vec.push((i, instance_series));
                }

                let use_df_height = if let Some(df) = &df { df.height() } else { 1 };

                let results: Vec<_> = expand_params_vec
                    .par_drain(..)
                    .enumerate()
                    .map(|(i, (instance, series_vec))| {
                        let target_template = self
                            .template_dataset
                            .get(instance.template_name.as_str())
                            .unwrap();
                        if let Some((
                            instance_df,
                            instance_dynamic_columns,
                            instance_static_columns,
                            new_unique_subsets,
                            updated_blank_node_counter,
                        )) = create_remapped(
                            self.blank_node_counter,
                            layer,
                            pattern_num,
                            instance,
                            &target_template.signature,
                            calling_signature,
                            series_vec,
                            &dynamic_columns,
                            &static_columns,
                            &unique_subsets,
                            use_df_height,
                        )? {
                            Ok::<_, MappingError>(Some(self._expand(
                                layer + 1,
                                i,
                                updated_blank_node_counter,
                                instance.template_name.as_str(),
                                Some(instance_df),
                                &target_template.signature,
                                instance_dynamic_columns,
                                instance_static_columns,
                                new_unique_subsets,
                            )?))
                        } else {
                            Ok(None)
                        }
                    })
                    .collect();
                let mut results_ok = vec![];
                for r in results {
                    if let Some((r, new_counter)) = r? {
                        results_ok.push(r);
                        blank_node_counter = max(blank_node_counter, new_counter);
                    }
                }

                Ok((flatten(results_ok), blank_node_counter))
            }
        } else {
            Err(MappingError::TemplateNotFound(name.to_string()))
        }
    }

    fn process_results(
        &mut self,
        mut result_vec: Vec<OTTRTripleInstance>,
        call_uuid: &String,
        new_blank_node_counter: usize,
    ) -> Result<(), MappingError> {
        let now = Instant::now();
        let triples: Vec<_> = result_vec.par_drain(..).map(create_triples).collect();
        let mut ok_triples = vec![];
        for t in triples {
            ok_triples.push(t?);
        }
        let mut all_triples_to_add = vec![];
        for (mut df, subj_rdf_node_type, obj_rdf_node_type, verb, has_unique_subset) in ok_triples {
            let mut coltypes_names = vec![
                (&subj_rdf_node_type, SUBJECT_COL_NAME),
                (&obj_rdf_node_type, OBJECT_COL_NAME),
            ];
            if verb.is_none() {
                coltypes_names.push((&RDFNodeType::IRI, VERB_COL_NAME));
            }
            let mut fix_iris = vec![];
            for (coltype, colname) in coltypes_names {
                if coltype == &RDFNodeType::IRI {
                    let nonnull = df.column(colname).unwrap().str().unwrap().first_non_null();
                    if let Some(i) = nonnull {
                        let first_iri = df.column(colname).unwrap().str().unwrap().get(i).unwrap();
                        {
                            if first_iri.starts_with('<') {
                                fix_iris.push(colname);
                            }
                        }
                    }
                }
            }
            let mut lf = df.lazy();
            for colname in fix_iris {
                lf = lf.with_column(
                    col(colname)
                        .str()
                        .strip_prefix(lit("<"))
                        .str()
                        .strip_suffix(lit(">")),
                );
            }
            df = lf.collect().unwrap();

            all_triples_to_add.push(TriplesToAdd {
                df,
                subject_type: subj_rdf_node_type,
                object_type: obj_rdf_node_type,
                static_verb_column: verb,
                has_unique_subset,
            });
        }
        self.base_triplestore
            .add_triples_vec(all_triples_to_add, call_uuid, false)
            .map_err(MappingError::TriplestoreError)?;

        self.blank_node_counter = new_blank_node_counter;
        debug!(
            "Result processing took {} seconds",
            now.elapsed().as_secs_f32()
        );
        Ok(())
    }
}

fn get_variable_names(i: &Instance) -> Vec<&String> {
    let mut out_vars = vec![];
    for a in &i.argument_list {
        get_term_names(&mut out_vars, &a.term)
    }
    out_vars
}

fn get_variable_name<'a>(out_vars: &mut Vec<&'a String>, var: &'a StottrVariable) {
    out_vars.push(&var.name);
}

fn get_term_names<'a>(out_vars: &mut Vec<&'a String>, term: &'a StottrTerm) {
    if let StottrTerm::Variable(v) = term {
        get_variable_name(out_vars, v);
    } else if let StottrTerm::List(l) = term {
        for t in l {
            get_term_names(out_vars, t);
        }
    }
}

fn create_triples(
    i: OTTRTripleInstance,
) -> Result<(DataFrame, RDFNodeType, RDFNodeType, Option<NamedNode>, bool), MappingError> {
    let OTTRTripleInstance {
        mut df,
        mut dynamic_columns,
        static_columns,
        has_unique_subset,
    } = i;

    let mut expressions = vec![];

    let mut verb = None;

    if dynamic_columns.is_empty() && df.height() == 0 {
        df.with_column(Series::new("dummy_column", vec!["dummy_row"]))
            .unwrap();
    }

    for (k, sc) in static_columns {
        if k == VERB_COL_NAME {
            if let ConstantTerm::Constant(ConstantLiteral::Iri(nn)) = &sc.constant_term {
                verb = Some(nn.clone());
            } else {
                return Err(MappingError::InvalidPredicateConstant(
                    sc.constant_term.clone(),
                ));
            }
        } else {
            let (expr, mapped_column) =
                create_dynamic_expression_from_static(&k, &sc.constant_term, &sc.ptype)?;

            expressions.push(expr.alias(&k));
            dynamic_columns.insert(k, mapped_column);
        }
    }

    let mut lf = df.lazy();
    for e in expressions {
        lf = lf.with_column(e);
    }

    let mut keep_cols = vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)];
    if verb.is_none() {
        keep_cols.push(col(VERB_COL_NAME));
    }
    lf = lf.select(keep_cols.as_slice());
    let df = lf.collect().expect("Collect problem");
    let PrimitiveColumn {
        rdf_node_type: subj_rdf_node_type,
    } = dynamic_columns.remove(SUBJECT_COL_NAME).unwrap();
    let PrimitiveColumn {
        rdf_node_type: obj_rdf_node_type,
    } = dynamic_columns.remove(OBJECT_COL_NAME).unwrap();
    Ok((
        df,
        subj_rdf_node_type,
        obj_rdf_node_type,
        verb,
        has_unique_subset,
    ))
}

fn create_dynamic_expression_from_static(
    column_name: &str,
    constant_term: &ConstantTerm,
    ptype: &Option<PType>,
) -> Result<(Expr, PrimitiveColumn), MappingError> {
    let (mut expr, _, rdf_node_type) = constant_to_expr(constant_term, ptype)?;
    let mapped_column = PrimitiveColumn { rdf_node_type };
    expr = expr.alias(column_name);
    Ok((expr, mapped_column))
}

fn create_series_from_blank_node_constant(
    layer: usize,
    pattern_num: usize,
    blank_node_counter: usize,
    column_name: &str,
    constant_term: &ConstantTerm,
    n_rows: usize,
) -> Result<(Series, PrimitiveColumn), MappingError> {
    let (mut series, _, rdf_node_type) = constant_blank_node_to_series(
        layer,
        pattern_num,
        blank_node_counter,
        constant_term,
        n_rows,
    )?;
    series.rename(column_name);
    let mapped_column = PrimitiveColumn { rdf_node_type };
    Ok((series, mapped_column))
}

fn create_remapped(
    blank_node_counter: usize,
    layer: usize,
    pattern_num: usize,
    instance: &Instance,
    signature: &Signature,
    calling_signature: &Signature,
    mut series_vec: Vec<Series>,
    dynamic_columns: &HashMap<String, PrimitiveColumn>,
    constant_columns: &HashMap<String, StaticColumn>,
    unique_subsets: &Vec<Vec<String>>,
    input_df_height: usize,
) -> Result<
    Option<(
        DataFrame,
        HashMap<String, PrimitiveColumn>,
        HashMap<String, StaticColumn>,
        Vec<Vec<String>>,
        usize,
    )>,
    MappingError,
> {
    let now = Instant::now();
    let mut new_dynamic_columns = HashMap::new();
    let mut new_constant_columns = HashMap::new();
    let mut new_series = vec![];

    let mut new_dynamic_from_constant = vec![];
    let mut to_expand = vec![];
    let mut expressions = vec![];
    let mut existing = vec![];
    let mut new = vec![];
    let mut rename_map: HashMap<&String, Vec<&String>> = HashMap::new();
    let mut out_blank_node_counter = blank_node_counter;

    for (original, target) in instance
        .argument_list
        .iter()
        .zip(signature.parameter_list.iter())
    {
        let target_colname = &target.stottr_variable.name;
        if original.list_expand {
            to_expand.push(target_colname.clone());
        }
        match &original.term {
            StottrTerm::Variable(v) => {
                if let Some(c) = dynamic_columns.get(&v.name) {
                    if let Some(target_names) = rename_map.get_mut(&&v.name) {
                        target_names.push(target_colname)
                    } else {
                        rename_map.insert(&v.name, vec![target_colname]);
                    }

                    existing.push(&v.name);
                    new.push(target_colname);
                    new_dynamic_columns.insert(target_colname.clone(), c.clone());
                } else if let Some(c) = constant_columns.get(&v.name) {
                    new_constant_columns.insert(target_colname.clone(), c.clone());
                } else {
                    if let Some(calling_formal_arg) = calling_signature
                        .parameter_list
                        .iter()
                        .find(|x| x.stottr_variable.name == v.name)
                    {
                        if target.optional {
                            continue;
                        } else if calling_formal_arg.optional {
                            return Ok(None);
                        }
                    }
                    return Err(MappingError::UnknownVariableError(v.name.clone()));
                }
            }
            StottrTerm::ConstantTerm(ct) => {
                if ct.has_blank_node() {
                    let (series, primitive_column) = create_series_from_blank_node_constant(
                        layer,
                        pattern_num,
                        blank_node_counter,
                        target_colname,
                        ct,
                        input_df_height,
                    )?;
                    new_series.push(series);
                    new_dynamic_columns.insert(target_colname.clone(), primitive_column);
                    new_dynamic_from_constant.push(target_colname);
                    out_blank_node_counter =
                        max(out_blank_node_counter, blank_node_counter + input_df_height);
                } else if original.list_expand {
                    let (expr, primitive_column) =
                        create_dynamic_expression_from_static(target_colname, ct, &target.ptype)?;
                    expressions.push(expr);
                    new_dynamic_columns.insert(target_colname.clone(), primitive_column);
                    new_dynamic_from_constant.push(target_colname);
                } else {
                    let static_column = StaticColumn {
                        constant_term: ct.clone(),
                        ptype: target.ptype.clone(),
                    };
                    new_constant_columns.insert(target_colname.clone(), static_column);
                }
            }
            StottrTerm::List(_l) => {
                todo!()
            }
        }
    }

    for s in &mut series_vec {
        let sname = s.name().to_string();
        s.rename(rename_map.get_mut(&sname).unwrap().pop().unwrap());
    }
    for s in new_series {
        series_vec.push(s);
    }
    let mut lf = DataFrame::new(series_vec).unwrap().lazy();

    for expr in expressions {
        lf = lf.with_column(expr);
    }
    let new_column_expressions: Vec<Expr> = new
        .iter()
        .chain(new_dynamic_from_constant.iter())
        .map(|x| col(x))
        .collect();
    lf = lf.select(new_column_expressions.as_slice());

    let mut new_unique_subsets = vec![];
    if let Some(le) = &instance.list_expander {
        let to_expand_cols: Vec<Expr> = to_expand.iter().map(|x| col(x)).collect();
        match le {
            ListExpanderType::Cross => {
                for c in to_expand_cols {
                    lf = lf.explode(vec![c]);
                }
            }
            ListExpanderType::ZipMin => {
                lf = lf.explode(to_expand_cols.clone());
                lf = lf.drop_nulls(Some(to_expand_cols));
            }
            ListExpanderType::ZipMax => {
                lf = lf.explode(to_expand_cols);
            }
        }
        //Todo: List expanders for constant terms..
    } else {
        for unique_subset in unique_subsets {
            if unique_subset.iter().all(|x| existing.contains(&x)) {
                let mut new_subset = vec![];
                for x in unique_subset.iter() {
                    new_subset.push(
                        new.get(existing.iter().position(|e| e == &x).unwrap())
                            .unwrap()
                            .to_string(),
                    );
                }
                new_unique_subsets.push(new_subset);
            }
        }
    }
    debug!(
        "Creating remapped took {} seconds",
        now.elapsed().as_secs_f32()
    );
    Ok(Some((
        lf.collect().unwrap(),
        new_dynamic_columns,
        new_constant_columns,
        new_unique_subsets,
        out_blank_node_counter,
    )))
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
