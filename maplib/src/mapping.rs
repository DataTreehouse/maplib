mod constant_terms;
pub mod default;
pub mod errors;
mod validation_inference;

use crate::ast::{
    ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Signature, StottrTerm,
    Template,
};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_str;
use crate::errors::MaplibError;
use crate::mapping::constant_terms::constant_to_expr;
use crate::mapping::errors::MappingError;
use crate::templates::TemplateDataset;
use log::debug;
use oxrdf::Triple;
use polars::lazy::prelude::{col, Expr};
use polars::prelude::{DataFrame, IntoLazy};
use polars_core::series::Series;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use representation::RDFNodeType;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use triplestore::{TriplesToAdd, Triplestore};
use uuid::Uuid;

pub struct Mapping {
    template_dataset: TemplateDataset,
    pub triplestore: Triplestore,
    use_caching: bool,
}

pub struct ExpandOptions {
    pub language_tags: Option<HashMap<String, String>>,
    pub unique_subsets: Option<Vec<Vec<String>>>,
}

struct OTTRTripleInstance {
    df: DataFrame,
    dynamic_columns: HashMap<String, PrimitiveColumn>,
    static_columns: HashMap<String, StaticColumn>,
    has_unique_subset: bool,
}

#[derive(Clone)]
struct StaticColumn {
    constant_term: ConstantTerm,
    ptype: Option<PType>,
}

impl Default for ExpandOptions {
    fn default() -> Self {
        ExpandOptions {
            language_tags: None,
            unique_subsets: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrimitiveColumn {
    pub rdf_node_type: RDFNodeType,
    pub language_tag: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct MappingReport {}

impl Mapping {
    pub fn new(
        template_dataset: &TemplateDataset,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        match env_logger::try_init() {
            _ => {}
        }
        let use_caching = caching_folder.is_some();
        Ok(Mapping {
            template_dataset: template_dataset.clone(),
            triplestore: Triplestore::new(caching_folder)
                .map_err(|x| MappingError::TriplestoreError(x))?,
            use_caching,
        })
    }

    pub fn from_folder<P: AsRef<Path>>(
        path: P,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset =
            TemplateDataset::from_folder(path).map_err(|x| MaplibError::TemplateError(x))?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let dataset =
            TemplateDataset::from_file(path).map_err(|x| MaplibError::TemplateError(x))?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_str(s: &str, caching_folder: Option<String>) -> Result<Mapping, MaplibError> {
        let doc = document_from_str(s.into())?;
        let dataset = TemplateDataset::new(vec![doc]).map_err(|x| MaplibError::TemplateError(x))?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn from_strs(
        ss: Vec<&str>,
        caching_folder: Option<String>,
    ) -> Result<Mapping, MaplibError> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s.into())?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::new(docs).map_err(|x| MaplibError::TemplateError(x))?;
        Mapping::new(&dataset, caching_folder)
    }

    pub fn write_n_triples(&mut self, buffer: &mut dyn Write) -> Result<(), MappingError> {
        self.triplestore
            .write_n_triples_all_dfs(buffer, 1024)
            .unwrap();
        Ok(())
    }

    pub fn write_native_parquet(&mut self, path: &str) -> Result<(), MappingError> {
        self.triplestore
            .write_native_parquet(Path::new(path))
            .map_err(|x| MappingError::TriplestoreError(x))
    }

    pub fn export_oxrdf_triples(&mut self) -> Result<Vec<Triple>, MappingError> {
        self.triplestore
            .export_oxrdf_triples()
            .map_err(|x| MappingError::TriplestoreError(x))
    }

    fn resolve_template(&self, s: &str) -> Result<&Template, MappingError> {
        if let Some(t) = self.template_dataset.get(s) {
            return Ok(t);
        } else {
            let mut split_colon = s.split(":");
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
        df: DataFrame,
        options: ExpandOptions,
    ) -> Result<MappingReport, MappingError> {
        let now = Instant::now();
        let target_template = self.resolve_template(template)?.clone();
        let target_template_name = target_template.signature.template_name.as_str().to_string();
        let columns =
            self.validate_infer_dataframe_columns(&target_template.signature, &df, &options)?;
        let ExpandOptions {
            language_tags: _,
            unique_subsets: unique_subsets_opt,
        } = options;
        let unique_subsets = if let Some(unique_subsets) = unique_subsets_opt {
            unique_subsets
        } else {
            vec![]
        };
        let call_uuid = Uuid::new_v4().to_string();

        if self.use_caching {
            let n_50_mb = (df.estimated_size() / 50_000_000) + 1;
            let chunk_size = df.height() / n_50_mb;
            let mut offset = 0i64;
            loop {
                let to_row = min(df.height(), offset as usize + chunk_size);
                let df_slice = df.slice_par(offset, to_row);
                offset += chunk_size as i64;
                let result_vec = self._expand(
                    &target_template_name,
                    df_slice,
                    columns.clone(),
                    HashMap::new(),
                    unique_subsets.clone(),
                )?;
                self.process_results(result_vec, &call_uuid)?;
                debug!("Finished processing {} rows", to_row);
                if offset >= df.height() as i64 {
                    break;
                }
            }
        } else {
            let result_vec = self._expand(
                &target_template_name,
                df,
                columns,
                HashMap::new(),
                unique_subsets,
            )?;
            self.process_results(result_vec, &call_uuid)?;
            debug!("Expansion took {} seconds", now.elapsed().as_secs_f32());
        }
        Ok(MappingReport {})
    }

    fn _expand(
        &self,
        name: &str,
        mut df: DataFrame,
        dynamic_columns: HashMap<String, PrimitiveColumn>,
        static_columns: HashMap<String, StaticColumn>,
        unique_subsets: Vec<Vec<String>>,
    ) -> Result<Vec<OTTRTripleInstance>, MappingError> {
        //At this point, the lf should have columns with names appropriate for the template to be instantiated (named_node).
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                Ok(vec![OTTRTripleInstance {
                    df,
                    dynamic_columns,
                    static_columns,
                    has_unique_subset: !unique_subsets.is_empty(),
                }])
            } else {
                let now = Instant::now();

                let mut expand_params_vec = vec![];
                let colnames:HashSet<_> = df.get_column_names().iter().map(|x|x.to_string()).collect();
                for i in &template.pattern_list {
                    let mut instance_series = vec![];
                    let vs = get_variable_names(i);
                    for v in vs {
                        if colnames.contains(v) {
                            instance_series.push(df.column(v).unwrap().clone());
                        }
                    }
                    expand_params_vec.push((i, instance_series));
                }

                debug!("Cloning args took {} seconds", now.elapsed().as_secs_f64());

                let results: Vec<Result<Vec<OTTRTripleInstance>, MappingError>> = expand_params_vec
                    .par_drain(..)
                    .map(|(i, series_vec)| {
                        let target_template =
                            self.template_dataset.get(i.template_name.as_str()).unwrap();
                        let (
                            instance_df,
                            instance_dynamic_columns,
                            instance_static_columns,
                            new_unique_subsets,
                        ) = create_remapped(
                            i,
                            &target_template.signature,
                            series_vec,
                            &dynamic_columns,
                            &static_columns,
                            &unique_subsets,
                        )?;

                        self._expand(
                            i.template_name.as_str(),
                            instance_df,
                            instance_dynamic_columns,
                            instance_static_columns,
                            new_unique_subsets,
                        )
                    })
                    .collect();
                let mut results_ok = vec![];
                for r in results {
                    results_ok.push(r?)
                }

                Ok(flatten(results_ok))
            }
        } else {
            Err(MappingError::TemplateNotFound(name.to_string()))
        }
    }

    fn process_results(
        &mut self,
        mut result_vec: Vec<OTTRTripleInstance>,
        call_uuid: &String,
    ) -> Result<(), MappingError> {
        let now = Instant::now();
        let triples: Vec<
            Result<(DataFrame, RDFNodeType, Option<String>, Option<String>, bool), MappingError>,
        > = result_vec
            .par_drain(..)
            .map(|i| create_triples(i))
            .collect();
        let mut ok_triples = vec![];
        for t in triples {
            ok_triples.push(t?);
        }
        let mut all_triples_to_add = vec![];
        for (df, rdf_node_type, language_tag, verb, has_unique_subset) in ok_triples {
            all_triples_to_add.push(TriplesToAdd {
                df,
                object_type: rdf_node_type,
                language_tag,
                static_verb_column: verb,
                has_unique_subset,
            });
        }
        self.triplestore
            .add_triples_vec(all_triples_to_add, call_uuid)
            .map_err(|x| MappingError::TriplestoreError(x))?;

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
        if let StottrTerm::Variable(v) = &a.term {
            out_vars.push(&v.name);
        } else if let StottrTerm::List(..) = &a.term {
            todo!();
        }
    }
    out_vars
}

fn create_triples(
    i: OTTRTripleInstance,
) -> Result<(DataFrame, RDFNodeType, Option<String>, Option<String>, bool), MappingError> {
    let OTTRTripleInstance {
        df,
        mut dynamic_columns,
        static_columns,
        has_unique_subset,
    } = i;

    let mut expressions = vec![];

    let mut verb = None;
    for (k, sc) in static_columns {
        if k == "verb" {
            if let ConstantTerm::Constant(c) = &sc.constant_term {
                if let ConstantLiteral::IRI(nn) = c {
                    verb = Some(nn.as_str().to_string());
                } else {
                    return Err(MappingError::InvalidPredicateConstant(
                        sc.constant_term.clone(),
                    ));
                }
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

    let mut keep_cols = vec![col("subject"), col("object")];
    if verb.is_none() {
        keep_cols.push(col("verb"));
    }
    lf = lf.select(keep_cols.as_slice());
    let df = lf.collect().expect("Collect problem");
    let PrimitiveColumn {
        rdf_node_type,
        language_tag,
    } = dynamic_columns.remove("object").unwrap();
    Ok((df, rdf_node_type, language_tag, verb, has_unique_subset))
}

fn create_dynamic_expression_from_static(
    column_name: &str,
    constant_term: &ConstantTerm,
    ptype: &Option<PType>,
) -> Result<(Expr, PrimitiveColumn), MappingError> {
    let (mut expr, _, rdf_node_type, language_tag) = constant_to_expr(constant_term, ptype)?;
    let mapped_column = PrimitiveColumn {
        rdf_node_type,
        language_tag,
    };
    expr = expr.alias(column_name);
    Ok((expr, mapped_column))
}

fn create_remapped(
    instance: &Instance,
    signature: &Signature,
    mut series_vec: Vec<Series>,
    dynamic_columns: &HashMap<String, PrimitiveColumn>,
    constant_columns: &HashMap<String, StaticColumn>,
    unique_subsets: &Vec<Vec<String>>,
) -> Result<
    (
        DataFrame,
        HashMap<String, PrimitiveColumn>,
        HashMap<String, StaticColumn>,
        Vec<Vec<String>>,
    ),
    MappingError,
> {
    let now = Instant::now();
    let mut new_dynamic_columns = HashMap::new();
    let mut new_constant_columns = HashMap::new();

    let mut new_dynamic_from_constant = vec![];
    let mut to_expand = vec![];
    let mut expressions = vec![];
    let mut existing = vec![];
    let mut new = vec![];
    let mut rename_map: HashMap<&String, Vec<&String>> = HashMap::new();

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
                    return Err(MappingError::UnknownVariableError(v.name.clone()));
                }
            }
            StottrTerm::ConstantTerm(ct) => {
                if original.list_expand {
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
            StottrTerm::List(_) => {
                todo!()
            }
        }
    }

    for s in &mut series_vec {
        let sname = s.name().to_string();
        s.rename(rename_map.get_mut(&sname).unwrap().pop().unwrap());
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
    Ok((
        lf.collect().unwrap(),
        new_dynamic_columns,
        new_constant_columns,
        new_unique_subsets,
    ))
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
