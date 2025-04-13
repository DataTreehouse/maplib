pub mod validation;

use super::{ExpandOptions, Mapping, MappingReport, OTTRTripleInstance, StaticColumn};
use crate::errors::MaplibError;
use crate::mapping::constant_terms::{constant_blank_node_to_series, constant_to_expr};
use crate::mapping::errors::MappingError;
use crate::mapping::expansion::validation::validate;
use log::debug;
use oxrdf::vocab::rdf;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{
    col, lit, Column, DataFrame, DataType, Expr, IntoColumn, IntoLazy, LazyFrame, NamedFrom, Series,
};
use rayon::iter::{IndexedParallelIterator, ParallelDrainRange, ParallelIterator};
use representation::multitype::split_df_multicols;
use representation::rdf_to_polars::rdf_named_node_to_polars_literal_value;
use representation::RDFNodeType;
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME, VERB_COL_NAME};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use templates::ast::{
    ConstantTerm, ConstantTermOrList, DefaultValue, Instance, ListExpanderType, PType, Signature,
    StottrTerm,
};
use templates::constants::OTTR_TRIPLE;
use templates::MappingColumnType;
use triplestore::{TriplesToAdd, Triplestore};
use uuid::Uuid;

const LIST_COL: &str = "list";
const FIRST_COL: &str = "first";

const REST_COL: &str = "rest";

impl Mapping {
    pub fn expand_triples(
        &mut self,
        mut df: DataFrame,
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        verb: Option<NamedNode>,
        expand_options: ExpandOptions,
    ) -> Result<MappingReport, MaplibError> {
        if let Some(verb) = verb {
            df = df
                .lazy()
                .with_column(
                    lit(rdf_named_node_to_polars_literal_value(&verb)).alias(VERB_COL_NAME),
                )
                .collect()
                .unwrap();
        }
        self.expand(OTTR_TRIPLE, Some(df), mapping_column_types, expand_options)
    }

    pub fn expand(
        &mut self,
        template: &str,
        df: Option<DataFrame>,
        mapping_column_types: Option<HashMap<String, MappingColumnType>>,
        options: ExpandOptions,
    ) -> Result<MappingReport, MaplibError> {
        if !self.template_dataset.inferred_types {
            self.template_dataset.infer_types()?;
        }
        let now = Instant::now();
        let target_template = self.resolve_template(template)?.clone();
        let target_template_name = target_template.signature.template_name.as_str().to_string();

        let ExpandOptions {
            graph,
            validate_iris,
            delay_index,
        } = options;
        let (mut df, mut columns) =
            validate(df, mapping_column_types, &target_template, validate_iris)?;

        let call_uuid = Uuid::new_v4().to_string();

        let mut static_columns = HashMap::new();
        let mut lf = df.map(|df| df.lazy());
        for p in &target_template.signature.parameter_list {
            if let Some(default) = &p.default_value {
                if lf.is_none() || !columns.contains_key(p.variable.as_str()) {
                    add_default_value(&mut static_columns, p.variable.as_str(), default);
                } else {
                    lf = Some(fill_nulls_with_defaults(
                        lf.unwrap(),
                        &mut columns,
                        p.variable.as_str(),
                        default,
                    )?);
                }
            }
        }
        df = lf.map(|lf| lf.collect().unwrap());

        let (result_vec, new_blank_node_counter) = self._expand(
            0,
            0,
            self.blank_node_counter,
            &target_template_name,
            df,
            &target_template.signature,
            columns,
            static_columns,
        )?;
        self.process_results(
            result_vec,
            &call_uuid,
            new_blank_node_counter,
            &graph,
            delay_index,
        )?;
        debug!("Expansion took {} seconds", now.elapsed().as_secs_f32());
        Ok(MappingReport {})
    }

    #[allow(clippy::too_many_arguments)]
    fn _expand(
        &self,
        layer: usize,
        pattern_num: usize,
        mut blank_node_counter: usize,
        name: &str,
        df: Option<DataFrame>,
        calling_signature: &Signature,
        dynamic_columns: HashMap<String, MappingColumnType>,
        static_columns: HashMap<String, StaticColumn>,
    ) -> Result<(Vec<OTTRTripleInstance>, usize), MappingError> {
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                if let Some(df) = df {
                    Ok((
                        vec![OTTRTripleInstance {
                            df,
                            dynamic_columns,
                            static_columns,
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
                    let mut instance_columns = vec![];
                    let vs = get_variable_names(i);
                    if let Some(df) = &df {
                        for v in vs {
                            if colnames.contains(v) {
                                instance_columns.push(df.column(v).unwrap().clone());
                            }
                        }
                    }
                    expand_params_vec.push((i, instance_columns));
                }

                let use_df_height = if let Some(df) = &df { df.height() } else { 1 };

                let results: Vec<_> = expand_params_vec
                    .par_drain(..)
                    .enumerate()
                    .map(|(i, (instance, series_vec))| {
                        let target_template = if let Some(target_template) =
                            self.template_dataset.get(instance.template_name.as_str())
                        {
                            target_template
                        } else {
                            return Err(MappingError::TemplateNotFound(
                                instance.template_name.to_string(),
                            ));
                        };
                        if let Some(RemapResult {
                            df: instance_df,
                            dynamic_columns: instance_dynamic_columns,
                            constant_columns: instance_constant_columns,
                            blank_node_counter,
                        }) = create_remapped(
                            self.blank_node_counter,
                            layer,
                            pattern_num,
                            instance,
                            &target_template.signature,
                            calling_signature,
                            series_vec,
                            &dynamic_columns,
                            &static_columns,
                            use_df_height,
                        )? {
                            Ok::<_, MappingError>(Some(self._expand(
                                layer + 1,
                                i,
                                blank_node_counter,
                                instance.template_name.as_str(),
                                Some(instance_df),
                                &target_template.signature,
                                instance_dynamic_columns,
                                instance_constant_columns,
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
        call_uuid: &str,
        mut new_blank_node_counter: usize,
        graph: &Option<NamedNode>,
        delay_index: bool,
    ) -> Result<(), MappingError> {
        let now = Instant::now();
        let triples: Vec<_> = result_vec
            .par_drain(..)
            .enumerate()
            .map(|(i, x)| create_triples(i * 2, new_blank_node_counter, x))
            .collect();
        let mut ok_triples = vec![];
        for t in triples {
            let (rs, updated_blank_node_counter) = t?;
            new_blank_node_counter = max(new_blank_node_counter, updated_blank_node_counter);
            ok_triples.extend(rs);
        }
        let mut all_triples_to_add = vec![];
        for CreateTriplesResult {
            df,
            subject_type,
            object_type,
            verb,
        } in ok_triples
        {
            let mut coltypes_names = vec![
                (&subject_type, SUBJECT_COL_NAME),
                (&object_type, OBJECT_COL_NAME),
            ];
            if verb.is_none() {
                coltypes_names.push((&RDFNodeType::IRI, VERB_COL_NAME));
            }

            let has_multi = matches!(subject_type, RDFNodeType::MultiType(_))
                || matches!(object_type, RDFNodeType::MultiType(_));

            let types = HashMap::from([
                (SUBJECT_COL_NAME.to_string(), subject_type),
                (OBJECT_COL_NAME.to_string(), object_type),
            ]);
            let dfs = if has_multi {
                split_df_multicols(df, &types)
            } else {
                vec![(df, types)]
            };
            for (df, mut types) in dfs {
                all_triples_to_add.push(TriplesToAdd {
                    df,
                    subject_type: types.remove(SUBJECT_COL_NAME).unwrap(),
                    object_type: types.remove(OBJECT_COL_NAME).unwrap(),
                    static_verb_column: verb.clone(),
                });
            }
        }
        let use_triplestore = if let Some(graph) = graph {
            if !self.triplestores_map.contains_key(graph) {
                self.triplestores_map.insert(
                    graph.clone(),
                    Triplestore::new(None, Some(self.indexing.clone())).unwrap(),
                );
            }
            self.triplestores_map.get_mut(graph).unwrap()
        } else {
            &mut self.base_triplestore
        };

        use_triplestore
            .add_triples_vec(all_triples_to_add, call_uuid, false, delay_index)
            .map_err(MappingError::TriplestoreError)?;

        self.blank_node_counter = new_blank_node_counter;
        debug!(
            "Result processing took {} seconds",
            now.elapsed().as_secs_f32()
        );
        Ok(())
    }
}

fn fill_nulls_with_defaults(
    mut lf: LazyFrame,
    current_types: &mut HashMap<String, MappingColumnType>,
    c: &str,
    default: &DefaultValue,
) -> Result<LazyFrame, MappingError> {
    if default.constant_term.has_blank_node() {
        let df = lf.collect().unwrap();
        if df.column(c).unwrap().is_null().any() {
            todo!();
        } else {
            return Ok(df.lazy());
        }
    }
    let (expr, _, mct) = constant_to_expr(&default.constant_term, &None)?;
    if matches!(
        current_types.get(c).unwrap(),
        MappingColumnType::Flat(RDFNodeType::None)
    ) {
        current_types.insert(c.to_string(), mct);
        lf = lf.with_column(expr.alias(c));
    } else {
        let current_mct = current_types.get(c).unwrap();
        if current_mct != &mct {
            return Err(MappingError::DefaultDataTypeMismatch(
                current_mct.clone(),
                mct,
            ));
        }
        lf = lf.with_column(col(c).fill_null(expr));
    }
    Ok(lf)
}

fn get_variable_names(i: &Instance) -> Vec<&str> {
    let mut out_vars = vec![];
    for a in &i.argument_list {
        get_term_names(&mut out_vars, &a.term)
    }
    out_vars
}

fn get_variable_name<'a>(out_vars: &mut Vec<&'a str>, var: &'a Variable) {
    out_vars.push(var.as_str());
}

fn get_term_names<'a>(out_vars: &mut Vec<&'a str>, term: &'a StottrTerm) {
    if let StottrTerm::Variable(v) = term {
        get_variable_name(out_vars, v);
    } else if let StottrTerm::List(l) = term {
        for t in l {
            get_term_names(out_vars, t);
        }
    }
}

struct CreateTriplesResult {
    df: DataFrame,
    subject_type: RDFNodeType,
    object_type: RDFNodeType,
    verb: Option<NamedNode>,
}

fn create_triples(
    i: usize,
    blank_node_counter: usize,
    triple_instance: OTTRTripleInstance,
) -> Result<(Vec<CreateTriplesResult>, usize), MappingError> {
    let mut new_subject_blank_node_counter = blank_node_counter;
    let mut new_object_blank_node_counter = blank_node_counter;

    let OTTRTripleInstance {
        mut df,
        mut dynamic_columns,
        static_columns,
    } = triple_instance;

    let mut results = vec![];
    let mut expressions = vec![];

    let mut verb = None;

    if dynamic_columns.is_empty() && df.height() == 0 {
        df.with_column(Series::new("dummy_column".into(), vec!["dummy_row"]))
            .unwrap();
    }

    for (k, sc) in static_columns {
        if k == VERB_COL_NAME {
            if let ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(nn)) = &sc.constant_term {
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
    let mut df = lf.collect().expect("Collect problem");
    let subj_t = dynamic_columns.remove(SUBJECT_COL_NAME).unwrap();
    let subj_rdf_node_type = match subj_t {
        MappingColumnType::Flat(f) => f,
        MappingColumnType::Nested(n) => {
            if let MappingColumnType::Flat(r) = *n {
                results.extend(create_list_triples(
                    &mut df,
                    SUBJECT_COL_NAME,
                    &r,
                    i,
                    &mut new_subject_blank_node_counter,
                )?);
                RDFNodeType::BlankNode
            } else {
                return Err(MappingError::TooDeeplyNestedError(format!(
                    "Expected subject of ottr:Triple to be flat or nested in one layer, but was list containing {n:?}"
                )));
            }
        }
    };
    let obj_t = dynamic_columns.remove(OBJECT_COL_NAME).unwrap();
    let obj_rdf_node_type = match obj_t {
        MappingColumnType::Flat(f) => f,
        MappingColumnType::Nested(n) => {
            if let MappingColumnType::Flat(r) = *n {
                results.extend(create_list_triples(
                    &mut df,
                    OBJECT_COL_NAME,
                    &r,
                    i + 1,
                    &mut new_object_blank_node_counter,
                )?);
                RDFNodeType::BlankNode
            } else {
                return Err(MappingError::TooDeeplyNestedError(format!(
                    "Expected object of ottr:Triple to be flat or nested in one layer, but was list containing {n:?}"
                )));
            }
        }
    };

    results.push(CreateTriplesResult {
        df,
        subject_type: subj_rdf_node_type,
        object_type: obj_rdf_node_type,
        verb,
    });
    Ok((
        results,
        max(
            new_subject_blank_node_counter,
            new_object_blank_node_counter,
        ),
    ))
}

fn create_list_triples(
    df: &mut DataFrame,
    c: &str,
    rdf_node_type: &RDFNodeType,
    i: usize,
    blank_node_counter: &mut usize,
) -> Result<Vec<CreateTriplesResult>, MappingError> {
    let my_df = df.with_row_index(LIST_COL.into(), None).unwrap();
    let mut list_df = my_df.select([c, LIST_COL]).unwrap();
    list_df = list_df
        .explode([c])
        .unwrap()
        .with_row_index(FIRST_COL.into(), None)
        .unwrap();
    list_df = list_df
        .lazy()
        .with_column(
            (lit(format!("l_{i}_"))
                + (col(LIST_COL) + lit(*blank_node_counter as u32)).cast(DataType::String))
            .alias(LIST_COL),
        )
        .with_column(
            (col(LIST_COL) + lit("_") + col(FIRST_COL).cast(DataType::String)).alias(FIRST_COL),
        )
        .with_column(col(FIRST_COL).shift(lit(-1)).alias(REST_COL))
        .collect()
        .unwrap();
    *df = my_df
        .lazy()
        .drop([c])
        .with_column(
            (lit(format!("l_{i}_"))
                + (col(LIST_COL) + lit(*blank_node_counter as u32)).cast(DataType::String)
                + lit("_0"))
            .alias(c),
        )
        .collect()
        .unwrap();

    let mut first_df = list_df.select([FIRST_COL, c]).unwrap();
    first_df
        .rename(c, OBJECT_COL_NAME.into())
        .unwrap()
        .rename(FIRST_COL, SUBJECT_COL_NAME.into())
        .unwrap();

    let rest_df = list_df
        .lazy()
        .select([
            col(FIRST_COL).alias(SUBJECT_COL_NAME),
            col(REST_COL).alias(OBJECT_COL_NAME),
        ])
        .collect()
        .unwrap();

    let rest_nil_df = rest_df
        .clone()
        .lazy()
        .filter(col(OBJECT_COL_NAME).is_null())
        .with_column(
            lit(rdf_named_node_to_polars_literal_value(
                &rdf::NIL.into_owned(),
            ))
            .alias(OBJECT_COL_NAME),
        )
        .collect()
        .unwrap();

    let rest_df = rest_df.lazy().drop_nulls(None).collect().unwrap();

    let results = vec![
        CreateTriplesResult {
            df: rest_df,
            subject_type: RDFNodeType::BlankNode,
            object_type: RDFNodeType::BlankNode,
            verb: Some(rdf::REST.into_owned()),
        },
        CreateTriplesResult {
            df: rest_nil_df,
            subject_type: RDFNodeType::BlankNode,
            object_type: RDFNodeType::IRI,
            verb: Some(rdf::REST.into_owned()),
        },
        CreateTriplesResult {
            df: first_df,
            subject_type: RDFNodeType::BlankNode,
            object_type: rdf_node_type.clone(),
            verb: Some(rdf::FIRST.into_owned()),
        },
    ];
    *blank_node_counter += df.height();
    Ok(results)
}

fn create_dynamic_expression_from_static(
    column_name: &str,
    constant_term: &ConstantTermOrList,
    ptype: &Option<PType>,
) -> Result<(Expr, MappingColumnType), MappingError> {
    let (mut expr, _, mapped_column) = constant_to_expr(constant_term, ptype)?;
    expr = expr.alias(column_name);
    Ok((expr, mapped_column))
}

fn create_series_from_blank_node_constant(
    layer: usize,
    pattern_num: usize,
    blank_node_counter: usize,
    column_name: &str,
    constant_term: &ConstantTermOrList,
    n_rows: usize,
) -> Result<(Series, MappingColumnType), MappingError> {
    let (mut series, _, rdf_node_type) = constant_blank_node_to_series(
        layer,
        pattern_num,
        blank_node_counter,
        constant_term,
        n_rows,
    )?;
    series.rename(column_name.into());
    let mapped_column = MappingColumnType::Flat(rdf_node_type);
    Ok((series, mapped_column))
}

#[allow(clippy::too_many_arguments)]
fn create_remapped(
    blank_node_counter: usize,
    layer: usize,
    pattern_num: usize,
    instance: &Instance,
    signature: &Signature,
    calling_signature: &Signature,
    mut columns_vec: Vec<Column>,
    dynamic_columns: &HashMap<String, MappingColumnType>,
    constant_columns: &HashMap<String, StaticColumn>,
    input_df_height: usize,
) -> Result<Option<RemapResult>, MappingError> {
    let now = Instant::now();
    let mut new_dynamic_columns = HashMap::new();
    let mut new_constant_columns = HashMap::new();
    let mut new_columns = vec![];

    let mut new_dynamic_from_constant = vec![];
    let mut to_expand = vec![];
    let mut expressions = vec![];
    let mut existing = vec![];
    let mut new = vec![];
    let mut rename_map: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut out_blank_node_counter = blank_node_counter;

    for (original, target) in instance
        .argument_list
        .iter()
        .zip(signature.parameter_list.iter())
    {
        let target_colname = target.variable.as_str();
        if original.list_expand {
            to_expand.push(target_colname);
        }
        match &original.term {
            StottrTerm::Variable(v) => {
                if let Some(c) = dynamic_columns.get(v.as_str()) {
                    if let Some(target_names) = rename_map.get_mut(&v.as_str()) {
                        target_names.push(target_colname)
                    } else {
                        rename_map.insert(v.as_str(), vec![target_colname]);
                    }

                    existing.push(v.as_str());
                    new.push(target_colname);
                    new_dynamic_columns.insert(target_colname.to_string(), c.clone());
                } else if let Some(c) = constant_columns.get(v.as_str()) {
                    if original.list_expand {
                        let (expr, primitive_column) = create_dynamic_expression_from_static(
                            target_colname,
                            &c.constant_term,
                            &target.ptype,
                        )?;
                        expressions.push(expr);
                        new_dynamic_columns.insert(target_colname.to_string(), primitive_column);
                        new_dynamic_from_constant.push(target_colname);
                    } else {
                        new_constant_columns.insert(target_colname.to_string(), c.clone());
                    }
                } else if let Some(default) = &target.default_value {
                    add_default_value(&mut new_constant_columns, target_colname, default)
                } else {
                    if let Some(calling_formal_arg) = calling_signature
                        .parameter_list
                        .iter()
                        .find(|x| x.variable.as_str() == v.as_str())
                    {
                        if target.optional {
                            continue;
                        } else if calling_formal_arg.optional {
                            return Ok(None);
                        }
                    }
                    return Err(MappingError::UnknownVariableError(v.as_str().to_string()));
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
                    new_columns.push(series.into_column());
                    new_dynamic_columns.insert(target_colname.to_string(), primitive_column);
                    new_dynamic_from_constant.push(target_colname);
                    out_blank_node_counter =
                        max(out_blank_node_counter, blank_node_counter + input_df_height);
                } else if original.list_expand {
                    let (expr, primitive_column) =
                        create_dynamic_expression_from_static(target_colname, ct, &target.ptype)?;
                    expressions.push(expr);
                    new_dynamic_columns.insert(target_colname.to_string(), primitive_column);
                    new_dynamic_from_constant.push(target_colname);
                } else {
                    let mut added_default_static = false;
                    if matches!(ct.ptype(), PType::None) {
                        if let Some(default) = &target.default_value {
                            add_default_value(&mut new_constant_columns, target_colname, default);
                            added_default_static = true;
                        }
                    }
                    if !added_default_static {
                        let static_column = StaticColumn {
                            constant_term: ct.clone(),
                            ptype: target.ptype.clone(),
                        };
                        new_constant_columns.insert(target_colname.to_string(), static_column);
                    }
                }
            }
            StottrTerm::List(_l) => {
                todo!()
            }
        }
    }

    for s in &mut columns_vec {
        let sname = s.name().to_string();
        s.rename(
            rename_map
                .get_mut(sname.as_str())
                .unwrap()
                .pop()
                .unwrap()
                .into(),
        );
    }
    for s in new_columns {
        columns_vec.push(s);
    }
    let mut column_vec = Vec::new();
    for s in columns_vec {
        column_vec.push(s.into_column());
    }

    let mut lf = DataFrame::new(column_vec).unwrap().lazy();

    for expr in expressions {
        lf = lf.with_column(expr);
    }
    let new_column_expressions: Vec<Expr> = new
        .iter()
        .chain(new_dynamic_from_constant.iter())
        .map(|x| col(*x))
        .collect();
    lf = lf.select(new_column_expressions.as_slice());

    if let Some(le) = &instance.list_expander {
        for e in &to_expand {
            if let Some((k, MappingColumnType::Nested(v))) = new_dynamic_columns.remove_entry(*e) {
                new_dynamic_columns.insert(k, *v);
            } else {
                panic!("This situation should never arise")
            }
        }
        let to_expand_cols: Vec<Expr> = to_expand.iter().map(|x| col(*x)).collect();
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
    }

    for p in &signature.parameter_list {
        if dynamic_columns.contains_key(p.variable.as_str()) {
            if let Some(default) = &p.default_value {
                lf = fill_nulls_with_defaults(
                    lf,
                    &mut new_dynamic_columns,
                    p.variable.as_str(),
                    default,
                )?;
            }
        }
    }

    debug!(
        "Creating remapped took {} seconds",
        now.elapsed().as_secs_f32()
    );
    Ok(Some(RemapResult {
        df: lf.collect().unwrap(),
        dynamic_columns: new_dynamic_columns,
        constant_columns: new_constant_columns,
        blank_node_counter: out_blank_node_counter,
    }))
}

fn add_default_value(
    static_columns: &mut HashMap<String, StaticColumn>,
    name: &str,
    default: &DefaultValue,
) {
    static_columns.insert(
        name.to_string(),
        StaticColumn {
            constant_term: default.constant_term.clone(),
            ptype: Some(default.constant_term.ptype()),
        },
    );
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}

struct RemapResult {
    df: DataFrame,
    dynamic_columns: HashMap<String, MappingColumnType>,
    constant_columns: HashMap<String, StaticColumn>,
    blank_node_counter: usize,
}
