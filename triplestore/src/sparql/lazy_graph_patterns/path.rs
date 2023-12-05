use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use crate::sparql::lazy_graph_patterns::triple::create_term_pattern_filter;
use crate::sparql::multitype::{convert_df_col_to_multitype, multi_series_to_string_series};
use crate::sparql::query_context::{Context, PathEntry};
use crate::sparql::solution_mapping::SolutionMappings;
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use oxrdf::{NamedNode, Variable};
use polars::prelude::{col, lit, DataFrameJoinOps, Expr, IntoLazy};
use polars::prelude::{ChunkAgg, JoinArgs, JoinType};
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::series::{IntoSeries, Series};
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use sprs::{CsMatBase, TriMatBase};
use std::cmp::max;
use std::collections::hash_map::Values;
use std::collections::HashMap;

type SparseMatrix = CsMatBase<u32, usize, Vec<usize>, Vec<usize>, Vec<u32>, usize>;

struct SparsePathReturn {
    sparmat: SparseMatrix,
    dt_subj: RDFNodeType,
    dt_obj: RDFNodeType,
}

impl Triplestore {
    pub fn lazy_path(
        &self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let create_sparse = need_sparse_matrix(ppe);

        let mut var_cols = vec![];
        match subject {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.as_str().to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        match object {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.as_str().to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        if !create_sparse {
            let gp = create_graph_pattern(ppe, subject, object);
            let mut sms = self.lazy_graph_pattern(
                &gp,
                solution_mappings,
                &context.extension_with(PathEntry::PathRewrite),
            )?;
            let select: Vec<_> = var_cols.iter().map(|x| col(x)).collect();
            sms.mappings = sms.mappings.select(select);
            return Ok(sms);
        }

        let mut out_df;
        let out_dt_subj;
        let out_dt_obj;

        let cat_df_map = self.create_unique_cat_dfs(ppe, Some(subject), Some(object))?;
        let max_index = find_max_index(cat_df_map.values());

        if let Some(SparsePathReturn {
            sparmat,
            dt_subj,
            dt_obj,
        }) = sparse_path(ppe, &cat_df_map, max_index as usize, false)
        {
            let mut subject_vec = vec![];
            let mut object_vec = vec![];
            for (i, row) in sparmat.outer_iterator().enumerate() {
                for (j, v) in row.iter() {
                    if v > &0 {
                        subject_vec.push(i as u32);
                        object_vec.push(j as u32);
                    }
                }
            }
            let mut lookup_df_map = find_lookup(&cat_df_map);
            let mut subject_series = Series::from_iter(subject_vec);
            subject_series.rename("subject_key");
            let mut object_series = Series::from_iter(object_vec);
            object_series.rename("object_key");
            out_df = DataFrame::new(vec![subject_series, object_series]).unwrap();

            let subject_lookup_df = lookup_df_map.get_mut(&dt_subj).unwrap();
            subject_lookup_df.rename("value", "subject").unwrap();
            out_df = out_df
                .join(
                    &subject_lookup_df,
                    &["subject_key"],
                    &["key"],
                    JoinArgs::new(JoinType::Inner),
                )
                .unwrap();
            subject_lookup_df.rename("subject", "value").unwrap();

            let object_lookup_df = lookup_df_map.get_mut(&dt_subj).unwrap();
            object_lookup_df.rename("value", "object").unwrap();
            out_df = out_df
                .join(
                    &object_lookup_df,
                    &["object_key"],
                    &["key"],
                    JoinArgs::new(JoinType::Inner),
                )
                .unwrap();
            out_df = out_df.select(["subject", "object"]).unwrap();
            out_dt_obj = dt_obj;
            out_dt_subj = dt_subj;
        } else {
            out_df = DataFrame::new(vec![
                Series::new_empty("subject", &DataType::Utf8),
                Series::new_empty("object", &DataType::Utf8),
            ])
            .unwrap();
            out_dt_obj = RDFNodeType::IRI;
            out_dt_subj = RDFNodeType::IRI;
        }
        let mut var_cols = vec![];
        match subject {
            TermPattern::NamedNode(nn) => {
                let l = sparql_named_node_to_polars_literal_value(nn);
                out_df = out_df
                    .lazy()
                    .filter(col("subject").eq(lit(l)))
                    .collect()
                    .unwrap();
                out_df = out_df.drop("subject").unwrap();
            }
            TermPattern::BlankNode(b) => {
                var_cols.push(b.as_str().to_string());
                out_df.rename("subject", b.as_str()).unwrap();
            }
            TermPattern::Literal(l) => {
                let l = sparql_literal_to_polars_literal_value(l);
                out_df = out_df
                    .lazy()
                    .filter(col("subject").eq(lit(l)))
                    .collect()
                    .unwrap();
                out_df = out_df.drop("subject").unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                out_df.rename("subject", v.as_str()).unwrap();
            }
        }

        match object {
            TermPattern::NamedNode(nn) => {
                let l = sparql_named_node_to_polars_literal_value(nn);
                out_df = out_df
                    .lazy()
                    .filter(col("object").eq(lit(l)))
                    .collect()
                    .unwrap();
                out_df = out_df.drop("object").unwrap();
            }
            TermPattern::BlankNode(b) => {
                var_cols.push(b.as_str().to_string());
                out_df.rename("object", b.as_str()).unwrap();
            }
            TermPattern::Literal(l) => {
                let l = sparql_literal_to_polars_literal_value(l);
                out_df = out_df
                    .lazy()
                    .filter(col("object").eq(lit(l)))
                    .collect()
                    .unwrap();
                out_df = out_df.drop("object").unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                out_df.rename("object", v.as_str()).unwrap();
            }
        }

        if let Some(mut mappings) = solution_mappings {
            let join_cols: Vec<String> = var_cols
                .clone()
                .into_iter()
                .filter(|x| mappings.columns.contains(x))
                .collect();

            for j in &join_cols {
                mappings.mappings = mappings
                    .mappings
                    .with_column(col(j).cast(DataType::Categorical(None)));
            }

            let join_on: Vec<Expr> = join_cols.iter().map(|x| col(x)).collect();

            //workaround for object registry bug
            let mut multicols = vec![];
            for (k, v) in &mappings.rdf_node_types {
                if v == &RDFNodeType::MultiType {
                    multicols.push(k.clone())
                }
            }

            if out_df.height() == 0 {
                out_df = out_df
                    .lazy()
                    .drop_columns(join_cols.as_slice())
                    .collect()
                    .unwrap();

                for c in &multicols {
                    mappings.mappings = mappings.mappings.with_column(lit("").alias(c));
                }
            }

            if out_df.height() == 0 || join_on.is_empty() {
                mappings.mappings = mappings.mappings.join(
                    out_df.lazy(),
                    vec![],
                    vec![],
                    JoinArgs::new(JoinType::Cross),
                );
                for m in &multicols {
                    let mut df = mappings.mappings.collect().unwrap();
                    convert_df_col_to_multitype(&mut df, &m, &RDFNodeType::IRI);
                    mappings.mappings = df.lazy();
                }
            } else {
                let join_col_exprs: Vec<Expr> = join_cols.iter().map(|x| col(x)).collect();
                let all_false = [false].repeat(join_cols.len());
                let lf = out_df.lazy().sort_by_exprs(
                    join_col_exprs.as_slice(),
                    all_false.as_slice(),
                    false,
                    false,
                );
                mappings.mappings = mappings.mappings.sort_by_exprs(
                    join_col_exprs.as_slice(),
                    all_false.as_slice(),
                    false,
                    false,
                );
                mappings.mappings = mappings.mappings.join(
                    lf,
                    join_on.as_slice(),
                    join_on.as_slice(),
                    JoinArgs::new(JoinType::Inner),
                );
            }
            //Update mapping columns
            for c in &var_cols {
                mappings.columns.insert(c.to_string());
            }
            //TODO: THIS IS WRONG
            if let TermPattern::Variable(v) = subject {
                mappings
                    .rdf_node_types
                    .insert(v.as_str().to_string(), out_dt_subj);
            }
            if let TermPattern::Variable(v) = object {
                mappings
                    .rdf_node_types
                    .insert(v.as_str().to_string(), out_dt_obj);
            }

            Ok(mappings)
        } else {
            let mut datatypes = HashMap::new();
            if let TermPattern::Variable(v) = subject {
                datatypes.insert(v.as_str().to_string(), out_dt_subj);
            }
            if let TermPattern::Variable(v) = object {
                datatypes.insert(v.as_str().to_string(), out_dt_obj);
            }
            Ok(SolutionMappings {
                mappings: out_df.lazy(),
                columns: var_cols.into_iter().map(|x| x.to_string()).collect(),
                rdf_node_types: datatypes,
            })
        }
    }

    fn create_unique_cat_dfs(
        &self,
        ppe: &PropertyPathExpression,
        subject: Option<&TermPattern>,
        object: Option<&TermPattern>,
    ) -> Result<HashMap<String, (DataFrame, RDFNodeType, RDFNodeType)>, SparqlError> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let subject_filter = if let Some(subject) = subject {
                    create_term_pattern_filter(subject, "subject")
                } else {
                    None
                };

                let object_filter = if let Some(object) = object {
                    create_term_pattern_filter(object, "object")
                } else {
                    None
                };

                let res =
                    self.get_single_nn_df(nn, subject, object, subject_filter, object_filter)?;
                if let Some((df, subj_dt, obj_dt)) = res {
                    let mut unique_cat_df = df_with_cats(df, &subj_dt, &obj_dt);
                    unique_cat_df = unique_cat_df
                        .unique(None, UniqueKeepStrategy::First, None)
                        .unwrap();
                    Ok(HashMap::from([(
                        nn.as_str().to_string(),
                        (unique_cat_df, subj_dt, obj_dt),
                    )]))
                } else {
                    Ok(HashMap::new())
                }
            }
            PropertyPathExpression::Reverse(inner) => {
                self.create_unique_cat_dfs(inner, object, subject)
            }
            PropertyPathExpression::Sequence(left, right) => {
                let mut left_df_map = self.create_unique_cat_dfs(left, subject, None)?;
                let right_df_map = self.create_unique_cat_dfs(right, None, object)?;
                left_df_map.extend(right_df_map);
                Ok(left_df_map)
            }
            PropertyPathExpression::Alternative(left, right) => {
                let mut left_df_map = self.create_unique_cat_dfs(left, subject, object)?;
                let right_df_map = self.create_unique_cat_dfs(right, subject, object)?;
                left_df_map.extend(right_df_map);
                Ok(left_df_map)
            }
            PropertyPathExpression::ZeroOrMore(inner) => {
                self.create_unique_cat_dfs(inner, subject, object)
            }
            PropertyPathExpression::OneOrMore(inner) => {
                self.create_unique_cat_dfs(inner, subject, object)
            }
            PropertyPathExpression::ZeroOrOne(inner) => {
                self.create_unique_cat_dfs(inner, subject, object)
            }
            PropertyPathExpression::NegatedPropertySet(_nns) => {
                todo!()
            }
        }
    }

    fn get_single_nn_df(
        &self,
        nn: &NamedNode,
        subject: Option<&TermPattern>,
        object: Option<&TermPattern>,
        subject_filter: Option<Expr>,
        object_filter: Option<Expr>,
    ) -> Result<Option<(DataFrame, RDFNodeType, RDFNodeType)>, SparqlError> {
        let map_opt = self.df_map.get(nn);
        if let Some(m) = map_opt {
            if m.is_empty() {
                panic!("Empty map should never happen");
            } else {
                let tp_opt_to_dt_req = |x: Option<&TermPattern>| {
                    if let Some(tp) = x {
                        match tp {
                            TermPattern::NamedNode(_) => Some(RDFNodeType::IRI),
                            TermPattern::BlankNode(_) => None,
                            TermPattern::Literal(lit) => {
                                Some(RDFNodeType::Literal(lit.datatype().into_owned()))
                            }
                            TermPattern::Variable(_) => None,
                        }
                    } else {
                        None
                    }
                };

                let subj_datatype_req = tp_opt_to_dt_req(subject);
                let obj_datatype_req = tp_opt_to_dt_req(object);

                let ret = multiple_tt_to_lf(
                    m,
                    self.transient_df_map.get(nn),
                    subj_datatype_req.as_ref(),
                    obj_datatype_req.as_ref(),
                    subject_filter,
                    object_filter,
                )?;
                if let Some((subj_dt, obj_dt, mut lf)) = ret {
                    if let Some(subject) = subject {
                        if let TermPattern::NamedNode(nn) = subject {
                            lf =
                                lf.filter(col("subject").eq(Expr::Literal(
                                    sparql_named_node_to_polars_literal_value(nn),
                                )))
                        } else if let TermPattern::Literal(l) = subject {
                            lf = lf.filter(
                                col("subject")
                                    .eq(Expr::Literal(sparql_literal_to_polars_literal_value(l))),
                            )
                        }
                    }
                    if let Some(object) = object {
                        if let TermPattern::NamedNode(nn) = object {
                            lf =
                                lf.filter(col("object").eq(Expr::Literal(
                                    sparql_named_node_to_polars_literal_value(nn),
                                )))
                        } else if let TermPattern::Literal(l) = object {
                            lf = lf.filter(
                                col("object")
                                    .eq(Expr::Literal(sparql_literal_to_polars_literal_value(l))),
                            )
                        }
                    }
                    Ok(Some((lf.collect().unwrap(), subj_dt, obj_dt)))
                } else {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}

fn create_graph_pattern(
    ppe: &PropertyPathExpression,
    subject: &TermPattern,
    object: &TermPattern,
) -> GraphPattern {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => GraphPattern::Bgp {
            patterns: vec![TriplePattern {
                subject: subject.clone(),
                predicate: NamedNodePattern::NamedNode(nn.to_owned()),
                object: object.clone(),
            }],
        },
        PropertyPathExpression::Reverse(r) => create_graph_pattern(r, object, subject),
        PropertyPathExpression::Sequence(l, r) => {
            let intermediary = uuid::Uuid::new_v4().to_string();
            let intermediary_tp =
                TermPattern::Variable(Variable::new_unchecked(format!("v{}", intermediary)));
            GraphPattern::Join {
                left: Box::new(create_graph_pattern(l, subject, &intermediary_tp)),
                right: Box::new(create_graph_pattern(r, &intermediary_tp, object)),
            }
        }
        PropertyPathExpression::Alternative(a, b) => GraphPattern::Union {
            left: Box::new(create_graph_pattern(a, subject, object)),
            right: Box::new(create_graph_pattern(b, subject, object)),
        },
        PropertyPathExpression::ZeroOrMore(_)
        | PropertyPathExpression::OneOrMore(_)
        | PropertyPathExpression::ZeroOrOne(_) => {
            panic!("Should never happen")
        }
        PropertyPathExpression::NegatedPropertySet(_) => {
            todo!()
        }
    }
}

fn find_lookup(
    map: &HashMap<String, (DataFrame, RDFNodeType, RDFNodeType)>,
) -> HashMap<RDFNodeType, DataFrame> {
    let mut all_values_map = HashMap::new();
    for (df, dt_subj, dt_obj) in map.values() {
        for (c, dt) in [("subject", dt_subj), ("object", dt_obj)] {
            if !all_values_map.contains_key(dt) {
                all_values_map.insert(dt.clone(), vec![]);
            }
            let mut ser = df.column(c).unwrap().clone();
            ser.rename("value");
            let mut include_series = vec![ser];
            if dt == &RDFNodeType::MultiType {
                let multiname = format!("{c}_multi");
                let mut ser = df.column(&multiname).unwrap().clone();
                ser.rename("value_multi");
                include_series.push(ser);
            }

            all_values_map
                .get_mut(dt)
                .unwrap()
                .push(DataFrame::new(include_series).unwrap());
        }
    }
    let mut out_map = HashMap::new();
    for (dt, all_values) in all_values_map {
        let mut df = concat_df(all_values.as_slice())
            .unwrap()
            .unique(None, UniqueKeepStrategy::First, None)
            .unwrap();
        let mut key_col = df
            .column("value")
            .unwrap()
            .categorical()
            .unwrap()
            .physical()
            .clone()
            .into_series();
        key_col.rename("key");
        if df.column("value_multi").is_ok() {
            df = df.drop("value").unwrap();
            df.rename("value_multi", "value").unwrap();
        }
        df.with_column(key_col).unwrap();
        out_map.insert(dt, df);
    }
    out_map
}

fn df_with_cats(mut df: DataFrame, subj_dt: &RDFNodeType, obj_dt: &RDFNodeType) -> DataFrame {
    if subj_dt == &RDFNodeType::MultiType {
        let subj_string_series = multi_series_to_string_series(df.column("subject").unwrap());
        df.rename("subject", "subject_multi").unwrap();
        df.with_column(subj_string_series).unwrap();
    }
    if obj_dt == &RDFNodeType::MultiType {
        let obj_string_series = multi_series_to_string_series(df.column("object").unwrap());
        df.rename("object", "object_multi").unwrap();
        df.with_column(obj_string_series).unwrap();
    }
    let subject = df
        .column("subject")
        .unwrap()
        .cast(&DataType::Categorical(None))
        .unwrap();
    let object = df
        .column("object")
        .unwrap()
        .cast(&DataType::Categorical(None))
        .unwrap();
    df.with_column(subject).unwrap();
    df.with_column(object).unwrap();
    df
}

fn find_max_index(vals: Values<String, (DataFrame, RDFNodeType, RDFNodeType)>) -> u32 {
    let mut max_index = 0u32;
    for (df, _, _) in vals {
        if let Some(max_subject) = df
            .column("subject")
            .unwrap()
            .categorical()
            .unwrap()
            .physical()
            .max()
        {
            max_index = max(max_index, max_subject);
        }
        if let Some(max_object) = df
            .column("object")
            .unwrap()
            .categorical()
            .unwrap()
            .physical()
            .max()
        {
            max_index = max(max_index, max_object);
        }
    }
    max_index
}

fn to_csr(df: &DataFrame, max_index: usize) -> SparseMatrix {
    let sub = df
        .column("subject")
        .unwrap()
        .categorical()
        .unwrap()
        .physical()
        .clone()
        .into_series();
    let obj = df
        .column("object")
        .unwrap()
        .categorical()
        .unwrap()
        .physical()
        .clone()
        .into_series();
    let df = DataFrame::new(vec![sub, obj]).unwrap();
    let df = df
        .sort(vec!["subject", "object"], vec![false, false], false)
        .unwrap();
    let subject = df.column("subject").unwrap();
    let object = df.column("object").unwrap();
    let mut subjects_vec = vec![];
    let mut objects_vec = vec![];
    for s in subject.iter() {
        if let AnyValue::UInt32(s) = s {
            subjects_vec.push(s as usize);
        } else {
            panic!("Should never happen");
        }
    }

    for s in object.iter() {
        if let AnyValue::UInt32(o) = s {
            objects_vec.push(o as usize);
        } else {
            panic!("Should never happen");
        }
    }

    let trimat = TriMatBase::from_triplets(
        (max_index + 1, max_index + 1),
        subjects_vec,
        objects_vec,
        [1_u32].repeat(df.height()),
    );
    trimat.to_csr()
}

fn zero_or_more(mut rel_mat: SparseMatrix) -> SparseMatrix {
    let rows = rel_mat.rows();
    let eye = SparseMatrix::eye(rows);
    rel_mat = (&rel_mat + &eye).to_csr();
    rel_mat = rel_mat.map(|x| (x > &0) as u32);
    let mut last_sum = sum_mat(&rel_mat);
    let mut fixed_point = false;
    while !fixed_point {
        rel_mat = (&rel_mat * &rel_mat).to_csr();
        rel_mat = rel_mat.map(|x| (x > &0) as u32);
        let new_sum = sum_mat(&rel_mat);
        if last_sum == new_sum {
            fixed_point = true;
        } else {
            last_sum = new_sum;
        }
    }
    rel_mat
}

fn zero_or_one(mut rel_mat: SparseMatrix) -> SparseMatrix {
    let rows = rel_mat.rows();
    let eye = SparseMatrix::eye(rows);
    rel_mat = (&rel_mat + &eye).to_csr();
    rel_mat = rel_mat.map(|x| (x > &0) as u32);
    rel_mat
}

fn one_or_more(mut rel_mat: SparseMatrix) -> SparseMatrix {
    let mut last_sum = sum_mat(&rel_mat);
    let mut fixed_point = false;
    while !fixed_point {
        let new_rels = (&rel_mat * &rel_mat).to_csr();
        rel_mat = (&new_rels + &rel_mat).to_csr();
        rel_mat = rel_mat.map(|x| (x > &0) as u32);
        let new_sum = sum_mat(&rel_mat);
        if last_sum == new_sum {
            fixed_point = true;
        } else {
            last_sum = new_sum;
        }
    }
    rel_mat
}

fn sum_mat(mat: &SparseMatrix) -> u32 {
    let mut s = 0;
    //Todo parallel
    for out in mat.outer_iterator() {
        s += out.data().iter().sum::<u32>()
    }
    s
}

fn need_sparse_matrix(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::NamedNode(_) => false,
        PropertyPathExpression::Reverse(inner) => need_sparse_matrix(inner),
        PropertyPathExpression::Sequence(left, right) => {
            need_sparse_matrix(left) || need_sparse_matrix(right)
        }
        PropertyPathExpression::Alternative(a, b) => need_sparse_matrix(a) || need_sparse_matrix(b),
        PropertyPathExpression::ZeroOrMore(_) => true,
        PropertyPathExpression::OneOrMore(_) => true,
        PropertyPathExpression::ZeroOrOne(_) => true,
        PropertyPathExpression::NegatedPropertySet(_) => false,
    }
}

fn sparse_path(
    ppe: &PropertyPathExpression,
    cat_df_map: &HashMap<String, (DataFrame, RDFNodeType, RDFNodeType)>,
    max_index: usize,
    reflexive: bool,
) -> Option<SparsePathReturn> {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            if let Some((df, dt_subj, dt_obj)) = cat_df_map.get(nn.as_str()) {
                let sparmat = to_csr(df, max_index);
                Some(SparsePathReturn {
                    sparmat,
                    dt_subj: dt_subj.clone(),
                    dt_obj: dt_obj.clone(),
                })
            } else {
                None
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            if let Some(SparsePathReturn {
                sparmat,
                dt_subj,
                dt_obj,
            }) = sparse_path(inner, cat_df_map, max_index, reflexive)
            {
                Some(SparsePathReturn {
                    sparmat: sparmat.transpose_into(),
                    dt_subj: dt_obj.clone(),
                    dt_obj: dt_subj.clone(),
                })
            } else {
                None
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let res_left = sparse_path(left, cat_df_map, max_index, false);
            let res_right = sparse_path(right, cat_df_map, max_index, false);
            if let Some(SparsePathReturn {
                sparmat: sparmat_left,
                dt_subj: dt_subj_left,
                dt_obj: dt_obj_left,
            }) = res_left
            {
                if let Some(SparsePathReturn {
                    sparmat: sparmat_right,
                    dt_subj: dt_subj_right,
                    dt_obj: dt_obj_right,
                }) = res_right
                {
                    let sparmat = (&sparmat_left * &sparmat_right).to_csr();
                    Some(SparsePathReturn {
                        sparmat,
                        dt_subj: dt_subj_left.union(&dt_subj_right),
                        dt_obj: dt_obj_left.union(&dt_obj_right),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
        PropertyPathExpression::Alternative(left, right) => {
            let res_left = sparse_path(left, cat_df_map, max_index, reflexive);
            let res_right = sparse_path(right, cat_df_map, max_index, reflexive);
            if let Some(SparsePathReturn {
                sparmat: sparmat_left,
                dt_subj: dt_subj_left,
                dt_obj: dt_obj_left,
            }) = res_left
            {
                if let Some(SparsePathReturn {
                    sparmat: sparmat_right,
                    dt_subj: dt_subj_right,
                    dt_obj: dt_obj_right,
                }) = res_right
                {
                    let sparmat = (&sparmat_left + &sparmat_right)
                        .to_csr()
                        .map(|x| (x > &0) as u32);
                    Some(SparsePathReturn {
                        sparmat,
                        dt_subj: dt_subj_left.union(&dt_subj_right),
                        dt_obj: dt_obj_left.union(&dt_obj_right),
                    })
                } else {
                    Some(SparsePathReturn {
                        sparmat: sparmat_left,
                        dt_subj: dt_subj_left,
                        dt_obj: dt_obj_left,
                    })
                }
            } else if let Some(r) = res_right {
                Some(r)
            } else {
                None
            }
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
                dt_subj,
                dt_obj,
            }) = sparse_path(inner, cat_df_map, max_index, true)
            {
                let sparmat = zero_or_more(sparmat_inner);
                Some(SparsePathReturn {
                    sparmat,
                    dt_subj,
                    dt_obj,
                })
            } else {
                None
            }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
                dt_subj,
                dt_obj,
            }) = sparse_path(inner, cat_df_map, max_index, false)
            {
                let sparmat = one_or_more(sparmat_inner);
                Some(SparsePathReturn {
                    sparmat,
                    dt_subj,
                    dt_obj,
                })
            } else {
                None
            }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
                dt_subj,
                dt_obj,
            }) = sparse_path(inner, cat_df_map, max_index, true)
            {
                let sparmat = zero_or_one(sparmat_inner);
                Some(SparsePathReturn {
                    sparmat,
                    dt_subj,
                    dt_obj,
                })
            } else {
                None
            }
        }
        PropertyPathExpression::NegatedPropertySet(_nns) => {
            todo!()
        }
    }
}

fn nns_name(nns: &Vec<NamedNode>) -> String {
    let mut names = vec![];
    for nn in nns {
        names.push(nn.as_str())
    }
    names.join(",")
}
