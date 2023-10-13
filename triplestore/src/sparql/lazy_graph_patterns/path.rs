use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::lazy_graph_patterns::load_tt::multiple_tt_to_lf;
use crate::sparql::query_context::Context;
use crate::sparql::solution_mapping::SolutionMappings;
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use oxrdf::NamedNode;
use polars::prelude::{col, DataFrameJoinOps, Expr, IntoLazy};
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{ChunkAgg, JoinArgs, JoinType};
use polars_core::series::{IntoSeries, Series};
use polars_core::utils::concat_df;
use representation::RDFNodeType;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::TermPattern;
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

struct DFPathReturn {
    df: DataFrame,
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
        _context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let create_sparse = need_sparse_matrix(ppe);
        let mut out_df;
        let out_dt_subj;
        let out_dt_obj;

        let cat_df_map = self.create_unique_cat_dfs(ppe, Some(subject), Some(object))?;
        let max_index = find_max_index(cat_df_map.values());
        if create_sparse {
            if let Some(SparsePathReturn {
                sparmat,
                dt_subj,
                dt_obj,
            }) = sparse_path(ppe, &cat_df_map, max_index as usize) {
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
                todo!()
            }
        } else {
            let res = df_path(ppe, &cat_df_map, max_index);
            if let Some(DFPathReturn {
                df,
                dt_subj,
                dt_obj,
            }) = res
            {
                out_df = df;
                out_dt_obj = dt_obj;
                out_dt_subj = dt_subj;
            } else {
                todo!()
            }
        }
        let mut var_cols = vec![];
        if let TermPattern::Variable(v) = subject {
            var_cols.push(v.as_str().to_string());
            out_df.rename("subject", v.as_str()).unwrap();
        } else {
            out_df = out_df.drop("subject").unwrap();
        }
        if let TermPattern::Variable(v) = object {
            var_cols.push(v.as_str().to_string());
            out_df.rename("object", v.as_str()).unwrap();
        } else {
            out_df = out_df.drop("object").unwrap();
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

            if join_on.is_empty() {
                mappings.mappings = mappings.mappings.join(
                    out_df.lazy(),
                    join_on.as_slice(),
                    join_on.as_slice(),
                    JoinArgs::new(JoinType::Cross),
                );
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
                let res = self.get_single_nn_df(nn.as_str(), subject, object)?;
                if let Some((df, subj_dt, obj_dt)) = res {
                    let unique_cat_df = df_with_cats(df)
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
            PropertyPathExpression::NegatedPropertySet(nns) => {
                todo!()
                // let lookup: HashSet<_> = nns.iter().map(|x| x.as_str().to_string()).collect();
                // let mut dfs = vec![];
                // let mut dt_subj = None;
                // let mut dt_obj = None;
                // for nn in self.df_map.keys() {
                //     if !lookup.contains(nn) {
                //         let df = self.get_single_nn_df(nn, subject, object)?;
                //         if let Some((df, dt_subj_new, dt_obj_new)) = df {
                //             dfs.push(df_with_cats(df));
                //         }
                //     }
                // }
                // if !dfs.is_empty() {
                //     let df = concat_df(dfs.as_slice())
                //         .unwrap()
                //         .unique(None, UniqueKeepStrategy::First, None)
                //         .unwrap();
                //     Ok( HashMap::from([(nns_name(nns), df)]))
                // } else {
                //     Ok(HashMap::new())
                // }
            }
        }
    }

    fn get_single_nn_df(
        &self,
        nn: &str,
        subject: Option<&TermPattern>,
        object: Option<&TermPattern>,
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

                let ret = multiple_tt_to_lf(m, &subj_datatype_req, &obj_datatype_req)?;
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
            all_values_map
                .get_mut(dt)
                .unwrap()
                .push(DataFrame::new(vec![ser]).unwrap());
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
            .logical()
            .clone()
            .into_series();
        key_col.rename("key");
        df.with_column(key_col).unwrap();
        out_map.insert(dt, df);
    }
    out_map
}

fn df_path(
    ppe: &PropertyPathExpression,
    cat_df_map: &HashMap<String, (DataFrame, RDFNodeType, RDFNodeType)>,
    max_index: u32,
) -> Option<DFPathReturn> {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            let res = cat_df_map.get(nn.as_str());
            if let Some((df, dt_subj, dt_obj)) = res {
                Some(DFPathReturn {
                    df: df.clone(),
                    dt_subj: dt_subj.clone(),
                    dt_obj: dt_obj.clone(),
                })
            } else {
                None
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            let res = df_path(inner, cat_df_map, max_index);
            if let Some(DFPathReturn {
                df,
                dt_subj,
                dt_obj,
            }) = res
            {
                let df = df
                    .lazy()
                    .rename(["subject", "object"], ["object", "subject"])
                    .collect()
                    .unwrap();
                Some(DFPathReturn {
                    df,
                    dt_subj: dt_obj,
                    dt_obj: dt_subj,
                })
            } else {
                None
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let left_res = df_path(left, cat_df_map, max_index);
            let right_res = df_path(right, cat_df_map, max_index);
            if let Some(DFPathReturn {
                df: mut df_left,
                dt_subj,
                dt_obj: _,
            }) = left_res
            {
                if let Some(DFPathReturn {
                    df: mut df_right,
                    dt_subj: _,
                    dt_obj,
                }) = right_res
                {
                    df_left.rename("object", "on").unwrap();
                    df_right.rename("subject", "on").unwrap();
                    df_left = df_left.sort(vec!["on"], vec![false], false).unwrap();
                    df_right = df_right.sort(vec!["on"], vec![false], false).unwrap();
                    let df = df_left
                        .join(&df_right, ["on"], ["on"], JoinArgs::new(JoinType::Inner))
                        .unwrap();
                    Some(DFPathReturn {
                        df,
                        dt_subj,
                        dt_obj,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
        PropertyPathExpression::Alternative(left, _right) => {
            let left_res = df_path(left, cat_df_map, max_index);
            let right_res = df_path(left, cat_df_map, max_index);
            if let Some(DFPathReturn {
                df: df_left,
                dt_subj: dt_subj_left,
                dt_obj: dt_obj_left,
            }) = left_res
            {
                if let Some(DFPathReturn {
                    df: df_right,
                    dt_subj: dt_subj_right,
                    dt_obj: dt_obj_right,
                }) = right_res
                {
                    let df = df_left
                        .vstack(&df_right)
                        .unwrap()
                        .unique(None, UniqueKeepStrategy::First, None)
                        .unwrap();
                    Some(DFPathReturn {
                        df,
                        dt_subj: dt_subj_left.union(&dt_subj_right),
                        dt_obj: dt_obj_left.union(&dt_obj_right),
                    })
                } else {
                    Some(DFPathReturn {
                        df: df_left,
                        dt_subj: dt_subj_left,
                        dt_obj: dt_obj_left,
                    })
                }
            } else if let Some(r) = right_res {
                Some(r)
            } else {
                None
            }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let res = df_path(inner, cat_df_map, max_index);
            if let Some(DFPathReturn {
                df,
                dt_subj,
                dt_obj,
            }) = res
            {
                let mut all_subjects = Series::from_iter(0..max_index)
                    .cast(&DataType::Categorical(None))
                    .unwrap();
                all_subjects.rename("subject");
                let mut all_objects = Series::from_iter(0..max_index)
                    .cast(&DataType::Categorical(None))
                    .unwrap();
                all_objects.rename("object");
                let id_df = DataFrame::new(vec![all_subjects, all_objects]).unwrap();
                let df = concat_df([&df, &id_df]).unwrap();
                Some(DFPathReturn {
                    df: df.unique(None, UniqueKeepStrategy::First, None).unwrap(),
                    dt_subj,
                    dt_obj,
                })
            } else {
                None
            }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            todo!()
            // let cat_df = cat_df_map.get(&nns_name(nns)).unwrap();
            // DFPathReturn {
            //     df: cat_df.clone(),
            //     soo: SubjectOrObject::Subject,
            //     dt: RDFNodeType::IRI, //TODO: Fix properly
            // }
        }
        _ => {
            panic!("Should never happen")
        }
    }
}

fn df_with_cats(df: DataFrame) -> DataFrame {
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
    DataFrame::new(vec![subject, object]).unwrap()
}

fn find_max_index(vals: Values<String, (DataFrame, RDFNodeType, RDFNodeType)>) -> u32 {
    let mut max_index = 0u32;
    for (df, _, _) in vals {
        if let Some(max_subject) = df
            .column("subject")
            .unwrap()
            .categorical()
            .unwrap()
            .logical()
            .max()
        {
            max_index = max(max_index, max_subject);
        }
        if let Some(max_object) = df
            .column("object")
            .unwrap()
            .categorical()
            .unwrap()
            .logical()
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
        .logical()
        .clone()
        .into_series();
    let obj = df
        .column("object")
        .unwrap()
        .categorical()
        .unwrap()
        .logical()
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
        PropertyPathExpression::ZeroOrOne(_) => false,
        PropertyPathExpression::NegatedPropertySet(_) => false,
    }
}

fn sparse_path(
    ppe: &PropertyPathExpression,
    cat_df_map: &HashMap<String, (DataFrame, RDFNodeType, RDFNodeType)>,
    max_index: usize,
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
            }) = sparse_path(inner, cat_df_map, max_index)
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
            let res_left = sparse_path(left, cat_df_map, max_index);
            let res_right = sparse_path(right, cat_df_map, max_index);
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
            let res_left = sparse_path(left, cat_df_map, max_index);
            let res_right = sparse_path(right, cat_df_map, max_index);
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
                    let sparmat = (&sparmat_left + &sparmat_right).to_csr().map(|x| (x > &0) as u32);
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
            }) = sparse_path(inner, cat_df_map, max_index) {
                let sparmat = zero_or_more(sparmat_inner);
                Some(SparsePathReturn {
                    sparmat,
                    dt_subj,
                    dt_obj,
                })
            } else {
                todo!("Should be just the diagonal..")
            }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
                dt_subj,
                dt_obj,
            }) = sparse_path(inner, cat_df_map, max_index) {
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
                sparmat: sparmat_inner, dt_subj, dt_obj,
                        }) = sparse_path(inner, cat_df_map, max_index) {
                let sparmat = zero_or_one(sparmat_inner);
                Some(SparsePathReturn { sparmat, dt_subj, dt_obj })
            } else {
                None
            }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            todo!()
            // let cat_df = cat_df_map.get(&nns_name(nns)).unwrap();
            // let sparmat = to_csr(cat_df, max_index);
            // SparsePathReturn {
            //     sparmat,
            //     soo: SubjectOrObject::Subject,
            //     dt: RDFNodeType::IRI,
            // }
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
