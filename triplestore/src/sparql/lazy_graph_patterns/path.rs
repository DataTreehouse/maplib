use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::query_context::Context;
use crate::sparql::solution_mapping::SolutionMappings;
use crate::sparql::sparql_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use oxrdf::NamedNode;
use polars::prelude::{col, concat, DataFrameJoinOps, Expr, IntoLazy, UnionArgs};
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

enum SubjectOrObject {
    Subject,
    Object,
}

type SparseMatrix = CsMatBase<u32, usize, Vec<usize>, Vec<usize>, Vec<u32>, usize>;

struct SparsePathReturn {
    sparmat: SparseMatrix,
    soo: SubjectOrObject,
    dt: RDFNodeType,
}

struct DFPathReturn {
    df: DataFrame,
    soo: SubjectOrObject,
    dt: RDFNodeType,
}

impl SubjectOrObject {
    fn flip(&self) -> SubjectOrObject {
        match self {
            &SubjectOrObject::Subject => SubjectOrObject::Object,
            &SubjectOrObject::Object => SubjectOrObject::Subject,
        }
    }
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
        let mut out_df;
        let out_soo;
        let out_dt;

        let cat_df_map = self.create_unique_cat_dfs(ppe, Some(subject), Some(object))?;
        let max_index = find_max_index(cat_df_map.values());
        if create_sparse {
            let SparsePathReturn { sparmat, soo, dt } =
                sparse_path(ppe, &cat_df_map, max_index as usize);
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
            let mut lookup_df = find_lookup(&cat_df_map);
            let mut subject_series = Series::from_iter(subject_vec.into_iter());
            subject_series.rename("subject_key");
            let mut object_series = Series::from_iter(object_vec.into_iter());
            object_series.rename("object_key");
            out_df = DataFrame::new(vec![subject_series, object_series]).unwrap();
            lookup_df.rename("value", "subject").unwrap();
            out_df = out_df
                .join(
                    &lookup_df,
                    &["subject_key"],
                    &["key"],
                    JoinArgs::new(JoinType::Inner),
                )
                .unwrap();
            lookup_df.rename("subject", "object").unwrap();
            out_df = out_df
                .join(
                    &lookup_df,
                    &["object_key"],
                    &["key"],
                    JoinArgs::new(JoinType::Inner),
                )
                .unwrap();
            out_df = out_df.select(["subject", "object"]).unwrap();
        } else {
            let DFPathReturn { df, soo, dt } = df_path(ppe, &cat_df_map, max_index);
            out_df = df;
            out_soo = soo;
            out_dt = dt;
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
                    .insert(v.as_str().to_string(), RDFNodeType::IRI);
            }
            if let TermPattern::Variable(v) = object {
                mappings
                    .rdf_node_types
                    .insert(v.as_str().to_string(), RDFNodeType::IRI);
            }

            return Ok(mappings);
        } else {
            let mut datatypes = HashMap::new();
            if let TermPattern::Variable(v) = subject {
                datatypes.insert(v.as_str().to_string(), RDFNodeType::IRI);
            }
            if let TermPattern::Variable(v) = object {
                datatypes.insert(v.as_str().to_string(), RDFNodeType::IRI);
            }
            return Ok(SolutionMappings {
                mappings: out_df.lazy(),
                columns: var_cols.into_iter().map(|x| x.to_string()).collect(),
                rdf_node_types: datatypes,
            });
        }
    }

    fn create_unique_cat_dfs(
        &self,
        ppe: &PropertyPathExpression,
        subject: Option<&TermPattern>,
        object: Option<&TermPattern>,
    ) -> Result<HashMap<String, DataFrame>, SparqlError> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let df = self.get_single_nn_df(nn.as_str(), subject, object)?;
                if let Some(df) = df {
                    let unique_cat_df = df_with_cats(df)
                        .unique(None, UniqueKeepStrategy::First, None)
                        .unwrap();
                    Ok(HashMap::from([(nn.as_str().to_string(), unique_cat_df)]))
                } else {
                    let df = DataFrame::new(vec![
                        Series::new_empty("subject", &DataType::Categorical(None)),
                        Series::new_empty("object", &DataType::Categorical(None)),
                    ])
                    .unwrap();
                    Ok(HashMap::from([(nn.as_str().to_string(), df)]))
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
                let mut left_df_map =
                    self.create_unique_cat_dfs(left, subject.clone(), object.clone())?;
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
                let lookup: Vec<_> = nns.iter().map(|x| x.as_str().to_string()).collect();
                let mut dfs = vec![];
                for nn in self.df_map.keys() {
                    if !lookup.contains(nn) {
                        let df = self.get_single_nn_df(nn, subject, object)?;
                        if let Some(df) = df {
                            dfs.push(df_with_cats(df));
                        }
                    }
                }
                let df;
                if dfs.len() > 0 {
                    df = concat_df(dfs.as_slice())
                        .unwrap()
                        .unique(None, UniqueKeepStrategy::First, None)
                        .unwrap();
                } else {
                    df = DataFrame::new(vec![
                        Series::new_empty("subject", &DataType::Categorical(None)),
                        Series::new_empty("object", &DataType::Categorical(None)),
                    ])
                    .unwrap();
                }
                Ok(HashMap::from([(nns_name(nns), df)]))
            }
        }
    }

    fn get_single_nn_df(
        &self,
        nn: &str,
        subject: Option<&TermPattern>,
        object: Option<&TermPattern>,
    ) -> Result<Option<DataFrame>, SparqlError> {
        let map_opt = self.df_map.get(nn);
        if let Some(m) = map_opt {
            if m.is_empty() {
                panic!("Empty map should never happen");
            } else if m.len() > 1 {
                todo!("Multiple datatypes not supported yet")
            } else {
                let (dt, tt) = m.iter().next().unwrap();
                assert!(tt.unique, "Should be deduplicated");
                let mut lf = concat(
                    tt.get_lazy_frames()
                        .map_err(|x| SparqlError::TripleTableReadError(x))?,
                    UnionArgs::default(),
                )
                .unwrap()
                .select([col("subject"), col("object")]);
                if let Some(subject) = subject {
                    if let TermPattern::NamedNode(nn) = subject {
                        lf = lf.filter(
                            col("subject")
                                .eq(Expr::Literal(sparql_named_node_to_polars_literal_value(nn))),
                        )
                    } else if let TermPattern::Literal(l) = subject {
                        lf = lf.filter(
                            col("subject")
                                .eq(Expr::Literal(sparql_literal_to_polars_literal_value(l))),
                        )
                    }
                }
                if let Some(object) = object {
                    if let TermPattern::NamedNode(nn) = object {
                        lf = lf.filter(
                            col("object")
                                .eq(Expr::Literal(sparql_named_node_to_polars_literal_value(nn))),
                        )
                    } else if let TermPattern::Literal(l) = object {
                        lf = lf.filter(
                            col("object")
                                .eq(Expr::Literal(sparql_literal_to_polars_literal_value(l))),
                        )
                    }
                }
                Ok(Some(lf.collect().unwrap()))
            }
        } else {
            Ok(None)
        }
    }
}

fn find_lookup(map: &HashMap<String, DataFrame>) -> DataFrame {
    let mut all_values = vec![];
    for (k, v) in map {
        let mut obj = v.column("object").unwrap().unique().unwrap();
        obj.rename("value");
        let mut sub = v.column("subject").unwrap().unique().unwrap();
        sub.rename("value");
        all_values.push(DataFrame::new(vec![obj]).unwrap());
        all_values.push(DataFrame::new(vec![sub]).unwrap());
    }
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
    df
}

fn df_path(
    ppe: &PropertyPathExpression,
    cat_df_map: &HashMap<String, DataFrame>,
    max_index: u32,
) -> DFPathReturn {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            let df = cat_df_map.get(nn.as_str()).unwrap();
            DFPathReturn {
                df: df.clone(),
                soo: SubjectOrObject::Subject,
                dt: RDFNodeType::IRI,
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            let DFPathReturn { df, soo, dt } = df_path(inner, cat_df_map, max_index);
            let df = df
                .lazy()
                .rename(["subject", "object"], ["object", "subject"])
                .collect()
                .unwrap();
            DFPathReturn {
                df,
                soo: soo.flip(),
                dt,
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let DFPathReturn {
                df: mut df_left,
                soo: _,
                dt: _,
            } = df_path(left, cat_df_map, max_index);
            let DFPathReturn {
                df: mut df_right,
                soo: soo_right,
                dt: dt_right,
            } = df_path(left, cat_df_map, max_index);
            df_left.rename("object", "on").unwrap();
            df_right.rename("subject", "on").unwrap();
            df_left = df_left.sort(vec!["on"], vec![false], false).unwrap();
            df_right = df_right.sort(vec!["on"], vec![false], false).unwrap();
            let df = df_left
                .join(&df_right, ["on"], ["on"], JoinArgs::new(JoinType::Inner))
                .unwrap();
            DFPathReturn {
                df,
                soo: soo_right,
                dt: dt_right,
            }
        }
        PropertyPathExpression::Alternative(left, right) => {
            let DFPathReturn {
                df: df_left,
                soo: soo_left,
                dt: dt_left,
            } = df_path(left, cat_df_map, max_index);
            let DFPathReturn {
                df: df_right,
                soo: _soo_right,
                dt: _dt_right,
            } = df_path(left, cat_df_map, max_index);
            let df = df_left
                .vstack(&df_right)
                .unwrap()
                .unique(None, UniqueKeepStrategy::First, None)
                .unwrap();
            DFPathReturn {
                df,
                soo: soo_left,
                dt: dt_left,
            }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let DFPathReturn { df, soo, dt } = df_path(inner, cat_df_map, max_index);
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
            DFPathReturn {
                df: df.unique(None, UniqueKeepStrategy::First, None).unwrap(),
                soo,
                dt,
            }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let cat_df = cat_df_map.get(&nns_name(nns)).unwrap();
            DFPathReturn {
                df: cat_df.clone(),
                soo: SubjectOrObject::Subject,
                dt: RDFNodeType::IRI, //TODO: Fix properly
            }
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

fn find_max_index(vals: Values<String, DataFrame>) -> u32 {
    let mut max_index = 0u32;
    for df in vals {
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
    cat_df_map: &HashMap<String, DataFrame>,
    max_index: usize,
) -> SparsePathReturn {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            let sparmat = to_csr(cat_df_map.get(nn.as_str()).unwrap(), max_index);
            SparsePathReturn {
                sparmat,
                soo: SubjectOrObject::Subject,
                dt: RDFNodeType::IRI,
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            let SparsePathReturn { sparmat, soo, dt } = sparse_path(inner, cat_df_map, max_index);
            SparsePathReturn {
                sparmat: sparmat.transpose_into(),
                soo: soo.flip(),
                dt: dt,
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let SparsePathReturn {
                sparmat: sparmat_left,
                soo: _,
                dt: _,
            } = sparse_path(left, cat_df_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_right,
                soo: soo_right,
                dt: dt_right,
            } = sparse_path(right, cat_df_map, max_index);
            let sparmat = (&sparmat_left * &sparmat_right).to_csr();
            SparsePathReturn {
                sparmat,
                soo: soo_right,
                dt: dt_right,
            }
        }
        PropertyPathExpression::Alternative(a, b) => {
            let SparsePathReturn {
                sparmat: sparmat_a,
                soo: soo_a,
                dt: dt_a,
            } = sparse_path(a, cat_df_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_b,
                soo: soo_b,
                dt: dt_b,
            } = sparse_path(b, cat_df_map, max_index);
            let sparmat = (&sparmat_a + &sparmat_b).to_csr().map(|x| (x > &0) as u32);
            SparsePathReturn {
                sparmat,
                soo: soo_a,
                dt: dt_a,
            }
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = zero_or_more(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = one_or_more(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = zero_or_one(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let cat_df = cat_df_map.get(&nns_name(nns)).unwrap();
            let sparmat = to_csr(cat_df, max_index);
            SparsePathReturn {
                sparmat,
                soo: SubjectOrObject::Subject,
                dt: RDFNodeType::IRI,
            }
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
