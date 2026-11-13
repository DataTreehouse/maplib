use super::Triplestore;
use crate::sparql::errors::SparqlError;
use crate::sparql::QuerySettings;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{
    by_name, col, lit, AnyValue, DataFrame, IntoLazy, IntoSeries, JoinArgs, JoinType, Series,
    UniqueKeepStrategy,
};
use polars_core::prelude::{DataType, ExplodeOptions, SortMultipleOptions};
use polars_core::utils::Container;
use query_processing::errors::QueryProcessingError;
use query_processing::expressions::{literal_enc, named_node_enc, named_node_local_enc};
use query_processing::graph_patterns::{distinct, group_by_workaround, join, union};
use query_processing::pushdowns::Pushdowns;
use rayon::prelude::*;
use representation::cats::{Cats, LockedCats};
use representation::dataset::{NamedGraph, QueryGraph};
use representation::multitype::{
    compress_actual_multitypes, force_convert_multicol_to_single_col, nest_multicolumns,
};
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use sprs::{CsMatBase, TriMatBase};
use std::collections::{HashMap, HashSet};

const NAMED_NODE_INDEX_COL: &str = "named_node_index_column";
const VALUE_COLUMN: &str = "value";
const LOOKUP_COLUMN: &str = "key";

type SparseMatrix = CsMatBase<u32, usize, Vec<usize>, Vec<usize>, Vec<u32>, usize>;

struct SparsePathReturn {
    sparmat: SparseMatrix,
    eye: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum PathLeaf {
    NamedNode(NamedNode),
    NegatedSet(Vec<NamedNode>),
}

impl PathLeaf {
    fn negated(nns: &[NamedNode]) -> Self {
        let mut v = nns.to_vec();
        v.sort();
        v.dedup();
        PathLeaf::NegatedSet(v)
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
        pushdowns: Pushdowns,
        query_settings: &QuerySettings,
        dataset: &QueryGraph,
    ) -> Result<SolutionMappings, SparqlError> {
        let create_sparse = need_sparse_matrix(ppe);

        let mut var_cols = vec![];
        match subject {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        match object {
            TermPattern::BlankNode(b) => {
                var_cols.push(b.to_string());
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
            }
            _ => {}
        }

        if !create_sparse {
            let mut intermediaries = vec![];
            let mut gp = create_graph_pattern(ppe, subject, object, &mut intermediaries);
            gp = GraphPattern::Distinct {
                inner: Box::new(gp),
            };
            let mut sms = self.lazy_graph_pattern(
                &gp,
                None,
                &context.extension_with(PathEntry::PathRewrite),
                None,
                pushdowns,
                query_settings,
                dataset,
            )?;
            for i in &intermediaries {
                sms.rdf_node_types.remove(i).unwrap();
            }
            sms.mappings = sms.mappings.drop(by_name(intermediaries, true, false));
            if let Some(solution_mappings) = solution_mappings {
                sms = join(
                    sms,
                    solution_mappings,
                    JoinType::Inner,
                    self.global_cats.clone(),
                    query_settings.max_rows,
                )?;
            }
            return Ok(sms);
        }

        let mut out_df;

        // Values that the ends of the path are bound to, from the solution mappings so far.
        let mut pushdowns = pushdowns;
        let focus: Vec<&str> = [subject, object]
            .into_iter()
            .filter_map(|t| match t {
                TermPattern::Variable(v) => Some(v.as_str()),
                _ => None,
            })
            .collect();
        let solution_mappings = solution_mappings
            .map(|sm| pushdowns.add_from_solution_mappings(sm, self.global_cats.clone(), &focus));

        let mut df_creator = U32DataFrameCreator::new(dataset.clone());
        df_creator.gather_namednode_dfs(ppe, self)?;
        let (lookup_df, lookup_dtypes, namednode_dfs) =
            df_creator.create_u32_dfs(self.global_cats.clone())?;
        let max_index: u32 = lookup_df
            .column(LOOKUP_COLUMN)
            .unwrap()
            .as_materialized_series()
            .max()
            .unwrap()
            .unwrap_or(0);
        let bound = {
            let cats = self.global_cats.read().unwrap();
            bound_start_keys(
                subject,
                object,
                &pushdowns,
                &lookup_df,
                &lookup_dtypes,
                &cats,
            )
        };
        let (subject_vec, object_vec) = if let Some((starts, reversed)) = bound {
            // Only the paths from the nodes an end is bound to are followed.
            paths_from(ppe, starts, reversed, &namednode_dfs, max_index as usize)
        } else {
            let SparsePathReturn { sparmat, .. } =
                sparse_path(ppe, &namednode_dfs, max_index as usize);
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
            (subject_vec, object_vec)
        };
        let mut subject_series = Series::from_iter(subject_vec);
        subject_series.rename("subject_key".into());
        let mut object_series = Series::from_iter(object_vec);
        object_series.rename("object_key".into());
        let mut out_lf = DataFrame::new(
            subject_series.len(),
            vec![subject_series.into(), object_series.into()],
        )
        .unwrap()
        .lazy();
        let lookup_subject_lf =
            lookup_df
                .clone()
                .lazy()
                .rename([VALUE_COLUMN], [SUBJECT_COL_NAME], true);
        out_lf = out_lf
            .join(
                lookup_subject_lf,
                &[col("subject_key")],
                &[col(LOOKUP_COLUMN)],
                JoinArgs::new(JoinType::Inner),
            )
            .unwrap()
            .drop(by_name(["subject_key"], true, false));
        let lookup_object_lf = lookup_df
            .lazy()
            .rename([VALUE_COLUMN], [OBJECT_COL_NAME], true);
        out_lf = out_lf
            .join(
                lookup_object_lf,
                &[col("object_key")],
                &[col(LOOKUP_COLUMN)],
                JoinArgs::new(JoinType::Inner),
            )
            .unwrap()
            .drop(by_name(["object_key"], true, false));
        out_df = out_lf.collect().unwrap();
        // With the same variable at both ends, the path must end where it starts.
        let same_variable = matches!(
            (term_variable_name(subject), term_variable_name(object)),
            (Some(s), Some(o)) if s == o
        );
        if same_variable {
            out_df = out_df
                .lazy()
                .filter(col(SUBJECT_COL_NAME).eq(col(OBJECT_COL_NAME)))
                .collect()
                .unwrap();
        }
        let mut dtypes = HashMap::new();
        dtypes.insert(
            SUBJECT_COL_NAME.to_string(),
            lookup_dtypes.get(VALUE_COLUMN).unwrap().clone(),
        );
        dtypes.insert(
            OBJECT_COL_NAME.to_string(),
            lookup_dtypes.get(VALUE_COLUMN).unwrap().clone(),
        );

        if lookup_dtypes.get(VALUE_COLUMN).unwrap().is_multi() {
            if let TermPattern::NamedNode(_) = subject {
                out_df = force_convert_multicol_to_single_col(
                    out_df.lazy(),
                    SUBJECT_COL_NAME,
                    &BaseRDFNodeType::IRI,
                )
                .collect()
                .unwrap();
                dtypes.insert(
                    SUBJECT_COL_NAME.to_string(),
                    BaseRDFNodeType::IRI.into_default_stored_rdf_node_state(),
                );
            }
            if let TermPattern::NamedNode(_) = object {
                out_df = force_convert_multicol_to_single_col(
                    out_df.lazy(),
                    OBJECT_COL_NAME,
                    &BaseRDFNodeType::IRI,
                )
                .collect()
                .unwrap();
                dtypes.insert(
                    OBJECT_COL_NAME.to_string(),
                    BaseRDFNodeType::IRI.into_default_stored_rdf_node_state(),
                );
            }
            if let TermPattern::Literal(l) = subject {
                out_df = force_convert_multicol_to_single_col(
                    out_df.lazy(),
                    SUBJECT_COL_NAME,
                    &BaseRDFNodeType::from_literal(l),
                )
                .collect()
                .unwrap();
                dtypes.insert(
                    SUBJECT_COL_NAME.to_string(),
                    BaseRDFNodeType::from_literal(l).into_default_stored_rdf_node_state(),
                );
            }
            if let TermPattern::Literal(l) = object {
                out_df = force_convert_multicol_to_single_col(
                    out_df.lazy(),
                    OBJECT_COL_NAME,
                    &BaseRDFNodeType::from_literal(l),
                )
                .collect()
                .unwrap();
                dtypes.insert(
                    OBJECT_COL_NAME.to_string(),
                    BaseRDFNodeType::from_literal(l).into_default_stored_rdf_node_state(),
                );
            }
        }
        (out_df, dtypes) = compress_actual_multitypes(out_df, dtypes);
        let out_dt_subj = dtypes.remove(SUBJECT_COL_NAME).unwrap();
        let out_dt_obj = dtypes.remove(OBJECT_COL_NAME).unwrap();

        let mut var_cols = vec![];
        let mut rename_src = vec![];
        let mut rename_trg = vec![];

        let mut constant_match_height = 0;
        let cats = self.global_cats.read().unwrap();
        match subject {
            TermPattern::NamedNode(nn) => {
                let e = named_node_enc(nn, &cats);
                let e = e.unwrap_or(lit(false));
                out_df = out_df
                    .lazy()
                    .filter(col(SUBJECT_COL_NAME).eq(e))
                    .collect()
                    .unwrap();
                out_df = out_df.drop(SUBJECT_COL_NAME).unwrap();
            }
            TermPattern::BlankNode(b) => {
                let bname = b.to_string();
                var_cols.push(bname.clone());
                rename_src.push(SUBJECT_COL_NAME);
                rename_trg.push(bname);
            }
            TermPattern::Literal(l) => {
                let (l, ..) = literal_enc(l, &cats)?;
                out_df = out_df
                    .lazy()
                    .filter(col(SUBJECT_COL_NAME).eq(l))
                    .collect()
                    .unwrap();
                out_df = out_df.drop(SUBJECT_COL_NAME).unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                if v.as_str() != SUBJECT_COL_NAME {
                    rename_src.push(SUBJECT_COL_NAME);
                    rename_trg.push(v.as_str().into());
                }
            }
        }

        match object {
            TermPattern::NamedNode(nn) => {
                let e = named_node_enc(nn, &cats);
                let e = e.unwrap_or(lit(false));
                out_df = out_df
                    .lazy()
                    .filter(col(OBJECT_COL_NAME).eq(e))
                    .collect()
                    .unwrap();
                constant_match_height = out_df.height();
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            // The subject column already holds the variable, and equals the object column.
            TermPattern::BlankNode(_) | TermPattern::Variable(_) if same_variable => {
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            TermPattern::BlankNode(b) => {
                let bname = b.to_string();
                var_cols.push(bname.clone());
                rename_src.push(OBJECT_COL_NAME);
                rename_trg.push(bname);
            }
            TermPattern::Literal(l) => {
                let (l, ..) = literal_enc(l, &cats)?;
                out_df = out_df
                    .lazy()
                    .filter(col(OBJECT_COL_NAME).eq(l))
                    .collect()
                    .unwrap();
                constant_match_height = out_df.height();
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            TermPattern::Variable(v) => {
                var_cols.push(v.as_str().to_string());
                if v.as_str() != OBJECT_COL_NAME {
                    rename_src.push(OBJECT_COL_NAME);
                    rename_trg.push(v.as_str().into());
                }
            }
        }

        let mut datatypes = HashMap::new();
        if let TermPattern::Variable(v) = subject {
            datatypes.insert(v.as_str().to_string(), out_dt_subj);
        } else if let TermPattern::BlankNode(b) = subject {
            datatypes.insert(b.to_string(), out_dt_subj);
        }
        if same_variable {
            // Only the subject column is kept.
        } else if let TermPattern::Variable(v) = object {
            datatypes.insert(v.as_str().to_string(), out_dt_obj);
        } else if let TermPattern::BlankNode(b) = object {
            datatypes.insert(b.to_string(), out_dt_obj);
        }
        let subject_constant = is_constant(subject);
        let object_constant = is_constant(object);
        if subject_constant && object_constant {
            // A frame without columns must have its height set explicitly.
            let matches =
                constant_match_height > 0 || (matches_zero_length(ppe) && subject == object);
            out_df = DataFrame::empty_with_height(if matches { 1 } else { 0 });
        }
        let out_df_height = out_df.height();

        let mut out_lf = out_df.lazy();
        if !rename_src.is_empty() {
            out_lf = out_lf.rename(rename_src, rename_trg, true);
        }
        let mut path_solution_mappings = SolutionMappings {
            mappings: out_lf,
            rdf_node_types: datatypes,
            height_estimate: out_df_height,
        };
        // A zero length path from a constant term matches the term itself,
        // even if the term does not occur in the graph.
        if matches_zero_length(ppe) && subject_constant != object_constant {
            let (constant, variable) = if subject_constant {
                (subject, object)
            } else {
                (object, subject)
            };
            let variable = match variable {
                TermPattern::Variable(v) => v.as_str().to_string(),
                TermPattern::BlankNode(b) => b.to_string(),
                _ => unreachable!("Should never happen"),
            };
            let (expr, state) = match constant {
                TermPattern::NamedNode(nn) => {
                    let (e, local) = named_node_local_enc(nn, &cats);
                    (
                        e,
                        RDFNodeState::from_bases(
                            BaseRDFNodeType::IRI,
                            BaseCatState::CategoricalNative(local.map(LockedCats::new)),
                        ),
                    )
                }
                TermPattern::Literal(l) => {
                    let (e, bt, bs) = literal_enc(l, &cats)?;
                    (e, RDFNodeState::from_bases(bt, bs))
                }
                _ => unreachable!("Should never happen"),
            };
            drop(cats);
            let zero_length = SolutionMappings::new(
                DataFrame::empty_with_height(1)
                    .lazy()
                    .with_column(expr.alias(&variable)),
                HashMap::from([(variable, state)]),
                1,
            );
            path_solution_mappings = distinct(union(
                vec![path_solution_mappings, zero_length],
                false,
                self.global_cats.clone(),
            )?)?;
        } else {
            drop(cats);
        }
        if let Some(mappings) = solution_mappings {
            path_solution_mappings = join(
                path_solution_mappings,
                mappings,
                JoinType::Inner,
                self.global_cats.clone(),
                query_settings.max_rows,
            )?;
        }
        Ok(path_solution_mappings)
    }
}

pub fn create_graph_pattern(
    ppe: &PropertyPathExpression,
    subject: &TermPattern,
    object: &TermPattern,
    intermediaries: &mut Vec<String>,
) -> GraphPattern {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => GraphPattern::Bgp {
            patterns: vec![TriplePattern {
                subject: subject.clone(),
                predicate: NamedNodePattern::NamedNode(nn.to_owned()),
                object: object.clone(),
            }],
        },
        PropertyPathExpression::Reverse(r) => {
            create_graph_pattern(r, object, subject, intermediaries)
        }
        PropertyPathExpression::Sequence(l, r) => {
            let intermediary = format!("v{}", uuid::Uuid::new_v4());
            let intermediary_tp = TermPattern::Variable(Variable::new_unchecked(&intermediary));
            intermediaries.push(intermediary);
            GraphPattern::Join {
                left: Box::new(create_graph_pattern(
                    l,
                    subject,
                    &intermediary_tp,
                    intermediaries,
                )),
                right: Box::new(create_graph_pattern(
                    r,
                    &intermediary_tp,
                    object,
                    intermediaries,
                )),
            }
        }
        PropertyPathExpression::Alternative(a, b) => GraphPattern::Union {
            left: Box::new(create_graph_pattern(a, subject, object, intermediaries)),
            right: Box::new(create_graph_pattern(b, subject, object, intermediaries)),
        },
        PropertyPathExpression::ZeroOrMore(_)
        | PropertyPathExpression::OneOrMore(_)
        | PropertyPathExpression::ZeroOrOne(_) => {
            panic!("Should never happen")
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let pred_name = format!("v{}", uuid::Uuid::new_v4());
            let pred_var = Variable::new_unchecked(&pred_name);
            intermediaries.push(pred_name);
            let excluded = nns
                .iter()
                .map(|nn| Expression::NamedNode(nn.clone()))
                .collect();
            GraphPattern::Filter {
                expr: Expression::Not(Box::new(Expression::In(
                    Box::new(Expression::Variable(pred_var.clone())),
                    excluded,
                ))),
                inner: Box::new(GraphPattern::Bgp {
                    patterns: vec![TriplePattern {
                        subject: subject.clone(),
                        predicate: NamedNodePattern::Variable(pred_var),
                        object: object.clone(),
                    }],
                }),
            }
        }
    }
}

fn to_csr(df: &DataFrame, max_index: usize) -> SparseMatrix {
    let sub = df
        .column(SUBJECT_COL_NAME)
        .unwrap()
        .u32()
        .unwrap()
        .clone()
        .into_series();
    let obj = df
        .column(OBJECT_COL_NAME)
        .unwrap()
        .u32()
        .unwrap()
        .clone()
        .into_series();
    let df = DataFrame::new(sub.len(), vec![sub.into(), obj.into()]).unwrap();
    let mut df = df
        .sort(
            vec![SUBJECT_COL_NAME, SUBJECT_COL_NAME],
            SortMultipleOptions::default()
                .with_maintain_order(false)
                .with_order_descending(false),
        )
        .unwrap();
    df.rechunk_mut();
    let subject = df
        .column(SUBJECT_COL_NAME)
        .unwrap()
        .as_materialized_series();
    let object = df.column(OBJECT_COL_NAME).unwrap().as_materialized_series();
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

fn empty_csr(max_index: usize) -> SparseMatrix {
    let trimat = TriMatBase::from_triplets(
        (max_index + 1, max_index + 1),
        Vec::new(),
        Vec::new(),
        Vec::new(),
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

pub fn need_sparse_matrix(ppe: &PropertyPathExpression) -> bool {
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
    namednode_map: &HashMap<PathLeaf, DataFrame>,
    max_index: usize,
) -> SparsePathReturn {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            if let Some(df) = namednode_map.get(&PathLeaf::NamedNode(nn.clone())) {
                let sparmat = to_csr(df, max_index);
                SparsePathReturn {
                    sparmat,
                    eye: false,
                }
            } else {
                let sparmat = empty_csr(max_index);
                SparsePathReturn {
                    sparmat,
                    eye: false,
                }
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            let SparsePathReturn { sparmat, eye } = sparse_path(inner, namednode_map, max_index);

            SparsePathReturn {
                sparmat: sparmat.transpose_into(),
                eye,
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let res_left = sparse_path(left, namednode_map, max_index);
            let res_right = sparse_path(right, namednode_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_left,
                eye: eye_left,
            } = res_left;

            let SparsePathReturn {
                sparmat: sparmat_right,
                eye: eye_right,
            } = res_right;

            let mut sparmat = (&sparmat_left * &sparmat_right).to_csr();
            if eye_left {
                sparmat = (&sparmat + &sparmat_right).to_csr();
            }
            if eye_right {
                sparmat = (&sparmat + &sparmat_left).to_csr();
            }
            sparmat = sparmat.map(|x| (x > &0) as u32);
            SparsePathReturn {
                sparmat,
                eye: eye_left && eye_right,
            }
        }
        PropertyPathExpression::Alternative(left, right) => {
            let res_left = sparse_path(left, namednode_map, max_index);
            let res_right = sparse_path(right, namednode_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_left,
                eye: eye_left,
            } = res_left;

            let SparsePathReturn {
                sparmat: sparmat_right,
                eye: eye_right,
            } = res_right;

            let sparmat = (&sparmat_left + &sparmat_right)
                .to_csr()
                .map(|x| (x > &0) as u32);
            SparsePathReturn {
                sparmat,
                eye: eye_left || eye_right,
            }
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                ..
            } = sparse_path(inner, namednode_map, max_index);

            let sparmat = zero_or_more(sparmat_inner);
            SparsePathReturn { sparmat, eye: true }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                eye,
            } = sparse_path(inner, namednode_map, max_index);

            let sparmat = one_or_more(sparmat_inner);
            SparsePathReturn { sparmat, eye }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                ..
            } = sparse_path(inner, namednode_map, max_index);
            let sparmat = zero_or_one(sparmat_inner);
            SparsePathReturn { sparmat, eye: true }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let sparmat = match namednode_map.get(&PathLeaf::negated(nns)) {
                Some(df) => to_csr(df, max_index).map(|x| (x > &0) as u32),
                None => empty_csr(max_index),
            };
            SparsePathReturn {
                sparmat,
                eye: false,
            }
        }
    }
}

struct U32DataFrameCreator {
    pub named_nodes: HashMap<PathLeaf, (DataFrame, RDFNodeState, RDFNodeState)>,
    dataset: QueryGraph,
}

impl U32DataFrameCreator {
    pub fn new(dataset: QueryGraph) -> Self {
        U32DataFrameCreator {
            named_nodes: Default::default(),
            dataset,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn create_u32_dfs(
        self,
        global_cats: LockedCats,
    ) -> Result<
        (
            DataFrame,
            HashMap<String, RDFNodeState>,
            HashMap<PathLeaf, DataFrame>,
        ),
        QueryProcessingError,
    > {
        // TODO! Possible to constrain lookup to only nodes that may occur as subj/obj in path expr.
        // Can reduce size of a join
        let mut nns: Vec<_> = self.named_nodes.keys().cloned().collect();
        nns.sort();

        let mut soln_mappings = vec![];
        for (nn, (df, subject_dt, object_dt)) in self.named_nodes {
            let nn_idx = nns.iter().position(|x| x == &nn).unwrap();
            let df_height = df.height();
            let mut lf = df.lazy();
            lf = lf.with_column(lit(nn_idx as u32).alias(NAMED_NODE_INDEX_COL));
            let mut types = HashMap::new();
            types.insert(
                NAMED_NODE_INDEX_COL.to_string(),
                BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                    .into_default_stored_rdf_node_state(),
            );
            types.insert(SUBJECT_COL_NAME.to_string(), subject_dt);
            types.insert(OBJECT_COL_NAME.to_string(), object_dt);

            soln_mappings.push(SolutionMappings::new(lf, types, df_height));
        }
        let SolutionMappings {
            mut mappings,
            rdf_node_types,
            ..
        } = union(soln_mappings, false, global_cats.clone())?;

        let subject_row_index = uuid::Uuid::new_v4().to_string();
        let object_row_index = uuid::Uuid::new_v4().to_string();
        let mut df = mappings.collect().unwrap();
        let df_height = df.height();
        mappings = df.lazy();
        mappings = mappings
            .with_row_index(&subject_row_index, None)
            .with_column(
                (col(&subject_row_index) + lit(df_height as u32)).alias(&object_row_index),
            );
        df = mappings.collect().unwrap();

        let lf_subj = df.clone().lazy().select([
            col(SUBJECT_COL_NAME).alias(VALUE_COLUMN),
            col(&subject_row_index),
        ]);
        let lf_obj = df.clone().lazy().select([
            col(OBJECT_COL_NAME).alias(VALUE_COLUMN),
            col(&object_row_index),
        ]);

        let mut subj_types = HashMap::new();
        subj_types.insert(
            VALUE_COLUMN.to_string(),
            rdf_node_types.get(SUBJECT_COL_NAME).unwrap().clone(),
        );
        subj_types.insert(
            subject_row_index.clone(),
            BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );

        let mut obj_types = HashMap::new();
        obj_types.insert(
            VALUE_COLUMN.to_string(),
            rdf_node_types.get(OBJECT_COL_NAME).unwrap().clone(),
        );
        obj_types.insert(
            object_row_index.clone(),
            BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned(), None)
                .into_default_stored_rdf_node_state(),
        );

        let subj_soln_mappings = SolutionMappings::new(lf_subj, subj_types, df_height);
        let obj_soln_mappings = SolutionMappings::new(lf_obj, obj_types, df_height);
        let SolutionMappings {
            mut mappings,
            rdf_node_types: lookup_df_types,
            ..
        } = union(
            vec![subj_soln_mappings, obj_soln_mappings],
            false,
            global_cats.clone(),
        )?;
        let (mappings_grby, maps) =
            group_by_workaround(mappings, &lookup_df_types, vec![VALUE_COLUMN.to_string()]);
        mappings = mappings_grby.agg([
            col(&subject_row_index).alias(&subject_row_index),
            col(&object_row_index).alias(&object_row_index),
        ]);
        mappings = nest_multicolumns(mappings, maps);

        mappings = mappings.with_row_index(LOOKUP_COLUMN, None);
        mappings = mappings.explode(
            by_name([&subject_row_index, &object_row_index], true, false),
            ExplodeOptions {
                empty_as_null: false,
                keep_nulls: true,
            },
        );
        let mut lookup_df = mappings.collect().unwrap();

        let out_dfs = df.partition_by([NAMED_NODE_INDEX_COL], true).unwrap();
        let mut out_df_map = HashMap::new();
        for mut df in out_dfs {
            let nn_ser = df.drop_in_place(NAMED_NODE_INDEX_COL).unwrap();
            //TODO: Investigate why cast is needed..
            let nn_idx = nn_ser
                .cast(&DataType::UInt32)
                .unwrap()
                .u32()
                .unwrap()
                .get(0)
                .unwrap();
            let mut lf = df
                .select([&subject_row_index, &object_row_index])
                .unwrap()
                .lazy();
            lf = lf
                .join(
                    lookup_df
                        .clone()
                        .lazy()
                        .rename([LOOKUP_COLUMN], [SUBJECT_COL_NAME], true)
                        .select([col(&subject_row_index), col(SUBJECT_COL_NAME)]),
                    [col(&subject_row_index)],
                    [col(&subject_row_index)],
                    JoinType::Inner.into(),
                )
                .unwrap()
                .join(
                    lookup_df
                        .clone()
                        .lazy()
                        .rename([LOOKUP_COLUMN], [OBJECT_COL_NAME], true)
                        .select([col(&object_row_index), col(OBJECT_COL_NAME)]),
                    [col(&object_row_index)],
                    [col(&object_row_index)],
                    JoinType::Inner.into(),
                )
                .unwrap()
                .select([col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);
            out_df_map.insert(
                nns.get(nn_idx as usize).unwrap().clone(),
                lf.collect().unwrap(),
            );
        }
        lookup_df = lookup_df
            .drop(&subject_row_index)
            .unwrap()
            .drop(&object_row_index)
            .unwrap()
            .unique::<(), ()>(None, UniqueKeepStrategy::Any, None)
            .unwrap();
        Ok((lookup_df, lookup_df_types, out_df_map))
    }

    fn gather_namednode_dfs(
        &mut self,
        ppe: &PropertyPathExpression,
        triplestore: &Triplestore,
    ) -> Result<(), SparqlError> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let leaf = PathLeaf::NamedNode(nn.clone());
                let predicates = vec![nn.clone()];

                if self.named_nodes.contains_key(&leaf) {
                    return Ok(());
                }
                let SolutionMappings {
                    mappings,
                    mut rdf_node_types,
                    ..
                } = triplestore.get_multi_predicates_solution_mappings(
                    Some(predicates),
                    &Some(SUBJECT_COL_NAME.to_string()),
                    &None,
                    &Some(OBJECT_COL_NAME.to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    &self.dataset,
                )?;
                let df = mappings.collect().unwrap();
                self.named_nodes.insert(
                    leaf,
                    (
                        df,
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    ),
                );
                Ok(())
            }
            PropertyPathExpression::Reverse(inner) => self.gather_namednode_dfs(inner, triplestore),
            PropertyPathExpression::Sequence(left, right) => {
                self.gather_namednode_dfs(left, triplestore)?;
                self.gather_namednode_dfs(right, triplestore)?;
                Ok(())
            }
            PropertyPathExpression::Alternative(left, right) => {
                self.gather_namednode_dfs(left, triplestore)?;
                self.gather_namednode_dfs(right, triplestore)?;
                Ok(())
            }
            PropertyPathExpression::ZeroOrMore(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::OneOrMore(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::ZeroOrOne(inner) => {
                self.gather_namednode_dfs(inner, triplestore)
            }
            PropertyPathExpression::NegatedPropertySet(nns) => {
                let graphs: Vec<NamedGraph> = match &self.dataset {
                    QueryGraph::NamedGraph(g) => vec![g.clone()],
                    QueryGraph::QueryDataset(qd) => qd
                        .default
                        .iter()
                        .map(|nn| NamedGraph::from_maybe_named_node(Some(nn)))
                        .collect(),
                };
                let mut allowed: Vec<NamedNode> = graphs
                    .iter()
                    .flat_map(|g| triplestore.all_predicates(g))
                    .filter(|p| !nns.contains(p))
                    .collect();
                allowed.sort();
                allowed.dedup();

                if self.named_nodes.contains_key(&PathLeaf::negated(nns)) {
                    return Ok(());
                }
                let SolutionMappings {
                    mappings,
                    mut rdf_node_types,
                    ..
                } = triplestore.get_multi_predicates_solution_mappings(
                    Some(allowed),
                    &Some(SUBJECT_COL_NAME.to_string()),
                    &None,
                    &Some(OBJECT_COL_NAME.to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    &self.dataset,
                )?;
                let df = mappings.collect().unwrap();
                self.named_nodes.insert(
                    PathLeaf::negated(nns),
                    (
                        df,
                        rdf_node_types.remove(SUBJECT_COL_NAME).unwrap(),
                        rdf_node_types.remove(OBJECT_COL_NAME).unwrap(),
                    ),
                );
                Ok(())
            }
        }
    }
}

fn is_constant(t: &TermPattern) -> bool {
    matches!(t, TermPattern::NamedNode(_) | TermPattern::Literal(_))
}

fn matches_zero_length(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::ZeroOrMore(_) | PropertyPathExpression::ZeroOrOne(_) => true,
        PropertyPathExpression::Sequence(a, b) => matches_zero_length(a) && matches_zero_length(b),
        PropertyPathExpression::Alternative(a, b) => {
            matches_zero_length(a) || matches_zero_length(b)
        }
        PropertyPathExpression::Reverse(a) => matches_zero_length(a),
        _ => false,
    }
}

fn term_variable_name(t: &TermPattern) -> Option<String> {
    match t {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The lookup keys of the nodes that one end of the path is bound to, and whether that end is
/// the object (so the path is followed backwards), or None if neither end is bound to IRIs that
/// can be listed. The end with fewer values is chosen.
fn bound_start_keys(
    subject: &TermPattern,
    object: &TermPattern,
    pushdowns: &Pushdowns,
    lookup_df: &DataFrame,
    lookup_dtypes: &HashMap<String, RDFNodeState>,
    cats: &Cats,
) -> Option<(Vec<u32>, bool)> {
    let subject_codes = bound_iri_codes(subject, pushdowns, cats);
    let object_codes = bound_iri_codes(object, pushdowns, cats);
    let (codes, reversed) = match (subject_codes, object_codes) {
        (Some(s), Some(o)) if o.len() < s.len() => (o, true),
        (Some(s), _) => (s, false),
        (None, Some(o)) => (o, true),
        (None, None) => return None,
    };
    let keys = lookup_keys_of_iris(lookup_df, lookup_dtypes, codes)?;
    Some((keys, reversed))
}

/// The global codes of the IRIs that an end of the path is bound to: a constant IRI, or a
/// variable whose possible values are pushed down and are all IRIs. IRIs that are not in the
/// dictionary are left out, since they do not occur in the graph.
fn bound_iri_codes(t: &TermPattern, pushdowns: &Pushdowns, cats: &Cats) -> Option<Vec<u32>> {
    let iris: Vec<Option<&str>> = match t {
        TermPattern::NamedNode(nn) => vec![Some(nn.as_str())],
        TermPattern::Variable(v) => {
            let values = pushdowns.variables_values.get(v.as_str())?;
            let mut iris = Vec::with_capacity(values.len());
            for x in values {
                let Term::NamedNode(nn) = x else {
                    return None;
                };
                iris.push(Some(nn.as_str()));
            }
            iris
        }
        _ => return None,
    };
    Some(
        cats.maybe_encode_iri_slice(&iris)
            .into_iter()
            .flatten()
            .collect(),
    )
}

/// The sorted lookup keys of the IRIs with the given global codes, or None if the IRIs of the
/// lookup are not encoded with global codes.
fn lookup_keys_of_iris(
    lookup_df: &DataFrame,
    lookup_dtypes: &HashMap<String, RDFNodeState>,
    codes: Vec<u32>,
) -> Option<Vec<u32>> {
    let state = lookup_dtypes.get(VALUE_COLUMN).unwrap();
    match state.map.get(&BaseRDFNodeType::IRI) {
        None => return Some(vec![]),
        Some(BaseCatState::CategoricalNative(None)) => {}
        Some(_) => return None,
    }
    let value = if state.is_multi() {
        col(VALUE_COLUMN)
            .struct_()
            .field_by_name(&BaseRDFNodeType::IRI.field_col_name())
    } else {
        col(VALUE_COLUMN)
    };
    let df = lookup_df
        .clone()
        .lazy()
        .select([
            value.alias(VALUE_COLUMN),
            col(LOOKUP_COLUMN).cast(DataType::UInt32),
        ])
        .collect()
        .unwrap();
    let wanted: HashSet<u32> = codes.into_iter().collect();
    let values = df.column(VALUE_COLUMN).unwrap().u32().unwrap();
    let keys = df.column(LOOKUP_COLUMN).unwrap().u32().unwrap();
    let mut out: Vec<u32> = values
        .iter()
        .zip(keys.iter())
        .filter_map(|(v, k)| match (v, k) {
            (Some(v), Some(k)) if wanted.contains(&v) => Some(k),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// The edges of a path leaf as adjacency lists over lookup keys (compressed sparse rows).
struct Adjacency {
    offsets: Vec<usize>,
    targets: Vec<u32>,
}

impl Adjacency {
    /// From a frame of subject and object keys, followed backwards if `reversed`.
    fn new(df: &DataFrame, n: usize, reversed: bool) -> Self {
        let (from, to) = if reversed {
            (OBJECT_COL_NAME, SUBJECT_COL_NAME)
        } else {
            (SUBJECT_COL_NAME, OBJECT_COL_NAME)
        };
        let from = df.column(from).unwrap().u32().unwrap();
        let to = df.column(to).unwrap().u32().unwrap();
        let edges: Vec<(u32, u32)> = from
            .iter()
            .zip(to.iter())
            .filter_map(|(f, t)| Some((f?, t?)))
            .collect();
        let mut offsets = vec![0usize; n + 1];
        for (f, _) in &edges {
            offsets[*f as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut next = offsets.clone();
        let mut targets = vec![0u32; edges.len()];
        for (f, t) in edges {
            targets[next[f as usize]] = t;
            next[f as usize] += 1;
        }
        Adjacency { offsets, targets }
    }

    fn neighbours(&self, u: u32) -> &[u32] {
        let u = u as usize;
        if u + 1 < self.offsets.len() {
            &self.targets[self.offsets[u]..self.offsets[u + 1]]
        } else {
            &[]
        }
    }
}

/// For each start node, the sorted set of nodes reached from it.
type Reached = Vec<Vec<u32>>;

/// The (subject, object) key pairs of the paths matching `ppe` that start (or, if `reversed`,
/// end) at one of `starts`. Each node set is extended one step at a time, so the cost depends on
/// the part of the graph reachable from the starts rather than on the whole graph.
fn paths_from(
    ppe: &PropertyPathExpression,
    starts: Vec<u32>,
    reversed: bool,
    namednode_map: &HashMap<PathLeaf, DataFrame>,
    max_index: usize,
) -> (Vec<u32>, Vec<u32>) {
    let mut leaves = HashSet::new();
    path_leaves(ppe, reversed, &mut leaves);
    let adjacencies: HashMap<(PathLeaf, bool), Adjacency> = leaves
        .into_par_iter()
        .filter_map(|(leaf, rev)| {
            let df = namednode_map.get(&leaf)?;
            let adj = Adjacency::new(df, max_index + 1, rev);
            Some(((leaf, rev), adj))
        })
        .collect();
    let initial: Reached = starts.iter().map(|s| vec![*s]).collect();
    let reached = follow(ppe, initial, reversed, &adjacencies);
    let n: usize = reached.iter().map(|r| r.len()).sum();
    let mut from = Vec::with_capacity(n);
    let mut to = Vec::with_capacity(n);
    for (s, nodes) in starts.into_iter().zip(reached) {
        for o in nodes {
            from.push(s);
            to.push(o);
        }
    }
    if reversed {
        (to, from)
    } else {
        (from, to)
    }
}

/// The leaves of a path expression, with whether each is followed backwards.
fn path_leaves(
    ppe: &PropertyPathExpression,
    reversed: bool,
    leaves: &mut HashSet<(PathLeaf, bool)>,
) {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            leaves.insert((PathLeaf::NamedNode(nn.clone()), reversed));
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            leaves.insert((PathLeaf::negated(nns), reversed));
        }
        PropertyPathExpression::Reverse(inner) => path_leaves(inner, !reversed, leaves),
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            path_leaves(a, reversed, leaves);
            path_leaves(b, reversed, leaves);
        }
        PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => path_leaves(inner, reversed, leaves),
    }
}

/// The nodes reached from each set of `reached` nodes by one match of `ppe` (backwards if
/// `reversed`).
fn follow(
    ppe: &PropertyPathExpression,
    reached: Reached,
    reversed: bool,
    adjacencies: &HashMap<(PathLeaf, bool), Adjacency>,
) -> Reached {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => step(
            adjacencies.get(&(PathLeaf::NamedNode(nn.clone()), reversed)),
            &reached,
        ),
        PropertyPathExpression::NegatedPropertySet(nns) => step(
            adjacencies.get(&(PathLeaf::negated(nns), reversed)),
            &reached,
        ),
        PropertyPathExpression::Reverse(inner) => follow(inner, reached, !reversed, adjacencies),
        PropertyPathExpression::Sequence(a, b) => {
            let (first, second) = if reversed { (b, a) } else { (a, b) };
            let mid = follow(first, reached, reversed, adjacencies);
            follow(second, mid, reversed, adjacencies)
        }
        PropertyPathExpression::Alternative(a, b) => {
            let left = follow(a, reached.clone(), reversed, adjacencies);
            let right = follow(b, reached, reversed, adjacencies);
            zip_reached(left, right, |l, r| union_sorted(&l, &r))
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let once = follow(inner, reached.clone(), reversed, adjacencies);
            zip_reached(reached, once, |l, r| union_sorted(&l, &r))
        }
        PropertyPathExpression::OneOrMore(inner) => {
            one_or_more_from(inner, reached, reversed, adjacencies)
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let more = one_or_more_from(inner, reached.clone(), reversed, adjacencies);
            zip_reached(reached, more, |l, r| union_sorted(&l, &r))
        }
    }
}

/// The nodes reached by one or more matches of `inner`: the newly reached nodes are followed
/// until no new nodes are reached.
fn one_or_more_from(
    inner: &PropertyPathExpression,
    reached: Reached,
    reversed: bool,
    adjacencies: &HashMap<(PathLeaf, bool), Adjacency>,
) -> Reached {
    let mut all = follow(inner, reached, reversed, adjacencies);
    let mut frontier = all.clone();
    loop {
        let next = follow(inner, frontier, reversed, adjacencies);
        let new: Reached = next
            .into_par_iter()
            .zip(all.par_iter())
            .map(|(n, a)| difference_sorted(&n, a))
            .collect();
        if new.iter().all(|x| x.is_empty()) {
            return all;
        }
        all = all
            .into_par_iter()
            .zip(new.par_iter())
            .map(|(a, n)| if n.is_empty() { a } else { union_sorted(&a, n) })
            .collect();
        frontier = new;
    }
}

fn step(adjacency: Option<&Adjacency>, reached: &Reached) -> Reached {
    let Some(adjacency) = adjacency else {
        return vec![vec![]; reached.len()];
    };
    reached
        .par_iter()
        .map(|nodes| {
            let mut out = vec![];
            for n in nodes {
                out.extend_from_slice(adjacency.neighbours(*n));
            }
            out.sort_unstable();
            out.dedup();
            out
        })
        .collect()
}

fn zip_reached(
    left: Reached,
    right: Reached,
    f: impl Fn(Vec<u32>, Vec<u32>) -> Vec<u32> + Sync + Send,
) -> Reached {
    left.into_par_iter()
        .zip(right.into_par_iter())
        .map(|(l, r)| f(l, r))
        .collect()
}

fn union_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    if a.is_empty() {
        return b.to_vec();
    }
    if b.is_empty() {
        return a.to_vec();
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// The elements of `a` that are not in `b`.
fn difference_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = vec![];
    let mut j = 0;
    for x in a {
        while j < b.len() && b[j] < *x {
            j += 1;
        }
        if j >= b.len() || b[j] != *x {
            out.push(*x);
        }
    }
    out
}
