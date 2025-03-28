use super::Triplestore;
use crate::sparql::errors::SparqlError;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{
    col, lit, AnyValue, DataFrame, IntoLazy, IntoSeries, JoinArgs, JoinType, Series,
    UniqueKeepStrategy,
};
use polars_core::prelude::{DataType, SortMultipleOptions};
use query_processing::errors::QueryProcessingError;
use query_processing::graph_patterns::{join, union};
use query_processing::pushdowns::Pushdowns;
use representation::multitype::{
    compress_actual_multitypes, force_convert_multicol_to_single_col, group_by_workaround,
    nest_multicolumns,
};
use representation::query_context::{Context, PathEntry};
use representation::rdf_to_polars::{
    rdf_literal_to_polars_literal_value, rdf_named_node_to_polars_literal_value,
};
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeType};
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use sprs::{CsMatBase, TriMatBase};
use std::collections::HashMap;

const NAMED_NODE_INDEX_COL: &str = "named_node_index_column";
const VALUE_COLUMN: &str = "value";
const LOOKUP_COLUMN: &str = "key";

type SparseMatrix = CsMatBase<u32, usize, Vec<usize>, Vec<usize>, Vec<u32>, usize>;

struct SparsePathReturn {
    sparmat: SparseMatrix,
}

impl Triplestore {
    pub fn lazy_path(
        &mut self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
        pushdowns: Pushdowns,
        include_transient: bool,
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
                &None,
                pushdowns,
                include_transient,
            )?;
            for i in &intermediaries {
                sms.rdf_node_types.remove(i).unwrap();
            }
            sms.mappings = sms.mappings.drop(intermediaries);
            if let Some(solution_mappings) = solution_mappings {
                sms = join(sms, solution_mappings, JoinType::Inner)?;
            }
            return Ok(sms);
        }

        let mut out_df;
        let out_dt_subj;
        let out_dt_obj;

        let mut df_creator = U32DataFrameCreator::new(include_transient);
        df_creator.gather_namednode_dfs(ppe, self)?;
        let (lookup_df, lookup_dtypes, namednode_dfs) = df_creator.create_u32_dfs()?;
        let max_index: Option<u32> = lookup_df
            .column(LOOKUP_COLUMN)
            .unwrap()
            .as_materialized_series()
            .max()
            .unwrap();

        if let Some(max_index) = max_index {
            if let Some(SparsePathReturn { sparmat }) =
                sparse_path(ppe, &namednode_dfs, max_index as usize)
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
                let mut subject_series = Series::from_iter(subject_vec);
                subject_series.rename("subject_key".into());
                let mut object_series = Series::from_iter(object_vec);
                object_series.rename("object_key".into());
                let mut out_lf = DataFrame::new(vec![subject_series.into(), object_series.into()])
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
                    .drop(["subject_key"]);
                let lookup_object_lf =
                    lookup_df
                        .lazy()
                        .rename([VALUE_COLUMN], [OBJECT_COL_NAME], true);
                out_lf = out_lf
                    .join(
                        lookup_object_lf,
                        &[col("object_key")],
                        &[col(LOOKUP_COLUMN)],
                        JoinArgs::new(JoinType::Inner),
                    )
                    .drop(["object_key"]);
                out_df = out_lf.collect().unwrap();
                let mut dtypes = HashMap::new();
                dtypes.insert(
                    SUBJECT_COL_NAME.to_string(),
                    lookup_dtypes.get(VALUE_COLUMN).unwrap().clone(),
                );
                dtypes.insert(
                    OBJECT_COL_NAME.to_string(),
                    lookup_dtypes.get(VALUE_COLUMN).unwrap().clone(),
                );

                if matches!(
                    lookup_dtypes.get(VALUE_COLUMN).unwrap(),
                    RDFNodeType::MultiType(..)
                ) {
                    if let TermPattern::NamedNode(_) = subject {
                        out_df = force_convert_multicol_to_single_col(
                            out_df.lazy(),
                            SUBJECT_COL_NAME,
                            &BaseRDFNodeType::IRI,
                        )
                        .collect()
                        .unwrap();
                        dtypes.insert(SUBJECT_COL_NAME.to_string(), RDFNodeType::IRI);
                    }
                    if let TermPattern::NamedNode(_) = object {
                        out_df = force_convert_multicol_to_single_col(
                            out_df.lazy(),
                            OBJECT_COL_NAME,
                            &BaseRDFNodeType::IRI,
                        )
                        .collect()
                        .unwrap();
                        dtypes.insert(OBJECT_COL_NAME.to_string(), RDFNodeType::IRI);
                    }
                    if let TermPattern::Literal(l) = subject {
                        out_df = force_convert_multicol_to_single_col(
                            out_df.lazy(),
                            SUBJECT_COL_NAME,
                            &BaseRDFNodeType::Literal(l.datatype().into_owned()),
                        )
                        .collect()
                        .unwrap();
                        dtypes.insert(
                            SUBJECT_COL_NAME.to_string(),
                            RDFNodeType::Literal(l.datatype().into_owned()),
                        );
                    }
                    if let TermPattern::Literal(l) = object {
                        out_df = force_convert_multicol_to_single_col(
                            out_df.lazy(),
                            OBJECT_COL_NAME,
                            &BaseRDFNodeType::Literal(l.datatype().into_owned()),
                        )
                        .collect()
                        .unwrap();
                        dtypes.insert(
                            OBJECT_COL_NAME.to_string(),
                            RDFNodeType::Literal(l.datatype().into_owned()),
                        );
                    }
                }
                (out_df, dtypes) = compress_actual_multitypes(out_df, dtypes);
                out_dt_subj = dtypes.remove(SUBJECT_COL_NAME).unwrap();
                out_dt_obj = dtypes.remove(OBJECT_COL_NAME).unwrap();
            } else {
                out_df = DataFrame::new(vec![
                    Series::new_empty(
                        SUBJECT_COL_NAME.into(),
                        &BaseRDFNodeType::None.polars_data_type(),
                    )
                    .into(),
                    Series::new_empty(
                        OBJECT_COL_NAME.into(),
                        &BaseRDFNodeType::None.polars_data_type(),
                    )
                    .into(),
                ])
                .unwrap();
                out_dt_obj = RDFNodeType::None;
                out_dt_subj = RDFNodeType::None;
            }
        } else {
            out_df = DataFrame::new(vec![
                Series::new_empty(
                    SUBJECT_COL_NAME.into(),
                    &BaseRDFNodeType::None.polars_data_type(),
                )
                .into(),
                Series::new_empty(
                    OBJECT_COL_NAME.into(),
                    &BaseRDFNodeType::None.polars_data_type(),
                )
                .into(),
            ])
            .unwrap();
            out_dt_obj = RDFNodeType::None;
            out_dt_subj = RDFNodeType::None;
        }
        let mut var_cols = vec![];
        let mut rename_src = vec![];
        let mut rename_trg = vec![];
        match subject {
            TermPattern::NamedNode(nn) => {
                let l = rdf_named_node_to_polars_literal_value(nn);
                out_df = out_df
                    .lazy()
                    .filter(col(SUBJECT_COL_NAME).eq(lit(l)))
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
                let l = rdf_literal_to_polars_literal_value(l);
                out_df = out_df
                    .lazy()
                    .filter(col(SUBJECT_COL_NAME).eq(lit(l)))
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
                let l = rdf_named_node_to_polars_literal_value(nn);
                out_df = out_df
                    .lazy()
                    .filter(col(OBJECT_COL_NAME).eq(lit(l)))
                    .collect()
                    .unwrap();
                out_df = out_df.drop(OBJECT_COL_NAME).unwrap();
            }
            TermPattern::BlankNode(b) => {
                let bname = b.to_string();
                var_cols.push(bname.clone());
                rename_src.push(OBJECT_COL_NAME);
                rename_trg.push(bname);
            }
            TermPattern::Literal(l) => {
                let l = rdf_literal_to_polars_literal_value(l);
                out_df = out_df
                    .lazy()
                    .filter(col(OBJECT_COL_NAME).eq(lit(l)))
                    .collect()
                    .unwrap();
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
        if let TermPattern::Variable(v) = object {
            datatypes.insert(v.as_str().to_string(), out_dt_obj);
        } else if let TermPattern::BlankNode(b) = object {
            datatypes.insert(b.to_string(), out_dt_obj);
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

        if let Some(mappings) = solution_mappings {
            path_solution_mappings = join(path_solution_mappings, mappings, JoinType::Inner)?;
        }
        Ok(path_solution_mappings)
    }
}

fn create_graph_pattern(
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
        PropertyPathExpression::NegatedPropertySet(_) => {
            todo!()
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
    let df = DataFrame::new(vec![sub.into(), obj.into()]).unwrap();
    let df = df
        .sort(
            vec![SUBJECT_COL_NAME, SUBJECT_COL_NAME],
            SortMultipleOptions::default()
                .with_maintain_order(false)
                .with_order_descending(false),
        )
        .unwrap();
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
        PropertyPathExpression::NegatedPropertySet(_) => todo!("Not supported yet"),
    }
}

fn sparse_path(
    ppe: &PropertyPathExpression,
    namednode_map: &HashMap<NamedNode, DataFrame>,
    max_index: usize,
) -> Option<SparsePathReturn> {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            if let Some(df) = namednode_map.get(nn) {
                let sparmat = to_csr(df, max_index);
                Some(SparsePathReturn { sparmat })
            } else {
                None
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            if let Some(SparsePathReturn { sparmat }) = sparse_path(inner, namednode_map, max_index)
            {
                Some(SparsePathReturn {
                    sparmat: sparmat.transpose_into(),
                })
            } else {
                None
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let res_left = sparse_path(left, namednode_map, max_index);
            let res_right = sparse_path(right, namednode_map, max_index);
            if let Some(SparsePathReturn {
                sparmat: sparmat_left,
            }) = res_left
            {
                if let Some(SparsePathReturn {
                    sparmat: sparmat_right,
                }) = res_right
                {
                    let sparmat = (&sparmat_left * &sparmat_right).to_csr();
                    Some(SparsePathReturn { sparmat })
                } else {
                    None
                }
            } else {
                None
            }
        }
        PropertyPathExpression::Alternative(left, right) => {
            let res_left = sparse_path(left, namednode_map, max_index);
            let res_right = sparse_path(right, namednode_map, max_index);
            if let Some(SparsePathReturn {
                sparmat: sparmat_left,
            }) = res_left
            {
                if let Some(SparsePathReturn {
                    sparmat: sparmat_right,
                }) = res_right
                {
                    let sparmat = (&sparmat_left + &sparmat_right)
                        .to_csr()
                        .map(|x| (x > &0) as u32);
                    Some(SparsePathReturn { sparmat })
                } else {
                    Some(SparsePathReturn {
                        sparmat: sparmat_left,
                    })
                }
            } else {
                res_right
            }
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
            }) = sparse_path(inner, namednode_map, max_index)
            {
                let sparmat = zero_or_more(sparmat_inner);
                Some(SparsePathReturn { sparmat })
            } else {
                None
            }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
            }) = sparse_path(inner, namednode_map, max_index)
            {
                let sparmat = one_or_more(sparmat_inner);
                Some(SparsePathReturn { sparmat })
            } else {
                None
            }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            if let Some(SparsePathReturn {
                sparmat: sparmat_inner,
            }) = sparse_path(inner, namednode_map, max_index)
            {
                let sparmat = zero_or_one(sparmat_inner);
                Some(SparsePathReturn { sparmat })
            } else {
                None
            }
        }
        PropertyPathExpression::NegatedPropertySet(_nns) => {
            todo!()
        }
    }
}

struct U32DataFrameCreator {
    pub named_nodes: HashMap<NamedNode, (DataFrame, RDFNodeType, RDFNodeType)>,
    include_transient: bool,
}

impl U32DataFrameCreator {
    pub fn new(include_transient: bool) -> Self {
        U32DataFrameCreator {
            named_nodes: Default::default(),
            include_transient,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn create_u32_dfs(
        self,
    ) -> Result<
        (
            DataFrame,
            HashMap<String, RDFNodeType>,
            HashMap<NamedNode, DataFrame>,
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
                RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
            );
            types.insert(SUBJECT_COL_NAME.to_string(), subject_dt);
            types.insert(OBJECT_COL_NAME.to_string(), object_dt);

            soln_mappings.push(SolutionMappings::new(lf, types, df_height));
        }
        let SolutionMappings {
            mut mappings,
            rdf_node_types,
            ..
        } = union(soln_mappings, false)?;

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
            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
        );

        let mut obj_types = HashMap::new();
        obj_types.insert(
            VALUE_COLUMN.to_string(),
            rdf_node_types.get(OBJECT_COL_NAME).unwrap().clone(),
        );
        obj_types.insert(
            object_row_index.clone(),
            RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned()),
        );

        let subj_soln_mappings = SolutionMappings::new(lf_subj, subj_types, df_height);
        let obj_soln_mappings = SolutionMappings::new(lf_obj, obj_types, df_height);
        let SolutionMappings {
            mut mappings,
            rdf_node_types: lookup_df_types,
            ..
        } = union(vec![subj_soln_mappings, obj_soln_mappings], false)?;
        let (mappings_grby, maps) =
            group_by_workaround(mappings, &lookup_df_types, vec![VALUE_COLUMN.to_string()]);
        mappings = mappings_grby.agg([
            col(&subject_row_index).alias(&subject_row_index),
            col(&object_row_index).alias(&object_row_index),
        ]);
        mappings = nest_multicolumns(mappings, maps);

        mappings = mappings.with_row_index(LOOKUP_COLUMN, None);
        mappings = mappings.explode([col(&subject_row_index), col(&object_row_index)]);
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
        triplestore: &mut Triplestore,
    ) -> Result<(), SparqlError> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let SolutionMappings {
                    mappings,
                    mut rdf_node_types,
                    ..
                } = triplestore.get_multi_predicates_solution_mappings(
                    Some(vec![nn.clone()]),
                    &Some(SUBJECT_COL_NAME.to_string()),
                    &None,
                    &Some(OBJECT_COL_NAME.to_string()),
                    &None,
                    &None,
                    &None,
                    &None,
                    self.include_transient,
                )?;
                self.named_nodes.insert(
                    nn.clone(),
                    (
                        mappings.collect().unwrap(),
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
            PropertyPathExpression::NegatedPropertySet(_nns) => {
                todo!()
            }
        }
    }
}
