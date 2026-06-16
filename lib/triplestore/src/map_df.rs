use crate::errors::TriplestoreError;
use crate::{TriplesToAdd, Triplestore};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{col, lit, DataFrame, IntoLazy};
use polars_core::prelude::{Column, IntoColumn, NamedFrom, Series};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::constants::MAPLIB_PREFIX_IRI;
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;

const FACADE_X_ROOT: &str = "http://sparql.xyz/facade-x/ns/root";
const FACADE_X_CHILD: &str = "http://sparql.xyz/facade-x/ns/child";
const FACADE_X_CHILD_NUMBER: &str = "http://sparql.xyz/facade-x/ns/childNumber";
const DEFAULT_FACADE_X_DATA_PREFIX: &str = "http://sparql.xyz/facade-x/data/";

impl Triplestore {
    pub fn map_df(
        &mut self,
        df: &DataFrame,
        named_graph: &NamedGraph,
    ) -> Result<(), TriplestoreError> {
        let mut column_types = HashMap::new();
        let col_names: Vec<_> = df.columns().iter().map(|x| x.name().to_string()).collect();
        for c in df.columns() {
            //Todo handle
            let dt = polars_type_to_literal_type(c.dtype()).unwrap();
            column_types.insert(c.name().to_string(), dt);
        }
        let id_col = uuid::Uuid::new_v4().to_string();
        let mut df = df.clone();
        let root_node_uuri = format!("{}{}", MAPLIB_PREFIX_IRI, uuid::Uuid::new_v4().to_string());
        let uuids: Vec<_> = (0..df.height())
            .into_par_iter()
            .map(|_| format!("{}{}", MAPLIB_PREFIX_IRI, uuid::Uuid::new_v4().to_string()))
            .collect();
        df.with_column(Series::new(id_col.as_str().into(), uuids).into_column())
            .unwrap();
        let mut triples_to_add = Vec::new();
        for c in &col_names {
            let subj_type = BaseRDFNodeType::IRI;
            let obj_type = column_types.get(c).unwrap().clone();
            let subj_state = subj_type.default_input_cat_state();

            if obj_type.is_multi() {
                todo!()
            } else {
                let base_obj_type = obj_type.get_base_type().unwrap();
                let base_obj_state = obj_type.get_base_state().unwrap();
                let tta = TriplesToAdd {
                    df: df
                        .clone()
                        .lazy()
                        .select([
                            col(id_col.clone()).alias(SUBJECT_COL_NAME),
                            col(c).alias(OBJECT_COL_NAME),
                        ])
                        .collect()
                        .unwrap(),
                    subject_type: subj_type,
                    object_type: base_obj_type.clone(),
                    predicate: Some(NamedNode::new_unchecked(format!(
                        "{}{}",
                        DEFAULT_FACADE_X_DATA_PREFIX,
                        uri_encode::encode_uri(c)
                    ))),
                    graph: named_graph.clone(),
                    subject_cat_state: subj_state,
                    object_cat_state: base_obj_state.clone(),
                    predicate_cat_state: None,
                };
                triples_to_add.push(tta);
            }
        }
        let tta_children = TriplesToAdd {
            df: df
                .clone()
                .lazy()
                .select([
                    lit(root_node_uuri.as_str()).alias(SUBJECT_COL_NAME),
                    col(id_col.clone()).alias(OBJECT_COL_NAME),
                ])
                .collect()
                .unwrap(),
            subject_type: BaseRDFNodeType::IRI,
            object_type: BaseRDFNodeType::IRI,
            predicate: Some(NamedNode::new_unchecked(FACADE_X_CHILD.to_string())),
            graph: named_graph.clone(),
            subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            object_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            predicate_cat_state: None,
        };
        let num_dt = BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned());
        let tta_child_num = TriplesToAdd {
            df: df
                .clone()
                .lazy()
                .select([col(id_col).alias(SUBJECT_COL_NAME)])
                .with_row_index(OBJECT_COL_NAME.to_string(), None)
                .collect()
                .unwrap(),
            subject_type: BaseRDFNodeType::IRI,
            object_type: num_dt.clone(),
            predicate: Some(NamedNode::new_unchecked(FACADE_X_CHILD_NUMBER.to_string())),
            graph: named_graph.clone(),
            subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            object_cat_state: num_dt.default_input_cat_state(),
            predicate_cat_state: None,
        };

        let mut root_cols = Vec::new();
        root_cols.push(Column::new(SUBJECT_COL_NAME.into(), vec![root_node_uuri]));
        root_cols.push(Column::new(OBJECT_COL_NAME.into(), vec![FACADE_X_ROOT]));
        let root = TriplesToAdd {
            df: DataFrame::new(1, root_cols).unwrap(),
            subject_type: BaseRDFNodeType::IRI,
            object_type: BaseRDFNodeType::IRI,
            predicate: Some(rdf::TYPE.into_owned()),
            graph: named_graph.clone(),
            subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            object_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            predicate_cat_state: None,
        };
        triples_to_add.push(tta_children);
        triples_to_add.push(tta_child_num);
        triples_to_add.push(root);
        self.add_triples_vec(triples_to_add, false)?;
        Ok(())
    }
}
