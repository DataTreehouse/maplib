use crate::errors::TriplestoreError;
use crate::{TriplesToAdd, Triplestore};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{col, lit, DataFrame, IntoLazy};
use polars_core::prelude::{Column, IntoColumn, NamedFrom, Series};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use representation::constants::{FX_CHILD, FX_CHILD_NUMBER, FX_ROOT, XYZ_PREFIX_IRI};
use representation::dataset::NamedGraph;
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;
use uuid::Uuid;

impl Triplestore {
    pub fn map_df(
        &mut self,
        df: &DataFrame,
        named_graph: &NamedGraph,
        uuid_namespace: Option<String>,
    ) -> Result<(), TriplestoreError> {
        let use_uuid_namespace = if let Some(uuid_namespace) = uuid_namespace {
            Uuid::new_v5(&Uuid::NAMESPACE_DNS, uuid_namespace.as_bytes())
        } else {
            Uuid::new_v4()
        };
        let root_node_uuri = new_iri_subject(&use_uuid_namespace, "".as_bytes());
        let mut column_types = HashMap::new();
        let col_names: Vec<_> = df.columns().iter().map(|x| x.name().to_string()).collect();
        for c in df.columns() {
            //Todo handle
            let dt = polars_type_to_literal_type(c.dtype()).unwrap();
            column_types.insert(c.name().to_string(), dt);
        }
        let id_col = Uuid::new_v5(&use_uuid_namespace, "id".as_bytes()).to_string();
        let mut df = df.clone();
        let uuids: Vec<_> = (0..df.height())
            .into_par_iter()
            .map(|i| new_iri_subject(&use_uuid_namespace, &i.to_string().into_bytes()))
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
                        XYZ_PREFIX_IRI,
                        urlencoding::encode(c)
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
            predicate: Some(NamedNode::new_unchecked(FX_CHILD.to_string())),
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
            predicate: Some(NamedNode::new_unchecked(FX_CHILD_NUMBER.to_string())),
            graph: named_graph.clone(),
            subject_cat_state: BaseRDFNodeType::IRI.default_input_cat_state(),
            object_cat_state: num_dt.default_input_cat_state(),
            predicate_cat_state: None,
        };

        let root_cols = vec![
            Column::new(SUBJECT_COL_NAME.into(), vec![root_node_uuri]),
            Column::new(OBJECT_COL_NAME.into(), vec![FX_ROOT]),
        ];

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
fn new_iri_subject(namespace: &Uuid, name: &[u8]) -> String {
    format!("urn:maplib:{}", Uuid::new_v5(namespace, name))
}
