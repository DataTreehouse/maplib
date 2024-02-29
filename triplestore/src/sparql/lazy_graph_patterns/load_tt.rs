use crate::constants::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use crate::sparql::errors::SparqlError;
use crate::TripleTable;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, UnionArgs};
use representation::multitype::{convert_lf_col_to_multitype, explode_multicols, implode_multicolumns};
use representation::{BaseRDFNodeType, RDFNodeType};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn single_tt_to_lf(tt: &TripleTable) -> Result<LazyFrame, SparqlError> {
    assert!(tt.unique, "Should be deduplicated");
    let lf = concat(
        tt.get_lazy_frames()
            .map_err(SparqlError::TripleTableReadError)?,
        UnionArgs::default(),
    )
    .unwrap()
    .select(vec![col(SUBJECT_COL_NAME), col(OBJECT_COL_NAME)]);

    Ok(lf)
}

pub fn multiple_tt_to_lf(
    m1: &HashMap<(RDFNodeType, RDFNodeType), TripleTable>,
    m2: Option<&HashMap<(RDFNodeType, RDFNodeType), TripleTable>>,
    subj_datatype_req: Option<&RDFNodeType>,
    obj_datatype_req: Option<&RDFNodeType>,
    subject_filter: Option<Expr>,
    object_filter: Option<Expr>,
) -> Result<Option<(RDFNodeType, RDFNodeType, LazyFrame)>, SparqlError> {
    let mut filtered = vec![];
    let m: Vec<_> = if let Some(m2) = m2 {
        m1.iter().chain(m2).collect()
    } else {
        m1.iter().collect()
    };
    for ((subj_type, obj_type), tt) in &m {
        let mut keep = true;
        if let Some(subj_req) = subj_datatype_req {
            keep = subj_req == subj_type;
        }
        if let Some(obj_req) = obj_datatype_req {
            keep = keep && obj_req == obj_type;
        }
        if keep {
            let mut lf = single_tt_to_lf(tt)?;
            if let Some(subject_filter) = &subject_filter {
                lf = lf.filter(subject_filter.clone());
            }
            if let Some(object_filter) = &object_filter {
                lf = lf.filter(object_filter.clone())
            }
            let df = lf.collect().unwrap();
            if df.height() > 0 {
                filtered.push((subj_type, obj_type, df.lazy()));
            }
        }
    }
    if filtered.is_empty() {
        Ok(None)
    } else if filtered.len() == 1 {
        let (subj_dt, obj_dt, lf) = filtered.remove(0);
        Ok(Some((subj_dt.clone(), obj_dt.clone(), lf)))
    } else {
        let mut lfs = vec![];
        let set_subj_dt: HashSet<_> = filtered.iter().map(|(x, _, _)| *x).collect();
        let set_obj_dt: HashSet<_> = filtered.iter().map(|(_, x, _)| *x).collect();

        let mut use_subj_dt = None;
        let mut use_obj_dt = None;

        let mut exploded_subjects = None;
        let mut exploded_objects = None;
        let prefix = Uuid::new_v4().to_string();
        for (subj_dt, obj_dt, mut lf) in filtered {
            if set_subj_dt.len() > 1 && subj_dt != &RDFNodeType::MultiType {
                lf = convert_lf_col_to_multitype(lf, SUBJECT_COL_NAME, subj_dt);
                let (new_lf, mut new_exploded_map) = explode_multicols(lf, &HashMap::from([(SUBJECT_COL_NAME.to_string(), subj_dt.clone())]), &prefix);
                exploded_subjects = if let Some((new_inner_cols, new_prefixed_inner_cols)) = new_exploded_map.remove(SUBJECT_COL_NAME) {
                    if let Some((mut inner_cols, mut prefixed_inner_cols)) = exploded_subjects {
                        inner_cols.extend(new_inner_cols);
                        prefixed_inner_cols.extend(new_prefixed_inner_cols);
                        Some((inner_cols, prefixed_inner_cols))
                    } else {
                        Some((new_inner_cols, new_prefixed_inner_cols))
                    }
                } else {
                    None
                };
                lf = new_lf;
            }
            if set_obj_dt.len() > 1 && obj_dt != &RDFNodeType::MultiType {
                let (new_lf, mut new_exploded_map) = explode_multicols(lf, &HashMap::from([(OBJECT_COL_NAME.to_string(), subj_dt.clone())]), &prefix);
                lf = new_lf;
                exploded_objects = if let Some((new_inner_cols, new_prefixed_inner_cols)) = new_exploded_map.remove(OBJECT_COL_NAME) {
                    if let Some((mut inner_cols, mut prefixed_inner_cols)) = exploded_objects {
                        inner_cols.extend(new_inner_cols);
                        prefixed_inner_cols.extend(new_prefixed_inner_cols);
                        Some((inner_cols, prefixed_inner_cols))
                    } else {
                        Some((new_inner_cols, new_prefixed_inner_cols))
                    }
                } else {
                    None
                };
            } else {
                use_obj_dt = Some(obj_dt.clone());
            }
            lfs.push(lf)
        }
        let mut lf = concat(lfs, Default::default()).unwrap();
        let use_subj_dt = if set_subj_dt.len() > 1 {
            let subject_col_name = SUBJECT_COL_NAME.to_string();
            lf = implode_multicolumns(lf, HashMap::from([(&subject_col_name, exploded_subjects)]));
            let mut types: Vec<_> = set_subj_dt.iter().map(|x|BaseRDFNodeType::from_rdf_node_type(*x)).collect();
            types.sort();
            RDFNodeType::MultiType(types)
        } else {
            set_subj_dt.iter().next().unwrap().clone()
        };
        let use_obj_dt = if set_obj_dt.len() > 1 {
            let object_col_name = OBJECT_COL_NAME.to_string();
            lf = implode_multicolumns(lf, HashMap::from([(&object_col_name, exploded_objects)]));
            let mut types: Vec<_> = set_obj_dt.iter().map(|x|BaseRDFNodeType::from_rdf_node_type(*x)).collect();
            types.sort();
            RDFNodeType::MultiType(types)
        } else {
            set_obj_dt.iter().next().unwrap()
        };


        Ok(Some((use_subj_dt.unwrap(), use_obj_dt.unwrap(), lf)))
    }
}
