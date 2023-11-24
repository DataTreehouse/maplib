use crate::sparql::errors::SparqlError;
use crate::sparql::multitype::unitype_to_multitype;
use crate::TripleTable;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, UnionArgs};
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};

fn single_tt_to_lf(tt: &TripleTable) -> Result<LazyFrame, SparqlError> {
    assert!(tt.unique, "Should be deduplicated");
    let lf = concat(
        tt.get_lazy_frames()
            .map_err(SparqlError::TripleTableReadError)?,
        UnionArgs::default(),
    )
    .unwrap()
    .select(vec![col("subject"), col("object")]);
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

        for (subj_dt, obj_dt, lf) in filtered {
            let mut df = lf.collect().unwrap();
            if set_subj_dt.len() > 1 && subj_dt != &RDFNodeType::MultiType {
                let subjects = df.column("subject").unwrap();
                let out_subjects = unitype_to_multitype(subjects, subj_dt);
                df.with_column(out_subjects).unwrap();
                use_subj_dt = Some(RDFNodeType::MultiType)
            } else {
                use_subj_dt = Some(subj_dt.clone())
            }
            if set_obj_dt.len() > 1 && obj_dt != &RDFNodeType::MultiType {
                let objects = df.column("object").unwrap();
                let out_objects = unitype_to_multitype(objects, obj_dt);
                df.with_column(out_objects).unwrap();
                use_obj_dt = Some(RDFNodeType::MultiType)
            } else {
                use_obj_dt = Some(obj_dt.clone());
            }
            lfs.push(df.lazy())
        }
        let lf = concat(lfs, Default::default()).unwrap();
        Ok(Some((use_subj_dt.unwrap(), use_obj_dt.unwrap(), lf)))
    }
}
