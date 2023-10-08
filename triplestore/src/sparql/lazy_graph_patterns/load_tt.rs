use std::collections::{HashMap, HashSet};
use polars::prelude::{col, concat, IntoLazy, LazyFrame, UnionArgs};
use representation::RDFNodeType;
use crate::sparql::errors::SparqlError;
use crate::sparql::multitype::unitype_to_multitype;
use crate::TripleTable;

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
    m: &HashMap<(RDFNodeType, RDFNodeType), TripleTable>,
    subj_datatype_req: &Option<RDFNodeType>,
    obj_datatype_req: &Option<RDFNodeType>,
) -> Result<Option<(RDFNodeType, RDFNodeType, LazyFrame)>, SparqlError> {
    let mut filtered = vec![];
    for ((subj_type, obj_type), tt) in m {
        let mut keep = true;
        if let Some(subj_req) = subj_datatype_req {
            keep = subj_req == subj_type;
        }
        if let Some(obj_req) = obj_datatype_req {
            keep = keep && obj_req == obj_type;
        }
        if keep {
            filtered.push((subj_type, obj_type, tt));
        }
    }
    if filtered.is_empty() {
        Ok(None)
    } else if filtered.len() == 1 {
        let (subj_dt, obj_dt, tt) = filtered.remove(0);
        Ok(Some((subj_dt.clone(), obj_dt.clone(), single_tt_to_lf(tt)?)))
    } else {
        let mut lfs = vec![];
        let set_subj_dt: HashSet<_> = m.keys().map(|(x, _)| x).collect();
        let set_obj_dt: HashSet<_> = m.keys().map(|(_, x)| x).collect();

        let mut use_subj_dt = None;
        let mut use_obj_dt = None;

        for ((subj_dt, obj_dt), tt) in m {
            let mut df = single_tt_to_lf(tt)?.collect().unwrap();
            if set_subj_dt.len() > 1 {
                let subjects = df.column("subject").unwrap();
                let out_subjects = unitype_to_multitype(subjects, subj_dt);
                df.with_column(out_subjects).unwrap();
                use_subj_dt = Some(RDFNodeType::MultiType)
            } else {
                use_subj_dt = Some(subj_dt.clone())
            }
            if set_obj_dt.len() > 1 {
                let objects = df.column("object").unwrap();
                let out_objects = unitype_to_multitype(objects, obj_dt);
                df.with_column(out_objects).unwrap();
                use_obj_dt = Some(RDFNodeType::MultiType)
            } else {
                use_obj_dt = Some(obj_dt.clone());
            }
            lfs.push(df.lazy())
        }
        Ok(Some((
            use_subj_dt.unwrap(),
            use_obj_dt.unwrap(),
            concat(lfs, Default::default()).unwrap(),
        )))
    }
}