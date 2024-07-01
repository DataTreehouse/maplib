use crate::sparql::errors::SparqlError;
use crate::TripleTable;
use polars::prelude::{col, concat, Expr, LazyFrame, UnionArgs};
use query_processing::graph_patterns::union;
use representation::solution_mapping::SolutionMappings;
use representation::RDFNodeType;
use representation::{OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashMap;

fn single_tt_to_lf(tt: &TripleTable) -> Result<LazyFrame, SparqlError> {
    assert!(tt.unique, "Should be deduplicated");
    //TODO: Check if this rechunk is needed
    let mut lfs = tt
        .get_lazy_frames()
        .map_err(SparqlError::TripleTableReadError)?;

    if lfs.len() == 1 {
        return Ok(lfs.remove(0));
    }

    let lf = concat(
        tt.get_lazy_frames()
            .map_err(SparqlError::TripleTableReadError)?,
        UnionArgs {
            parallel: true,
            rechunk: true,
            to_supertypes: false,
            diagonal: false,
            from_partitioned_ds: false,
        },
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
) -> Result<Option<SolutionMappings>, SparqlError> {
    let mut filtered = vec![];
    let m: Vec<_> = if let Some(m2) = m2 {
        m1.iter().chain(m2).collect()
    } else {
        m1.iter().collect()
    };
    for ((subj_type, obj_type), tt) in m {
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
            let rdf_node_types = HashMap::from([
                (SUBJECT_COL_NAME.to_string(), subj_type.clone()),
                (OBJECT_COL_NAME.to_string(), obj_type.clone()),
            ]);
            let sm = SolutionMappings::new(lf, rdf_node_types);
            filtered.push(sm);
        }
    }
    if filtered.is_empty() {
        Ok(None)
    } else {
        let sm = union(filtered, false)?;
        Ok(Some(sm))
    }
}
