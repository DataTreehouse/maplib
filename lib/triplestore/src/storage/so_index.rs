use oxrdf::vocab::xsd;
use polars::prelude::PlSmallStr;
use polars_core::frame::DataFrame;
use polars_core::prelude::{IntoColumn, IntoSeries, UInt32Chunked};
use representation::cats::{OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME};
use representation::{BaseRDFNodeType, OBJECT_COL_NAME, SUBJECT_COL_NAME};
use std::collections::HashSet;

#[derive(Clone)]
pub struct SubjectObjectIndex {
    set: HashSet<(u32, u32)>,
}

pub fn is_so_indexed(object_type: &BaseRDFNodeType) -> bool {
    //We disable the SO-index for the time being.
    return false;
    object_type.is_iri() || object_type.is_blank_node() || object_type.is_lit_type(xsd::STRING)
}

impl SubjectObjectIndex {
    pub fn maybe_create(
        df: &DataFrame,
        object_type: &BaseRDFNodeType,
    ) -> Option<SubjectObjectIndex> {
        if is_so_indexed(object_type) {
            let s = df.column(SUBJECT_COL_NAME).unwrap();
            let o = df.column(OBJECT_COL_NAME).unwrap();
            let mut set = HashSet::with_capacity(df.height());
            for (s, o) in s.u32().unwrap().iter().zip(o.u32().unwrap().iter()) {
                set.insert((s.unwrap(), o.unwrap()));
            }
            let so = SubjectObjectIndex { set };
            Some(so)
        } else {
            None
        }
    }

    pub fn update_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
        let s = df.column(SUBJECT_COL_NAME).unwrap();
        let o = df.column(OBJECT_COL_NAME).unwrap();
        let s_rank = df.column(SUBJECT_RANK_COL_NAME).unwrap();
        let o_rank = df.column(OBJECT_RANK_COL_NAME).unwrap();
        let mut new_ss = vec![];
        let mut new_os = vec![];
        let mut new_sranks = vec![];
        let mut new_oranks = vec![];
        for (((s, o), s_rank), o_rank) in s
            .u32()
            .unwrap()
            .iter()
            .zip(o.u32().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.set.insert((s, o));
            if new {
                new_ss.push(s);
                new_os.push(o);
                new_sranks.push(s_rank);
                new_oranks.push(o_rank);
            }
        }
        if new_ss.is_empty() {
            None
        } else {
            let s_series = UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_COL_NAME), new_ss)
                .into_series();
            let o_series = UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os)
                .into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(vec![
                s_series.into_column(),
                o_series.into_column(),
                s_rank_series.into_column(),
                o_rank_series.into_column(),
            ])
            .unwrap();
            Some(new_df)
        }
    }
}
