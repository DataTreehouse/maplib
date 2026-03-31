use ordered_float::OrderedFloat;
use oxrdf::vocab::xsd;
use polars::prelude::{as_struct, col, IntoLazy, PlSmallStr};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    Float32Chunked, Float64Chunked, Int32Chunked, Int64Chunked, IntoColumn, IntoSeries,
    UInt32Chunked,
};
use polars_core::series::Series;
use representation::cats::{OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME};
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub enum BinaryIndex {
    NormalIndex(HashSet<(u32, u32)>),
    LangStringIndex(HashSet<(u32, u32, u32)>),
    Int32Index(HashSet<(u32, i32)>),
    Int64Index(HashSet<(u32, i64)>),
    BoolIndex(HashSet<(u32, bool)>),
    Float32Index(HashSet<(u32, OrderedFloat<f32>)>),
    Float64Index(HashSet<(u32, OrderedFloat<f64>)>),
}

impl BinaryIndex {
    pub fn insert_normal(&mut self, s: u32, o: u32) -> bool {
        match self {
            BinaryIndex::NormalIndex(set) => set.insert((s, o)),
            _ => unreachable!(),
        }
    }
    pub fn insert_i32(&mut self, s: u32, o: i32) -> bool {
        match self {
            BinaryIndex::Int32Index(set) => set.insert((s, o)),
            _ => unreachable!(),
        }
    }
    pub fn insert_i64(&mut self, s: u32, o: i64) -> bool {
        match self {
            BinaryIndex::Int64Index(set) => set.insert((s, o)),
            _ => unreachable!(),
        }
    }
    pub fn insert_f32(&mut self, s: u32, o: f32) -> bool {
        match self {
            BinaryIndex::Float32Index(set) => set.insert((s, OrderedFloat(o))),
            _ => unreachable!(),
        }
    }
    pub fn insert_f64(&mut self, s: u32, o: f64) -> bool {
        match self {
            BinaryIndex::Float64Index(set) => set.insert((s, OrderedFloat(o))),
            _ => unreachable!(),
        }
    }
    pub fn insert_bool(&mut self, s: u32, o: bool) -> bool {
        match self {
            BinaryIndex::BoolIndex(set) => set.insert((s, o)),
            _ => unreachable!(),
        }
    }

    pub fn insert_lang_string(&mut self, s: u32, v: u32, l: u32) -> bool {
        match self {
            BinaryIndex::LangStringIndex(set) => set.insert((s, v, l)),
            _ => unreachable!(),
        }
    }
}

pub fn is_so_indexed(object_type: &BaseRDFNodeType, subject_object_indexing: bool) -> bool {
    let ind = subject_object_indexing
        && (object_type.stored_cat()
            || object_type.is_lit_type(xsd::DATE)
            || object_type.is_lit_type(xsd::INT)
            || object_type.is_lit_type(xsd::BOOLEAN)
            || object_type.is_lit_type(xsd::INTEGER)
            || object_type.is_lit_type(xsd::LONG)
            || object_type.is_lit_type(xsd::FLOAT)
            || object_type.is_lit_type(xsd::DOUBLE)
            || object_type.is_lit_type(xsd::DECIMAL));
    ind
}

impl BinaryIndex {
    pub fn maybe_create(
        df: &DataFrame,
        object_type: &BaseRDFNodeType,
        subject_object_indexing: bool,
    ) -> Option<BinaryIndex> {
        if is_so_indexed(object_type, subject_object_indexing) {
            if object_type.is_lang_string() {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let v = df
                    .column(OBJECT_COL_NAME)
                    .unwrap()
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap();
                let l = df
                    .column(OBJECT_COL_NAME)
                    .unwrap()
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for ((s, o), l) in s
                    .u32()
                    .unwrap()
                    .iter()
                    .zip(v.u32().unwrap().iter())
                    .zip(l.u32().unwrap().iter())
                {
                    set.insert((s.unwrap(), o.unwrap(), l.unwrap()));
                }
                let so = BinaryIndex::LangStringIndex(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::BOOLEAN) {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.bool().unwrap().iter()) {
                    set.insert((s.unwrap(), o.unwrap()));
                }
                let so = BinaryIndex::BoolIndex(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::DATE) {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s
                    .u32()
                    .unwrap()
                    .iter()
                    .zip(o.date().unwrap().physical().iter())
                {
                    set.insert((s.unwrap(), o.unwrap()));
                }
                let so = BinaryIndex::Int32Index(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::INT) {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.i32().unwrap().iter()) {
                    set.insert((s.unwrap(), o.unwrap()));
                }
                let so = BinaryIndex::Int32Index(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::INTEGER) || object_type.is_lit_type(xsd::LONG) {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.i64().unwrap().iter()) {
                    set.insert((s.unwrap(), o.unwrap()));
                }
                let so = BinaryIndex::Int64Index(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::DOUBLE) || object_type.is_lit_type(xsd::DECIMAL)
            {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.f64().unwrap().iter()) {
                    set.insert((s.unwrap(), OrderedFloat(o.unwrap())));
                }
                let so = BinaryIndex::Float64Index(set);
                Some(so)
            } else if object_type.is_lit_type(xsd::FLOAT) {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.f32().unwrap().iter()) {
                    set.insert((s.unwrap(), OrderedFloat(o.unwrap())));
                }
                let so = BinaryIndex::Float32Index(set);
                Some(so)
            } else {
                let s = df.column(SUBJECT_COL_NAME).unwrap();
                let o = df.column(OBJECT_COL_NAME).unwrap();
                let mut set = HashSet::with_capacity(df.height());
                for (s, o) in s.u32().unwrap().iter().zip(o.u32().unwrap().iter()) {
                    set.insert((s.unwrap(), o.unwrap()));
                }
                let so = BinaryIndex::NormalIndex(set);
                Some(so)
            }
        } else {
            None
        }
    }

    pub fn update_so_index(&mut self, df: DataFrame, b: &BaseRDFNodeType) -> Option<DataFrame> {
        if b.is_lang_string() {
            self.update_lang_so_index(df)
        } else if b.is_lit_type(xsd::BOOLEAN) {
            self.update_bool_so_index(df)
        } else if b.is_lit_type(xsd::DECIMAL) || b.is_lit_type(xsd::DOUBLE) {
            self.update_f64_so_index(df)
        } else if b.is_lit_type(xsd::FLOAT) {
            self.update_f32_so_index(df)
        } else if b.is_lit_type(xsd::DATE) {
            self.update_date_so_index(df)
        } else if b.is_lit_type(xsd::INT) {
            self.update_int32_so_index(df)
        } else if b.is_lit_type(xsd::INTEGER) || b.is_lit_type(xsd::LONG) {
            self.update_int64_so_index(df)
        } else {
            self.update_normal_so_index(df)
        }
    }

    fn update_normal_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            let new = self.insert_normal(s, o);
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
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_bool_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.bool().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o: bool = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_bool(s, o);
            if new {
                new_ss.push(s);
                new_os.push(Some(o));
                new_sranks.push(s_rank);
                new_oranks.push(o_rank);
            }
        }
        if new_ss.is_empty() {
            None
        } else {
            let s_series = UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_COL_NAME), new_ss)
                .into_series();
            let mut o_series = Series::from_iter(new_os);
            o_series.rename(PlSmallStr::from_str(OBJECT_COL_NAME));
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_f64_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.f64().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_f64(s, o);
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
            let o_series = Float64Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os)
                .into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_f32_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.f32().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_f32(s, o);
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
            let o_series = Float32Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os)
                .into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_date_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.date().unwrap().physical().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_i32(s, o);
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
            let o_series =
                Int32Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os).into_date();
            let o_series = o_series.into_series();

            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_int32_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.i32().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_i32(s, o);
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
            let o_series =
                Int32Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os).into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_int64_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
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
            .zip(o.i64().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let o = o.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_i64(s, o);
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
            let o_series =
                Int64Chunked::from_vec(PlSmallStr::from_str(OBJECT_COL_NAME), new_os).into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    o_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            Some(new_df)
        }
    }

    fn update_lang_so_index(&mut self, df: DataFrame) -> Option<DataFrame> {
        let s = df.column(SUBJECT_COL_NAME).unwrap();
        let v = df
            .column(OBJECT_COL_NAME)
            .unwrap()
            .struct_()
            .unwrap()
            .field_by_name(LANG_STRING_VALUE_FIELD)
            .unwrap();
        let l = df
            .column(OBJECT_COL_NAME)
            .unwrap()
            .struct_()
            .unwrap()
            .field_by_name(LANG_STRING_LANG_FIELD)
            .unwrap();
        let s_rank = df.column(SUBJECT_RANK_COL_NAME).unwrap();
        let o_rank = df.column(OBJECT_RANK_COL_NAME).unwrap();
        let mut new_ss = vec![];
        let mut new_vs = vec![];
        let mut new_ls = vec![];
        let mut new_sranks = vec![];
        let mut new_oranks = vec![];
        for ((((s, v), l), s_rank), o_rank) in s
            .u32()
            .unwrap()
            .iter()
            .zip(v.u32().unwrap().iter())
            .zip(l.u32().unwrap().iter())
            .zip(s_rank.u32().unwrap().iter())
            .zip(o_rank.u32().unwrap().iter())
        {
            let s = s.unwrap();
            let v = v.unwrap();
            let l = l.unwrap();
            let s_rank = s_rank.unwrap();
            let o_rank = o_rank.unwrap();
            let new = self.insert_lang_string(s, v, l);
            if new {
                new_ss.push(s);
                new_vs.push(v);
                new_ls.push(l);
                new_sranks.push(s_rank);
                new_oranks.push(o_rank);
            }
        }
        if new_ss.is_empty() {
            None
        } else {
            let s_series = UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_COL_NAME), new_ss)
                .into_series();
            let v_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(LANG_STRING_VALUE_FIELD), new_vs)
                    .into_series();
            let l_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(LANG_STRING_LANG_FIELD), new_ls)
                    .into_series();
            let s_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(SUBJECT_RANK_COL_NAME), new_sranks)
                    .into_series();
            let o_rank_series =
                UInt32Chunked::from_vec(PlSmallStr::from_str(OBJECT_RANK_COL_NAME), new_oranks)
                    .into_series();
            let mut new_df = DataFrame::new(
                s_series.len(),
                vec![
                    s_series.into_column(),
                    v_series.into_column(),
                    l_series.into_column(),
                    s_rank_series.into_column(),
                    o_rank_series.into_column(),
                ],
            )
            .unwrap();
            new_df = new_df
                .lazy()
                .with_column(
                    as_struct(vec![
                        col(LANG_STRING_VALUE_FIELD),
                        col(LANG_STRING_LANG_FIELD),
                    ])
                    .alias(OBJECT_COL_NAME),
                )
                .select([
                    col(SUBJECT_COL_NAME),
                    col(OBJECT_COL_NAME),
                    col(SUBJECT_RANK_COL_NAME),
                    col(OBJECT_RANK_COL_NAME),
                ])
                .collect()
                .unwrap();
            Some(new_df)
        }
    }
}
