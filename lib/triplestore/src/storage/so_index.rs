use crate::{OBJECT_RANK_COL_NAME, SUBJECT_RANK_COL_NAME};
use ordered_float::OrderedFloat;
use oxrdf::vocab::xsd;
use polars::prelude::{as_struct, col, IntoLazy, PlSmallStr};
use polars_core::datatypes::{
    BooleanChunked, Int16Chunked, Int8Chunked, UInt16Chunked, UInt8Chunked,
};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    Column, DecimalChunked, Float32Chunked, Float64Chunked, Int128Chunked, Int32Chunked,
    Int64Chunked, IntoColumn, NewChunkedArray, UInt32Chunked, UInt64Chunked,
};
use polars_core::series::Series;
use representation::rdf_to_polars::{
    default_decimal_precision, default_decimal_scale, default_time_unit, default_time_zone,
};
use representation::{
    BaseRDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use std::collections::HashSet;

pub fn bool_chunked(c: &Column) -> &BooleanChunked {
    c.bool().unwrap()
}
pub fn u8_chunked(c: &Column) -> &UInt8Chunked {
    c.u8().unwrap()
}

pub fn i8_chunked(c: &Column) -> &Int8Chunked {
    c.i8().unwrap()
}
pub fn u16_chunked(c: &Column) -> &UInt16Chunked {
    c.u16().unwrap()
}

pub fn i16_chunked(c: &Column) -> &Int16Chunked {
    c.i16().unwrap()
}
pub fn u32_chunked(c: &Column) -> &UInt32Chunked {
    c.u32().unwrap()
}

pub fn i32_chunked(c: &Column) -> &Int32Chunked {
    c.i32().unwrap()
}
pub fn u64_chunked(c: &Column) -> &UInt64Chunked {
    c.u64().unwrap()
}

pub fn i64_chunked(c: &Column) -> &Int64Chunked {
    c.i64().unwrap()
}

pub fn f32_chunked(c: &Column) -> &Float32Chunked {
    c.f32().unwrap()
}

pub fn f64_chunked(c: &Column) -> &Float64Chunked {
    c.f64().unwrap()
}

pub fn date_chunked(c: &Column) -> &Int32Chunked {
    c.date().unwrap().physical()
}

pub fn datetime_chunked(c: &Column) -> &Int64Chunked {
    c.datetime().unwrap().physical()
}

pub fn decimal_chunked(c: &Column) -> &Int128Chunked {
    c.decimal().unwrap().physical()
}

pub fn bool_vec_to_column(col_name: &str, vec: Vec<bool>) -> Column {
    let mut c = Series::from_iter(vec).into_column();
    c.rename(PlSmallStr::from_str(col_name));
    c
}

pub fn u8_vec_to_column(col_name: &str, vec: Vec<u8>) -> Column {
    UInt8Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn i8_vec_to_column(col_name: &str, vec: Vec<i8>) -> Column {
    Int8Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn u16_vec_to_column(col_name: &str, vec: Vec<u16>) -> Column {
    UInt16Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn i16_vec_to_column(col_name: &str, vec: Vec<i16>) -> Column {
    Int16Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}

pub fn u32_vec_to_column(col_name: &str, vec: Vec<u32>) -> Column {
    UInt32Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn i32_vec_to_column(col_name: &str, vec: Vec<i32>) -> Column {
    Int32Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}

pub fn u64_vec_to_column(col_name: &str, vec: Vec<u64>) -> Column {
    UInt64Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn i64_vec_to_column(col_name: &str, vec: Vec<i64>) -> Column {
    Int64Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}

pub fn f32_vec_to_column(col_name: &str, vec: Vec<f32>) -> Column {
    Float32Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}
pub fn f64_vec_to_column(col_name: &str, vec: Vec<f64>) -> Column {
    Float64Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_column()
}

pub fn date_vec_to_column(col_name: &str, vec: Vec<i32>) -> Column {
    let o_series = Int32Chunked::from_vec(PlSmallStr::from_str(col_name), vec).into_date();
    o_series.into_column()
}

pub fn datetime_vec_to_column(col_name: &str, vec: Vec<i64>) -> Column {
    let o_series = Int64Chunked::from_vec(PlSmallStr::from_str(col_name), vec)
        .into_datetime(default_time_unit(), Some(default_time_zone()));
    o_series.into_column()
}

pub fn decimal_vec_to_column(col_name: &str, vec: Vec<i128>) -> Column {
    let o_series = Int128Chunked::from_vec(PlSmallStr::from_str(col_name), vec)
        .into_decimal(default_decimal_precision(), default_decimal_scale())
        .unwrap();
    o_series.into_column()
}

pub fn unwrap_ordered_float<T>(o: Option<T>) -> OrderedFloat<T> {
    OrderedFloat(o.unwrap())
}

pub fn unwrap_t<T>(o: Option<T>) -> T {
    o.unwrap()
}

pub fn noop_t<T>(o: T) -> T {
    o
}

#[macro_export]
macro_rules! binary_nonlang_nonlang_index_impl {
    ($struct_name:ident, $t:ty, $subj_chunked_func:expr, $obj_chunked_func:expr, $maybe_unwrap:expr, $prep_index_subj:expr, $prep_index_obj:expr, $subject_func:expr, $object_func:expr) => {
        #[derive(Clone, Debug)]
        pub struct $struct_name {
            index: HashSet<$t>,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    index: HashSet::new(),
                }
            }

            pub fn insert(&mut self, df: &DataFrame) -> Option<DataFrame> {
                let subj_col = df.column(SUBJECT_COL_NAME).unwrap();
                let obj_col = df.column(OBJECT_COL_NAME).unwrap();
                let subj_ch = $subj_chunked_func(&subj_col);
                let obj_ch = $obj_chunked_func(&obj_col);
                let mut new_ss = vec![];
                let mut new_os = vec![];
                let mut new_sranks = vec![];
                let mut new_oranks = vec![];

                let subj_rank = df.column(SUBJECT_RANK_COL_NAME).unwrap();
                let obj_rank = df.column(OBJECT_RANK_COL_NAME).unwrap();

                for (((subj, obj), subj_rank), obj_rank) in subj_ch
                    .iter()
                    .zip(obj_ch.iter())
                    .zip(subj_rank.u32().unwrap().iter())
                    .zip(obj_rank.u32().unwrap().iter())
                {
                    let subj = $maybe_unwrap(subj);
                    let obj = $maybe_unwrap(obj);

                    let new = self
                        .index
                        .insert(($prep_index_subj(subj), $prep_index_obj(obj)));
                    if new {
                        new_ss.push(subj);
                        new_os.push(obj);
                        let subj_rank = subj_rank.unwrap();
                        let obj_rank = obj_rank.unwrap();
                        new_sranks.push(subj_rank);
                        new_oranks.push(obj_rank);
                    }
                }
                if new_ss.is_empty() {
                    None
                } else {
                    let l = new_ss.len();
                    let subj_col = $subject_func(SUBJECT_COL_NAME, new_ss);
                    let obj_col = $object_func(OBJECT_COL_NAME, new_os);
                    let subj_rank_col = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(SUBJECT_RANK_COL_NAME),
                        new_sranks,
                    )
                    .into_column();
                    let obj_rank_col = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(OBJECT_RANK_COL_NAME),
                        new_oranks,
                    )
                    .into_column();
                    let cols = vec![subj_col, obj_col, subj_rank_col, obj_rank_col];

                    let new_df = DataFrame::new(l, cols).unwrap();
                    Some(new_df)
                }
            }

            pub fn delete(&mut self, df: &DataFrame) {
                let subj_col = df.column(SUBJECT_COL_NAME).unwrap();
                let obj_col = df.column(OBJECT_COL_NAME).unwrap();
                let subj_ch = $subj_chunked_func(&subj_col);
                let obj_ch = $obj_chunked_func(&obj_col);

                for (subj, obj) in subj_ch.iter().zip(obj_ch.iter()) {
                    let subj = $maybe_unwrap(subj);
                    let obj = $maybe_unwrap(obj);

                    self.index
                        .remove(&($prep_index_subj(subj), $prep_index_obj(obj)));
                }
            }
        }
    };
}

#[macro_export]
macro_rules! binary_nonlang_lang_index_impl {
    ($struct_name:ident, $t:ty, $subj_chunked_func:expr, $maybe_unwrap:expr, $prep_index_a:expr, $subject_func:expr) => {
        #[derive(Clone, Debug)]
        pub struct $struct_name {
            index: HashSet<$t>,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    index: HashSet::new(),
                }
            }

            pub fn insert(&mut self, df: &DataFrame) -> Option<DataFrame> {
                let subj_col = df.column(SUBJECT_COL_NAME).unwrap();
                let obj_col = df.column(OBJECT_COL_NAME).unwrap();
                let subj_ch = $subj_chunked_func(&subj_col);
                let obj_v = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap();
                let obj_l = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap();
                let obj_v_iter = obj_v.u32().unwrap().iter();
                let obj_l_iter = obj_l.u32().unwrap().iter();
                let mut new_ss = vec![];
                let mut new_o_vs = vec![];
                let mut new_o_ls = vec![];
                let mut new_aranks = vec![];
                let mut new_branks = vec![];

                let subj_rank = df.column(SUBJECT_RANK_COL_NAME).unwrap();
                let obj_rank = df.column(OBJECT_RANK_COL_NAME).unwrap();

                for ((((subj, obj_v), obj_l), subj_rank), obj_rank) in subj_ch
                    .iter()
                    .zip(obj_v_iter)
                    .zip(obj_l_iter)
                    .zip(subj_rank.u32().unwrap().iter())
                    .zip(obj_rank.u32().unwrap().iter())
                {
                    let subj = $maybe_unwrap(subj);
                    let obj_v = $maybe_unwrap(obj_v);
                    let obj_l = $maybe_unwrap(obj_l);

                    let new = self.index.insert(($prep_index_a(subj), obj_v, obj_l));
                    if new {
                        new_ss.push(subj);
                        new_o_vs.push(obj_v);
                        new_o_ls.push(obj_l);
                        let subj_rank = subj_rank.unwrap();
                        let obj_rank = obj_rank.unwrap();
                        new_aranks.push(subj_rank);
                        new_branks.push(obj_rank);
                    }
                }
                if new_ss.is_empty() {
                    None
                } else {
                    let l = new_ss.len();
                    let subj_column = $subject_func(SUBJECT_COL_NAME, new_ss);
                    let obj_v_column = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(LANG_STRING_VALUE_FIELD),
                        new_o_vs,
                    )
                    .into_column();
                    let obj_l_column = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(LANG_STRING_LANG_FIELD),
                        new_o_ls,
                    )
                    .into_column();
                    let subj_rank = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(SUBJECT_RANK_COL_NAME),
                        new_aranks,
                    )
                    .into_column();
                    let obj_rank = UInt32Chunked::from_vec(
                        PlSmallStr::from_str(OBJECT_RANK_COL_NAME),
                        new_branks,
                    )
                    .into_column();
                    let cols = vec![subj_column, obj_v_column, obj_l_column, subj_rank, obj_rank];
                    let mut new_df = DataFrame::new(l, cols).unwrap();
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

            pub fn delete(&mut self, df: &DataFrame) {
                let subj_col = df.column(SUBJECT_COL_NAME).unwrap();
                let obj_col = df.column(OBJECT_COL_NAME).unwrap();
                let subj_ch = $subj_chunked_func(&subj_col);
                let obj_v = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap();
                let obj_l = obj_col
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap();
                let obj_v_iter = obj_v.u32().unwrap().iter();
                let obj_l_iter = obj_l.u32().unwrap().iter();

                for ((subj, obj_v), obj_l) in subj_ch.iter().zip(obj_v_iter).zip(obj_l_iter) {
                    let subj = $maybe_unwrap(subj);
                    let obj_v = $maybe_unwrap(obj_v);
                    let obj_l = $maybe_unwrap(obj_l);

                    self.index.remove(&($prep_index_a(subj), obj_v, obj_l));
                }
            }
        }
    };
}

binary_nonlang_nonlang_index_impl!(
    U32BoolIndex,
    (u32, bool),
    u32_chunked,
    bool_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    bool_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32U8Index,
    (u32, u8),
    u32_chunked,
    u8_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    u8_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32I8Index,
    (u32, i8),
    u32_chunked,
    i8_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    i8_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32U16Index,
    (u32, u16),
    u32_chunked,
    u16_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    u16_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32I16Index,
    (u32, i16),
    u32_chunked,
    i16_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    i16_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32U32Index,
    (u32, u32),
    u32_chunked,
    u32_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    u32_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32I32Index,
    (u32, i32),
    u32_chunked,
    i32_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    i32_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32U64Index,
    (u32, u64),
    u32_chunked,
    u64_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    u64_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32I64Index,
    (u32, i64),
    u32_chunked,
    i64_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    i64_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32F32Index,
    (u32, OrderedFloat<f32>),
    u32_chunked,
    f32_chunked,
    unwrap_t,
    noop_t,
    OrderedFloat,
    u32_vec_to_column,
    f32_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32F64Index,
    (u32, OrderedFloat<f64>),
    u32_chunked,
    f64_chunked,
    unwrap_t,
    noop_t,
    OrderedFloat,
    u32_vec_to_column,
    f64_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32DateIndex,
    (u32, i32),
    u32_chunked,
    date_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    date_vec_to_column
);
binary_nonlang_nonlang_index_impl!(
    U32DateTimeIndex,
    (u32, i64),
    u32_chunked,
    datetime_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    datetime_vec_to_column
);

binary_nonlang_nonlang_index_impl!(
    U32DecimalIndex,
    (u32, i128),
    u32_chunked,
    decimal_chunked,
    unwrap_t,
    noop_t,
    noop_t,
    u32_vec_to_column,
    decimal_vec_to_column
);

binary_nonlang_lang_index_impl!(
    U32LangIndex,
    (u32, u32, u32),
    u32_chunked,
    unwrap_t,
    noop_t,
    u32_vec_to_column
);

#[derive(Clone, Debug)]
pub enum SubjectObjectIndex {
    U32LangIndex(U32LangIndex),
    U32BoolIndex(U32BoolIndex),
    U32U8Index(U32U8Index),
    U32I8Index(U32I8Index),
    U32U16Index(U32U16Index),
    U32I16Index(U32I16Index),
    U32I32Index(U32I32Index),
    U32U32Index(U32U32Index),
    U32U64Index(U32U64Index),
    U32I64Index(U32I64Index),
    U32F32Index(U32F32Index),
    U32F64Index(U32F64Index),
    U32DecimalIndex(U32DecimalIndex),
    U32DateIndex(U32DateIndex),
    U32DateTimeIndex(U32DateTimeIndex),
}

impl SubjectObjectIndex {
    pub fn new(subject_type: &BaseRDFNodeType, object_type: &BaseRDFNodeType) -> Self {
        if subject_type.is_iri()
            || subject_type.is_blank_node()
            || subject_type.is_lit_type(xsd::UNSIGNED_INT)
        {
            if object_type.is_lang_string() {
                SubjectObjectIndex::U32LangIndex(U32LangIndex::new())
            } else if object_type.stored_cat() || object_type.is_lit_type(xsd::UNSIGNED_INT) {
                SubjectObjectIndex::U32U32Index(U32U32Index::new())
            } else if object_type.is_lit_type(xsd::DATE) {
                SubjectObjectIndex::U32DateIndex(U32DateIndex::new())
            } else if object_type.is_lit_type(xsd::BOOLEAN) {
                SubjectObjectIndex::U32BoolIndex(U32BoolIndex::new())
            } else if object_type.is_lit_type(xsd::FLOAT) {
                SubjectObjectIndex::U32F32Index(U32F32Index::new())
            } else if object_type.is_lit_type(xsd::DOUBLE) {
                SubjectObjectIndex::U32F64Index(U32F64Index::new())
            } else if object_type.is_lit_type(xsd::DECIMAL) {
                SubjectObjectIndex::U32DecimalIndex(U32DecimalIndex::new())
            } else if object_type.is_lit_type(xsd::BYTE) {
                SubjectObjectIndex::U32I8Index(U32I8Index::new())
            } else if object_type.is_lit_type(xsd::UNSIGNED_BYTE) {
                SubjectObjectIndex::U32U8Index(U32U8Index::new())
            } else if object_type.is_lit_type(xsd::SHORT) {
                SubjectObjectIndex::U32I16Index(U32I16Index::new())
            } else if object_type.is_lit_type(xsd::UNSIGNED_SHORT) {
                SubjectObjectIndex::U32U16Index(U32U16Index::new())
            } else if object_type.is_lit_type(xsd::INT) {
                SubjectObjectIndex::U32I32Index(U32I32Index::new())
            } else if object_type.is_lit_type(xsd::LONG) || object_type.is_lit_type(xsd::INTEGER) {
                SubjectObjectIndex::U32I64Index(U32I64Index::new())
            } else if object_type.is_lit_type(xsd::UNSIGNED_LONG) {
                SubjectObjectIndex::U32U64Index(U32U64Index::new())
            } else if object_type.is_lit_type(xsd::DATE_TIME)
                || object_type.is_lit_type(xsd::DATE_TIME_STAMP)
            {
                SubjectObjectIndex::U32DateTimeIndex(U32DateTimeIndex::new())
            } else {
                todo!("B type {:?}", object_type);
            }
        } else {
            todo!("A type {}", subject_type);
        }
    }

    pub fn insert(&mut self, df: &DataFrame) -> Option<DataFrame> {
        match self {
            SubjectObjectIndex::U32LangIndex(i) => i.insert(df),
            SubjectObjectIndex::U32U8Index(i) => i.insert(df),
            SubjectObjectIndex::U32I8Index(i) => i.insert(df),
            SubjectObjectIndex::U32U16Index(i) => i.insert(df),
            SubjectObjectIndex::U32I16Index(i) => i.insert(df),
            SubjectObjectIndex::U32I32Index(i) => i.insert(df),
            SubjectObjectIndex::U32U64Index(i) => i.insert(df),
            SubjectObjectIndex::U32I64Index(i) => i.insert(df),
            SubjectObjectIndex::U32F32Index(i) => i.insert(df),
            SubjectObjectIndex::U32F64Index(i) => i.insert(df),
            SubjectObjectIndex::U32DecimalIndex(i) => i.insert(df),
            SubjectObjectIndex::U32DateIndex(i) => i.insert(df),
            SubjectObjectIndex::U32DateTimeIndex(i) => i.insert(df),
            SubjectObjectIndex::U32BoolIndex(i) => i.insert(df),
            SubjectObjectIndex::U32U32Index(i) => i.insert(df),
        }
    }

    pub fn delete(&mut self, df: &DataFrame) {
        match self {
            SubjectObjectIndex::U32LangIndex(i) => i.delete(df),
            SubjectObjectIndex::U32U8Index(i) => i.delete(df),
            SubjectObjectIndex::U32I8Index(i) => i.delete(df),
            SubjectObjectIndex::U32U16Index(i) => i.delete(df),
            SubjectObjectIndex::U32I16Index(i) => i.delete(df),
            SubjectObjectIndex::U32I32Index(i) => i.delete(df),
            SubjectObjectIndex::U32U64Index(i) => i.delete(df),
            SubjectObjectIndex::U32I64Index(i) => i.delete(df),
            SubjectObjectIndex::U32F32Index(i) => i.delete(df),
            SubjectObjectIndex::U32F64Index(i) => i.delete(df),
            SubjectObjectIndex::U32DecimalIndex(i) => i.delete(df),
            SubjectObjectIndex::U32DateIndex(i) => i.delete(df),
            SubjectObjectIndex::U32DateTimeIndex(i) => i.delete(df),
            SubjectObjectIndex::U32BoolIndex(i) => i.delete(df),
            SubjectObjectIndex::U32U32Index(i) => i.delete(df),
        }
    }
}
