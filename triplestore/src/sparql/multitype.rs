use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{as_struct, col, lit, ternary_expr, Expr, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::DataFrame;
use polars_core::prelude::DataType;
use representation::{RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use std::collections::HashMap;

pub const MULTI_VALUE_COL: &str = "v";
pub const MULTI_LANG_COL: &str = "l";
pub const MULTI_DT_COL: &str = "d";

pub const MULTI_IRI_DT: &str = "I";
pub const MULTI_BLANK_DT: &str = "B";

pub fn convert_lf_col_to_multitype(lf: LazyFrame, c: &str, dt: &RDFNodeType) -> LazyFrame {
    let lf = match dt {
        RDFNodeType::IRI => lf.with_column(
            as_struct(vec![
                col(c)
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_VALUE_COL),
                lit(non_multi_type_string(&dt))
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_DT_COL),
                lit(LiteralValue::Null)
                    .cast(DataType::Utf8)
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_LANG_COL),
            ])
            .alias(c),
        ),
        RDFNodeType::BlankNode => lf.with_column(
            as_struct(vec![
                col(c)
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_VALUE_COL),
                lit(non_multi_type_string(&dt))
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_DT_COL),
                lit(LiteralValue::Null)
                    .cast(DataType::Utf8)
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_LANG_COL),
            ])
            .alias(c),
        ),
        RDFNodeType::Literal(l) => {
            if rdf::LANG_STRING == l.as_ref() {
                lf.with_column(
                    as_struct(vec![
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_VALUE_COL),
                        lit(non_multi_type_string(dt))
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_DT_COL),
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_LANG_COL),
                    ])
                    .alias(c),
                )
            } else {
                lf.with_column(
                    as_struct(vec![
                        col(c)
                            .cast(DataType::Utf8)
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_VALUE_COL),
                        lit(non_multi_type_string(&dt))
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_DT_COL),
                        lit(LiteralValue::Null)
                            .cast(DataType::Utf8)
                            .cast(DataType::Categorical(None))
                            .alias(MULTI_LANG_COL),
                    ])
                    .alias(c),
                )
            }
        }
        RDFNodeType::None => {
            panic!()
        }
        RDFNodeType::MultiType => lf,
    };
    lf
}

pub fn non_multi_type_string(r: &RDFNodeType) -> String {
    match r {
        RDFNodeType::IRI => MULTI_IRI_DT.to_string(),
        RDFNodeType::BlankNode => MULTI_BLANK_DT.to_string(),
        RDFNodeType::Literal(l) => l.to_string(),
        RDFNodeType::None => {
            panic!("")
        }
        RDFNodeType::MultiType => {
            panic!("")
        }
    }
}

fn str_non_multi_type(s: &str) -> RDFNodeType {
    match s {
        MULTI_IRI_DT => RDFNodeType::IRI,
        MULTI_BLANK_DT => RDFNodeType::BlankNode,
        s => RDFNodeType::Literal(NamedNode::new_unchecked(s)),
    }
}

pub fn create_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeType>,
    LazyFrame,
    HashMap<String, RDFNodeType>,
) {
    for (v, dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if dt != left_dt {
                if left_dt != &RDFNodeType::MultiType {
                    left_mappings = convert_lf_col_to_multitype(left_mappings, v, left_dt);
                }
                if dt != &RDFNodeType::MultiType {
                    right_mappings = convert_lf_col_to_multitype(right_mappings, v, &dt);
                }
                left_datatypes.insert(v.clone(), RDFNodeType::MultiType);
            }
        }
    }
    for (v, dt) in &left_datatypes {
        if right_datatypes.contains_key(v) {
            right_datatypes.insert(v.clone(), dt.clone());
        }
    }
    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}

pub fn create_join_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
    inner: bool,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeType>,
    LazyFrame,
    HashMap<String, RDFNodeType>,
) {
    let mut new_left_datatypes = HashMap::new();
    let mut new_right_datatypes = HashMap::new();
    for (v, right_dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if right_dt != left_dt {
                if left_dt == &RDFNodeType::MultiType {
                    if inner {
                        left_mappings =
                            force_convert_multicol_to_single_col(left_mappings, v, right_dt);
                        new_left_datatypes.insert(v.clone(), right_dt.clone());
                    } else {
                        right_mappings = convert_lf_col_to_multitype(right_mappings, v, right_dt);
                        new_right_datatypes.insert(v.clone(), RDFNodeType::MultiType);
                    }
                } else {
                    if right_dt == &RDFNodeType::MultiType {
                        right_mappings =
                            force_convert_multicol_to_single_col(right_mappings, v, left_dt);
                        new_right_datatypes.insert(v.clone(), left_dt.clone());
                    } else {
                        right_mappings = right_mappings.drop_columns([v]).with_column(
                            lit(LiteralValue::Null)
                                .cast(left_dt.polars_data_type())
                                .alias(v),
                        );
                        new_right_datatypes.insert(v.clone(), left_dt.clone());
                    }
                }
            }
        }
    }
    left_datatypes.extend(new_left_datatypes);
    right_datatypes.extend(new_right_datatypes);

    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}

pub fn force_convert_multicol_to_single_col(
    mut lf: LazyFrame,
    c: &str,
    dt: &RDFNodeType,
) -> LazyFrame {
    lf = lf.filter(
        col(c)
            .struct_()
            .field_by_name(MULTI_DT_COL)
            .eq(lit(non_multi_type_string(dt))),
    );
    definitively_convert_lf_multicol_to_single(lf, c, dt)
}

pub fn unicol_to_multitype_value(c: &str, dt: &RDFNodeType) -> Expr {
    match dt {
        RDFNodeType::IRI | RDFNodeType::BlankNode => col(c),
        RDFNodeType::Literal(l) => match l.as_ref() {
            xsd::STRING
            | xsd::BYTE
            | xsd::SHORT
            | xsd::UNSIGNED_BYTE
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_LONG
            | xsd::INTEGER
            | xsd::LONG
            | xsd::NON_NEGATIVE_INTEGER
            | xsd::INT
            | xsd::DOUBLE
            | xsd::FLOAT
            | xsd::BOOLEAN
            | xsd::DECIMAL => col(c).cast(DataType::Utf8),
            _ => todo!("Not yet implemented: {:?}", dt),
        },
        RDFNodeType::None => {
            unimplemented!()
        }
        RDFNodeType::MultiType => {
            unimplemented!()
        }
    }
}

pub fn multi_col_to_string_col(lf: LazyFrame, c: &str) -> LazyFrame {
    lf.with_column(
        (ternary_expr(
            col(c)
                .struct_()
                .field_by_name(MULTI_DT_COL)
                .eq(lit(non_multi_type_string(&RDFNodeType::IRI)))
                .or(col(c)
                    .struct_()
                    .field_by_name(MULTI_DT_COL)
                    .eq(lit(non_multi_type_string(&RDFNodeType::BlankNode)))),
            col(c).struct_().field_by_name(MULTI_VALUE_COL),
            lit("\"") + col(c).struct_().field_by_name(MULTI_VALUE_COL) + lit("\""),
        ) + ternary_expr(
            col(c).struct_().field_by_name(MULTI_LANG_COL).is_not_null(),
            lit("@@") + col(c).struct_().field_by_name(MULTI_LANG_COL).is_not_null(),
            lit(""),
        ) + ternary_expr(
            col(c)
                .struct_()
                .field_by_name(MULTI_DT_COL)
                .eq(lit(non_multi_type_string(&RDFNodeType::IRI)))
                .or(col(c)
                    .struct_()
                    .field_by_name(MULTI_DT_COL)
                    .eq(lit(non_multi_type_string(&RDFNodeType::BlankNode))))
                .or(col(c)
                    .struct_()
                    .field_by_name(MULTI_DT_COL)
                    .eq(lit(non_multi_type_string(&RDFNodeType::Literal(
                        NamedNode::from(rdf::LANG_STRING),
                    )))))
                .or(col(c)
                    .struct_()
                    .field_by_name(MULTI_DT_COL)
                    .eq(lit(non_multi_type_string(&RDFNodeType::Literal(
                        NamedNode::from(xsd::STRING),
                    ))))),
            lit(""),
            lit("^^") + col(c).struct_().field_by_name(MULTI_DT_COL),
        ))
        .alias(c),
    )
}

pub fn maybe_convert_df_multicol_to_single(
    df: &mut DataFrame,
    c: &str,
    dt: Option<&RDFNodeType>,
) -> bool {
    let c_ser = df.column(c).unwrap();
    if let DataType::Struct(fields) = c_ser.dtype() {
        if fields.len() != 3 {
            return true;
        }
    } else {
        return true;
    }
    let dt = if let Some(dt) = dt {
        dt.clone()
    } else {
        let unique_datatypes = c_ser
            .struct_()
            .unwrap()
            .field_by_name(MULTI_DT_COL)
            .unwrap()
            .unique()
            .unwrap();
        if unique_datatypes.len() == 1 {
            str_non_multi_type(
                unique_datatypes
                    .cast(&DataType::Utf8)
                    .unwrap()
                    .utf8()
                    .unwrap()
                    .get(0)
                    .unwrap(),
            )
        } else {
            return false;
        }
    };
    df.with_column(
        c_ser
            .cast(&DataType::Utf8)
            .unwrap()
            .cast(&dt.polars_data_type())
            .unwrap(),
    )
    .unwrap();
    true
}

pub fn definitively_convert_lf_multicol_to_single(
    lf: LazyFrame,
    c: &str,
    dt: &RDFNodeType,
) -> LazyFrame {
    let polars_dt = dt.polars_data_type();
    lf.with_column(
        col(c)
            .struct_()
            .field_by_name(MULTI_VALUE_COL)
            .cast(polars_dt)
            .alias(c),
    )
}

pub fn split_df_multicol(df: &mut DataFrame, c: &str) -> Vec<(DataFrame, RDFNodeType)> {
    let c_ser = df.column(c).unwrap();
    let tmp_colname = uuid::Uuid::new_v4().to_string();
    let mut c_type_ser = c_ser
        .struct_()
        .unwrap()
        .field_by_name(MULTI_DT_COL)
        .unwrap();
    c_type_ser.rename(&tmp_colname);
    df.with_column(c_type_ser).unwrap();
    df.sort_in_place(vec![&tmp_colname], vec![false], true)
        .unwrap();
    let mut dfs = df
        .select([&tmp_colname, c])
        .unwrap()
        .partition_by_stable(vec![&tmp_colname], true)
        .unwrap();
    dfs.sort_by_key(|x| {
        x.column(&tmp_colname)
            .unwrap()
            .utf8()
            .unwrap()
            .get(0)
            .unwrap()
            .to_string()
    });
    let mut dfs_dts = vec![];
    for mut df in dfs {
        let s = df.drop_in_place(&tmp_colname).unwrap();
        let dt = str_non_multi_type(s.utf8().unwrap().get(0).unwrap());
        maybe_convert_df_multicol_to_single(&mut df, c, Some(&dt));
        dfs_dts.push((df, dt));
    }
    let _ = df.drop_in_place(&tmp_colname).unwrap();
    dfs_dts
}

pub fn split_df_multicols(
    mut df: DataFrame,
    cs: Vec<&str>,
) -> Vec<(DataFrame, HashMap<String, RDFNodeType>)> {
    let mut sort_colnames = vec![];
    let mut sort_cols = vec![];
    let mut sort_by_exprs = vec![];
    for c in &cs {
        let tmp_colname = uuid::Uuid::new_v4().to_string();
        sort_cols.push(
            col(&c)
                .struct_()
                .field_by_name(MULTI_DT_COL)
                .alias(&tmp_colname),
        );
        sort_by_exprs.push(col(&tmp_colname));
        sort_colnames.push(tmp_colname);
    }
    df = df
        .lazy()
        .with_columns(sort_cols)
        .sort_by_exprs(sort_by_exprs, vec![false].repeat(cs.len()), false, false)
        .collect()
        .unwrap();

    let dfs = df.partition_by(sort_colnames.as_slice(), true).unwrap();
    let mut dfs_dts = vec![];
    for mut df in dfs {
        let mut map = HashMap::new();
        for (i, key_col) in sort_colnames.iter().enumerate() {
            let s = df.drop_in_place(key_col).unwrap();
            if let Some(dt) = s.cast(&DataType::Utf8).unwrap().utf8().unwrap().get(0) {
                let dt = str_non_multi_type(dt);
                maybe_convert_df_multicol_to_single(&mut df, cs.get(i).unwrap(), Some(&dt));
                map.insert(cs.get(i).unwrap().to_string(), dt);
            } else {
                df = df
                    .lazy()
                    .with_column(lit(LiteralValue::Null).alias(cs.get(i).unwrap()))
                    .collect()
                    .unwrap();
                map.insert(cs.get(i).unwrap().to_string(), RDFNodeType::None);
            }
        }
        dfs_dts.push((df, map));
    }
    dfs_dts
}
