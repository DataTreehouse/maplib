use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNode;
use polars::prelude::{
    as_struct, col, lit, ternary_expr, Expr, IntoLazy, JoinArgs, LazyFrame, LiteralValue,
};
use polars_core::frame::DataFrame;
use polars_core::prelude::{DataType, Series};
use crate::{
    literal_iri_to_namednode, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use std::collections::HashMap;

pub const MULTI_VALUE_COL: &str = "v";
pub const MULTI_LANG_COL: &str = "l";
pub const MULTI_DT_COL: &str = "d";

pub const MULTI_IRI_DT: &str = "I";
pub const MULTI_BLANK_DT: &str = "B";

pub const MULTI_PLACEHOLDER_LANG: &str = "?";

pub fn convert_lf_col_to_multitype(lf: LazyFrame, c: &str, dt: &RDFNodeType) -> LazyFrame {
    match dt {
        RDFNodeType::IRI => lf.with_column(
            as_struct(vec![
                col(c)
                    .cast(DataType::Categorical(None))
                    .alias(MULTI_VALUE_COL),
                lit(non_multi_type_string(dt))
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
                lit(non_multi_type_string(dt))
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
                        lit(non_multi_type_string(dt))
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
    }
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
        s => RDFNodeType::Literal(literal_iri_to_namednode(s)),
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
                    right_mappings = convert_lf_col_to_multitype(right_mappings, v, dt);
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
                } else if right_dt == &RDFNodeType::MultiType {
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

pub fn get_unique_datatype(series: &Series) -> Option<RDFNodeType> {
    //TODO: cast to null type if all nulls and uniques=0 (after dropnull)?
    let uniques = series
        .struct_()
        .unwrap()
        .field_by_name(MULTI_DT_COL)
        .unwrap()
        .unique()
        .unwrap()
        .drop_nulls();
    if uniques.len() == 1 {
        let unique = str_non_multi_type(
            uniques
                .cast(&DataType::Utf8)
                .unwrap()
                .utf8()
                .unwrap()
                .get(0)
                .unwrap(),
        );
        Some(unique)
    } else {
        None
    }
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

pub fn definitively_convert_lf_multicol_to_single(
    lf: LazyFrame,
    c: &str,
    dt: &RDFNodeType,
) -> LazyFrame {
    let polars_dt = dt.polars_data_type();
    if let RDFNodeType::Literal(l) = dt {
        match l.as_ref() {
            xsd::BOOLEAN => {
                return lf.with_column(
                    col(c)
                        .struct_()
                        .field_by_name(MULTI_VALUE_COL)
                        .eq(lit("true"))
                        .alias(c),
                )
            }
            _ => {}
        }
    }
    lf.with_column(
        col(c)
            .struct_()
            .field_by_name(MULTI_VALUE_COL)
            .cast(DataType::Utf8)
            .cast(polars_dt)
            .alias(c),
    )
}

pub fn split_lf_multicol(mut lf: LazyFrame, c: &str, new_c: &str) -> Vec<(LazyFrame, RDFNodeType)> {
    let dt_suffix = uuid::Uuid::new_v4().to_string();
    let col_dt_string = format!("{}_{}_String", c, dt_suffix);
    lf = lf.with_column(
        col(c)
            .struct_()
            .field_by_name(MULTI_DT_COL)
            .cast(DataType::Utf8)
            .alias(&col_dt_string),
    );
    let df = lf.collect().unwrap();
    let dfs = df.partition_by(vec![&col_dt_string], true).unwrap();
    let mut lfs_dts = vec![];
    for df in dfs {
        let s = df.column(&col_dt_string).unwrap();
        let dt = str_non_multi_type(s.utf8().unwrap().get(0).unwrap());
        let mut lf = df
            .lazy()
            .with_column(col(c).alias(new_c))
            .drop_columns(vec![&col_dt_string]);
        lf = force_convert_multicol_to_single_col(lf, new_c, &dt);
        lfs_dts.push((lf, dt));
    }
    lfs_dts
}

pub fn split_df_multicols(
    mut lf: LazyFrame,
    cs: Vec<&str>,
) -> Vec<(LazyFrame, HashMap<String, RDFNodeType>)> {
    let dt_suffix = uuid::Uuid::new_v4().to_string();
    let mut helper_cols = vec![];
    for c in &cs {
        let col_dt_string = format!("{}_{}_String", c, dt_suffix);
        lf = lf.with_column(
            col(c)
                .struct_()
                .field_by_name(MULTI_DT_COL)
                .cast(DataType::Utf8)
                .alias(&col_dt_string),
        );
        helper_cols.push(col_dt_string)
    }
    let dfs = lf
        .collect()
        .unwrap()
        .partition_by(helper_cols.as_slice(), true)
        .unwrap();
    let mut lfs_dts = vec![];
    for mut df in dfs {
        let mut map = HashMap::new();
        //TODO:Extract dts before this iteration so we can be lazy all the time.
        for (c, col_dt_string) in cs.iter().zip(helper_cols.iter()) {
            let s = df.column(col_dt_string).unwrap();
            let dt = str_non_multi_type(s.utf8().unwrap().get(0).unwrap());
            let mut lf = df.lazy();
            lf = lf.drop_columns(vec![&col_dt_string]);
            lf = force_convert_multicol_to_single_col(lf, c, &dt);
            map.insert(c.to_string(), dt);
            df = lf.collect().unwrap();
        }
        lfs_dts.push((df.lazy(), map));
    }
    lfs_dts
}

pub fn lf_printer(lf: &LazyFrame) {
    let df = lf_destruct(lf);
    println!("DF: {}", df);
}

pub fn lf_destruct(lf: &LazyFrame) -> DataFrame {
    let df = lf.clone().collect().unwrap();
    let colnames: Vec<_> = df
        .get_column_names()
        .iter()
        .map(|x| x.to_string())
        .collect();
    let mut series_vec = vec![];
    for c in colnames {
        let ser = df.column(&c).unwrap();
        if let DataType::Categorical(_) = ser.dtype() {
            series_vec.push(ser.cast(&DataType::Utf8).unwrap());
        } else if let DataType::Struct(fields) = ser.dtype() {
            if fields.len() == 3 {
                let mut tmp_lf = DataFrame::new(vec![ser.clone()]).unwrap().lazy();
                let value_name = format!("{}_{}", c, MULTI_VALUE_COL);
                let lang_name = format!("{}_{}", c, MULTI_LANG_COL);
                let dt_name = format!("{}_{}", c, MULTI_DT_COL);
                tmp_lf = tmp_lf.with_columns([
                    col(&c)
                        .struct_()
                        .field_by_name(MULTI_VALUE_COL)
                        .cast(DataType::Utf8)
                        .alias(&value_name),
                    col(&c)
                        .struct_()
                        .field_by_name(MULTI_LANG_COL)
                        .cast(DataType::Utf8)
                        .alias(&lang_name),
                    col(&c)
                        .struct_()
                        .field_by_name(MULTI_DT_COL)
                        .cast(DataType::Utf8)
                        .alias(&dt_name),
                ]);
                let mut tmp_df = tmp_lf.collect().unwrap();
                series_vec.push(tmp_df.drop_in_place(&value_name).unwrap());
                series_vec.push(tmp_df.drop_in_place(&lang_name).unwrap());
                series_vec.push(tmp_df.drop_in_place(&dt_name).unwrap());
            } else {
                series_vec.push(ser.clone());
            }
        } else {
            series_vec.push(ser.clone());
        }
    }
    DataFrame::new(series_vec).unwrap()
}

pub fn join_workaround(
    mut left_mappings: LazyFrame,
    left_datatypes: &HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    right_datatypes: &HashMap<String, RDFNodeType>,
    how: JoinArgs,
) -> LazyFrame {
    let value_suffix = uuid::Uuid::new_v4().to_string();
    let lang_suffix = uuid::Uuid::new_v4().to_string();
    let dt_suffix = uuid::Uuid::new_v4().to_string();
    let mut multi_cols = vec![];
    let mut join_cols = vec![];
    for (k, dt) in left_datatypes {
        if right_datatypes.contains_key(k) {
            if dt == &RDFNodeType::MultiType {
                let col_value = format!("{}_{}", k, value_suffix);
                let col_lang = format!("{}_{}", k, lang_suffix);
                let col_dt = format!("{}_{}", k, dt_suffix);

                left_mappings = split_multi_col(left_mappings, k, &col_value, &col_lang, &col_dt);
                right_mappings = split_multi_col(right_mappings, k, &col_value, &col_lang, &col_dt);

                join_cols.push(col_value.clone());
                join_cols.push(col_lang.clone());
                join_cols.push(col_dt.clone());

                multi_cols.push((k, col_value, col_lang, col_dt));
            } else {
                join_cols.push(k.to_string());
            }
        }
    }
    let join_col_expr: Vec<_> = join_cols.into_iter().map(|x| col(&x)).collect();
    let mut joined = left_mappings.join(right_mappings, &join_col_expr, &join_col_expr, how);
    for (k, v, lang, dt) in multi_cols {
        joined = combine_multi_col(joined, k, &v, &lang, &dt);
    }
    joined
}

fn split_multi_col(
    lf: LazyFrame,
    k: &str,
    col_value: &str,
    col_lang: &str,
    col_dt: &str,
) -> LazyFrame {
    lf.with_columns([
        col(k)
            .struct_()
            .field_by_name(MULTI_VALUE_COL)
            .alias(col_value),
        col(k)
            .struct_()
            .field_by_name(MULTI_LANG_COL)
            .alias(col_lang),
        col(k).struct_().field_by_name(MULTI_DT_COL).alias(col_dt),
    ])
    .drop_columns(vec![k])
}

fn combine_multi_col(
    lf: LazyFrame,
    k: &str,
    col_value: &str,
    col_lang: &str,
    col_dt: &str,
) -> LazyFrame {
    lf.with_column(
        as_struct(vec![
            col(col_value).alias(MULTI_VALUE_COL),
            col(col_dt).alias(MULTI_DT_COL),
            col(col_lang).alias(MULTI_LANG_COL),
        ])
        .alias(k),
    )
    .drop_columns(vec![col_value, col_lang, col_dt])
}
