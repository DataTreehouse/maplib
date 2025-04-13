use crate::solution_mapping::SolutionMappings;
use crate::{BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::{rdf, xsd};
use polars::datatypes::PlSmallStr;
use polars::prelude::{
    all_horizontal, as_struct, col, lit, when, CategoricalOrdering, Column, DataFrame, DataType,
    Expr, IntoColumn, IntoLazy, JoinArgs, JoinType, LazyFrame, LazyGroupBy, LiteralValue,
    MaintainOrderJoin, Selector, UniqueKeepStrategy,
};

use std::collections::{HashMap, HashSet};

pub const MULTI_IRI_DT: &str = "I";
pub const MULTI_BLANK_DT: &str = "B";
pub const MULTI_NONE_DT: &str = "N";

pub fn convert_lf_col_to_multitype(c: &str, dt: &RDFNodeType) -> Expr {
    match dt {
        RDFNodeType::IRI => as_struct(vec![col(c).alias(MULTI_IRI_DT)]).alias(c),
        RDFNodeType::BlankNode => as_struct(vec![col(c).alias(MULTI_BLANK_DT)]).alias(c),
        RDFNodeType::Literal(l) => {
            if rdf::LANG_STRING == l.as_ref() {
                col(c)
            } else {
                let colname = base_col_name(&BaseRDFNodeType::from_rdf_node_type(dt));
                as_struct(vec![col(c).alias(&colname)]).alias(c)
            }
        }
        RDFNodeType::None => as_struct(vec![col(c).alias(MULTI_NONE_DT)]).alias(c),
        RDFNodeType::MultiType(..) => col(c),
    }
}

/// Takes a Column containing a multitype and extracts a column corresponding to the subtype specified
pub fn extract_column_from_multitype(
    multitype_column: &Column,
    subtype: &BaseRDFNodeType,
) -> Column {
    let colname = base_col_name(subtype);
    match subtype {
        BaseRDFNodeType::Literal(_) if subtype.is_lang_string() => filter_multitype(
            multitype_column,
            &vec![LANG_STRING_VALUE_FIELD, LANG_STRING_LANG_FIELD],
            LANG_STRING_VALUE_FIELD,
        ),
        _ => filter_multitype(multitype_column, &vec![&colname], &colname),
    }
}

/// Takes a column containing a multitype and creates a new column with only the fields specified in the names argument
fn filter_multitype(multitype_column: &Column, names: &Vec<&str>, col_name: &str) -> Column {
    let mt_struct = multitype_column.struct_().unwrap();

    match names.len() {
        0 => panic!("There are probably better ways of constructing an empty column"),
        1 => {
            let mut new_column = mt_struct.field_by_name(names[0]).unwrap().into_column();
            new_column.rename(col_name.into());
            new_column
        }
        _ => {
            let new_columns: Vec<Column> = names
                .iter()
                .map(|n| mt_struct.field_by_name(n).unwrap().clone().into_column())
                .collect();
            let mut lf = DataFrame::new(new_columns).unwrap().lazy();

            let struct_exprs: Vec<Expr> = names.iter().map(|&n| col(n)).collect();
            lf = lf.with_column(as_struct(struct_exprs).alias(col_name));

            let columns_drop_iter = names
                .iter()
                .filter(|&n| *n != col_name) // Make sure not to drop col_name even if it overlaps with one of the provided field names
                .map(|&n| Selector::new(col(n)));
            lf = lf.drop(columns_drop_iter);
            let df = lf.collect();

            df.unwrap().drop_in_place(col_name).unwrap()
        }
    }
}

pub fn base_col_name(dt: &BaseRDFNodeType) -> String {
    match dt {
        BaseRDFNodeType::IRI => MULTI_IRI_DT.to_string(),
        BaseRDFNodeType::BlankNode => MULTI_BLANK_DT.to_string(),
        BaseRDFNodeType::Literal(l) => l.to_string(),
        BaseRDFNodeType::None => MULTI_NONE_DT.to_string(),
    }
}

pub fn lf_column_from_categorical(
    mut lf: LazyFrame,
    c: &str,
    rdf_node_types: &HashMap<String, RDFNodeType>,
) -> LazyFrame {
    match rdf_node_types.get(c).unwrap() {
        RDFNodeType::IRI | RDFNodeType::BlankNode => {
            lf = lf.with_column(col(c).cast(DataType::String))
        }
        RDFNodeType::Literal(l) => {
            if l.as_ref() == xsd::STRING {
                lf = lf.with_column(col(c).cast(DataType::String))
            } else if l.as_ref() == rdf::LANG_STRING {
                lf = lf.with_column(
                    as_struct(vec![
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .cast(DataType::String),
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .cast(DataType::String),
                    ])
                    .alias(c),
                )
            }
        }
        RDFNodeType::None => {}
        RDFNodeType::MultiType(types) => {
            let mut fields = vec![];
            for t in types {
                match t {
                    BaseRDFNodeType::IRI => fields.push(
                        col(c)
                            .struct_()
                            .field_by_name(MULTI_IRI_DT)
                            .cast(DataType::String),
                    ),
                    BaseRDFNodeType::BlankNode => fields.push(
                        col(c)
                            .struct_()
                            .field_by_name(MULTI_BLANK_DT)
                            .cast(DataType::String),
                    ),
                    BaseRDFNodeType::Literal(l) => {
                        if l.as_ref() == xsd::STRING {
                            fields.push(
                                col(c)
                                    .struct_()
                                    .field_by_name(&base_col_name(t))
                                    .cast(DataType::String),
                            );
                        } else if l.as_ref() == rdf::LANG_STRING {
                            fields.push(
                                col(c)
                                    .struct_()
                                    .field_by_name(LANG_STRING_VALUE_FIELD)
                                    .cast(DataType::String),
                            );
                            fields.push(
                                col(c)
                                    .struct_()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .cast(DataType::String),
                            );
                        } else {
                            fields.push(col(c).struct_().field_by_name(&base_col_name(t)));
                        }
                    }
                    BaseRDFNodeType::None => {
                        fields.push(col(c).struct_().field_by_name(MULTI_NONE_DT));
                    }
                }
            }
            lf = lf.with_column(as_struct(fields).alias(c));
        }
    }
    lf
}

pub fn lf_columns_to_categorical(
    mut lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    cat_order: CategoricalOrdering,
) -> LazyFrame {
    for c in rdf_node_types.keys() {
        lf = lf_column_to_categorical(lf, c, rdf_node_types.get(c).unwrap(), cat_order);
    }
    lf
}

pub fn lf_column_to_categorical(
    mut lf: LazyFrame,
    c: &str,
    rdf_node_type: &RDFNodeType,
    cat_order: CategoricalOrdering,
) -> LazyFrame {
    match rdf_node_type {
        RDFNodeType::IRI | RDFNodeType::BlankNode => {
            lf = lf.with_column(col(c).cast(DataType::Categorical(None, cat_order)))
        }
        RDFNodeType::Literal(l) => {
            if l.as_ref() == xsd::STRING {
                lf = lf.with_column(col(c).cast(DataType::Categorical(None, cat_order)))
            } else if l.as_ref() == rdf::LANG_STRING {
                lf = lf.with_column(
                    as_struct(vec![
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .cast(DataType::Categorical(None, cat_order)),
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .cast(DataType::Categorical(None, cat_order)),
                    ])
                    .alias(c),
                )
            }
        }
        RDFNodeType::None => {}
        RDFNodeType::MultiType(types) => {
            let mut found_cat_expr = false;
            let mut fields = vec![];
            for t in types {
                match t {
                    BaseRDFNodeType::IRI => {
                        found_cat_expr = true;
                        fields.push(
                            col(c)
                                .struct_()
                                .field_by_name(MULTI_IRI_DT)
                                .cast(DataType::Categorical(None, cat_order)),
                        )
                    }
                    BaseRDFNodeType::BlankNode => {
                        found_cat_expr = true;
                        fields.push(
                            col(c)
                                .struct_()
                                .field_by_name(MULTI_BLANK_DT)
                                .cast(DataType::Categorical(None, cat_order)),
                        )
                    }
                    BaseRDFNodeType::Literal(l) => {
                        if l.as_ref() == xsd::STRING {
                            found_cat_expr = true;
                            fields.push(
                                col(c).struct_().field_by_name(&base_col_name(t)).cast(
                                    DataType::Categorical(None, CategoricalOrdering::Lexical),
                                ),
                            );
                        } else if l.as_ref() == rdf::LANG_STRING {
                            found_cat_expr = true;
                            fields.push(
                                col(c)
                                    .struct_()
                                    .field_by_name(LANG_STRING_VALUE_FIELD)
                                    .cast(DataType::Categorical(
                                        None,
                                        CategoricalOrdering::Lexical,
                                    )),
                            );
                            fields.push(
                                col(c)
                                    .struct_()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .cast(DataType::Categorical(None, cat_order)),
                            );
                        } else {
                            fields.push(col(c).struct_().field_by_name(&base_col_name(t)));
                        }
                    }
                    BaseRDFNodeType::None => {
                        fields.push(col(c).struct_().field_by_name(MULTI_NONE_DT));
                    }
                }
            }
            if found_cat_expr {
                lf = lf.with_column(as_struct(fields).alias(c));
            }
        }
    }
    lf
}

pub fn create_join_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
    join_type: JoinType,
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
                if let RDFNodeType::MultiType(left_types) = left_dt {
                    if let RDFNodeType::MultiType(right_types) = right_dt {
                        let right_set: HashSet<BaseRDFNodeType> =
                            HashSet::from_iter(right_types.clone().into_iter());
                        let left_set: HashSet<BaseRDFNodeType> =
                            HashSet::from_iter(left_types.clone().into_iter());
                        match join_type {
                            JoinType::Inner => {
                                let mut keep: Vec<_> =
                                    left_set.intersection(&right_set).cloned().collect();
                                keep.sort();
                                if keep.is_empty() {
                                    left_mappings = left_mappings
                                        .filter(lit(false))
                                        .with_column(lit(true).alias(v));
                                    right_mappings = right_mappings
                                        .filter(lit(false))
                                        .with_column(lit(true).alias(v));
                                    new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                    new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                                } else if keep.len() == 1 {
                                    let t = keep.first().unwrap();
                                    left_mappings =
                                        force_convert_multicol_to_single_col(left_mappings, v, t);
                                    right_mappings =
                                        force_convert_multicol_to_single_col(right_mappings, v, t);
                                    new_left_datatypes.insert(v.clone(), t.as_rdf_node_type());
                                    new_right_datatypes.insert(v.clone(), t.as_rdf_node_type());
                                } else {
                                    let all_main_cols = all_multi_main_cols(&keep);
                                    let mut is_col_expr: Option<Expr> = None;
                                    for c in all_main_cols {
                                        let e = col(v).struct_().field_by_name(&c).is_not_null();
                                        is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                            Some(is_col_expr.or(e))
                                        } else {
                                            Some(e)
                                        };
                                    }
                                    let all_cols = all_multi_cols(&keep);
                                    let mut struct_cols = vec![];
                                    for c in &all_cols {
                                        struct_cols
                                            .push(col(v).struct_().field_by_name(c).alias(c));
                                    }

                                    left_mappings = left_mappings
                                        .filter(is_col_expr.as_ref().unwrap().clone())
                                        .with_column(as_struct(struct_cols.clone()).alias(v));

                                    right_mappings = right_mappings
                                        .filter(is_col_expr.as_ref().unwrap().clone())
                                        .with_column(as_struct(struct_cols.clone()).alias(v));
                                    new_left_datatypes.insert(
                                        v.to_string(),
                                        RDFNodeType::MultiType(keep.clone()),
                                    );
                                    new_right_datatypes.insert(
                                        v.to_string(),
                                        RDFNodeType::MultiType(keep.clone()),
                                    );
                                }
                            }
                            JoinType::Left => {
                                let mut right_keep: Vec<_> =
                                    left_set.intersection(&right_set).cloned().collect();
                                right_keep.sort();
                                if right_keep.is_empty() {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_right_datatypes.insert(v.to_string(), RDFNodeType::None);
                                } else if right_keep.len() == 1 {
                                    let t = right_keep.first().unwrap();
                                    right_mappings =
                                        force_convert_multicol_to_single_col(right_mappings, v, t);
                                    new_right_datatypes.insert(v.to_string(), t.as_rdf_node_type());
                                } else {
                                    let all_main_cols = all_multi_main_cols(&right_keep);
                                    let mut is_col_expr: Option<Expr> = None;
                                    for c in all_main_cols {
                                        let e = col(v).struct_().field_by_name(&c).is_not_null();
                                        is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                            Some(is_col_expr.or(e))
                                        } else {
                                            Some(e)
                                        };
                                    }
                                    let all_cols = all_multi_cols(&right_keep);
                                    let mut struct_cols = vec![];
                                    for c in &all_cols {
                                        struct_cols.push(col(v).struct_().field_by_name(c));
                                    }

                                    right_mappings = right_mappings
                                        .filter(is_col_expr.unwrap().clone())
                                        .with_column(as_struct(struct_cols.clone()).alias(v));
                                    new_right_datatypes.insert(
                                        v.to_string(),
                                        RDFNodeType::MultiType(right_keep.clone()),
                                    );
                                }
                            }
                            _ => {
                                todo!()
                            }
                        }
                    } else {
                        //right not multi
                        let base_right = BaseRDFNodeType::from_rdf_node_type(right_dt);
                        match join_type {
                            JoinType::Inner => {
                                if left_types.contains(&base_right) {
                                    left_mappings = force_convert_multicol_to_single_col(
                                        left_mappings,
                                        v,
                                        &base_right,
                                    );
                                    new_left_datatypes.insert(v.clone(), right_dt.clone());
                                } else {
                                    left_mappings = left_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                                }
                            }
                            JoinType::Left => {
                                if left_types.contains(&base_right) {
                                    right_mappings = right_mappings
                                        .with_column(convert_lf_col_to_multitype(v, right_dt));
                                    new_right_datatypes.insert(
                                        v.clone(),
                                        RDFNodeType::MultiType(vec![base_right]),
                                    );
                                } else {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                                }
                            }
                            _ => {
                                todo!()
                            }
                        }
                    }
                } else {
                    //left not multi
                    let left_basic_dt = BaseRDFNodeType::from_rdf_node_type(left_dt);
                    if let RDFNodeType::MultiType(right_types) = right_dt {
                        match join_type {
                            JoinType::Inner => {
                                if right_types.contains(&left_basic_dt) {
                                    right_mappings = force_convert_multicol_to_single_col(
                                        right_mappings,
                                        v,
                                        &left_basic_dt,
                                    );
                                    new_right_datatypes.insert(v.clone(), left_dt.clone());
                                } else {
                                    left_mappings = left_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                                }
                            }
                            JoinType::Left => {
                                if right_types.contains(&left_basic_dt) {
                                    right_mappings = force_convert_multicol_to_single_col(
                                        right_mappings,
                                        v,
                                        &left_basic_dt,
                                    );
                                    new_right_datatypes.insert(v.clone(), left_dt.clone());
                                } else {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(BaseRDFNodeType::None.polars_data_type())
                                            .alias(v),
                                    );
                                    new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                                }
                            }
                            _ => {
                                todo!()
                            }
                        }
                    } else {
                        //Right not multi
                        match join_type {
                            JoinType::Inner => {
                                left_mappings = left_mappings.filter(lit(false)).with_column(
                                    lit(LiteralValue::untyped_null())
                                        .cast(BaseRDFNodeType::None.polars_data_type())
                                        .alias(v),
                                );
                                new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                right_mappings = right_mappings.filter(lit(false)).with_column(
                                    lit(LiteralValue::untyped_null())
                                        .cast(BaseRDFNodeType::None.polars_data_type())
                                        .alias(v),
                                );
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            }
                            JoinType::Left => {
                                right_mappings = right_mappings.filter(lit(false)).with_column(
                                    lit(LiteralValue::untyped_null())
                                        .cast(BaseRDFNodeType::None.polars_data_type())
                                        .alias(v),
                                );
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            }
                            _ => {
                                todo!()
                            }
                        }
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

pub fn compress_actual_multitypes(
    mut df: DataFrame,
    rdf_node_types: HashMap<String, RDFNodeType>,
) -> (DataFrame, HashMap<String, RDFNodeType>) {
    let mut updated_types = HashMap::new();
    let mut col_exprs = vec![];
    let mut to_single = vec![];
    for (c, t) in rdf_node_types {
        if let RDFNodeType::MultiType(types) = t {
            let mut keep_types = vec![];
            let mut any_dropped = false;

            for t in types.into_iter() {
                let any_values = df
                    .column(&c)
                    .unwrap()
                    .struct_()
                    .unwrap()
                    .field_by_name(&base_col_name(&t))
                    .unwrap()
                    .is_not_null()
                    .any();
                if !any_values {
                    any_dropped = true;
                } else {
                    keep_types.push(t);
                }
            }
            if any_dropped {
                if keep_types.is_empty() {
                    col_exprs.push(
                        lit(LiteralValue::untyped_null())
                            .cast(BaseRDFNodeType::None.polars_data_type())
                            .alias(&c),
                    );
                    updated_types.insert(c, RDFNodeType::None);
                } else if keep_types.len() == 1 {
                    let t = keep_types.pop().unwrap();
                    to_single.push((c, t));
                } else if keep_types.len() == 2 && keep_types.contains(&BaseRDFNodeType::None) {
                    let t = keep_types
                        .iter()
                        .find(|x| *x != &BaseRDFNodeType::None)
                        .unwrap()
                        .clone();
                    to_single.push((c, t));
                } else {
                    let all_cols_exprs: Vec<_> = all_multi_cols(&keep_types)
                        .into_iter()
                        .map(|x| col(&c).struct_().field_by_name(&x))
                        .collect();
                    col_exprs.push(as_struct(all_cols_exprs).alias(&c));
                    updated_types.insert(c, RDFNodeType::MultiType(keep_types));
                }
            } else {
                updated_types.insert(c, RDFNodeType::MultiType(keep_types));
            }
        } else {
            updated_types.insert(c, t);
        }
    }
    let mut lf = df.lazy();
    for (c, t) in to_single {
        lf = known_convert_lf_multicol_to_single(lf, &c, &t);
        updated_types.insert(c, t.as_rdf_node_type());
    }

    if !col_exprs.is_empty() {
        lf = lf.with_columns(col_exprs);
    }
    df = lf.collect().unwrap();
    (df, updated_types)
}

pub fn split_df_multicols(
    df: DataFrame,
    types: &HashMap<String, RDFNodeType>,
) -> Vec<(DataFrame, HashMap<String, RDFNodeType>)> {
    let mut dfs_dtypes = vec![(df, types.clone())];
    for (c, t) in types {
        dfs_dtypes = if let RDFNodeType::MultiType(multitypes) = t {
            let mut new_dfs_dtypes = vec![];
            for (df, types) in dfs_dtypes {
                for t in multitypes {
                    let mut new_types = types.clone();
                    let lf = force_convert_multicol_to_single_col(df.clone().lazy(), c, t);
                    new_types.insert(c.clone(), t.as_rdf_node_type());
                    new_dfs_dtypes.push((lf.collect().unwrap(), new_types));
                }
            }
            new_dfs_dtypes
        } else {
            dfs_dtypes
        }
    }
    dfs_dtypes
}

pub fn all_multi_cols(dts: &Vec<BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = base_col_name(d);
        if d.is_lang_string() {
            all_cols.push(LANG_STRING_LANG_FIELD.to_string());
        }
        all_cols.push(colname);
    }
    all_cols
}

pub fn all_multi_main_cols(dts: &Vec<BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = base_col_name(d);
        all_cols.push(colname);
    }
    all_cols
}

pub fn force_convert_multicol_to_single_col(
    lf: LazyFrame,
    c: &str,
    dt: &BaseRDFNodeType,
) -> LazyFrame {
    known_convert_lf_multicol_to_single(lf, c, dt)
}

pub fn known_convert_lf_multicol_to_single(
    mut lf: LazyFrame,
    c: &str,
    dt: &BaseRDFNodeType,
) -> LazyFrame {
    if dt.is_lang_string() {
        lf = lf.with_column(
            as_struct(vec![
                col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD),
                col(c).struct_().field_by_name(LANG_STRING_LANG_FIELD),
            ])
            .alias(c),
        );
    } else {
        lf = lf.with_column(col(c).struct_().field_by_name(&base_col_name(dt)).alias(c))
    }
    lf
}

#[allow(clippy::type_complexity)]
pub fn unnest_multicols(
    mut mappings: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
) -> (LazyFrame, HashMap<String, (Vec<String>, Vec<String>)>) {
    let mut exprs = vec![];
    let mut out_map = HashMap::new();
    let mut drop_cols = vec![];
    for (c, t) in rdf_node_types {
        if let RDFNodeType::MultiType(types) = t {
            let inner_cols = all_multi_cols(types);
            let mut prefixed_inner_cols = vec![];
            for inner in &inner_cols {
                let prefixed_inner = format!("{c}{inner}");
                exprs.push(col(c).struct_().field_by_name(inner).alias(&prefixed_inner));
                prefixed_inner_cols.push(prefixed_inner);
            }
            out_map.insert(c.clone(), (inner_cols, prefixed_inner_cols));
            drop_cols.push(col(c));
        } else {
            //No action
        }
    }
    mappings = mappings.with_columns(exprs);
    mappings = mappings.drop_no_validate(drop_cols);
    (mappings, out_map)
}

pub fn nest_multicolumns(
    mapping: LazyFrame,
    map: HashMap<String, (Vec<String>, Vec<String>)>,
) -> LazyFrame {
    let mut structs = vec![];
    let mut drop_cols = vec![];
    for (c, (inner_cols, prefixed_inner_cols)) in map {
        let mut struct_exprs = vec![];
        for (inner_col, prefixed_inner_col) in inner_cols.iter().zip(prefixed_inner_cols.iter()) {
            struct_exprs.push(col(prefixed_inner_col).alias(inner_col));
        }
        drop_cols.extend(prefixed_inner_cols);
        structs.push(as_struct(struct_exprs).alias(&c));
    }
    mapping.with_columns(structs).drop_no_validate(drop_cols)
}

pub fn set_structs_all_null_to_null_row(sm: SolutionMappings) -> SolutionMappings {
    let SolutionMappings {
        mut mappings,
        rdf_node_types,
        height_estimate,
    } = sm;
    for (k, r) in &rdf_node_types {
        let mut is_null_exprs = vec![];
        if let RDFNodeType::MultiType(ts) = r {
            for t in ts {
                is_null_exprs.push(col(k).struct_().field_by_name(&base_col_name(t)).is_null());
            }
        }
        if r.is_lang_string() {
            is_null_exprs.push(
                col(k)
                    .struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .is_null(),
            );
        }
        if !is_null_exprs.is_empty() {
            mappings = mappings.with_column(
                when(all_horizontal(is_null_exprs).unwrap())
                    .then(lit(LiteralValue::untyped_null()).cast(r.polars_data_type()))
                    .otherwise(col(k))
                    .alias(k),
            );
        }
    }
    SolutionMappings::new(mappings, rdf_node_types, height_estimate)
}

pub fn join_workaround(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    left_height: usize,
    mut right_mappings: LazyFrame,
    right_datatypes: HashMap<String, RDFNodeType>,
    right_height: usize,
    join_type: JoinType,
) -> SolutionMappings {
    assert!(matches!(join_type, JoinType::Left | JoinType::Inner));
    for c in left_datatypes.keys() {
        if right_datatypes.contains_key(c) {
            left_mappings = lf_column_to_categorical(
                left_mappings,
                c,
                left_datatypes.get(c).unwrap(),
                CategoricalOrdering::Physical,
            );
            right_mappings = lf_column_to_categorical(
                right_mappings,
                c,
                right_datatypes.get(c).unwrap(),
                CategoricalOrdering::Physical,
            );
        }
    }

    let (mut left_mappings, left_exploded) = unnest_multicols(left_mappings, &left_datatypes);
    let (mut right_mappings, mut right_exploded) =
        unnest_multicols(right_mappings, &right_datatypes);

    let mut on = vec![];
    let mut no_join = false;
    for (c, left_type) in &left_datatypes {
        if let Some(right_type) = right_datatypes.get(c) {
            let intersection: Vec<_> =
                if let Some((_left_inner_columns, left_prefixed_inner_columns)) =
                    left_exploded.get(c)
                {
                    if let Some((_, right_prefixed_inner_columns)) = right_exploded.get(c) {
                        let left_set: HashSet<_> = left_prefixed_inner_columns.iter().collect();
                        let right_set: HashSet<_> = right_prefixed_inner_columns.iter().collect();
                        left_set
                            .intersection(&right_set)
                            .map(|x| col(PlSmallStr::from_str(x)))
                            .collect()
                    } else {
                        vec![]
                    }
                } else if left_type == right_type {
                    vec![col(c)]
                } else {
                    vec![]
                };
            if intersection.is_empty() {
                no_join = true;
                break;
            } else {
                on.extend(intersection);
            }
        }
    }
    let height_upper_bound = if no_join {
        0
    } else if on.is_empty() {
        left_height.saturating_mul(right_height)
    } else {
        left_height.saturating_mul(1 + 4usize.saturating_sub(on.len()))
    };

    if no_join {
        let dummycol = uuid::Uuid::new_v4().to_string();
        left_mappings = left_mappings.with_column(lit(false).alias(&dummycol));
        right_mappings = right_mappings.with_column(lit(true).alias(&dummycol));
        let mut to_drop_right = vec![];
        for k in right_datatypes.keys() {
            if left_datatypes.contains_key(k) {
                if let Some((_, right_prefixed_inner_columns)) = right_exploded.get(k) {
                    to_drop_right.extend(right_prefixed_inner_columns.iter().map(col));
                } else {
                    to_drop_right.push(col(k));
                }
            }
        }
        right_mappings = right_mappings.drop_no_validate(to_drop_right);
        left_mappings = left_mappings.join(
            right_mappings,
            &[col(&dummycol)],
            &[col(&dummycol)],
            join_type.into(),
        );
        left_mappings = left_mappings.drop_no_validate([dummycol]);
    } else if !on.is_empty() {
        let join_args = JoinArgs {
            how: join_type,
            validation: Default::default(),
            suffix: None,
            slice: None,
            nulls_equal: true,
            coalesce: Default::default(),
            maintain_order: MaintainOrderJoin::None,
        };
        left_mappings = left_mappings.join(right_mappings, &on, &on, join_args);
    } else {
        left_mappings = left_mappings.cross_join(right_mappings, None);
    }

    let mut unified_exploded = HashMap::new();
    for (c, (mut left_inner_columns, mut left_prefixed_inner_columns)) in left_exploded {
        if let Some((right_inner_columns, right_prefixed_inner_columns)) = right_exploded.remove(&c)
        {
            for (r, pr) in right_inner_columns
                .into_iter()
                .zip(right_prefixed_inner_columns.into_iter())
            {
                if !left_inner_columns.contains(&r) {
                    left_inner_columns.push(r);
                    left_prefixed_inner_columns.push(pr);
                }
            }
        }
        unified_exploded.insert(c, (left_inner_columns, left_prefixed_inner_columns));
    }
    unified_exploded.extend(right_exploded);

    left_mappings = nest_multicolumns(left_mappings, unified_exploded);

    for (c, dt) in right_datatypes {
        left_datatypes.entry(c).or_insert(dt);
    }

    SolutionMappings::new(left_mappings, left_datatypes, height_upper_bound)
}

pub fn unique_workaround(
    lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    subset: Option<Vec<String>>,
    stable: bool,
    unique_keep_strategy: UniqueKeepStrategy,
) -> LazyFrame {
    let (mut lf, maps) = unnest_multicols(lf, rdf_node_types);
    let unique_set = if let Some(subset) = subset {
        let mut u = vec![];
        for s in subset {
            if let Some((_, cols)) = maps.get(&s) {
                u.extend(cols.clone());
            } else {
                u.push(s);
            }
        }
        Some(u)
    } else {
        None
    };
    if stable {
        lf = lf.unique_stable(
            unique_set.map(|x| x.iter().map(|x| PlSmallStr::from_str(x)).collect()),
            unique_keep_strategy,
        );
    } else {
        lf = lf.unique(unique_set, unique_keep_strategy);
    }
    lf = nest_multicolumns(lf, maps);
    lf
}

#[allow(clippy::type_complexity)]
pub fn group_by_workaround(
    lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeType>,
    by: Vec<String>,
) -> (LazyGroupBy, HashMap<String, (Vec<String>, Vec<String>)>) {
    let mut to_explode = HashMap::new();
    for c in &by {
        let t = rdf_node_types.get(c).expect(c);
        if let RDFNodeType::MultiType(..) = t {
            to_explode.insert(c.clone(), t.clone());
        }
    }
    let (lf, maps) = unnest_multicols(lf, &to_explode);
    let mut new_by = vec![];
    for b in by {
        if let Some((_, cols)) = maps.get(&b) {
            new_by.extend(cols.iter().map(col));
        } else {
            new_by.push(col(&b));
        }
    }
    (lf.group_by(new_by), maps)
}
