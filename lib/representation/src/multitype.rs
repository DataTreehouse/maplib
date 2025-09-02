use crate::{BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::rdf;
use polars::prelude::{
    all_horizontal, as_struct, by_name, col, lit, when, Column, DataFrame, Expr, IntoColumn,
    IntoLazy, LazyFrame, LiteralValue,
};

use std::collections::HashMap;

pub const MULTI_IRI_DT: &str = "I";
pub const MULTI_BLANK_DT: &str = "B";
pub const MULTI_NONE_DT: &str = "N";

pub fn convert_lf_col_to_multitype(expr: Expr, dt: &RDFNodeState) -> Expr {
    if !dt.is_multi() {
        let base_type = dt.get_base_type().unwrap();
        match base_type {
            BaseRDFNodeType::IRI => as_struct(vec![expr.alias(MULTI_IRI_DT)]),
            BaseRDFNodeType::BlankNode => as_struct(vec![expr.alias(MULTI_BLANK_DT)]),
            BaseRDFNodeType::Literal(l) => {
                if rdf::LANG_STRING == l.as_ref() {
                    expr
                } else {
                    let colname = base_type.field_col_name();
                    as_struct(vec![expr.alias(&colname)])
                }
            }
            BaseRDFNodeType::None => as_struct(vec![expr.alias(MULTI_NONE_DT)]),
        }
    } else {
        expr
    }
}

/// Takes a Column containing a multitype and extracts a column corresponding to the subtype specified
pub fn extract_column_from_multitype(
    multitype_column: &Column,
    subtype: &BaseRDFNodeType,
) -> Column {
    let colname = subtype.field_col_name();
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

            let columns_drop: Vec<_> = names
                .iter()
                .filter(|&n| *n != col_name)
                .map(|x| *x) // Make sure not to drop col_name even if it overlaps with one of the provided field names
                .collect();
            lf = lf.drop(by_name(columns_drop, true));
            let df = lf.collect();

            df.unwrap().drop_in_place(col_name).unwrap()
        }
    }
}

pub fn compress_actual_multitypes(
    mut df: DataFrame,
    rdf_node_types: HashMap<String, RDFNodeState>,
) -> (DataFrame, HashMap<String, RDFNodeState>) {
    let mut updated_types = HashMap::new();
    let mut col_exprs = vec![];
    let mut to_single = vec![];
    for (c, mut t) in rdf_node_types {
        if t.is_multi() {
            let mut keep_types = vec![];
            let mut any_dropped = false;
            let sorted_types: Vec<_> = t.get_sorted_types().into_iter().cloned().collect();
            for base_t in sorted_types.into_iter() {
                let any_values = df
                    .column(&c)
                    .unwrap()
                    .struct_()
                    .unwrap()
                    .field_by_name(&base_t.field_col_name())
                    .unwrap()
                    .is_not_null()
                    .any();
                if !any_values {
                    any_dropped = true;
                } else {
                    keep_types.push(t.map.remove_entry(&base_t).unwrap());
                }
            }
            if any_dropped {
                if keep_types.is_empty() {
                    col_exprs.push(
                        lit(LiteralValue::untyped_null())
                            .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                            .alias(&c),
                    );
                    updated_types.insert(
                        c.clone(),
                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                    );
                } else if keep_types.len() == 1 {
                    let t = keep_types.pop().unwrap();
                    to_single.push((c, t));
                } else if keep_types.len() == 2 && keep_types.iter().any(|(x, _)| x.is_none()) {
                    let t = keep_types
                        .iter()
                        .find(|(x, _)| !x.is_none())
                        .unwrap()
                        .clone();
                    to_single.push((c, t));
                } else {
                    let all_cols_exprs: Vec<_> =
                        all_multi_cols(keep_types.iter().map(|(x, _)| x).collect())
                            .into_iter()
                            .map(|x| col(&c).struct_().field_by_name(&x))
                            .collect();
                    col_exprs.push(as_struct(all_cols_exprs).alias(&c));
                    updated_types
                        .insert(c, RDFNodeState::from_map(keep_types.into_iter().collect()));
                }
            } else {
                updated_types.insert(c, RDFNodeState::from_map(keep_types.into_iter().collect()));
            }
        } else {
            updated_types.insert(c, t);
        }
    }
    let mut lf = df.lazy();
    for (c, (t, s)) in to_single {
        lf = known_convert_lf_multicol_to_single(lf, &c, &t);
        updated_types.insert(c, RDFNodeState::from_bases(t, s));
    }

    if !col_exprs.is_empty() {
        lf = lf.with_columns(col_exprs);
    }
    df = lf.collect().unwrap();
    (df, updated_types)
}

pub fn split_df_multicols(
    df: DataFrame,
    types: &HashMap<String, RDFNodeState>,
) -> Vec<(DataFrame, HashMap<String, RDFNodeState>)> {
    let mut dfs_dtypes = vec![(df, types.clone())];
    for (c, t) in types {
        dfs_dtypes = if t.is_multi() {
            let mut new_dfs_dtypes = vec![];
            for (df, types) in dfs_dtypes {
                for (t, s) in &t.map {
                    let mut new_types = types.clone();
                    let lf = force_convert_multicol_to_single_col(df.clone().lazy(), c, t);
                    new_types.insert(c.clone(), RDFNodeState::from_bases(t.clone(), s.clone()));
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

pub fn all_multi_cols(dts: Vec<&BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = d.field_col_name();
        if d.is_lang_string() {
            all_cols.push(LANG_STRING_LANG_FIELD.to_string());
        }
        all_cols.push(colname);
    }
    all_cols
}

pub fn all_multi_main_cols(dts: Vec<&BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = d.field_col_name();
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
        lf = lf.with_column(
            col(c)
                .struct_()
                .field_by_name(&dt.field_col_name())
                .alias(c),
        )
    }
    lf
}

#[allow(clippy::type_complexity)]
pub fn unnest_multicols(
    mut mappings: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
) -> (LazyFrame, HashMap<String, (Vec<String>, Vec<String>)>) {
    let mut exprs = vec![];
    let mut out_map = HashMap::new();
    let mut drop_cols = vec![];
    for (c, t) in rdf_node_types {
        if t.is_multi() {
            let inner_cols = all_multi_cols(t.get_sorted_types());
            let mut prefixed_inner_cols = vec![];
            for inner in &inner_cols {
                let prefixed_inner = format!("{c}{inner}");
                exprs.push(col(c).struct_().field_by_name(inner).alias(&prefixed_inner));
                prefixed_inner_cols.push(prefixed_inner);
            }
            out_map.insert(c.clone(), (inner_cols, prefixed_inner_cols));
            drop_cols.push(c);
        } else {
            //No action
        }
    }
    mappings = mappings.with_columns(exprs);
    for c in drop_cols {
        mappings = mappings.drop(by_name([c], true));
    }
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
    let mapping = mapping.with_columns(structs).drop(by_name(drop_cols, true));
    mapping
}

pub fn set_struct_all_null_to_null_row(expr: Expr, t: &RDFNodeState) -> Expr {
    let mut is_null_exprs = vec![];
    if t.is_multi() {
        for t in t.get_sorted_types() {
            is_null_exprs.push(
                expr.clone()
                    .struct_()
                    .field_by_name(&t.field_col_name())
                    .is_null(),
            );
        }
    } else if t.is_lang_string() {
        is_null_exprs.push(
            expr.clone()
                .struct_()
                .field_by_name(LANG_STRING_VALUE_FIELD)
                .is_null(),
        );
    }
    if !is_null_exprs.is_empty() {
        when(all_horizontal(is_null_exprs).unwrap())
            .then(lit(LiteralValue::untyped_null()).cast(t.polars_data_type()))
            .otherwise(expr)
    } else {
        expr
    }
}
