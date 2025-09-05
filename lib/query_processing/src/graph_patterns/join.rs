use crate::errors::QueryProcessingError;
use crate::graph_patterns::cats::create_compatible_cats;
use polars::datatypes::PlSmallStr;
use polars::prelude::{
    as_struct, by_name, col, lit, Expr, JoinArgs, JoinType, LazyFrame, LiteralValue,
    MaintainOrderJoin,
};
use representation::cats::Cats;
use representation::multitype::{
    all_multi_cols, all_multi_main_cols, convert_lf_col_to_multitype,
    force_convert_multicol_to_single_col, nest_multicolumns, unnest_multicols,
};
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub fn join(
    left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
    join_type: JoinType,
    global_cats: Arc<Cats>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: left_mappings,
        rdf_node_types: left_datatypes,
        height_estimate: left_height,
    } = left_solution_mappings;

    let SolutionMappings {
        mappings: right_mappings,
        rdf_node_types: right_datatypes,
        height_estimate: right_height,
    } = right_solution_mappings;

    let (left_mappings, right_mappings, target_cat_state) = create_join_compatible_cats(
        left_mappings,
        &left_datatypes,
        right_mappings,
        &right_datatypes,
        global_cats,
    );
    let (left_mappings, left_datatypes, right_mappings, right_datatypes) =
        create_join_compatible_solution_mappings(
            left_mappings,
            left_datatypes,
            right_mappings,
            right_datatypes,
            join_type.clone(),
        );

    let mut solution_mappings = join_workaround(
        left_mappings,
        left_datatypes,
        left_height,
        right_mappings,
        right_datatypes,
        right_height,
        join_type,
    );
    update_state(&mut solution_mappings.rdf_node_types, target_cat_state);

    Ok(solution_mappings)
}

pub fn join_workaround(
    left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeState>,
    left_height: usize,
    right_mappings: LazyFrame,
    right_datatypes: HashMap<String, RDFNodeState>,
    right_height: usize,
    join_type: JoinType,
) -> SolutionMappings {
    assert!(matches!(join_type, JoinType::Left | JoinType::Inner));

    let (mut left_mappings, mut left_exploded) = unnest_multicols(left_mappings, &left_datatypes);

    //Fixes "unnesting" where only one side is multi.
    for (c, s) in &right_datatypes {
        if s.is_multi() {
            if let Some(sl) = left_datatypes.get(c) {
                if !sl.is_multi() {
                    let lbt = sl.get_base_type().unwrap();
                    let fcn = lbt.field_col_name();
                    let colname = format!("{c}{}", &fcn);
                    left_mappings = left_mappings.rename([c], [&colname], true);
                    left_exploded.insert(c.clone(), (vec![fcn], vec![colname]));
                }
            }
        }
    }

    let (mut right_mappings, mut right_exploded) =
        unnest_multicols(right_mappings, &right_datatypes);

    //Fixes "unnesting" where only one side is multi.
    for (c, s) in &left_datatypes {
        if s.is_multi() {
            if let Some(sr) = right_datatypes.get(c) {
                if !sr.is_multi() && !sr.is_none() {
                    let rbt = sr.get_base_type().unwrap();
                    let fcn = rbt.field_col_name();
                    let colname = format!("{c}{}", &fcn);
                    right_mappings = right_mappings.rename([c], [&colname], true);
                    right_exploded.insert(c.clone(), (vec![fcn], vec![colname]));
                }
            }
        }
    }

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
                } else if left_type.types_equal(right_type) {
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
                    to_drop_right.extend(right_prefixed_inner_columns.iter());
                } else {
                    to_drop_right.push(k);
                }
            }
        }
        right_mappings = right_mappings.drop(by_name(to_drop_right, true));
        left_mappings = left_mappings.join(
            right_mappings,
            &[col(&dummycol)],
            &[col(&dummycol)],
            join_type.into(),
        );
        left_mappings = left_mappings.drop(by_name([dummycol], true));
    } else {
        let dummy = if on.is_empty() {
            let dummy = uuid::Uuid::new_v4().to_string();
            left_mappings = left_mappings.with_column(lit(true).alias(&dummy));
            right_mappings = right_mappings.with_column(lit(true).alias(&dummy));
            on.push(col(&dummy));
            Some(dummy)
        } else {
            None
        };
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
        if let Some(dummy) = dummy {
            left_mappings = left_mappings.drop(by_name([dummy], true));
        }
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

fn create_join_compatible_cats(
    mut left_mappings: LazyFrame,
    left_types: &HashMap<String, RDFNodeState>,
    mut right_mappings: LazyFrame,
    right_types: &HashMap<String, RDFNodeState>,
    global_cats: Arc<Cats>,
) -> (LazyFrame, LazyFrame, HashMap<String, RDFNodeState>) {
    let mut state_map = HashMap::new();
    for (c, left_state) in left_types {
        if let Some(right_state) = right_types.get(c) {
            let mut new_bases = HashMap::new();
            for left_base in left_state.get_sorted_types() {
                if let Some(right_state) = right_state.map.get(left_base) {
                    let (new_state, left_oper, right_oper) =
                        create_compatible_cats(left_state.map.get(left_base).unwrap(), right_state);
                    if let Some(left_oper) = left_oper {
                        left_mappings = left_oper.apply(
                            left_mappings,
                            &c,
                            &left_state,
                            &left_base,
                            global_cats.clone(),
                        );
                    }
                    if let Some(right_oper) = right_oper {
                        let right_t = right_types.get(c).unwrap();
                        right_mappings = right_oper.apply(
                            right_mappings,
                            c,
                            right_t,
                            &left_base,
                            global_cats.clone(),
                        );
                    }
                    new_bases.insert(left_base.clone(), new_state);
                } else {
                    new_bases.insert(
                        left_base.clone(),
                        left_state.map.get(left_base).unwrap().clone(),
                    );
                }
            }
            new_bases.extend(
                right_state
                    .map
                    .iter()
                    .filter(|(t, s)| !left_state.map.contains_key(*t))
                    .map(|(x, y)| (x.clone(), y.clone())),
            );
            state_map.insert(c.clone(), RDFNodeState::from_map(new_bases));
        } else {
            state_map.insert(c.clone(), left_state.clone());
        }
    }
    for (c, s) in right_types {
        if !state_map.contains_key(c) {
            state_map.insert(c.clone(), s.clone());
        }
    }
    (left_mappings, right_mappings, state_map)
}

fn update_state(
    types: &mut HashMap<String, RDFNodeState>,
    target_types: HashMap<String, RDFNodeState>,
) {
    for (k, t) in target_types {
        if let Some(s) = types.get_mut(&k) {
            if !s.is_none() {
                for (key, val) in t.map {
                    if !key.is_none() && s.map.contains_key(&key) {
                        s.map.insert(key, val);
                    }
                }
            }
        }
    }
}

pub fn create_join_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeState>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeState>,
    join_type: JoinType,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeState>,
    LazyFrame,
    HashMap<String, RDFNodeState>,
) {
    let mut new_left_states = HashMap::new();
    let mut new_right_states = HashMap::new();
    for (v, right_dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if !right_dt.types_equal(left_dt) {
                if left_dt.is_multi() {
                    if right_dt.is_multi() {
                        let right_set: HashSet<BaseRDFNodeType> =
                            HashSet::from_iter(right_dt.map.keys().cloned());
                        let left_set: HashSet<BaseRDFNodeType> =
                            HashSet::from_iter(left_dt.map.keys().cloned());
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
                                    new_left_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                } else if keep.len() == 1 {
                                    let t = keep.first().unwrap();
                                    left_mappings =
                                        force_convert_multicol_to_single_col(left_mappings, v, t);
                                    right_mappings =
                                        force_convert_multicol_to_single_col(right_mappings, v, t);
                                    new_left_states.insert(
                                        v.clone(),
                                        RDFNodeState::from_bases(
                                            t.clone(),
                                            left_dt.map.get(t).unwrap().clone(),
                                        ),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        RDFNodeState::from_bases(
                                            t.clone(),
                                            right_dt.map.get(t).unwrap().clone(),
                                        ),
                                    );
                                } else {
                                    let all_main_cols = all_multi_main_cols(keep.iter().collect());
                                    let mut is_col_expr: Option<Expr> = None;
                                    for c in all_main_cols {
                                        let e = col(v).struct_().field_by_name(&c).is_not_null();
                                        is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                            Some(is_col_expr.or(e))
                                        } else {
                                            Some(e)
                                        };
                                    }
                                    let all_cols = all_multi_cols(keep.iter().collect());
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
                                    let left_map: HashMap<_, _> = keep
                                        .iter()
                                        .map(|x| {
                                            let s = left_dt.map.get(x).unwrap().clone();
                                            (x.clone(), s)
                                        })
                                        .collect();
                                    let right_map: HashMap<_, _> = keep
                                        .iter()
                                        .map(|x| {
                                            let s = right_dt.map.get(x).unwrap().clone();
                                            (x.clone(), s)
                                        })
                                        .collect();
                                    new_left_states
                                        .insert(v.to_string(), RDFNodeState::from_map(left_map));
                                    new_right_states
                                        .insert(v.to_string(), RDFNodeState::from_map(right_map));
                                }
                            }
                            JoinType::Left => {
                                let mut right_keep: Vec<_> =
                                    left_set.intersection(&right_set).cloned().collect();
                                right_keep.sort();
                                if right_keep.is_empty() {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_right_states.insert(
                                        v.to_string(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                } else if right_keep.len() == 1 {
                                    let t = right_keep.first().unwrap();
                                    right_mappings =
                                        force_convert_multicol_to_single_col(right_mappings, v, t);
                                    new_right_states.insert(
                                        v.to_string(),
                                        RDFNodeState::from_bases(
                                            t.clone(),
                                            right_dt.map.get(t).unwrap().clone(),
                                        ),
                                    );
                                } else {
                                    let all_main_cols =
                                        all_multi_main_cols(right_keep.iter().collect());
                                    let mut is_col_expr: Option<Expr> = None;
                                    for c in all_main_cols {
                                        let e = col(v).struct_().field_by_name(&c).is_not_null();
                                        is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                            Some(is_col_expr.or(e))
                                        } else {
                                            Some(e)
                                        };
                                    }
                                    let all_cols = all_multi_cols(right_keep.iter().collect());
                                    let mut struct_cols = vec![];
                                    for c in &all_cols {
                                        struct_cols.push(col(v).struct_().field_by_name(c));
                                    }
                                    let right_map: HashMap<_, _> = right_keep
                                        .iter()
                                        .map(|x| {
                                            let s = right_dt.map.get(x).unwrap().clone();
                                            (x.clone(), s)
                                        })
                                        .collect();
                                    right_mappings = right_mappings
                                        .filter(is_col_expr.unwrap().clone())
                                        .with_column(as_struct(struct_cols.clone()).alias(v));
                                    new_right_states
                                        .insert(v.to_string(), RDFNodeState::from_map(right_map));
                                }
                            }
                            _ => {
                                todo!()
                            }
                        }
                    } else {
                        //right not multi
                        let base_right = right_dt.get_base_type().unwrap();
                        match join_type {
                            JoinType::Inner => {
                                if left_dt.map.contains_key(&base_right) {
                                    left_mappings = force_convert_multicol_to_single_col(
                                        left_mappings,
                                        v,
                                        &base_right,
                                    );
                                    new_left_states.insert(v.clone(), right_dt.clone());
                                } else {
                                    left_mappings = left_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_left_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                }
                            }
                            JoinType::Left => {
                                if left_dt.map.contains_key(base_right) {
                                    //Do nothing
                                } else {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                }
                            }
                            _ => {
                                todo!()
                            }
                        }
                    }
                } else {
                    //left not multi
                    let left_basic_dt = left_dt.get_base_type().unwrap();
                    if right_dt.is_multi() {
                        match join_type {
                            JoinType::Inner => {
                                if right_dt.map.contains_key(&left_basic_dt) {
                                    right_mappings = force_convert_multicol_to_single_col(
                                        right_mappings,
                                        v,
                                        &left_basic_dt,
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        RDFNodeState::from_bases(
                                            left_basic_dt.clone(),
                                            right_dt.map.get(left_basic_dt).unwrap().clone(),
                                        ),
                                    );
                                } else {
                                    left_mappings = left_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_left_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
                                }
                            }
                            JoinType::Left => {
                                if right_dt.map.contains_key(&left_basic_dt) {
                                    right_mappings = force_convert_multicol_to_single_col(
                                        right_mappings,
                                        v,
                                        &left_basic_dt,
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        RDFNodeState::from_bases(
                                            left_basic_dt.clone(),
                                            right_dt.map.get(left_basic_dt).unwrap().clone(),
                                        ),
                                    );
                                } else {
                                    right_mappings = right_mappings.filter(lit(false)).with_column(
                                        lit(LiteralValue::untyped_null())
                                            .cast(
                                                BaseRDFNodeType::None
                                                    .default_input_polars_data_type(),
                                            )
                                            .alias(v),
                                    );
                                    new_right_states.insert(
                                        v.clone(),
                                        BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                    );
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
                                        .cast(
                                            BaseRDFNodeType::None.default_input_polars_data_type(),
                                        )
                                        .alias(v),
                                );
                                new_left_states.insert(
                                    v.clone(),
                                    BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                );
                                right_mappings = right_mappings.filter(lit(false)).with_column(
                                    lit(LiteralValue::untyped_null())
                                        .cast(
                                            BaseRDFNodeType::None.default_input_polars_data_type(),
                                        )
                                        .alias(v),
                                );
                                new_right_states.insert(
                                    v.clone(),
                                    BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                );
                            }
                            JoinType::Left => {
                                right_mappings = right_mappings.filter(lit(false)).with_column(
                                    lit(LiteralValue::untyped_null())
                                        .cast(
                                            BaseRDFNodeType::None.default_input_polars_data_type(),
                                        )
                                        .alias(v),
                                );
                                new_right_states.insert(
                                    v.clone(),
                                    BaseRDFNodeType::None.into_default_input_rdf_node_state(),
                                );
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
    left_datatypes.extend(new_left_states);
    right_datatypes.extend(new_right_states);
    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}
