use crate::errors::QueryProcessingError;
use crate::graph_patterns::cats::create_compatible_cats;
use crate::graph_patterns::union;
use polars::datatypes::PlSmallStr;
use polars::prelude::{
    as_struct, by_name, col, concat, len, lit, Expr, JoinArgs, JoinType, LazyFrame, LiteralValue,
    MaintainOrderJoin, UnionArgs,
};
use representation::cats::LockedCats;
use representation::multitype::{
    all_multi_cols, all_multi_main_cols, force_convert_multicol_to_single_col, nest_multicolumns,
    unnest_multicols, unnested_field_col_name,
};
use representation::null::expr_is_null_workaround;
use representation::solution_mapping::SolutionMappings;
use representation::{
    BaseRDFNodeType, RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use std::cmp;
use std::collections::{HashMap, HashSet};
use tracing::debug;

pub fn join(
    left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
    join_type: JoinType,
    global_cats: LockedCats,
    max_rows: Option<usize>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: mut left_mappings,
        rdf_node_types: mut left_datatypes,
        height_estimate: left_height,
    } = left_solution_mappings;

    let SolutionMappings {
        mappings: mut right_mappings,
        rdf_node_types: mut right_datatypes,
        height_estimate: right_height,
    } = right_solution_mappings;

    // A shared variable that is unbound in every solution of one side (e.g. a projected variable
    // the pattern does not bind) is compatible with any value on the other side, so it is not a
    // join key: drop it from that side and keep the other side's values. An anti join (MINUS)
    // keeps the left columns, and does not compare on the variable, so it is dropped from the
    // right side either way.
    let shared: Vec<String> = left_datatypes
        .keys()
        .filter(|k| right_datatypes.contains_key(*k))
        .cloned()
        .collect();
    for k in shared {
        if right_datatypes.get(&k).unwrap().is_none()
            || (join_type == JoinType::Anti && left_datatypes.get(&k).unwrap().is_none())
        {
            right_mappings = right_mappings.drop(by_name([k.as_str()], true, false));
            right_datatypes.remove(&k);
        } else if left_datatypes.get(&k).unwrap().is_none() {
            left_mappings = left_mappings.drop(by_name([k.as_str()], true, false));
            left_datatypes.remove(&k);
        }
    }

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
        max_rows,
    )?;

    update_state(&mut solution_mappings.rdf_node_types, target_cat_state);

    Ok(solution_mappings)
}

#[allow(clippy::too_many_arguments)]
pub fn join_workaround(
    left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeState>,
    left_height: usize,
    right_mappings: LazyFrame,
    right_datatypes: HashMap<String, RDFNodeState>,
    right_height: usize,
    join_type: JoinType,
    max_rows: Option<usize>,
) -> Result<SolutionMappings, QueryProcessingError> {
    assert!(matches!(
        join_type,
        JoinType::Left | JoinType::Inner | JoinType::Anti
    ));
    let anti = join_type == JoinType::Anti;

    let (mut left_mappings, mut left_exploded) = unnest_multicols(left_mappings, &left_datatypes);
    // Single-type left columns unnested below. An anti join has no right columns to nest them
    // with, so it keeps the original column and drops the unnested ones afterwards.
    let mut left_unnested_singles = vec![];

    //Fixes "unnesting" where only one side is multi.
    for (c, s) in &right_datatypes {
        if s.is_multi() {
            if let Some(sl) = left_datatypes.get(c) {
                if !sl.is_multi() {
                    let (lf, exploded) = unnest_single_col(
                        left_mappings,
                        c,
                        sl.get_base_type().unwrap(),
                        anti,
                    );
                    left_mappings = lf;
                    if anti {
                        left_unnested_singles.push((c.clone(), exploded.1.clone()));
                    }
                    left_exploded.insert(c.clone(), exploded);
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
                    let (lf, exploded) =
                        unnest_single_col(right_mappings, c, sr.get_base_type().unwrap(), false);
                    right_mappings = lf;
                    right_exploded.insert(c.clone(), exploded);
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
    let height_upper_bound = if anti {
        left_height
    } else if no_join {
        0
    } else if on.is_empty() {
        let left_cols: Vec<_> = left_datatypes.keys().cloned().collect();
        let right_cols: Vec<_> = right_datatypes.keys().cloned().collect();
        debug!(
            "Cross join detected left columns {} right columns {}",
            left_cols.join(","),
            right_cols.join(",")
        );
        left_height.saturating_mul(right_height)
    } else {
        let mut factor = 1.5 - 0.1 * (on.len() as f64);
        if factor < 1.1 {
            factor = 1.1;
        }
        let new_height = (cmp::max(left_height, right_height) as f64) * factor;
        new_height.trunc() as usize
    };

    if let Some(max_rows) = max_rows.filter(|_| !anti) {
        if height_upper_bound >= max_rows {
            let left_cols: Vec<_> = left_datatypes.keys().cloned().collect();
            let right_cols: Vec<_> = right_datatypes.keys().cloned().collect();
            return Err(QueryProcessingError::MaxRowsReached(
                height_upper_bound,
                max_rows,
                left_cols.join(","),
                right_cols.join(","),
            ));
        }
    }

    if anti {
        // MINUS removes the left solutions that are compatible with a right solution sharing a
        // variable with it. Without shared keys, or with keys of no common type, no right
        // solution qualifies and the left side is kept as it is.
        if !no_join && !on.is_empty() {
            let join_args = JoinArgs {
                how: JoinType::Anti,
                validation: Default::default(),
                suffix: None,
                slice: None,
                nulls_equal: true,
                coalesce: Default::default(),
                maintain_order: MaintainOrderJoin::None,
                build_side: None,
            };
            let right_keys = right_mappings.select(on.clone());
            left_mappings = left_mappings.join(right_keys, &on, &on, join_args).unwrap();
        }
    } else if no_join {
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
        // No right row can match, so only the schema of the right side is needed: an inner join
        // is empty, and a left join gives the left rows with the right columns unbound.
        right_mappings = right_mappings
            .drop(by_name(to_drop_right, true, false))
            .limit(0);
        if join_type == JoinType::Inner {
            left_mappings = left_mappings.limit(0);
        }
        left_mappings = left_mappings
            .join(
                right_mappings,
                &[col(&dummycol)],
                &[col(&dummycol)],
                join_type.into(),
            )
            .unwrap();
        left_mappings = left_mappings.drop(by_name([dummycol], true, false));
    } else if on.is_empty() {
        // No shared variables: the cartesian product. A join on a constant key would put every
        // row in one hash bucket and run on one thread.
        // A lazy diagonal concat drops the rows of an input without columns, such as the single
        // empty solution of an empty group, so the left side gets a placeholder column then.
        let placeholder = if join_type == JoinType::Left && left_datatypes.is_empty() {
            let placeholder = uuid::Uuid::new_v4().to_string();
            left_mappings = left_mappings.with_column(lit(true).alias(&placeholder));
            Some(placeholder)
        } else {
            None
        };
        let product = left_mappings
            .clone()
            .cross_join(right_mappings.clone(), None);
        left_mappings = if join_type == JoinType::Inner {
            product
        } else {
            // OPTIONAL keeps the left rows, with the right variables unbound, when the right side
            // has no rows. The count of right rows is joined onto the left rows to keep them only
            // then. Common subplan elimination evaluates each side once.
            let count = uuid::Uuid::new_v4().to_string();
            let unmatched = left_mappings
                .cross_join(right_mappings.select([len().alias(&count)]), None)
                .filter(col(&count).eq(lit(0u32)))
                .drop(by_name([count], true, false));
            concat(
                [product, unmatched],
                UnionArgs {
                    diagonal: true,
                    rechunk: false,
                    parallel: true,
                    ..Default::default()
                },
            )
            .unwrap()
        };
        if let Some(placeholder) = placeholder {
            left_mappings = left_mappings.drop(by_name([placeholder], true, false));
        }
    } else {
        let join_args = JoinArgs {
            how: join_type,
            validation: Default::default(),
            suffix: None,
            slice: None,
            nulls_equal: true,
            coalesce: Default::default(),
            maintain_order: MaintainOrderJoin::None,
            build_side: None,
        };
        left_mappings = left_mappings
            .join(right_mappings, &on, &on, join_args)
            .unwrap();
    }

    // Stops a long join soon after its query times out or is cancelled.
    left_mappings = utils::cancel::cancel_check_node(left_mappings);
    // Keeps the join order chosen from the graph patterns (see keep_join_order).
    left_mappings = utils::polars::keep_join_order(left_mappings);
    if anti {
        for (c, colnames) in left_unnested_singles {
            left_exploded.remove(&c);
            left_mappings = left_mappings.drop(by_name(colnames, true, false));
        }
        left_mappings = nest_multicolumns(left_mappings, left_exploded);
        return Ok(SolutionMappings::new(
            left_mappings,
            left_datatypes,
            height_upper_bound,
        ));
    }
    // The height estimate assumes joins on keys with few matches each. A join on a key that many
    // rows share gives up to the product of the input heights, e.g. `?s :kind ?k . ?t :kind ?k`.
    if height_upper_bound > utils::polars::SMALL_COLLECT_HEIGHT
        || (!no_join
            && left_height.saturating_mul(right_height) > utils::polars::LARGE_JOIN_PRODUCT)
    {
        utils::cancel::note_large_join();
    }

    let mut unified_exploded = HashMap::new();
    for (c, (mut left_inner_columns, mut left_prefixed_inner_columns)) in left_exploded {
        if let Some((right_inner_columns, right_prefixed_inner_columns)) = right_exploded.remove(&c)
        {
            for (r, pr) in right_inner_columns
                .into_iter()
                .zip(right_prefixed_inner_columns)
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

    Ok(SolutionMappings::new(
        left_mappings,
        left_datatypes,
        height_upper_bound,
    ))
}

/// The columns a single-type column `c` of type `t` has when unnested as part of a multi-type
/// column, as [`unnest_multicols`] gives them: a language-tagged string has a value and a language
/// field. With `keep`, the column itself is kept beside them.
fn unnest_single_col(
    lf: LazyFrame,
    c: &str,
    t: &BaseRDFNodeType,
    keep: bool,
) -> (LazyFrame, (Vec<String>, Vec<String>)) {
    let fcn = t.field_col_name();
    let colname = unnested_field_col_name(c, &fcn);
    if t.is_lang_string() {
        let langname = unnested_field_col_name(c, LANG_STRING_LANG_FIELD);
        let mut lf = lf.with_columns([
            col(c)
                .struct_()
                .field_by_name(LANG_STRING_LANG_FIELD)
                .alias(&langname),
            col(c)
                .struct_()
                .field_by_name(LANG_STRING_VALUE_FIELD)
                .alias(&colname),
        ]);
        if !keep {
            lf = lf.drop(by_name([c], true, false));
        }
        (
            lf,
            (
                vec![LANG_STRING_LANG_FIELD.to_string(), fcn],
                vec![langname, colname],
            ),
        )
    } else {
        let lf = if keep {
            lf.with_column(col(c).alias(&colname))
        } else {
            lf.rename([c], [&colname], true)
        };
        (lf, (vec![fcn], vec![colname]))
    }
}

pub(crate) fn create_join_compatible_cats(
    mut left_mappings: LazyFrame,
    left_types: &HashMap<String, RDFNodeState>,
    mut right_mappings: LazyFrame,
    right_types: &HashMap<String, RDFNodeState>,
    global_cats: LockedCats,
) -> (LazyFrame, LazyFrame, HashMap<String, RDFNodeState>) {
    let mut state_map = HashMap::new();
    for (c, left_state) in left_types {
        if let Some(right_state) = right_types.get(c) {
            let mut new_bases = HashMap::new();
            for left_base in left_state.get_sorted_types() {
                if let Some(right_state) = right_state.map.get(left_base) {
                    let (new_state, left_oper, right_oper) =
                        create_compatible_cats(
                            left_state.map.get(left_base).unwrap(),
                            right_state,
                            left_base,
                            &global_cats,
                        );
                    if let Some(left_oper) = left_oper {
                        left_mappings = left_oper.apply(
                            left_mappings,
                            c,
                            left_state,
                            left_base,
                            global_cats.clone(),
                        );
                    }
                    if let Some(right_oper) = right_oper {
                        let right_t = right_types.get(c).unwrap();
                        right_mappings = right_oper.apply(
                            right_mappings,
                            c,
                            right_t,
                            left_base,
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
                    .filter(|(t, _)| !left_state.map.contains_key(*t))
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

pub(crate) fn update_state(
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
                                        multicol_to_single_type(left_mappings, v, left_dt, t);
                                    right_mappings =
                                        multicol_to_single_type(right_mappings, v, right_dt, t);
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
                            JoinType::Left | JoinType::Anti => {
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
                                        multicol_to_single_type(right_mappings, v, right_dt, t);
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
                            _ => unreachable!("join_type is Inner, Left or Anti"),
                        }
                    } else {
                        //right not multi
                        let base_right = right_dt.get_base_type().unwrap();
                        match join_type {
                            JoinType::Inner => {
                                if left_dt.map.contains_key(base_right) {
                                    left_mappings = multicol_to_single_type(
                                        left_mappings,
                                        v,
                                        left_dt,
                                        base_right,
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
                            JoinType::Left | JoinType::Anti => {
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
                            _ => unreachable!("join_type is Inner, Left or Anti"),
                        }
                    }
                } else {
                    //left not multi
                    let left_basic_dt = left_dt.get_base_type().unwrap();
                    if right_dt.is_multi() {
                        match join_type {
                            JoinType::Inner => {
                                if right_dt.map.contains_key(left_basic_dt) {
                                    right_mappings = multicol_to_single_type(
                                        right_mappings,
                                        v,
                                        right_dt,
                                        left_basic_dt,
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
                            JoinType::Left | JoinType::Anti => {
                                if right_dt.map.contains_key(left_basic_dt) {
                                    right_mappings = multicol_to_single_type(
                                        right_mappings,
                                        v,
                                        right_dt,
                                        left_basic_dt,
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
                            _ => unreachable!("join_type is Inner, Left or Anti"),
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
                            JoinType::Left | JoinType::Anti => {
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
                            _ => unreachable!("join_type is Inner, Left or Anti"),
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

/// Makes the multi-type column `c` a column of type `t`, without the rows where `c` holds another
/// type. Those rows would otherwise be null in the new column, and the join matches nulls with
/// each other (`nulls_equal`, see [`join_workaround`]), so that e.g. a literal on one side would
/// join a blank node on the other. Rows where `c` is unbound are kept.
fn multicol_to_single_type(
    mut lf: LazyFrame,
    c: &str,
    state: &RDFNodeState,
    t: &BaseRDFNodeType,
) -> LazyFrame {
    let others: Vec<_> = state
        .map
        .keys()
        .filter(|x| *x != t && !x.is_none())
        .collect();
    let mut other_bound: Option<Expr> = None;
    for f in all_multi_main_cols(others) {
        let e = col(c).struct_().field_by_name(&f).is_not_null();
        other_bound = Some(match other_bound {
            Some(o) => o.or(e),
            None => e,
        });
    }
    if let Some(other_bound) = other_bound {
        lf = lf.filter(other_bound.not());
    }
    force_convert_multicol_to_single_col(lf, c, t)
}

/// Joins solution mappings where some shared variables may be unbound (null) in some rows.
/// An unbound variable is compatible with any value, while a plain join only matches nulls
/// with nulls. Each side is split by which of its possibly unbound shared variables are bound,
/// the unbound columns are dropped from each part, and the parts are joined pairwise on the
/// variables that are bound in both. A left join only splits the left side, since splitting the
/// right side would keep left rows that only fail to match one of its parts. An anti join
/// (MINUS) splits both sides, and removes from each left part the rows matching any right part.
pub fn join_with_unbound(
    left_solution_mappings: SolutionMappings,
    left_maybe_unbound: &HashSet<String>,
    right_solution_mappings: SolutionMappings,
    right_maybe_unbound: &HashSet<String>,
    join_type: JoinType,
    global_cats: LockedCats,
    max_rows: Option<usize>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let shared: HashSet<&String> = left_solution_mappings
        .rdf_node_types
        .keys()
        .filter(|x| right_solution_mappings.rdf_node_types.contains_key(*x))
        .collect();
    let mut left_split: Vec<_> = left_maybe_unbound
        .iter()
        .filter(|x| shared.contains(x))
        .cloned()
        .collect();
    let mut right_split: Vec<_> = if matches!(join_type, JoinType::Inner | JoinType::Anti) {
        right_maybe_unbound
            .iter()
            .filter(|x| shared.contains(x))
            .cloned()
            .collect()
    } else {
        vec![]
    };
    if left_split.is_empty() && right_split.is_empty() {
        return join(
            left_solution_mappings,
            right_solution_mappings,
            join_type,
            global_cats,
            max_rows,
        );
    }
    left_split.sort();
    right_split.sort();
    let left_parts = split_by_bound(left_solution_mappings, &left_split);
    let right_parts = split_by_bound(right_solution_mappings, &right_split);
    let mut sms = vec![];
    if join_type == JoinType::Anti {
        // A left solution is removed by any part of the right side, each compared on the
        // variables bound in both.
        for mut l in left_parts {
            for r in &right_parts {
                l = join(l, r.clone(), JoinType::Anti, global_cats.clone(), max_rows)?;
            }
            sms.push(l);
        }
        return union(sms, false, global_cats);
    }
    for l in &left_parts {
        for r in &right_parts {
            sms.push(join(
                l.clone(),
                r.clone(),
                join_type.clone(),
                global_cats.clone(),
                max_rows,
            )?);
        }
    }
    union(sms, false, global_cats)
}

/// One part per combination of bound and unbound variables in `vars`, without the columns of
/// the unbound ones.
fn split_by_bound(sm: SolutionMappings, vars: &[String]) -> Vec<SolutionMappings> {
    if vars.is_empty() {
        return vec![sm];
    }
    let mut is_null = vec![];
    for v in vars {
        is_null.push(expr_is_null_workaround(
            col(v),
            sm.rdf_node_types.get(v).unwrap(),
        ));
    }
    let mut parts = vec![];
    for mask in 0usize..(1 << vars.len()) {
        let mut cond: Option<Expr> = None;
        let mut types = sm.rdf_node_types.clone();
        let mut to_drop = vec![];
        for (i, v) in vars.iter().enumerate() {
            let bound = mask & (1 << i) != 0;
            let c = if bound {
                is_null[i].clone().not()
            } else {
                types.remove(v);
                to_drop.push(v.clone());
                is_null[i].clone()
            };
            cond = Some(if let Some(cond) = cond {
                cond.and(c)
            } else {
                c
            });
        }
        let mappings = sm
            .mappings
            .clone()
            .filter(cond.unwrap())
            .drop(by_name(to_drop, true, false));
        parts.push(SolutionMappings::new(mappings, types, sm.height_estimate));
    }
    parts
}
