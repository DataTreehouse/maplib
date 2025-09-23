use crate::cats::create_compatible_cats;
use crate::errors::QueryProcessingError;
use polars::prelude::{as_struct, col, concat_lf_diagonal, lit, LiteralValue, UnionArgs};
use representation::cats::LockedCats;
use representation::solution_mapping::SolutionMappings;
use representation::{RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use std::collections::HashMap;

pub fn union(
    mut sms: Vec<SolutionMappings>,
    rechunk: bool,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    assert!(!sms.is_empty());
    if sms.len() == 1 {
        return Ok(sms.remove(0));
    }
    let new_height = sms.iter().map(|x| x.height_estimate).sum();
    let to_union = create_to_union(&sms);
    for s in &to_union {
        let mut new_sms = Vec::with_capacity(sms.len());

        let mut to_union_exprs = vec![];
        let mut to_union_states = vec![];
        for sm in &sms {
            if let Some(rs) = sm.rdf_node_types.get(s) {
                to_union_exprs.push(Some(col(s)));
                to_union_states.push(Some(rs.clone()))
            } else {
                to_union_exprs.push(None);
                to_union_states.push(None);
            }
        }

        let exploded = create_compatible_cats(to_union_exprs, to_union_states, global_cats.clone());
        let mut target_states = HashMap::new();
        for m in &exploded {
            if let Some(m) = m {
                for (bt, (_, bs)) in m {
                    if !target_states.contains_key(bt) {
                        target_states.insert(bt.clone(), bs.clone());
                    }
                }
            }
        }
        let mut targets_vec: Vec<_> = target_states.keys().collect();
        targets_vec.sort();
        for (mut sms, m) in sms.into_iter().zip(exploded) {
            let mut m = m.unwrap_or(HashMap::new());
            let mut exprs = Vec::with_capacity(m.len());
            let mut states = HashMap::with_capacity(m.len());
            for bt in &targets_vec {
                if let Some((k, (v, s))) = m.remove_entry(*bt) {
                    states.insert(k, s);
                    exprs.extend(v);
                }

                if let Some(bs) = target_states.get(*bt) {
                    if !sms.rdf_node_types.contains_key(s)
                        || !sms.rdf_node_types.get(s).unwrap().map.contains_key(*bt)
                    {
                        if bt.is_lang_string() {
                            exprs.push(
                                lit(LiteralValue::untyped_null())
                                    .cast(bt.polars_data_type(bs, true))
                                    .alias(LANG_STRING_VALUE_FIELD),
                            );
                            exprs.push(
                                lit(LiteralValue::untyped_null())
                                    .cast(bt.polars_data_type(bs, true))
                                    .alias(LANG_STRING_LANG_FIELD),
                            );
                        } else {
                            exprs.push(
                                lit(LiteralValue::untyped_null())
                                    .cast(bt.polars_data_type(bs, true))
                                    .alias(bt.field_col_name()),
                            );
                        }
                        states.insert((*bt).clone(), bs.clone());
                    }
                }
            }
            if exprs.len() > 1 {
                sms.mappings = sms.mappings.with_column(as_struct(exprs).alias(s));
            } else {
                sms.mappings = sms
                    .mappings
                    .with_column(exprs.pop().unwrap().alias(s).alias(s));
            }

            sms.rdf_node_types
                .insert(s.to_string(), RDFNodeState::from_map(states));
            new_sms.push(sms);
        }
        sms = new_sms;
    }
    let mut target_state = HashMap::new();
    let mut to_concat = vec![];

    for SolutionMappings {
        mappings,
        rdf_node_types,
        ..
    } in sms
    {
        to_concat.push(mappings);
        for (k, v) in rdf_node_types {
            target_state.insert(k, v);
        }
    }
    let output_mappings = concat_lf_diagonal(
        to_concat,
        UnionArgs {
            parallel: true,
            rechunk,
            to_supertypes: false,
            diagonal: true,
            from_partitioned_ds: false,
            maintain_order: false,
        },
    )
    .expect("Concat problem");
    Ok(SolutionMappings::new(
        output_mappings,
        target_state,
        new_height,
    ))
}

fn create_to_union(sms: &Vec<SolutionMappings>) -> Vec<String> {
    let mut occurrences = HashMap::new();
    for sm in sms {
        for c in sm.rdf_node_types.keys() {
            if let Some(i) = occurrences.get_mut(c) {
                *i += 1;
            } else {
                occurrences.insert(c.clone(), 1u32);
            }
        }
    }
    let mut to_union = vec![];
    for (c, i) in occurrences {
        if i > 1 {
            to_union.push(c);
        }
    }
    to_union
}
