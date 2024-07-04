use crate::errors::QueryProcessingError;
use log::warn;
use oxrdf::Variable;
use polars::datatypes::{CategoricalOrdering, DataType};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{
    col, concat_lf_diagonal, lit, Expr, JoinArgs, JoinType, LiteralValue, SortMultipleOptions,
    UnionArgs,
};
use representation::multitype::{
    convert_lf_col_to_multitype, create_join_compatible_solution_mappings, explode_multicols,
    implode_multicolumns, lf_column_to_categorical,
};
use representation::multitype::{join_workaround, unique_workaround};
use representation::query_context::Context;
use representation::solution_mapping::{is_string_col, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeType};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub fn distinct(
    mut solution_mappings: SolutionMappings,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = unique_workaround(
        solution_mappings.mappings,
        &solution_mappings.rdf_node_types,
        None,
        true,
        UniqueKeepStrategy::Any,
    );
    Ok(solution_mappings)
}

pub fn extend(
    mut solution_mappings: SolutionMappings,
    expression_context: &Context,
    variable: &Variable,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings
        .mappings
        .rename([expression_context.as_str()], [variable.as_str()]);
    let existing_rdf_node_type = solution_mappings
        .rdf_node_types
        .remove(expression_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(variable.as_str().to_string(), existing_rdf_node_type);
    Ok(solution_mappings)
}

pub fn filter(
    mut solution_mappings: SolutionMappings,
    expression_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings
        .mappings
        .filter(col(expression_context.as_str()))
        .drop_no_validate([&expression_context.as_str()]);
    solution_mappings.rdf_node_types.remove(expression_context.as_str()).unwrap();
    Ok(solution_mappings)
}

pub fn prepare_group_by(
    mut solution_mappings: SolutionMappings,
    variables: &Vec<Variable>,
) -> (SolutionMappings, Vec<Expr>, Option<String>) {
    let by: Vec<Expr>;
    let dummy_varname = if variables.is_empty() {
        let dummy_varname = Uuid::new_v4().to_string();
        by = vec![col(&dummy_varname)];
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(lit(true).alias(&dummy_varname));
        Some(dummy_varname)
    } else {
        by = variables.iter().map(|v| col(v.as_str())).collect();
        None
    };
    (solution_mappings, by, dummy_varname)
}

pub fn group_by(
    solution_mappings: SolutionMappings,
    aggregate_expressions: Vec<Expr>,
    by: Vec<Expr>,
    dummy_varname: Option<String>,
    new_rdf_node_types: HashMap<String, RDFNodeType>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types: mut datatypes,
    } = solution_mappings;
    let grouped_mappings = mappings.group_by(by.as_slice());

    mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
    for (k, v) in new_rdf_node_types {
        datatypes.insert(k, v);
    }
    if let Some(dummy_varname) = dummy_varname {
        mappings = mappings.drop_no_validate([dummy_varname]);
    }
    Ok(SolutionMappings::new(mappings, datatypes))
}

pub fn join(
    left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
    join_type: JoinType,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: right_mappings,
        rdf_node_types: right_datatypes,
    } = right_solution_mappings;

    let SolutionMappings {
        mappings: left_mappings,
        rdf_node_types: left_datatypes,
    } = left_solution_mappings;

    let (left_mappings, left_datatypes, right_mappings, right_datatypes) =
        create_join_compatible_solution_mappings(
            left_mappings,
            left_datatypes,
            right_mappings,
            right_datatypes,
            join_type.clone(),
        );

    let solution_mappings = join_workaround(
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
        join_type,
    );

    Ok(solution_mappings)
}

//TODO: Fix datatypes??!?!?
pub fn minus(
    mut left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: mut right_mappings,
        rdf_node_types: right_datatypes,
    } = right_solution_mappings;

    let right_column_set: HashSet<_> = right_datatypes.keys().collect();
    let left_column_set: HashSet<_> = left_solution_mappings.rdf_node_types.keys().collect();

    let mut join_on: Vec<_> = left_column_set
        .intersection(&right_column_set)
        .cloned()
        .collect();
    join_on.sort();

    if join_on.is_empty() {
        Ok(left_solution_mappings)
    } else {
        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(x)).collect();
        for c in join_on {
            if is_string_col(left_solution_mappings.rdf_node_types.get(c).unwrap()) {
                right_mappings = right_mappings.with_column(
                    col(c).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
                );
                left_solution_mappings.mappings = left_solution_mappings.mappings.with_column(
                    col(c).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
                );
            }
        }
        right_mappings = right_mappings.sort_by_exprs(
            join_on_cols.as_slice(),
            SortMultipleOptions::default()
                .with_maintain_order(false)
                .with_nulls_last(true)
                .with_order_descending(false),
        );
        left_solution_mappings.mappings = left_solution_mappings.mappings.sort_by_exprs(
            join_on_cols.as_slice(),
            SortMultipleOptions::default()
                .with_maintain_order(false)
                .with_nulls_last(true)
                .with_order_descending(false),
        );
        left_solution_mappings.mappings = left_solution_mappings.mappings.join(
            right_mappings,
            join_on_cols.as_slice(),
            join_on_cols.as_slice(),
            JoinArgs::new(JoinType::Anti),
        );
        Ok(left_solution_mappings)
    }
}

pub fn order_by(
    solution_mappings: SolutionMappings,
    inner_contexts: &Vec<Context>,
    asc_ordering: Vec<bool>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types: datatypes,
    } = solution_mappings;

    mappings = mappings.sort_by_exprs(
        inner_contexts
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>(),
        SortMultipleOptions::default()
            .with_order_descending_multi(asc_ordering.iter().map(|asc| !asc).collect::<Vec<bool>>())
            .with_nulls_last(true)
            .with_maintain_order(false),
    );
    mappings = mappings.drop_no_validate(
        inner_contexts
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>(),
    );

    Ok(SolutionMappings::new(mappings, datatypes))
}

pub fn project(
    solution_mappings: SolutionMappings,
    variables: &Vec<Variable>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types: mut datatypes,
    } = solution_mappings;
    let cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
    let mut new_datatypes = HashMap::new();
    for v in variables {
        if !datatypes.contains_key(v.as_str()) {
            warn!("The variable {} does not exist in the solution mappings, adding as an unbound variable", v);
            mappings = mappings.with_column(
                lit(LiteralValue::Null)
                    .cast(BaseRDFNodeType::None.polars_data_type())
                    .alias(v.as_str()),
            );
            new_datatypes.insert(v.as_str().to_string(), RDFNodeType::None);
        } else {
            new_datatypes.insert(
                v.as_str().to_string(),
                datatypes.remove(v.as_str()).unwrap(),
            );
        }
    }
    mappings = mappings.select(cols.as_slice());
    Ok(SolutionMappings::new(mappings, new_datatypes))
}

pub fn union(
    mut mappings: Vec<SolutionMappings>,
    rechunk: bool,
) -> Result<SolutionMappings, QueryProcessingError> {
    assert!(!mappings.is_empty());
    if mappings.len() == 1 {
        return Ok(mappings.remove(0));
    }

    let mut cat_mappings = vec![];
    for SolutionMappings {
        mut mappings,
        rdf_node_types,
    } in mappings
    {
        for c in rdf_node_types.keys() {
            mappings = lf_column_to_categorical(mappings, c, &rdf_node_types);
        }
        cat_mappings.push(SolutionMappings::new(mappings, rdf_node_types));
    }
    mappings = cat_mappings;

    // Compute the target types
    let mut target_types = mappings.get(0).unwrap().rdf_node_types.clone();

    for m in &mappings[1..mappings.len()] {
        let mut updated_target_types = HashMap::new();
        let SolutionMappings {
            mappings: _,
            rdf_node_types: right_datatypes,
        } = m;
        for (right_col, right_type) in right_datatypes {
            if let Some(left_type) = target_types.get(right_col) {
                if left_type != right_type {
                    if let RDFNodeType::MultiType(left_types) = left_type {
                        let mut left_set: HashSet<_> = left_types.iter().collect();
                        if let RDFNodeType::MultiType(right_types) = right_type {
                            let right_set: HashSet<_> = right_types.iter().collect();
                            let mut union: Vec<_> = left_set
                                .union(&right_set)
                                .into_iter()
                                .map(|x| (*x).clone())
                                .collect();
                            union.sort();
                            updated_target_types
                                .insert(right_col.clone(), RDFNodeType::MultiType(union));
                        } else {
                            //Right not multi
                            let base_right = BaseRDFNodeType::from_rdf_node_type(right_type);
                            left_set.insert(&base_right);
                            let mut new_types: Vec<_> =
                                left_set.into_iter().map(|x| x.clone()).collect();
                            new_types.sort();
                            let new_type = RDFNodeType::MultiType(new_types);
                            updated_target_types.insert(right_col.clone(), new_type);
                        }
                    } else {
                        //Left not multi
                        if let RDFNodeType::MultiType(right_types) = right_type {
                            let mut right_set: HashSet<_> = right_types.iter().collect();
                            let base_left = BaseRDFNodeType::from_rdf_node_type(left_type);
                            right_set.insert(&base_left);
                            let mut new_types: Vec<_> =
                                right_set.into_iter().map(|x| x.clone()).collect();
                            new_types.sort();
                            let new_type = RDFNodeType::MultiType(new_types);
                            updated_target_types.insert(right_col.clone(), new_type);
                        } else {
                            //Both not multi
                            let base_left = BaseRDFNodeType::from_rdf_node_type(left_type);
                            let base_right = BaseRDFNodeType::from_rdf_node_type(right_type);
                            let mut new_types = vec![base_left.clone(), base_right.clone()];
                            new_types.sort();
                            let new_type = RDFNodeType::MultiType(new_types);
                            updated_target_types.insert(right_col.to_string(), new_type);
                        }
                    }
                }
            } else {
                updated_target_types.insert(right_col.clone(), right_type.clone());
            }
        }
        target_types.extend(updated_target_types);
    }

    let mut to_concat = vec![];
    let mut exploded_map: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
    //Change the mappings
    for SolutionMappings {
        mut mappings,
        mut rdf_node_types,
    } in mappings
    {
        let mut new_multi = HashMap::new();
        for (c, t) in &rdf_node_types {
            let target_type = target_types.get(c).unwrap();
            if t != target_type {
                if !matches!(t, RDFNodeType::MultiType(..)) {
                    mappings = mappings.with_column(convert_lf_col_to_multitype(c, t));
                    new_multi.insert(
                        c.clone(),
                        RDFNodeType::MultiType(vec![BaseRDFNodeType::from_rdf_node_type(t)]),
                    );
                }
            }
        }
        rdf_node_types.extend(new_multi);
        let (new_mappings, new_exploded_map) = explode_multicols(mappings, &rdf_node_types);

        to_concat.push(new_mappings);
        for (c, (right_inner_columns, prefixed_right_inner_columns)) in new_exploded_map {
            if let Some((left_inner_columns, prefixed_left_inner_columns)) =
                exploded_map.get_mut(&c)
            {
                for (r, pr) in right_inner_columns
                    .into_iter()
                    .zip(prefixed_right_inner_columns.into_iter())
                {
                    if !left_inner_columns.contains(&r) {
                        left_inner_columns.push(r);
                        prefixed_left_inner_columns.push(pr);
                    }
                }
            } else {
                exploded_map.insert(
                    c.clone(),
                    (right_inner_columns, prefixed_right_inner_columns),
                );
            }
        }
    }

    let mut output_mappings = concat_lf_diagonal(
        to_concat,
        UnionArgs {
            parallel: true,
            rechunk,
            to_supertypes: false,
            diagonal: true,
            from_partitioned_ds: false,
        },
    )
    .expect("Concat problem");
    output_mappings = implode_multicolumns(output_mappings, exploded_map);
    Ok(SolutionMappings::new(output_mappings, target_types))
}
