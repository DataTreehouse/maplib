mod cats;
mod group;
mod join;
mod order;
mod union;
mod values;

pub use group::*;
pub use join::*;
pub use order::*;
pub use union::*;
pub use values::*;

use crate::errors::QueryProcessingError;
use crate::type_constraints::{
    conjunction_variable_type, equal_variable_type, ConstraintBaseRDFNodeType, PossibleTypes,
};
use oxrdf::vocab::rdfs;
use oxrdf::Variable;
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{
    by_name, col, lit, Expr, JoinArgs, JoinType, LazyFrame, LazyGroupBy, LiteralValue,
    SortMultipleOptions,
};
use representation::multitype::{nest_multicolumns, unnest_multicols};
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::{HashMap, HashSet};
use tracing::warn;
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
    solution_mappings.mappings =
        solution_mappings
            .mappings
            .rename([expression_context.as_str()], [variable.as_str()], true);
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
        .drop(by_name([expression_context.as_str()], true));
    solution_mappings
        .rdf_node_types
        .remove(expression_context.as_str())
        .unwrap();
    Ok(solution_mappings)
}

pub fn prepare_group_by(
    mut solution_mappings: SolutionMappings,
    variables: &[Variable],
) -> (SolutionMappings, Vec<Expr>, Option<String>) {
    let mut by = vec![];
    let dummy_varname = if variables.is_empty() {
        let dummy_varname = Uuid::new_v4().to_string();
        by.push(col(&dummy_varname));
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(lit(true).alias(&dummy_varname));
        Some(dummy_varname)
    } else {
        let mut grouped = HashSet::new();
        for v in variables {
            if !grouped.contains(&v) {
                grouped.insert(v);
                by.push(col(v.as_str()));
            } else {
                warn!("GROUP BY contains duplicate variable: {v}")
            }
        }
        None
    };
    (solution_mappings, by, dummy_varname)
}

//TODO: Fix datatypes??!?!?
pub fn minus(
    mut left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: mut right_mappings,
        rdf_node_types: right_datatypes,
        ..
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
        let join_on_cols: Vec<Expr> = join_on.iter().map(|x| col(*x)).collect();
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

pub fn project(
    mut solution_mappings: SolutionMappings,
    variables: &Vec<Variable>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
    let mut new_datatypes = HashMap::new();
    for v in variables {
        if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
            warn!("The variable {v} does not exist in the solution mappings, adding as an unbound variable");
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                    .alias(v.as_str()),
            );
            new_datatypes.insert(
                v.as_str().to_string(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            );
        } else {
            new_datatypes.insert(
                v.as_str().to_string(),
                solution_mappings.rdf_node_types.remove(v.as_str()).unwrap(),
            );
        }
    }
    solution_mappings.mappings = solution_mappings.mappings.select(cols.as_slice());
    solution_mappings.rdf_node_types = new_datatypes;
    Ok(solution_mappings)
}

#[allow(dead_code)]
fn find_enforced_variable_type_constraints(
    e: &Expression,
) -> Option<HashMap<String, PossibleTypes>> {
    match e {
        Expression::If(_, left, right) | Expression::Or(left, right) => {
            let left = find_enforced_variable_type_constraints(left);
            let right = find_enforced_variable_type_constraints(right);
            if let (Some(left), Some(mut right)) = (left, right) {
                let mut new_map = HashMap::new();
                for (k, mut v) in left {
                    if let Some(vr) = right.remove(&k) {
                        v.or_other(vr);
                        new_map.insert(k, v);
                    }
                }
                return Some(new_map);
            }
        }
        Expression::And(left, right) => {
            let left = find_enforced_variable_type_constraints(left);
            let right = find_enforced_variable_type_constraints(right);
            return if left.is_some() && right.is_some() {
                let mut left = left.unwrap();
                let right = right.unwrap();
                Some(conjunction_variable_type(&mut left, right))
            } else if left.is_some() {
                left
            } else {
                right
            };
        }
        Expression::Equal(left, right) => {
            if let Some((v, t)) = equal_variable_type(left, right) {
                return Some(HashMap::from([(
                    v.as_str().to_string(),
                    PossibleTypes::singular(t),
                )]));
            } else if let Some((v, t)) = equal_variable_type(right, left) {
                return Some(HashMap::from([(
                    v.as_str().to_string(),
                    PossibleTypes::singular(t),
                )]));
            }
        }
        Expression::FunctionCall(f, args) => match f {
            Function::IsIri => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(ConstraintBaseRDFNodeType::IRI(None)),
                        )]));
                    }
                }
            }
            Function::IsBlank => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(ConstraintBaseRDFNodeType::BlankNode),
                        )]));
                    }
                }
            }
            Function::IsLiteral => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(ConstraintBaseRDFNodeType::Literal(
                                rdfs::LITERAL.into_owned(),
                            )),
                        )]));
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
    None
}

pub fn unique_workaround(
    lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
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
        lf = lf.unique_stable(unique_set.map(|x| by_name(x, true)), unique_keep_strategy);
    } else {
        lf = lf.unique(unique_set.map(|x| by_name(x, true)), unique_keep_strategy);
    }
    lf = nest_multicolumns(lf, maps);
    lf
}

#[allow(clippy::type_complexity)]
pub fn group_by_workaround(
    lf: LazyFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
    by: Vec<String>,
) -> (LazyGroupBy, HashMap<String, (Vec<String>, Vec<String>)>) {
    let mut to_explode = HashMap::new();
    for c in &by {
        let t = rdf_node_types.get(c).expect(c);
        if t.is_multi() {
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
