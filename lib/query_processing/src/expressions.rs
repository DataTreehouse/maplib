pub mod comparisons;
pub mod functions;
pub mod operations;

use crate::cats::create_compatible_cats;
use crate::errors::QueryProcessingError;
use crate::expressions::comparisons::{typed_comparison_expr, typed_equals_expr};
use crate::expressions::operations::typed_numerical_operation;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, NamedNode, Variable};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{
    as_struct, by_name, coalesce, col, lit, DataType, Expr, JoinArgs, JoinType, LazyFrame,
    LiteralValue, Operator, Scalar,
};
use representation::cats::{literal_is_cat, maybe_decode_expr, Cats, LockedCats};
use representation::multitype::all_multi_main_cols;
use representation::query_context::Context;
use representation::rdf_to_polars::rdf_literal_to_polars_expr;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState, LANG_STRING_VALUE_FIELD};
use spargebra::algebra::Expression;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub fn named_node_enc(nn: &NamedNode, global_cats: &Cats) -> Option<Expr> {
    let enc = global_cats.encode_iri_slice(&[nn.as_str()]).pop().unwrap();
    enc.map(|enc| lit(enc).cast(DataType::UInt32))
}

pub fn named_node_local_enc(nn: &NamedNode, global_cats: &Cats) -> (Expr, Option<Cats>) {
    if let Some(enc) = global_cats.encode_iri_slice(&[nn.as_str()]).pop().unwrap() {
        (lit(enc).cast(DataType::UInt32), None)
    } else {
        // We do not store these local cats to disk
        let (enc, local) = Cats::new_singular_iri(nn.as_str(), global_cats.get_iri_counter(), None);
        (lit(enc).cast(DataType::UInt32), Some(local))
    }
}

pub fn blank_node_enc(bl: &BlankNode, global_cats: &Cats) -> Option<Expr> {
    let enc = global_cats.encode_blanks(&[bl.as_str()]).pop().unwrap();
    enc.map(|enc| lit(enc).cast(DataType::UInt32))
}

pub fn blank_node_local_enc(bl: &BlankNode, global_cats: &Cats) -> (Expr, Option<Cats>) {
    if let Some(enc) = global_cats.encode_blanks(&[bl.as_str()]).pop().unwrap() {
        (lit(enc).cast(DataType::UInt32), None)
    } else {
        // Local cats not stored to disk, hence None path
        let (enc, local) =
            Cats::new_singular_blank(bl.as_str(), global_cats.get_iri_counter(), None);
        (lit(enc).cast(DataType::UInt32), Some(local))
    }
}

pub fn literal_enc(l: &Literal, global_cats: &Cats) -> (Expr, BaseRDFNodeType, BaseCatState) {
    let bt = BaseRDFNodeType::Literal(l.datatype().into_owned());
    if literal_is_cat(l.datatype()) {
        if let Some(enc) = global_cats
            .encode_literals(&[l.value()], l.datatype().into_owned())
            .pop()
            .unwrap()
        {
            (
                lit(enc).cast(DataType::UInt32),
                bt,
                BaseCatState::CategoricalNative(true, None),
            )
        } else {
            let dt = l.datatype().into_owned();
            let offset = global_cats.get_literal_counter(&dt);
            // Local cats are not stored to disk, hence None
            let (enc, local) = Cats::new_singular_literal(l.value(), dt, offset, None);
            (
                lit(enc).cast(DataType::UInt32),
                bt,
                BaseCatState::CategoricalNative(true, Some(LockedCats::new(local))),
            )
        }
    } else {
        let bs = bt.default_input_cat_state().clone();
        (rdf_literal_to_polars_expr(l), bt, bs)
    }
}

pub fn named_node(
    mut solution_mappings: SolutionMappings,
    nn: &NamedNode,
    context: &Context,
    global_cats: &Cats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let (enc, local) = named_node_local_enc(nn, global_cats);
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(enc.alias(context.as_str()));
    let state = RDFNodeState::from_bases(
        BaseRDFNodeType::IRI,
        BaseCatState::CategoricalNative(true, local.map(|x| LockedCats::new(x))),
    );
    solution_mappings
        .rdf_node_types
        .insert(context.as_str().to_string(), state);
    Ok(solution_mappings)
}

pub fn literal(
    mut solution_mappings: SolutionMappings,
    lit: &Literal,
    context: &Context,
    global_cats: &Cats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let (e, bt, bs) = literal_enc(lit, global_cats);
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(e.alias(context.as_str()));
    solution_mappings.rdf_node_types.insert(
        context.as_str().to_string(),
        RDFNodeState::from_bases(bt, bs),
    );
    Ok(solution_mappings)
}

pub fn variable(
    mut solution_mappings: SolutionMappings,
    v: &Variable,
    context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    if !solution_mappings.rdf_node_types.contains_key(v.as_str()) {
        return Err(QueryProcessingError::VariableNotFound(
            v.as_str().to_string(),
            context.as_str().to_string(),
        ));
    }
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(col(v.as_str()).alias(context.as_str()));
    let existing_type = solution_mappings.rdf_node_types.get(v.as_str()).unwrap();
    solution_mappings
        .rdf_node_types
        .insert(context.as_str().to_string(), existing_type.clone());
    Ok(solution_mappings)
}

pub fn binary_expression(
    mut solution_mappings: SolutionMappings,
    expression: &Expression,
    left_context: &Context,
    right_context: &Context,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let left_type = solution_mappings
        .rdf_node_types
        .get(left_context.as_str())
        .unwrap();
    let right_type = solution_mappings
        .rdf_node_types
        .get(right_context.as_str())
        .unwrap();
    if left_type.is_none() || right_type.is_none() {
        if matches!(
            expression,
            Expression::Equal(..)
                | Expression::Less(..)
                | Expression::Greater(..)
                | Expression::GreaterOrEqual(..)
                | Expression::LessOrEqual(..)
                | Expression::And(..)
                | Expression::Or(..)
        ) {
            let bool = BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned());
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(bool.default_input_polars_data_type())
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                bool.into_default_input_rdf_node_state(),
            );
        } else {
            solution_mappings.mappings = solution_mappings.mappings.with_column(
                lit(LiteralValue::untyped_null())
                    .cast(BaseRDFNodeType::None.default_input_polars_data_type())
                    .alias(outer_context.as_str()),
            );
            solution_mappings.rdf_node_types.insert(
                outer_context.as_str().to_string(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            );
        }
        solution_mappings =
            drop_inner_contexts(solution_mappings, &vec![left_context, right_context]);
        return Ok(solution_mappings);
    }
    if matches!(expression, Expression::Equal(..)) {
        let e = typed_equals_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
            global_cats,
        );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(e.alias(outer_context.as_str()));
        let t =
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state();
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t);
    } else if matches!(
        expression,
        Expression::Less(..)
            | Expression::Greater(..)
            | Expression::GreaterOrEqual(..)
            | Expression::LessOrEqual(..)
    ) {
        let e = typed_comparison_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
            expression,
        );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(e.alias(outer_context.as_str()));
        let t =
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state();
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t);
    } else if matches!(
        expression,
        Expression::Add(_, _)
            | Expression::Subtract(_, _)
            | Expression::Multiply(_, _)
            | Expression::Divide(_, _)
    ) {
        let (expr, t) = typed_numerical_operation(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
            expression,
            global_cats,
        );
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(expr.alias(outer_context.as_str()));
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t);
    } else {
        let op = match expression {
            Expression::Or(_, _) => Operator::Or,
            Expression::And(_, _) => Operator::And,
            _ => panic!("Should never happen"),
        };
        let expr = Expr::BinaryExpr {
            left: Arc::new(col(left_context.as_str())),
            op,
            right: Arc::new(col(right_context.as_str())),
        };
        let t =
            BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state();
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(expr.alias(outer_context.as_str()));
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), t);
    };

    solution_mappings = drop_inner_contexts(solution_mappings, &vec![left_context, right_context]);
    Ok(solution_mappings)
}

pub fn unary_plus(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(col(inner_context.as_str()).alias(outer_context.as_str()));
    let existing_type = solution_mappings
        .rdf_node_types
        .get(inner_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn unary_minus(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings //TODO: This is probably wrong
        .mappings
        .with_column(
            (Expr::BinaryExpr {
                left: Arc::new(Expr::Literal(LiteralValue::Scalar(Scalar::from(0i32)))),
                op: Operator::Minus,
                right: Arc::new(col(inner_context.as_str())),
            })
            .alias(outer_context.as_str()),
        );
    let existing_type = solution_mappings
        .rdf_node_types
        .get(inner_context.as_str())
        .unwrap();
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), existing_type.clone());
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn not_expression(
    mut solution_mappings: SolutionMappings,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    solution_mappings.mappings = solution_mappings.mappings.with_column(
        col(inner_context.as_str())
            .not()
            .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn bound(
    mut solution_mappings: SolutionMappings,
    v: &Variable,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let expr = expr_is_null_workaround(
        col(v.as_str()),
        solution_mappings.rdf_node_types.get(v.as_str()).unwrap(),
    )
    .not();
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    Ok(solution_mappings)
}

pub fn expr_is_null_workaround(expr: Expr, rdf_node_type: &RDFNodeState) -> Expr {
    if !rdf_node_type.is_multi() {
        let b = rdf_node_type.get_base_type().unwrap();
        non_multi_col_is_null_workaround(expr, b)
    } else {
        create_all_types_null_expression(expr, rdf_node_type.get_sorted_types())
    }
}

pub fn non_multi_col_is_null_workaround(expr: Expr, base_rdf_node_type: &BaseRDFNodeType) -> Expr {
    match base_rdf_node_type {
        BaseRDFNodeType::Literal(l) => {
            if l.as_ref() == rdf::LANG_STRING {
                expr.struct_()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .is_null()
            } else {
                expr.is_null()
            }
        }
        _ => expr.is_null(),
    }
}

pub fn if_expression(
    mut solution_mappings: SolutionMappings,
    left_context: &Context,
    middle_context: &Context,
    right_context: &Context,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    //
    let mut exploded: Vec<_> = create_compatible_cats(
        vec![
            Some(col(middle_context.as_str())),
            Some(col(right_context.as_str())),
        ],
        vec![
            Some(
                solution_mappings
                    .rdf_node_types
                    .get(middle_context.as_str())
                    .unwrap()
                    .clone(),
            ),
            Some(
                solution_mappings
                    .rdf_node_types
                    .get(right_context.as_str())
                    .unwrap()
                    .clone(),
            ),
        ],
        global_cats,
    )
    .into_iter()
    .map(|x| x.unwrap())
    .collect();
    assert_eq!(exploded.len(), 2);

    let right_exploded = exploded.pop().unwrap();
    let mid_exploded = exploded.pop().unwrap();

    let mut mid_exprs = vec![];
    let mut right_exprs = vec![];

    let mut base_type_map = HashMap::new();
    for (t, (exprs, base_state)) in right_exploded {
        base_type_map.insert(t, base_state);
        right_exprs.extend(exprs);
    }
    for (_, (exprs, _)) in mid_exploded {
        mid_exprs.extend(exprs);
    }
    let mid_expr = if mid_exprs.len() > 1 {
        as_struct(mid_exprs)
    } else {
        mid_exprs.pop().unwrap()
    };

    let right_expr = if right_exprs.len() > 1 {
        as_struct(right_exprs)
    } else {
        right_exprs.pop().unwrap()
    };

    solution_mappings.mappings = solution_mappings.mappings.with_column(
        (Expr::Ternary {
            predicate: Arc::new(col(left_context.as_str())),
            truthy: Arc::new(mid_expr),
            falsy: Arc::new(right_expr),
        })
        .alias(outer_context.as_str()),
    );
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        RDFNodeState::from_map(base_type_map),
    );
    solution_mappings = drop_inner_contexts(
        solution_mappings,
        &vec![left_context, middle_context, right_context],
    );
    Ok(solution_mappings)
}

pub fn coalesce_contexts(
    mut solution_mappings: SolutionMappings,
    inner_contexts: Vec<Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let mut expressions = vec![];
    let mut types = vec![];
    for c in &inner_contexts {
        expressions.push(col(c.as_str()));
        types.push(
            solution_mappings
                .rdf_node_types
                .get(c.as_str())
                .unwrap()
                .clone(),
        );
    }
    let (e, t) = coalesce_expressions(expressions, types, global_cats);
    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(e.alias(outer_context.as_str()));
    solution_mappings
        .rdf_node_types
        .insert(outer_context.as_str().to_string(), t);
    solution_mappings = drop_inner_contexts(solution_mappings, &inner_contexts.iter().collect());
    Ok(solution_mappings)
}

pub fn coalesce_expressions(
    expressions: Vec<Expr>,
    states: Vec<RDFNodeState>,
    global_cats: LockedCats,
) -> (Expr, RDFNodeState) {
    let mut basic_types = HashSet::new();
    let mut keep_exprs = vec![];
    let mut keep_states = vec![];
    for (e, t) in expressions.into_iter().zip(states) {
        if !t.is_none() {
            for t in t.map.keys() {
                if !t.is_none() {
                    basic_types.insert(t.clone());
                }
            }
            keep_states.push(Some(t));
            keep_exprs.push(Some(e));
        }
    }
    if keep_exprs.is_empty() {
        let e = lit(LiteralValue::untyped_null())
            .cast(BaseRDFNodeType::None.default_input_polars_data_type());
        let t = BaseRDFNodeType::None.into_default_input_rdf_node_state();
        (e, t)
    } else {
        let mut exploded: Vec<_> =
            create_compatible_cats(keep_exprs, keep_states, global_cats.clone())
                .into_iter()
                .map(|x| x.unwrap())
                .collect();
        let mut coalesce_map = HashMap::new();
        let mut state_map = HashMap::new();
        for t in &basic_types {
            let mut to_coalesce = HashMap::new();
            for m in exploded.iter_mut() {
                if let Some((t, (e_v, s))) = m.remove_entry(t) {
                    for (i, e) in e_v.into_iter().enumerate() {
                        if !to_coalesce.contains_key(&i) {
                            to_coalesce.insert(i, vec![]);
                        }
                        let to_coalesce_vec = to_coalesce.get_mut(&i).unwrap();
                        to_coalesce_vec.push(e);
                    }
                    state_map.insert(t.clone(), s);
                }
            }
            coalesce_map.insert(t.clone(), vec![]);
            let v = coalesce_map.get_mut(t).unwrap();
            if to_coalesce.get(&0).unwrap().len() == 1 {
                for (_, mut e) in to_coalesce {
                    v.push(e.pop().unwrap());
                }
            } else {
                for (_, e) in to_coalesce {
                    v.push(coalesce(e.as_slice()));
                }
            }
        }
        let new_state = RDFNodeState::from_map(state_map);
        let mut exprs = vec![];

        let mut base_types_sorted: Vec<_> = basic_types.into_iter().collect();
        base_types_sorted.sort();
        for b in base_types_sorted {
            exprs.extend(coalesce_map.remove(&b).unwrap())
        }
        let expr = if exprs.len() > 1 {
            as_struct(exprs)
        } else {
            exprs.pop().unwrap()
        };
        (expr, new_state)
    }
}

pub fn exists(
    solution_mappings: SolutionMappings,
    exists_lf: LazyFrame,
    inner_context: &Context,
    outer_context: &Context,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        mut rdf_node_types,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let exists_lf = exists_lf
        .select([col(inner_context.as_str())])
        .unique(None, UniqueKeepStrategy::Any)
        .with_column(lit(true).alias(outer_context.as_str()));
    mappings = mappings
        .join(
            exists_lf,
            [col(inner_context.as_str())],
            [col(inner_context.as_str())],
            JoinArgs {
                how: JoinType::Left,
                validation: Default::default(),
                suffix: None,
                slice: None,
                nulls_equal: false,
                coalesce: Default::default(),
                maintain_order: Default::default(),
            },
        )
        .with_column(col(outer_context.as_str()).fill_null(lit(false)));

    rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    let mut solution_mappings = SolutionMappings::new(mappings, rdf_node_types, height_upper_bound);
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![inner_context]);
    Ok(solution_mappings)
}

pub fn in_expression(
    mut solution_mappings: SolutionMappings,
    left_context: &Context,
    right_contexts: &Vec<Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    let mut expr = Expr::Literal(LiteralValue::Scalar(Scalar::from(false)));
    let left_type = solution_mappings
        .rdf_node_types
        .get(left_context.as_str())
        .unwrap();
    for right_context in right_contexts {
        let right_type = solution_mappings
            .rdf_node_types
            .get(right_context.as_str())
            .unwrap();
        expr = expr.or(typed_equals_expr(
            left_context.as_str(),
            right_context.as_str(),
            left_type,
            right_type,
            global_cats.clone(),
        ));
    }

    solution_mappings.mappings = solution_mappings
        .mappings
        .with_column(expr.alias(outer_context.as_str()));
    solution_mappings.rdf_node_types.insert(
        outer_context.as_str().to_string(),
        BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned()).into_default_input_rdf_node_state(),
    );
    solution_mappings = drop_inner_contexts(solution_mappings, &vec![left_context]);
    solution_mappings = drop_inner_contexts(solution_mappings, &right_contexts.iter().collect());

    Ok(solution_mappings)
}

pub fn drop_inner_contexts(mut sm: SolutionMappings, contexts: &Vec<&Context>) -> SolutionMappings {
    let mut inner = vec![];
    for c in contexts {
        let cstr = c.as_str();
        sm.rdf_node_types.remove(cstr);
        inner.push(cstr.to_string());
    }
    sm.mappings = sm.mappings.drop(by_name(inner, true));
    sm
}

pub fn create_all_types_null_expression(expr: Expr, types: Vec<&BaseRDFNodeType>) -> Expr {
    let mut all_types_null: Option<Expr> = None;
    for x in all_multi_main_cols(types) {
        let e = expr.clone().struct_().field_by_name(&x).is_null();
        all_types_null = if let Some(all_types_null) = all_types_null {
            Some(all_types_null.and(e))
        } else {
            Some(e)
        };
    }
    all_types_null.unwrap()
}

fn cast_lang_string_to_string(
    c: &str,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    global_cats: LockedCats,
) -> Expr {
    maybe_decode_expr(
        col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD),
        t,
        s,
        global_cats,
    )
}

pub fn contains_graph_pattern(e: &Expression) -> bool {
    match e {
        Expression::NamedNode(_)
        | Expression::Bound(_)
        | Expression::Literal(_)
        | Expression::Variable(_) => false,
        Expression::Or(l, r)
        | Expression::And(l, r)
        | Expression::Equal(l, r)
        | Expression::SameTerm(l, r)
        | Expression::Greater(l, r)
        | Expression::GreaterOrEqual(l, r)
        | Expression::Less(l, r)
        | Expression::LessOrEqual(l, r)
        | Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r) => contains_graph_pattern(l) | contains_graph_pattern(r),
        Expression::UnaryPlus(u) | Expression::UnaryMinus(u) | Expression::Not(u) => {
            contains_graph_pattern(u)
        }
        Expression::Exists(_) => true,
        Expression::In(l, r) => contains_graph_pattern(l) | r.iter().any(contains_graph_pattern),
        Expression::If(l, m, r) => {
            contains_graph_pattern(l) || contains_graph_pattern(m) || contains_graph_pattern(r)
        }
        Expression::Coalesce(e) | Expression::FunctionCall(_, e) => {
            e.iter().any(contains_graph_pattern)
        }
    }
}
