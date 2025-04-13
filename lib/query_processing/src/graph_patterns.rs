use crate::errors::QueryProcessingError;
use crate::type_constraints::{conjunction_variable_type, equal_variable_type, PossibleTypes};
use log::warn;
use oxrdf::vocab::{rdf, rdfs};
use oxrdf::{NamedNode, Term, Variable};
use polars::datatypes::{CategoricalOrdering, DataType, PlSmallStr};
use polars::frame::{DataFrame, UniqueKeepStrategy};
use polars::prelude::{
    as_struct, col, concat_lf_diagonal, lit, Expr, IntoColumn, IntoLazy, JoinArgs, JoinType,
    LiteralValue, SortMultipleOptions, UnionArgs,
};
use representation::multitype::{
    base_col_name, convert_lf_col_to_multitype, create_join_compatible_solution_mappings,
    known_convert_lf_multicol_to_single, lf_column_to_categorical, nest_multicolumns,
    unnest_multicols,
};
use representation::multitype::{join_workaround, unique_workaround};
use representation::polars_to_rdf::particular_opt_term_vec_to_series;
use representation::query_context::Context;
use representation::rdf_to_polars::string_rdf_literal;
use representation::solution_mapping::{is_string_col, EagerSolutionMappings, SolutionMappings};
use representation::{
    get_ground_term_datatype_ref, BaseRDFNodeType, BaseRDFNodeTypeRef, RDFNodeType,
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use spargebra::algebra::{Expression, Function};
use spargebra::term::GroundTerm;
use std::collections::{HashMap, HashSet};
use std::iter;
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
        .drop_no_validate([expression_context.as_str()]);
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
                warn!("GROUP BY contains duplicate variable: {}", v)
            }
        }
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
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let grouped_mappings = mappings.group_by(by.as_slice());

    mappings = grouped_mappings.agg(aggregate_expressions.as_slice());
    for (k, v) in new_rdf_node_types {
        datatypes.insert(k, v);
    }
    if let Some(dummy_varname) = dummy_varname {
        mappings = mappings.drop_no_validate([dummy_varname]);
    }
    Ok(SolutionMappings::new(
        mappings,
        datatypes,
        height_upper_bound,
    ))
}

pub fn join(
    left_solution_mappings: SolutionMappings,
    right_solution_mappings: SolutionMappings,
    join_type: JoinType,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mappings: mut right_mappings,
        rdf_node_types: mut right_datatypes,
        height_estimate: right_height,
    } = right_solution_mappings;

    let SolutionMappings {
        mappings: mut left_mappings,
        rdf_node_types: mut left_datatypes,
        height_estimate: left_height,
    } = left_solution_mappings;

    let mut lang_to_multi = vec![];
    for (lk, lt) in &left_datatypes {
        if lt.is_lang_string() {
            if let Some(rt) = right_datatypes.get(lk) {
                if rt.is_lang_string() {
                    lang_to_multi.push(lk.clone());
                }
            }
        }
    }
    if !lang_to_multi.is_empty() {
        let lang_base_type = BaseRDFNodeType::Literal(NamedNode::from(rdf::LANG_STRING));
        let lang_rdf_type = lang_base_type.as_rdf_node_type();
        let lang_multi = RDFNodeType::MultiType(vec![lang_base_type]);
        for k in lang_to_multi.iter() {
            left_mappings =
                left_mappings.with_column(convert_lf_col_to_multitype(k, &lang_rdf_type));
            left_datatypes.insert(k.clone(), lang_multi.clone());
            right_mappings =
                right_mappings.with_column(convert_lf_col_to_multitype(k, &lang_rdf_type));
            right_datatypes.insert(k.clone(), lang_multi.clone());
        }
    }

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

    if !lang_to_multi.is_empty() {
        let lang_base_type = BaseRDFNodeType::Literal(NamedNode::from(rdf::LANG_STRING));
        let lang_rdf_type = lang_base_type.as_rdf_node_type();
        for k in lang_to_multi.iter() {
            solution_mappings.mappings =
                known_convert_lf_multicol_to_single(solution_mappings.mappings, k, &lang_base_type);
            solution_mappings
                .rdf_node_types
                .insert(k.clone(), lang_rdf_type.clone());
        }
    }

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
    columns: &[String],
    asc_ordering: Vec<bool>,
) -> Result<SolutionMappings, QueryProcessingError> {
    if columns.is_empty() {
        return Ok(solution_mappings);
    }
    let SolutionMappings {
        mut mappings,
        rdf_node_types,
        height_estimate: height_upper_bound,
    } = solution_mappings;

    let mut order_exprs = vec![];
    let mut asc_bools = vec![];
    for (c, a) in columns.iter().zip(asc_ordering) {
        let t = rdf_node_types.get(c).unwrap();
        match t {
            RDFNodeType::IRI | RDFNodeType::BlankNode => {
                order_exprs
                    .push(col(c).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)));
                asc_bools.push(a);
            }
            RDFNodeType::Literal(l) => {
                if string_rdf_literal(l.as_ref()) {
                    order_exprs.push(
                        col(c).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
                    )
                } else {
                    order_exprs.push(col(c))
                }
                asc_bools.push(a);
            }
            RDFNodeType::None => {
                order_exprs.push(col(c));
                asc_bools.push(a);
            }
            RDFNodeType::MultiType(ts) => {
                let mut ts = ts.clone();
                ts.sort_by_key(|x| match x {
                    BaseRDFNodeType::IRI => 2,
                    BaseRDFNodeType::BlankNode => 1,
                    BaseRDFNodeType::Literal(_) => 3,
                    BaseRDFNodeType::None => 0,
                });
                // A bit complicated:
                // Nulls are first in ascending sort
                // By placing literals first, we are using the subsequent types to break ties where the literal is null.
                ts.reverse();
                for t in ts {
                    asc_bools.push(a);
                    match &t {
                        BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                            order_exprs.push(
                                col(c).struct_().field_by_name(&base_col_name(&t)).cast(
                                    DataType::Categorical(None, CategoricalOrdering::Lexical),
                                ),
                            );
                        }
                        BaseRDFNodeType::Literal(dt) => {
                            if string_rdf_literal(dt.as_ref()) {
                                order_exprs.push(
                                    col(c).struct_().field_by_name(&base_col_name(&t)).cast(
                                        DataType::Categorical(None, CategoricalOrdering::Lexical),
                                    ),
                                );
                            } else {
                                order_exprs
                                    .push(col(c).struct_().field_by_name(&base_col_name(&t)));
                            }
                        }
                        BaseRDFNodeType::None => {
                            order_exprs.push(col(c).struct_().field_by_name(&base_col_name(&t)));
                        }
                    }
                }
            }
        }
    }

    mappings = mappings.sort_by_exprs(
        order_exprs,
        SortMultipleOptions::default()
            .with_order_descending_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
            .with_nulls_last_multi(asc_bools.iter().map(|asc| !asc).collect::<Vec<bool>>())
            .with_maintain_order(false),
    );

    Ok(SolutionMappings::new(
        mappings,
        rdf_node_types,
        height_upper_bound,
    ))
}

pub fn project(
    solution_mappings: SolutionMappings,
    variables: &Vec<Variable>,
) -> Result<SolutionMappings, QueryProcessingError> {
    let SolutionMappings {
        mut mappings,
        rdf_node_types: mut datatypes,
        height_estimate: height_upper_bound,
    } = solution_mappings;
    let cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
    let mut new_datatypes = HashMap::new();
    for v in variables {
        if !datatypes.contains_key(v.as_str()) {
            warn!("The variable {} does not exist in the solution mappings, adding as an unbound variable", v);
            mappings = mappings.with_column(
                lit(LiteralValue::untyped_null())
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
    Ok(SolutionMappings::new(
        mappings,
        new_datatypes,
        height_upper_bound,
    ))
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
        height_estimate: height_upper_bound,
    } in mappings
    {
        for c in rdf_node_types.keys() {
            mappings = lf_column_to_categorical(
                mappings,
                c,
                rdf_node_types.get(c).unwrap(),
                CategoricalOrdering::Physical,
            );
        }
        cat_mappings.push(SolutionMappings::new(
            mappings,
            rdf_node_types,
            height_upper_bound,
        ));
    }
    mappings = cat_mappings;

    // Compute the target types
    let mut target_types = mappings.first().unwrap().rdf_node_types.clone();

    for m in &mappings[1..mappings.len()] {
        let mut updated_target_types = HashMap::new();
        let SolutionMappings {
            mappings: _,
            rdf_node_types: right_datatypes,
            ..
        } = m;
        for (right_col, right_type) in right_datatypes {
            if let Some(left_type) = target_types.get(right_col) {
                if left_type != right_type {
                    if let RDFNodeType::MultiType(left_types) = left_type {
                        let mut left_set: HashSet<_> = left_types.iter().collect();
                        if let RDFNodeType::MultiType(right_types) = right_type {
                            let right_set: HashSet<_> = right_types.iter().collect();
                            let mut union: Vec<_> =
                                left_set.union(&right_set).map(|x| (*x).clone()).collect();
                            union.sort();
                            updated_target_types
                                .insert(right_col.clone(), RDFNodeType::MultiType(union));
                        } else {
                            //Right not multi
                            let base_right = BaseRDFNodeType::from_rdf_node_type(right_type);
                            left_set.insert(&base_right);
                            let mut new_types: Vec<_> = left_set.into_iter().cloned().collect();
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
                            let mut new_types: Vec<_> = right_set.into_iter().cloned().collect();
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
    let mut new_height = 0usize;
    //Change the mappings
    for SolutionMappings {
        mut mappings,
        mut rdf_node_types,
        height_estimate: height_upper_bound,
    } in mappings
    {
        new_height = new_height.saturating_add(height_upper_bound);
        let mut new_multi = HashMap::new();
        for (c, t) in &rdf_node_types {
            let target_type = target_types.get(c).unwrap();
            if t != target_type && !matches!(t, RDFNodeType::MultiType(..)) {
                mappings = mappings.with_column(convert_lf_col_to_multitype(c, t));
                new_multi.insert(
                    c.clone(),
                    RDFNodeType::MultiType(vec![BaseRDFNodeType::from_rdf_node_type(t)]),
                );
            }
        }
        rdf_node_types.extend(new_multi);
        let (new_mappings, new_exploded_map) = unnest_multicols(mappings, &rdf_node_types);

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
            maintain_order: false,
        },
    )
    .expect("Concat problem");
    output_mappings = nest_multicolumns(output_mappings, exploded_map);
    Ok(SolutionMappings::new(
        output_mappings,
        target_types,
        new_height,
    ))
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
                            PossibleTypes::singular(BaseRDFNodeType::IRI),
                        )]));
                    }
                }
            }
            Function::IsBlank => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(BaseRDFNodeType::BlankNode),
                        )]));
                    }
                }
            }
            Function::IsLiteral => {
                if args.len() == 1 {
                    if let Expression::Variable(v) = args.first().unwrap() {
                        return Some(HashMap::from([(
                            v.as_str().to_string(),
                            PossibleTypes::singular(BaseRDFNodeType::Literal(
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

pub fn values_pattern(
    variables: &[Variable],
    bindings: &[Vec<Option<GroundTerm>>],
) -> SolutionMappings {
    let mut variable_datatype_opt_term_vecs: HashMap<usize, HashMap<BaseRDFNodeTypeRef, Vec<_>>> =
        HashMap::new();
    // Todo: this could be parallel.. but we have very small data here..
    for i in 0..variables.len() {
        variable_datatype_opt_term_vecs.insert(i, HashMap::new());
    }
    for (i, row) in bindings.iter().enumerate() {
        for (j, col) in row.iter().enumerate() {
            let map = variable_datatype_opt_term_vecs.get_mut(&j).unwrap();
            if let Some(gt) = col {
                let dt = get_ground_term_datatype_ref(gt);
                {
                    let vector = if let Some(vector) = map.get_mut(&dt) {
                        vector
                    } else {
                        map.insert(dt.clone(), iter::repeat(None).take(i).collect());
                        map.get_mut(&dt).unwrap()
                    };
                    //TODO: Stop copying data here!!
                    #[allow(unreachable_patterns)]
                    let term = match gt {
                        GroundTerm::NamedNode(nn) => Term::NamedNode(nn.clone()),
                        GroundTerm::Literal(l) => Term::Literal(l.clone()),
                        _ => unimplemented!(),
                    };
                    vector.push(Some(term));
                }
                for (k, v) in &mut *map {
                    if k != &dt {
                        v.push(None);
                    }
                }
            } else {
                for v in (*map).values_mut() {
                    v.push(None);
                }
            }
        }
    }

    let mut all_columns = vec![];
    let mut all_datatypes = HashMap::new();
    for (i, m) in variable_datatype_opt_term_vecs {
        let mut types_columns = vec![];
        for (t, v) in m {
            types_columns.push((
                t.clone().into_owned(),
                particular_opt_term_vec_to_series(v, t, "c").into_column(),
            ));
        }
        types_columns.sort_unstable_by(|(x, _), (y, _)| x.cmp(y));

        let (dt, column) = if types_columns.len() > 1 {
            let mut struct_exprs = vec![];
            let mut columns = vec![];
            let mut types = vec![];
            for (i, (t, mut c)) in types_columns.into_iter().enumerate() {
                let name = format!("c{i}");
                c.rename(PlSmallStr::from_str(&name));
                let tname = base_col_name(&t);
                if t.is_lang_string() {
                    struct_exprs.push(
                        col(&name)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .alias(LANG_STRING_VALUE_FIELD),
                    );
                    struct_exprs.push(
                        col(&name)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .alias(LANG_STRING_LANG_FIELD),
                    );
                } else {
                    struct_exprs.push(col(&name).alias(tname));
                }

                columns.push(c);
                types.push(t);
            }
            let t = RDFNodeType::MultiType(types);

            let mut df = DataFrame::new(columns)
                .unwrap()
                .lazy()
                .with_column(as_struct(struct_exprs).alias("struct"))
                .select([col("struct")])
                .rename(["struct"], [variables.get(i).unwrap().as_str()], true)
                .collect()
                .unwrap();
            let column = df
                .drop_in_place(variables.get(i).unwrap().as_str())
                .unwrap();
            (t, column)
        } else {
            let (t, mut column) = types_columns.pop().unwrap();
            column.rename(PlSmallStr::from_str(variables.get(i).unwrap().as_str()));
            (t.as_rdf_node_type(), column)
        };
        all_columns.push(column);
        all_datatypes.insert(variables.get(i).unwrap().as_str().to_string(), dt);
    }
    let varexpr: Vec<_> = variables.iter().map(|x| col(x.as_str())).collect();
    let df = DataFrame::new(all_columns)
        .unwrap()
        .lazy()
        .select(varexpr)
        .collect()
        .unwrap();

    EagerSolutionMappings::new(df, all_datatypes).as_lazy()
}
