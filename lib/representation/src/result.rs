use crate::cats::{Cats, LockedCats};
use crate::dataset::NamedGraph;
use crate::debug::DebugOutputs;
use crate::errors::RepresentationError;
use crate::multitype::{nest_multicolumns, unnest_multicols};
use crate::null::expr_is_null_workaround;
use crate::polars_to_rdf::{df_as_result, QuerySolutions};
use crate::query_context::Context;
use crate::rdf_to_polars::{rdf_literal_to_polars_expr, rdf_named_node_to_polars_literal_value};
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{BaseRDFNodeType, RDFNodeState, OBJECT_COL_NAME, PREDICATE_COL_NAME, SUBJECT_COL_NAME};
use oxrdf::{BlankNode, NamedNode, NamedOrBlankNode, Term, Triple, Variable};
use oxttl::TurtleSerializer;
use polars::prelude::{
    as_struct, by_name, col, lit, Expr, IntoLazy, LazyFrame, LiteralValue, PlSmallStr,
};
use polars_core::datatypes::DataType;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{ExplodeOptions, Series};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spargebra::term::{
    GraphNamePattern, GroundQuadPattern, NamedNodePattern, QuadPattern, TermPattern,
};
use std::collections::{HashMap, HashSet};

pub struct QueryResult {
    pub kind: QueryResultKind,
    pub debug: Option<DebugOutputs>,
    pub pushdown_paths: Vec<Context>,
}

#[derive(Debug)]
pub enum QueryResultKind {
    Select(EagerSolutionMappings),
    Construct(Vec<(EagerSolutionMappings, Option<NamedNode>)>),
}

impl QueryResultKind {
    pub fn json(&self, global_cats: LockedCats) -> String {
        match self {
            QueryResultKind::Select(sm) => {
                let QuerySolutions {
                    variables,
                    solutions,
                } = df_as_result(sm, global_cats);
                let json_serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json);
                let mut buffer = vec![];
                let mut serializer = json_serializer
                    .serialize_solutions_to_writer(&mut buffer, variables.clone())
                    .unwrap();
                for s in solutions {
                    let mut solvec = vec![];
                    for (i, t) in s.iter().enumerate() {
                        if let Some(t) = t {
                            solvec.push((variables.get(i).unwrap(), t.as_ref()));
                        }
                    }
                    serializer.serialize(solvec).unwrap()
                }
                serializer.finish().unwrap();
                String::from_utf8(buffer).unwrap()
            }
            QueryResultKind::Construct(c) => {
                let mut ser = TurtleSerializer::new().for_writer(vec![]);
                for (sm, predicate) in c {
                    let mut sm = sm.clone();
                    if let Some(predicate) = predicate {
                        sm.mappings = sm
                            .mappings
                            .lazy()
                            .with_column(
                                lit(rdf_named_node_to_polars_literal_value(predicate))
                                    .alias(PREDICATE_COL_NAME),
                            )
                            .select([
                                col(SUBJECT_COL_NAME),
                                col(PREDICATE_COL_NAME),
                                col(OBJECT_COL_NAME),
                            ])
                            .collect()
                            .unwrap();
                        sm.rdf_node_types.insert(
                            PREDICATE_COL_NAME.to_string(),
                            BaseRDFNodeType::IRI.into_default_input_rdf_node_state(),
                        );
                    }
                    let QuerySolutions {
                        variables,
                        solutions,
                    } = df_as_result(&sm, global_cats.clone());
                    for s in solutions {
                        let mut subject = None;
                        let mut predicate = None;
                        let mut object = None;
                        for (i, t) in s.into_iter().enumerate() {
                            let t = t.unwrap();
                            let v = variables.get(i).unwrap();
                            if v.as_str() == SUBJECT_COL_NAME {
                                subject = Some(match t {
                                    Term::NamedNode(nn) => NamedOrBlankNode::NamedNode(nn),
                                    Term::BlankNode(bl) => NamedOrBlankNode::BlankNode(bl),
                                    _ => todo!(),
                                });
                            } else if v.as_str() == PREDICATE_COL_NAME {
                                predicate = Some(match t {
                                    Term::NamedNode(nn) => nn,
                                    _ => panic!("Should never happen"),
                                });
                            } else if v.as_str() == OBJECT_COL_NAME {
                                object = Some(t);
                            } else {
                                panic!("Should never happen");
                            }
                        }
                        let t = Triple::new(subject.unwrap(), predicate.unwrap(), object.unwrap());
                        ser.serialize_triple(t.as_ref()).unwrap();
                    }
                }
                let res = ser.finish().unwrap();
                String::from_utf8(res).unwrap()
            }
        }
    }
}

pub fn template_to_solution_mappings(
    mut df: DataFrame,
    mut rdf_node_types: HashMap<String, RDFNodeState>,
    template: &Vec<QuadPattern>,
    global_cats: LockedCats,
) -> Result<HashMap<NamedGraph, Vec<(EagerSolutionMappings, Option<NamedNode>)>>, RepresentationError>
{
    df = mint_blank_nodes(df, &mut rdf_node_types, template);
    let mut solutions_map: HashMap<_, Vec<_>> = HashMap::new();
    for qp in template {
        let cats = global_cats.read()?;
        if let Some((sm, predicate, graph)) =
            quad_patterns_to_solution_mappings(&df, &rdf_node_types, qp, None, true, &cats)?
        {
            if let Some(solutions) = solutions_map.get_mut(&graph) {
                solutions.push((sm, predicate));
            } else {
                solutions_map.insert(graph, vec![(sm, predicate)]);
            }
        }
    }
    Ok(solutions_map)
}

pub fn ground_quad_pattern_to_quad_pattern(qp: &GroundQuadPattern) -> QuadPattern {
    let subject = TermPattern::from(qp.subject.clone());
    let predicate = qp.predicate.clone();
    let object = TermPattern::from(qp.object.clone());
    let graph_name = qp.graph_name.clone();
    QuadPattern {
        subject,
        predicate,
        object,
        graph_name,
    }
}
pub fn quad_patterns_to_solution_mappings(
    df: &DataFrame,
    rdf_node_types: &HashMap<String, RDFNodeState>,
    quad_pattern: &QuadPattern,
    preserve_column: Option<&str>,
    unique: bool,
    global_cats: &Cats,
) -> Result<Option<(EagerSolutionMappings, Option<NamedNode>, NamedGraph)>, RepresentationError> {
    let graph = match &quad_pattern.graph_name {
        GraphNamePattern::NamedNode(nn) => NamedGraph::from_maybe_named_node(Some(nn)),
        GraphNamePattern::DefaultGraph => NamedGraph::DefaultGraph,
        GraphNamePattern::Variable(_) => {
            todo!()
        }
    };
    let mut select_expr = vec![];
    let mut triple_types = HashMap::new();
    if let Some(preserve_column) = preserve_column {
        select_expr.push(col(preserve_column));
        triple_types.insert(
            preserve_column.to_string(),
            rdf_node_types.get(preserve_column).unwrap().clone(),
        );
    }
    let (subj_expr, subj_dt) = term_pattern_expression(
        rdf_node_types,
        &quad_pattern.subject,
        SUBJECT_COL_NAME,
        global_cats,
        true,
    )?;
    triple_types.insert(SUBJECT_COL_NAME.to_string(), subj_dt);
    let mut filter_expr = expr_is_null_workaround(
        col(SUBJECT_COL_NAME),
        triple_types.get(SUBJECT_COL_NAME).unwrap(),
    )
    .not();
    select_expr.push(subj_expr);
    let predicate = match &quad_pattern.predicate {
        NamedNodePattern::NamedNode(predicate) => Some(predicate.clone()),
        NamedNodePattern::Variable(_) => {
            let (predicate_expr, predicate_dt) = named_node_pattern_expr(
                rdf_node_types,
                &quad_pattern.predicate,
                PREDICATE_COL_NAME,
                global_cats,
            )?;
            triple_types.insert(PREDICATE_COL_NAME.to_string(), predicate_dt);
            filter_expr = filter_expr.and(
                expr_is_null_workaround(
                    col(PREDICATE_COL_NAME),
                    triple_types.get(PREDICATE_COL_NAME).unwrap(),
                )
                .not(),
            );
            select_expr.push(predicate_expr);
            None
        }
    };

    let (obj_expr, obj_dt) = term_pattern_expression(
        rdf_node_types,
        &quad_pattern.object,
        OBJECT_COL_NAME,
        global_cats,
        false,
    )?;
    select_expr.push(obj_expr);
    triple_types.insert(OBJECT_COL_NAME.to_string(), obj_dt);
    filter_expr = filter_expr.and(
        expr_is_null_workaround(
            col(OBJECT_COL_NAME),
            triple_types.get(OBJECT_COL_NAME).unwrap(),
        )
        .not(),
    );
    let mut lf = df.clone().lazy().select(select_expr).filter(filter_expr);
    if unique {
        lf = unique_workaround(lf, &triple_types, None, false, UniqueKeepStrategy::Any);
    }
    let df = lf.collect().unwrap();
    if df.height() > 0 {
        Ok(Some((
            EagerSolutionMappings::new(df, triple_types),
            predicate,
            graph,
        )))
    } else {
        Ok(None)
    }
}

fn term_pattern_expression(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    tp: &TermPattern,
    name: &str,
    global_cats: &Cats,
    subject: bool,
) -> Result<(Expr, RDFNodeState), RepresentationError> {
    match tp {
        TermPattern::NamedNode(nn) => Ok(named_node_u32_lit(nn, name, global_cats)),
        TermPattern::BlankNode(bl) => Ok(blank_node_expresson(rdf_node_types, bl, name)?),
        TermPattern::Literal(thelit) => {
            let l = rdf_literal_to_polars_expr(thelit)?.alias(name);
            Ok((
                l,
                BaseRDFNodeType::Literal(thelit.datatype().into_owned())
                    .into_default_input_rdf_node_state(),
            ))
        }
        TermPattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name, subject)?),
    }
}

fn named_node_pattern_expr(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    nnp: &NamedNodePattern,
    name: &str,
    global_cats: &Cats,
) -> Result<(Expr, RDFNodeState), RepresentationError> {
    match nnp {
        NamedNodePattern::NamedNode(nn) => Ok(named_node_u32_lit(nn, name, global_cats)),
        NamedNodePattern::Variable(v) => Ok(variable_expression(rdf_node_types, v, name, false)?),
    }
}

fn named_node_u32_lit(nn: &NamedNode, name: &str, global_cats: &Cats) -> (Expr, RDFNodeState) {
    let (u, state) = global_cats.encode_iri_or_local_cat(nn.as_str());
    (lit(u).cast(DataType::UInt32).alias(name), state)
}

fn variable_expression(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    v: &Variable,
    name: &str,
    subject: bool,
) -> Result<(Expr, RDFNodeState), RepresentationError> {
    if let Some(t) = rdf_node_types.get(v.as_str()) {
        if subject {
            let mut new_map = HashMap::new();
            let mut exprs = vec![];
            for (bt, bs) in &t.map {
                if matches!(bt, BaseRDFNodeType::BlankNode | BaseRDFNodeType::IRI) {
                    new_map.insert(bt.clone(), bs.clone());
                    if t.is_multi() {
                        exprs.push(
                            col(v.as_str())
                                .struct_()
                                .field_by_name(&bt.field_col_name()),
                        );
                    } else {
                        exprs.push(col(v.as_str()))
                    }
                }
            }
            if exprs.is_empty() {
                let bt = BaseRDFNodeType::None;
                let bs = bt.default_input_cat_state();
                let e = lit(LiteralValue::untyped_null())
                    .cast(bt.polars_data_type(&bs, true))
                    .alias(name);
                let s = RDFNodeState::from_bases(bt, bs);
                Ok((e, s))
            } else if exprs.len() == 1 {
                Ok((
                    exprs.pop().unwrap().alias(name),
                    RDFNodeState::from_map(new_map),
                ))
            } else {
                Ok((
                    as_struct(exprs).alias(name),
                    RDFNodeState::from_map(new_map),
                ))
            }
        } else {
            Ok((col(v.as_str()).alias(name), t.clone()))
        }
    } else {
        Err(RepresentationError::ConstructWithUndefinedVariable(
            v.as_str().to_string(),
        ))
    }
}

fn blank_node_expresson(
    rdf_node_types: &HashMap<String, RDFNodeState>,
    b: &BlankNode,
    name: &str,
) -> Result<(Expr, RDFNodeState), RepresentationError> {
    if let Some(t) = rdf_node_types.get(b.to_string().as_str()) {
        Ok((col(b.to_string()).alias(name), t.clone()))
    } else {
        Err(RepresentationError::ConstructWithUndefinedBlankNode(
            b.to_string(),
        ))
    }
}

fn mint_blank_nodes(
    df: DataFrame,
    rdf_node_types: &mut HashMap<String, RDFNodeState>,
    template: &Vec<QuadPattern>,
) -> DataFrame {
    let height = df.height();
    let mut blanks = HashSet::new();
    for t in template {
        if let TermPattern::BlankNode(bn) = &t.subject {
            blanks.insert(bn.to_string());
        }
        if let TermPattern::BlankNode(bn) = &t.object {
            blanks.insert(bn.to_string());
        }
    }
    let mut lf = df.lazy();
    for b in blanks {
        let mut blank_ser = Series::from_iter(
            (0..height)
                .into_par_iter()
                .map(|_| format!("b{}", uuid::Uuid::new_v4()))
                .collect::<Vec<_>>(),
        );
        blank_ser.rename(PlSmallStr::from_str(&b));
        lf = lf.with_column(lit(blank_ser).explode(ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        }));
        rdf_node_types.insert(
            b,
            RDFNodeState::from_bases(BaseRDFNodeType::BlankNode, BaseCatState::String),
        );
    }
    lf.collect().unwrap()
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
        lf = lf.unique_stable(
            unique_set.map(|x| by_name(x, true, false)),
            unique_keep_strategy,
        );
    } else {
        lf = lf.unique(
            unique_set.map(|x| by_name(x, true, false)),
            unique_keep_strategy,
        );
    }
    lf = nest_multicolumns(lf, maps);
    lf
}
