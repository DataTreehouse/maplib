use crate::errors::QueryProcessingError;
use oxrdf::vocab::xsd;
use polars::prelude::{as_struct, col, lit, Expr, IntoColumn, LiteralValue};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::set_struct_all_null_to_null_row;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
use representation::{
    literal_is_datetime, literal_time_zone, BaseRDFNodeType, RDFNodeState, SeriesBuilder,
};
use spargebra::algebra::{Expression, Function};
use std::collections::{HashMap, HashSet};
use tracing::warn;

/// Parses the lexical forms of a string column as literals of the type out_dt. With an offset,
/// only the dateTime values with that time zone offset are parsed, the others are null.
fn parse_expr(
    decoded_col: Expr,
    out_dt: &BaseRDFNodeType,
    offset: Option<Option<i16>>,
    name: &str,
) -> Expr {
    let out_dt_cloned = out_dt.clone();
    let out_polars_type = out_dt.default_input_polars_data_type();
    let name = name.to_string();
    decoded_col.map(
        move |x| {
            let mut builder = SeriesBuilder::new(&out_dt_cloned);
            let datatype = match &out_dt_cloned {
                BaseRDFNodeType::Literal(l, _) => Some(l.as_ref()),
                _ => None,
            };
            for v in x.str()?.iter() {
                let keep = match (v, offset, datatype) {
                    (Some(v), Some(o), Some(dt)) => literal_time_zone(v, dt) == o,
                    (Some(_), _, _) => true,
                    (None, _, _) => false,
                };
                if let (true, Some(v)) = (keep, v) {
                    // A lexical form that is not valid for the datatype is an error in SPARQL,
                    // which leaves this solution unbound rather than failing the query.
                    if let Err(e) = builder.parse_literal(v, None) {
                        warn!("STRDT leaves a value unbound: {}", e);
                        builder.push_none();
                    }
                } else {
                    builder.push_none();
                }
            }
            Ok(builder.into_series(&name).into_column())
        },
        move |_, f| {
            let mut field = f.clone();
            field.dtype = out_polars_type.clone();
            Ok(field)
        },
    )
}

pub fn str_dt(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: &HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    if args.len() != 2 {
        return Err(QueryProcessingError::BadNumberOfFunctionArguments(
            func.clone(),
            args.len(),
            "2".to_string(),
        ));
    }
    let first_context = args_contexts.get(&0).unwrap();
    let first_type = solution_mappings
        .rdf_node_types
        .get(first_context.as_str())
        .unwrap();
    let base_string = BaseRDFNodeType::Literal(xsd::STRING.into_owned(), None);
    let use_base = if first_type.is_lit_type(xsd::STRING) {
        Some((
            first_type.get_base_state().unwrap(),
            col(first_context.as_str()),
        ))
    } else if first_type.is_multi() {
        first_type.map.get(&base_string).map(|base_state| {
            (
                base_state,
                col(first_context.as_str())
                    .struct_()
                    .field_by_name(&base_string.field_col_name()),
            )
        })
    } else {
        None
    };
    if let Some((use_base_state, use_base_col)) = use_base {
        let dt_arg = args.get(1).unwrap();
        let dt_iri = if let Expression::NamedNode(nn) = &dt_arg {
            nn.clone()
        } else {
            return Err(QueryProcessingError::BadArgument(
                "STRDT second argument should be IRI".to_string(),
            ));
        };
        let decoded_col =
            maybe_decode_expr(use_base_col, &base_string, use_base_state, global_cats);
        // The time zone offset of a dateTime is a part of its type, so there is one type for
        // each offset among the values.
        let offsets: Vec<Option<i16>> = if literal_is_datetime(dt_iri.as_ref()) {
            let df = solution_mappings
                .mappings
                .clone()
                .select([decoded_col.clone().alias("lexical")])
                .collect()
                .map_err(|e| QueryProcessingError::BadArgument(e.to_string()))?;
            let lexicals = df
                .column("lexical")
                .unwrap()
                .str()
                .map_err(|e| QueryProcessingError::BadArgument(e.to_string()))?
                .clone();
            let mut offsets: Vec<_> = lexicals
                .iter()
                .flatten()
                .map(|v| literal_time_zone(v, dt_iri.as_ref()))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            offsets.sort();
            offsets
        } else {
            vec![]
        };
        let (expr, state) = if offsets.len() <= 1 {
            let offset = offsets.first().cloned().flatten();
            let out_dt = BaseRDFNodeType::Literal(dt_iri.clone(), offset);
            let expr = parse_expr(decoded_col, &out_dt, None, outer_context.as_str());
            (expr, out_dt.into_default_input_rdf_node_state())
        } else {
            let mut fields = vec![];
            let mut map = HashMap::new();
            for offset in offsets {
                let out_dt = BaseRDFNodeType::Literal(dt_iri.clone(), offset);
                let name = out_dt.field_col_name();
                fields.push(
                    parse_expr(decoded_col.clone(), &out_dt, Some(offset), &name).alias(&name),
                );
                let cat_state = out_dt.default_input_cat_state();
                map.insert(out_dt, cat_state);
            }
            let state = RDFNodeState::from_map(map);
            (
                set_struct_all_null_to_null_row(as_struct(fields), &state),
                state,
            )
        };
        solution_mappings.mappings = solution_mappings
            .mappings
            .with_column(expr.alias(outer_context.as_str()));
        solution_mappings
            .rdf_node_types
            .insert(outer_context.as_str().to_string(), state);
    } else {
        solution_mappings.mappings = solution_mappings.mappings.with_column(
            lit(LiteralValue::untyped_null())
                .cast(
                    BaseRDFNodeType::None
                        .into_default_input_rdf_node_state()
                        .polars_data_type(),
                )
                .alias(outer_context.as_str()),
        );
        solution_mappings.rdf_node_types.insert(
            outer_context.as_str().to_string(),
            BaseRDFNodeType::None.into_default_input_rdf_node_state(),
        );
    }
    Ok(solution_mappings)
}
