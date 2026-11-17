use crate::errors::QueryProcessingError;
use crate::expressions::functions::cast_iri_to_xsd_literal::cast_iri_to_xsd_literal;
use crate::expressions::functions::cast_literal_::cast_literal;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef};
use polars::prelude::{
    coalesce, col, lit, polars_err, ChunkUnique, Column, DataFrame, Engine, Expr, Field, IdxCa,
    IdxSize, IntoColumn, IntoLazy, IntoSeries, LiteralValue,
};
use representation::cats::LockedCats;
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState, LANG_STRING_VALUE_FIELD};
use std::collections::HashMap;

/// Whether `nn(x)` is a cast to an XSD datatype (SPARQL 1.1 17.5).
pub fn is_xsd_cast_function(nn: NamedNodeRef) -> bool {
    matches!(
        nn,
        xsd::INT
            | xsd::LONG
            | xsd::INTEGER
            | xsd::BOOLEAN
            | xsd::UNSIGNED_LONG
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_BYTE
            | xsd::BYTE
            | xsd::SHORT
            | xsd::DECIMAL
            | xsd::DOUBLE
            | xsd::FLOAT
            | xsd::STRING
            | xsd::DATE_TIME
            | xsd::DATE
            | xsd::DURATION
            | xsd::YEAR_MONTH_DURATION
            | xsd::DAY_TIME_DURATION
            | xsd::TIME
    )
}

/// Casts of a dictionary-encoded literal column, e.g. `xsd:float(xsd:string(?price))` with
/// `targets` [xsd:string, xsd:float], evaluated once for each distinct code in the column and
/// gathered back by code: an expression giving the last target's column, and its state.
///
/// Casting row by row decodes every row into a string first, and a cast from xsd:string builds
/// several more string columns (a pattern check, a stripped "+", the parse). Over BSBM BI Q4's
/// million feature × offer rows, which repeat 2,357 prices, that took about 40 MB. `None` if
/// the column is not a single dictionary-encoded literal type.
pub fn xsd_cast_chain_on_codes(
    c: &str,
    src: &RDFNodeState,
    targets: &[NamedNode],
    global_cats: LockedCats,
) -> Option<(Expr, RDFNodeState)> {
    let src_type = src.get_base_type()?;
    if !matches!(src.get_base_state()?, BaseCatState::CategoricalNative(_))
        || !matches!(src_type, BaseRDFNodeType::Literal(..))
        || src_type.is_lang_string()
    {
        return None;
    }
    let last = BaseRDFNodeType::Literal(targets.last()?.clone(), None);
    let out_dtype = last.default_input_polars_data_type();
    let out_state = last.into_default_input_rdf_node_state();
    let src = src.clone();
    let targets = targets.to_vec();
    let expr = col(c).map(
        move |codes: Column| {
            let name = codes.name().clone();
            let codes = codes.as_materialized_series().u32()?.clone();
            let distinct = codes.unique()?;
            let mut lf = DataFrame::new(
                distinct.len(),
                vec![distinct
                    .clone()
                    .into_series()
                    .with_name("v".into())
                    .into_column()],
            )?
            .lazy();
            let mut state = src.clone();
            for nn in &targets {
                let trg = BaseRDFNodeType::Literal(nn.clone(), None);
                let cast = xsd_cast_literal("v", &state, &trg, global_cats.clone())
                    .map_err(|e| polars_err!(ComputeError: "{e}"))?;
                lf = lf.with_column(cast.alias("v"));
                state = trg.into_default_input_rdf_node_state();
            }
            // Evaluated inside the running query, possibly on a Rayon worker, where only the
            // in-memory engine is safe (see utils::polars::collect_in_memory).
            let values = lf
                .collect_with_engine(Engine::InMemory)?
                .unwrap_single()
                .column("v")?
                .as_materialized_series()
                .clone();
            let positions: HashMap<u32, IdxSize> = distinct
                .iter()
                .enumerate()
                .filter_map(|(i, code)| code.map(|code| (code, i as IdxSize)))
                .collect();
            let idx: IdxCa = codes
                .iter()
                .map(|code| code.and_then(|code| positions.get(&code).copied()))
                .collect();
            Ok(values.take(&idx)?.with_name(name).into_column())
        },
        move |_, f| Ok(Field::new(f.name().clone(), out_dtype.clone())),
    );
    Some((expr, out_state))
}

pub fn xsd_cast_literal(
    c: &str,
    src: &RDFNodeState,
    trg: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Result<Expr, QueryProcessingError> {
    let trg_type = trg.default_input_polars_data_type();
    let trg_nn = if let BaseRDFNodeType::Literal(nn, _) = trg {
        nn.as_ref()
    } else {
        panic!("Invalid state")
    };
    if src.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &src.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => cast_iri_to_xsd_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    trg_nn,
                    trg_type.clone(),
                    global_cats.clone(),
                )?,
                // Casting a blank node is an error, which leaves the result unbound
                BaseRDFNodeType::BlankNode => {
                    lit(LiteralValue::untyped_null()).cast(trg_type.clone())
                }
                BaseRDFNodeType::Literal(src_nn, _) => cast_literal(
                    col(c).struct_().field_by_name(&t.field_col_name()),
                    t,
                    s,
                    global_cats.clone(),
                    src_nn.as_ref(),
                    trg_nn,
                    trg_type.clone(),
                ),
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(trg_type.clone()),
            });
        }
        Ok(coalesce(to_coalesce.as_slice()).alias(c))
    } else {
        let t = src.get_base_type().unwrap();
        let s = src.get_base_state().unwrap();
        match &t {
            BaseRDFNodeType::IRI => {
                cast_iri_to_xsd_literal(col(c), t, s, trg_nn, trg_type.clone(), global_cats.clone())
            }
            BaseRDFNodeType::BlankNode => Ok(lit(LiteralValue::untyped_null()).cast(trg_type)),
            BaseRDFNodeType::Literal(src_nn, _) => Ok(cast_literal(
                // A language-tagged string is a struct of its value and its language; its value
                // is cast. In a column of several types, above, the value is a field of its own.
                if t.is_lang_string() {
                    col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD)
                } else {
                    col(c)
                },
                t,
                s,
                global_cats.clone(),
                src_nn.as_ref(),
                trg_nn,
                trg_type.clone(),
            )),
            BaseRDFNodeType::None => Ok(lit(LiteralValue::untyped_null()).cast(trg_type)),
        }
    }
}
