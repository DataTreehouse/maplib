mod abs_;
mod ceil_;
mod concat_;
mod create_regex_expr;
mod create_regex_replace_expr;
mod create_regex_string;
mod custom_function;
mod datatype_;
mod day_;
mod encode_for_uri;
mod eval_uuid_namespace;
mod floor_;
mod hours_;
mod iri;
mod is_blank_;
mod is_iri;
mod is_literal;
mod lang_;
mod lang_matches;
mod lower_upper_substr;
mod md5_;
mod minutes_;
mod month_;
mod now_;
mod replace;
mod round_;
mod seconds_;
mod sha1_;
mod sparql_regex;
mod sparql_uuid;
mod starts_ends_contains;
mod str_;
mod str_before_or_after;
mod str_dt;
mod str_lang;
mod str_len;
mod struuid;
mod struuid_v5;
mod uuid_v5;
mod year_;

use crate::errors::QueryProcessingError;
use crate::expressions::functions::abs_::abs_;
use crate::expressions::functions::ceil_::ceil_;
use crate::expressions::functions::concat_::concat_;
use crate::expressions::functions::custom_function::custom_;
use crate::expressions::functions::datatype_::datatype_;
use crate::expressions::functions::day_::day_;
use crate::expressions::functions::encode_for_uri::encode_for_uri;
use crate::expressions::functions::floor_::floor_;
use crate::expressions::functions::hours_::hours_;
use crate::expressions::functions::iri::iri;
use crate::expressions::functions::is_blank_::is_blank_;
use crate::expressions::functions::is_iri::is_iri;
use crate::expressions::functions::is_literal::is_literal;
use crate::expressions::functions::lang_::lang_;
use crate::expressions::functions::lang_matches::lang_matches;
use crate::expressions::functions::lower_upper_substr::lower_upper_substr;
use crate::expressions::functions::md5_::md5_;
use crate::expressions::functions::minutes_::minutes_;
use crate::expressions::functions::month_::month_;
use crate::expressions::functions::now_::now_;
use crate::expressions::functions::replace::sparql_replace;
use crate::expressions::functions::round_::round_;
use crate::expressions::functions::seconds_::seconds_;
use crate::expressions::functions::sha1_::sha1_;
use crate::expressions::functions::sparql_regex::sparql_regex;
use crate::expressions::functions::sparql_uuid::uuid;
use crate::expressions::functions::starts_ends_contains::starts_ends_contains;
use crate::expressions::functions::str_::str_;
use crate::expressions::functions::str_before_or_after::str_before_or_after;
use crate::expressions::functions::str_dt::str_dt;
use crate::expressions::functions::str_lang::str_lang;
use crate::expressions::functions::str_len::str_len;
use crate::expressions::functions::struuid::struuid;
use crate::expressions::functions::year_::year_;
use crate::expressions::{cast_lang_string_to_string, drop_inner_contexts};
use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::datatypes::{DataType, Field};
use polars::error::PolarsError;
use polars::prelude::{
    coalesce, col, lit, Column, Expr, IntoColumn, LiteralValue, Schema, Series, StrptimeOptions,
};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT};
use representation::query_context::Context;
use representation::solution_mapping::{BaseCatState, SolutionMappings};
use representation::{BaseRDFNodeType, RDFNodeState};
use spargebra::algebra::{Expression, Function};
use std::collections::HashMap;

pub fn func_expression(
    mut solution_mappings: SolutionMappings,
    func: &Function,
    args: &[Expression],
    args_contexts: HashMap<usize, Context>,
    outer_context: &Context,
    global_cats: LockedCats,
) -> Result<SolutionMappings, QueryProcessingError> {
    match func {
        Function::Year => {
            solution_mappings =
                year_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Month => {
            solution_mappings =
                month_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Day => {
            solution_mappings = day_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Hours => {
            solution_mappings =
                hours_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Minutes => {
            solution_mappings =
                minutes_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Seconds => {
            solution_mappings =
                seconds_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Abs => {
            solution_mappings = abs_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Ceil => {
            solution_mappings =
                ceil_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Floor => {
            solution_mappings =
                floor_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Concat => {
            solution_mappings = concat_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Now => {
            solution_mappings = now_(solution_mappings, outer_context)?;
        }
        Function::Round => {
            solution_mappings =
                round_(solution_mappings, func, args, &args_contexts, outer_context)?;
        }
        Function::Str => {
            solution_mappings = str_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Lang => {
            solution_mappings = lang_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::LangMatches => {
            solution_mappings = lang_matches(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Regex => {
            solution_mappings = sparql_regex(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Uuid => {
            solution_mappings = uuid(solution_mappings, func, args, outer_context)?;
        }
        Function::Iri => {
            solution_mappings = iri(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrUuid => {
            solution_mappings = struuid(solution_mappings, func, args, outer_context)?;
        }
        Function::Replace => {
            solution_mappings = sparql_replace(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Custom(nn) => {
            solution_mappings = custom_(
                nn,
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrDt => {
            solution_mappings = str_dt(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrBefore | Function::StrAfter => {
            solution_mappings = str_before_or_after(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrLang => {
            solution_mappings = str_lang(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrLen => {
            solution_mappings = str_len(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::LCase | Function::UCase | Function::SubStr => {
            solution_mappings = lower_upper_substr(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::StrStarts | Function::StrEnds | Function::Contains => {
            solution_mappings = starts_ends_contains(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::IsBlank => {
            solution_mappings = is_blank_(solution_mappings, &args_contexts, outer_context)?;
        }
        Function::IsIri => {
            solution_mappings = is_iri(solution_mappings, &args_contexts, outer_context)?;
        }
        Function::IsLiteral => {
            solution_mappings = is_literal(solution_mappings, &args_contexts, outer_context)?;
        }
        Function::Datatype => {
            solution_mappings = datatype_(solution_mappings, &args_contexts, outer_context)?;
        }
        Function::EncodeForUri => {
            solution_mappings = encode_for_uri(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Sha1 => {
            solution_mappings = sha1_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        Function::Md5 => {
            solution_mappings = md5_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
            )?;
        }
        _ => {
            return Err(QueryProcessingError::UnimplementedFunction(
                func.to_string(),
            ))
        }
    }
    solution_mappings = drop_inner_contexts(solution_mappings, &args_contexts.values().collect());
    Ok(solution_mappings)
}

fn eval_expression_to_string(
    sparql_expression: &Expression,
    expect_string: bool,
) -> Result<String, QueryProcessingError> {
    if let Expression::Literal(l) = sparql_expression {
        if expect_string && l.datatype() != xsd::STRING {
            Err(QueryProcessingError::ExpectedConstantLiteralStringArgument(
                sparql_expression.clone(),
            ))
        } else {
            Ok(l.value().to_string())
        }
    } else if let Expression::NamedNode(nn) = sparql_expression {
        Ok(nn.as_str().to_string())
    } else if let Expression::FunctionCall(f, args) = sparql_expression {
        match f {
            Function::Str => eval_expression_to_string(args.get(0).unwrap(), false),
            _ => {
                return Err(QueryProcessingError::ExpectedConstantLiteralArgument(
                    sparql_expression.clone(),
                ))
            }
        }
    } else {
        return Err(QueryProcessingError::ExpectedConstantLiteralArgument(
            sparql_expression.clone(),
        ));
    }
}

pub fn str_function(c: &str, t: &RDFNodeState, global_cats: LockedCats) -> Expr {
    if t.is_multi() {
        let mut to_coalesce = vec![];
        for (t, s) in &t.map {
            to_coalesce.push(match t {
                BaseRDFNodeType::IRI => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_IRI_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::BlankNode => maybe_decode_expr(
                    col(c).struct_().field_by_name(MULTI_BLANK_DT),
                    t,
                    s,
                    global_cats.clone(),
                ),
                BaseRDFNodeType::Literal(_) => {
                    if t.is_lang_string() {
                        cast_lang_string_to_string(c, t, s, global_cats.clone())
                    } else {
                        maybe_decode_expr(
                            col(c).struct_().field_by_name(&t.field_col_name()),
                            t,
                            s,
                            global_cats.clone(),
                        )
                        .cast(DataType::String)
                    }
                }
                BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
            })
        }
        coalesce(to_coalesce.as_slice()).alias(c)
    } else {
        let b = t.get_base_type().unwrap();
        let s = t.get_base_state().unwrap();
        match b {
            BaseRDFNodeType::IRI | BaseRDFNodeType::BlankNode => {
                maybe_decode_expr(col(c), b, s, global_cats)
            }
            BaseRDFNodeType::Literal(_) => {
                if t.is_lang_string() {
                    cast_lang_string_to_string(c, b, s, global_cats)
                } else {
                    maybe_decode_expr(col(c), b, s, global_cats).cast(DataType::String)
                }
            }
            BaseRDFNodeType::None => lit(LiteralValue::untyped_null()).cast(DataType::String),
        }
    }
}

pub fn xsd_cast_literal(
    c: &str,
    src: &RDFNodeState,
    trg: &BaseRDFNodeType,
    global_cats: LockedCats,
) -> Result<Expr, QueryProcessingError> {
    let trg_type = trg.default_input_polars_data_type();
    let trg_nn = if let BaseRDFNodeType::Literal(nn) = trg {
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
                BaseRDFNodeType::BlankNode => {
                    return Err(QueryProcessingError::BadCastDatatype(
                        c.to_string(),
                        trg.clone(),
                        t.clone(),
                    ))
                }
                BaseRDFNodeType::Literal(src_nn) => cast_literal(
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
            BaseRDFNodeType::BlankNode => Err(QueryProcessingError::BadCastDatatype(
                c.to_string(),
                trg.clone(),
                t.clone(),
            )),
            BaseRDFNodeType::Literal(src_nn) => Ok(cast_literal(
                col(c),
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

fn cast_iri_to_xsd_literal(
    e: Expr,
    t: &BaseRDFNodeType,
    s: &BaseCatState,
    trg_nn: NamedNodeRef,
    trg_type: DataType,
    global_cats: LockedCats,
) -> Result<Expr, QueryProcessingError> {
    if trg_nn == xsd::STRING {
        Ok(maybe_decode_expr(e, t, s, global_cats))
    } else {
        Ok(lit(LiteralValue::untyped_null()).cast(trg_type.clone()))
    }
}

fn cast_literal(
    mut c: Expr,
    src_bt: &BaseRDFNodeType,
    src_bs: &BaseCatState,
    global_cats: LockedCats,
    src: NamedNodeRef,
    trg: NamedNodeRef,
    trg_type: DataType,
) -> Expr {
    if src == xsd::STRING && trg != xsd::STRING {
        c = maybe_decode_expr(c, src_bt, src_bs, global_cats);
    }
    if src == xsd::STRING && trg == xsd::BOOLEAN {
        c.cast(DataType::String)
            .str()
            .to_lowercase()
            .eq(lit("true"))
    } else if src == xsd::STRING && trg == xsd::DATE_TIME {
        c.cast(DataType::String).str().to_datetime(
            None,
            None,
            StrptimeOptions {
                format: None,
                strict: true,
                exact: false,
                cache: false,
            },
            lit("raise"),
        )
    } else if src == xsd::STRING && trg == xsd::DATE {
        c.cast(DataType::String).str().to_date(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::TIME {
        c.cast(DataType::String).str().to_time(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::DURATION {
        //Todo handle durations
        c
    } else {
        c.cast(trg_type)
    }
}

pub fn maybe_add_regex_feature_flags(pattern: &str, flags: Option<&str>) -> String {
    if let Some(flags) = flags {
        //TODO: Validate flags..
        format!("(?{}){}", flags, pattern)
    } else {
        pattern.to_string()
    }
}

pub fn str_starts_ends_contains(expr_decoded: Expr, second_decoded: Expr, f: &Function) -> Expr {
    match f {
        Function::StrStarts => expr_decoded.str().starts_with(second_decoded),
        Function::StrEnds => expr_decoded.str().ends_with(second_decoded),
        Function::Contains => expr_decoded.str().contains_literal(second_decoded),
        _ => unreachable!("Should never happen"),
    }
}

fn str_before(c: Column, s: String) -> Result<Column, PolarsError> {
    let bef = c.str()?.iter().map(|x: Option<&str>| {
        if let Some(x) = x {
            let range_to = x.find(&s);
            if let Some(range_to) = range_to {
                Some(&x[0..range_to])
            } else {
                Some(x)
            }
        } else {
            None
        }
    });
    let mut ser = Series::from_iter(bef);
    ser.rename(c.name().clone());
    Ok(ser.into_column())
}

fn str_after(c: Column, s: String) -> Result<Column, PolarsError> {
    let bef = c.str()?.iter().map(|x: Option<&str>| {
        if let Some(x) = x {
            let range_to = x.find(&s);
            if let Some(range_to) = range_to {
                Some(&x[range_to + s.len()..])
            } else {
                Some(x)
            }
        } else {
            None
        }
    });
    let mut ser = Series::from_iter(bef);
    ser.rename(c.name().clone());
    Ok(ser.into_column())
}

fn keep_field(_s: &Schema, f: &Field) -> Result<Field, PolarsError> {
    Ok(f.clone())
}
