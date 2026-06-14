mod abs_;
mod cast_iri_to_xsd_literal;
mod cast_literal_;
mod ceil_;
mod concat_;
mod create_regex_expr;
mod create_regex_replace_expr;
mod create_regex_string;
mod custom_function;
mod datatype_;
mod day_;
mod encode_for_uri;
mod eval_expression_to_string;
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
mod maybe_add_regex_feature_flags;
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
mod str_before;
mod str_before_or_after;
mod str_dt;
mod str_function;
mod str_lang;
mod str_len;
mod str_starts_ends_contains_;
mod struuid;
mod struuid_v5;
mod uuid_v5;
mod xsd_cast_literal;
mod year_;

use crate::errors::QueryProcessingError;
use crate::expressions::drop_inner_contexts;
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
use polars::datatypes::Field;
use polars::error::PolarsError;
use polars::prelude::{Column, IntoColumn, Schema, Series};
use representation::cats::LockedCats;
use representation::query_context::Context;
use representation::solution_mapping::SolutionMappings;
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
