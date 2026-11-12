use crate::errors::QueryProcessingError;
use crate::expressions::drop_inner_contexts;
use crate::expressions::functions::concat_::concat_;
use crate::expressions::functions::custom_function::custom_;
use crate::expressions::functions::datatype_::datatype_;
use crate::expressions::functions::date_part::{date_part, DatePart};
use crate::expressions::functions::encode_for_uri::encode_for_uri;
use crate::expressions::functions::hash_::{hash_, HashAlgorithm};
use crate::expressions::functions::iri::iri;
use crate::expressions::functions::is_blank_::is_blank_;
use crate::expressions::functions::is_iri::is_iri;
use crate::expressions::functions::is_literal::{is_literal, is_numeric};
use crate::expressions::functions::lang_::lang_;
use crate::expressions::functions::lang_matches::lang_matches;
use crate::expressions::functions::lower_upper_substr::lower_upper_substr;
use crate::expressions::functions::now_::now_;
use crate::expressions::functions::numeric_function::{numeric_function, NumericFunction};
use crate::expressions::functions::rand_::rand_;
use crate::expressions::functions::replace::sparql_replace;
use crate::expressions::functions::sparql_regex::sparql_regex;
use crate::expressions::functions::sparql_uuid::uuid;
use crate::expressions::functions::starts_ends_contains::starts_ends_contains;
use crate::expressions::functions::str_::str_;
use crate::expressions::functions::str_before_or_after::str_before_or_after;
use crate::expressions::functions::str_dt::str_dt;
use crate::expressions::functions::str_lang::str_lang;
use crate::expressions::functions::str_len::str_len;
use crate::expressions::functions::struuid::struuid;
use crate::udf::UdfRegistry;
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
    udf_registry: Option<&UdfRegistry>,
) -> Result<SolutionMappings, QueryProcessingError> {
    match func {
        Function::Year => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Year,
            )?;
        }
        Function::Month => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Month,
            )?;
        }
        Function::Day => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Day,
            )?;
        }
        Function::Hours => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Hours,
            )?;
        }
        Function::Minutes => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Minutes,
            )?;
        }
        Function::Seconds => {
            solution_mappings = date_part(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                DatePart::Seconds,
            )?;
        }
        Function::Abs | Function::Ceil | Function::Floor | Function::Round => {
            let f = match func {
                Function::Abs => NumericFunction::Abs,
                Function::Ceil => NumericFunction::Ceil,
                Function::Floor => NumericFunction::Floor,
                _ => NumericFunction::Round,
            };
            solution_mappings = numeric_function(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                f,
            )?;
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
        Function::Rand => {
            solution_mappings = rand_(solution_mappings, outer_context)?;
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
                udf_registry,
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
        Function::IsNumeric => {
            solution_mappings = is_numeric(solution_mappings, &args_contexts, outer_context)?;
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
        Function::Md5 | Function::Sha1 | Function::Sha256 | Function::Sha384 | Function::Sha512 => {
            let algorithm = match func {
                Function::Md5 => HashAlgorithm::Md5,
                Function::Sha1 => HashAlgorithm::Sha1,
                Function::Sha256 => HashAlgorithm::Sha256,
                Function::Sha384 => HashAlgorithm::Sha384,
                _ => HashAlgorithm::Sha512,
            };
            solution_mappings = hash_(
                solution_mappings,
                func,
                args,
                &args_contexts,
                outer_context,
                global_cats,
                algorithm,
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
