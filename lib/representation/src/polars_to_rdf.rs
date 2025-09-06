use crate::cats::{decode_column, Cats};
use crate::errors::RepresentationError;
use crate::multitype::{
    extract_column_from_multitype, MULTI_BLANK_DT, MULTI_IRI_DT, MULTI_NONE_DT,
};
use crate::rdf_to_polars::{
    polars_literal_values_to_series, rdf_literal_to_polars_literal_value,
    rdf_owned_blank_node_to_polars_literal_value, rdf_owned_named_node_to_polars_literal_value,
};
use crate::solution_mapping::{BaseCatState, EagerSolutionMappings};
use crate::{
    literal_blanknode_to_blanknode, literal_iri_to_namednode, BaseRDFNodeType, BaseRDFNodeTypeRef,
    RDFNodeState, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME,
    SUBJECT_COL_NAME,
};
use chrono::TimeZone as ChronoTimeZone;
use chrono::{Datelike, Timelike};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, Subject, Triple, Variable};
use polars::prelude::{
    as_struct, col, AnyValue, Column, DataFrame, DataType, IntoColumn, IntoLazy, LiteralValue,
    PlSmallStr, Scalar, Series, TimeUnit, TimeZone,
};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelRefIterator;
use spargebra::term::Term;
use std::collections::HashSet;
use std::sync::Arc;
use std::vec::IntoIter;

pub const XSD_DATETIME_WITHOUT_TZ_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.f";
pub const XSD_DATETIME_WITH_TZ_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.f%:z";
pub const XSD_DATE_WITHOUT_TZ_FORMAT: &str = "%Y-%m-%d";

//From sparesults, need public fields.
#[derive(Debug)]
pub struct QuerySolutions {
    pub variables: Vec<Variable>,
    pub solutions: Vec<Vec<Option<Term>>>,
}

pub fn column_as_terms(
    column: &Column,
    t: &RDFNodeState,
    global_cats: Arc<Cats>,
) -> Vec<Option<Term>> {
    let height = column.len();
    let terms: Vec<_> = if !t.is_multi() {
        basic_rdf_node_type_column_to_term_vec(
            column,
            t.get_base_type().unwrap(),
            t.get_base_state().unwrap(),
            global_cats,
        )
    } else {
        let mut iters: Vec<IntoIter<Option<Term>>> = vec![];
        for (t, s) in &t.map {
            let type_column = extract_column_from_multitype(column, &t);
            let v = basic_rdf_node_type_column_to_term_vec(&type_column, t, s, global_cats.clone());
            iters.push(v.into_iter())
        }
        let mut final_terms = vec![];
        for _ in 0..height {
            let mut use_term = None;
            for iter in iters.iter_mut() {
                if let Some(Some(term)) = iter.next() {
                    use_term = Some(term);
                }
            }
            final_terms.push(use_term);
        }
        final_terms
    };
    terms
}

pub fn df_as_result(sm: &EagerSolutionMappings, global_cats: Arc<Cats>) -> QuerySolutions {
    if sm.mappings.height() == 0 {
        let variables = sm
            .rdf_node_types
            .keys()
            .map(|x| Variable::new(x).unwrap())
            .collect();
        return QuerySolutions {
            variables,
            solutions: vec![],
        };
    }
    let mut all_terms = vec![];
    let mut variables = vec![];
    let height = sm.mappings.height();
    for (k, t) in &sm.rdf_node_types {
        if let Ok(ser) = sm.mappings.column(k) {
            //TODO: Perhaps correct this upstream?
            variables.push(Variable::new_unchecked(k));
            let terms = column_as_terms(ser, t, global_cats.clone());
            all_terms.push(terms);
        }
    }
    let mut solns = vec![];
    for _i in 0..height {
        let mut soln = vec![];
        for tl in &mut all_terms {
            soln.push(tl.pop().unwrap());
        }
        solns.push(soln);
    }
    solns.reverse();
    QuerySolutions {
        variables,
        solutions: solns,
    }
}

pub fn global_df_as_triples(
    df: DataFrame,
    subject_type: BaseRDFNodeType,
    object_type: BaseRDFNodeType,
    verb: &NamedNode,
    global_cats: Arc<Cats>,
) -> Vec<Triple> {
    let subjects = column_as_terms(
        df.column(SUBJECT_COL_NAME).unwrap(),
        &subject_type.into_default_stored_rdf_node_state(),
        global_cats.clone(),
    );
    let objects = column_as_terms(
        df.column(OBJECT_COL_NAME).unwrap(),
        &object_type.into_default_stored_rdf_node_state(),
        global_cats,
    );

    subjects
        .into_par_iter()
        .zip(objects.into_par_iter())
        .map(|(subject, object)| {
            let subject = match subject.unwrap() {
                Term::NamedNode(nn) => Subject::NamedNode(nn),
                Term::BlankNode(bl) => Subject::BlankNode(bl),
                _ => todo!(),
            };
            Triple::new(subject, verb.clone(), object.unwrap())
        })
        .collect()
}

pub fn basic_rdf_node_type_column_to_term_vec(
    column: &Column,
    base_type: &BaseRDFNodeType,
    base_state: &BaseCatState,
    global_cats: Arc<Cats>,
) -> Vec<Option<Term>> {
    match &base_type {
        BaseRDFNodeType::IRI => decode_column(column, base_type, base_state, global_cats)
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::NamedNode(literal_iri_to_namednode(x))))
            .collect(),
        BaseRDFNodeType::BlankNode => decode_column(column, base_type, base_state, global_cats)
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::BlankNode(literal_blanknode_to_blanknode(x))))
            .collect(),
        BaseRDFNodeType::Literal(l) => match l.as_ref() {
            rdf::LANG_STRING => {
                let col_struct = column.struct_().unwrap();
                let value_ser = col_struct
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap();
                let value_iter = value_ser.str().unwrap().into_iter();

                let lang_ser = col_struct
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap();
                let lang_iter = lang_ser.str().unwrap().into_iter();

                value_iter
                    .zip(lang_iter)
                    .map(|(value, lang)| match (value, lang) {
                        (Some(v), Some(l)) => Some(Term::Literal(
                            Literal::new_language_tagged_literal_unchecked(v, l),
                        )),
                        (None, None) => None,
                        _ => panic!(),
                    })
                    .collect()
            }
            xsd::STRING => decode_column(column, base_type, base_state, global_cats)
                .str()
                .unwrap()
                .par_iter()
                .map(|x| x.map(|x| Term::Literal(Literal::new_simple_literal(x))))
                .collect(),
            xsd::DATE => {
                let ser = date_column_to_strings(column);
                ser.str()
                    .unwrap()
                    .par_iter()
                    .map(|x| x.map(|x| Term::Literal(Literal::new_typed_literal(x, l.clone()))))
                    .collect()
            }
            xsd::DATE_TIME | xsd::DATE_TIME_STAMP => {
                if let DataType::Datetime(_, tz) = column.dtype() {
                    let ser = datetime_column_to_strings(column, tz);
                    ser.str()
                        .unwrap()
                        .par_iter()
                        .map(|x| x.map(|x| Term::Literal(Literal::new_typed_literal(x, l.clone()))))
                        .collect()
                } else {
                    panic!("Invalid state {:?}", column.dtype())
                }
            }
            dt => column
                .cast(&DataType::String)
                .unwrap()
                .str()
                .unwrap()
                .par_iter()
                .map(|x| x.map(|x| Term::Literal(Literal::new_typed_literal(x, dt.into_owned()))))
                .collect(),
        },
        BaseRDFNodeType::None => {
            let mut v = vec![];
            for _ in 0..column.len() {
                v.push(None);
            }
            v
        }
    }
}

//TODO: add exceptions with messages..
pub fn polars_type_to_literal_type(
    data_type: &DataType,
) -> Result<RDFNodeState, RepresentationError> {
    match data_type {
        DataType::Boolean => {
            Ok(BaseRDFNodeType::Literal(xsd::BOOLEAN.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::Int8 => Ok(
            BaseRDFNodeType::Literal(xsd::BYTE.into_owned()).into_default_input_rdf_node_state()
        ),
        DataType::Int16 => {
            Ok(BaseRDFNodeType::Literal(xsd::SHORT.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::UInt8 => Ok(BaseRDFNodeType::Literal(xsd::UNSIGNED_BYTE.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::UInt16 => Ok(BaseRDFNodeType::Literal(xsd::UNSIGNED_SHORT.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::UInt32 => Ok(BaseRDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::UInt64 => Ok(BaseRDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::Int32 => {
            Ok(BaseRDFNodeType::Literal(xsd::INT.into_owned()).into_default_input_rdf_node_state())
        }
        DataType::Int64 => Ok(
            BaseRDFNodeType::Literal(xsd::LONG.into_owned()).into_default_input_rdf_node_state()
        ),
        DataType::Float32 => {
            Ok(BaseRDFNodeType::Literal(xsd::FLOAT.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::Float64 => {
            Ok(BaseRDFNodeType::Literal(xsd::DOUBLE.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::String => {
            Ok(BaseRDFNodeType::Literal(xsd::STRING.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::Date => Ok(
            BaseRDFNodeType::Literal(xsd::DATE.into_owned()).into_default_input_rdf_node_state()
        ),
        DataType::Decimal(_, Some(0)) => {
            Ok(BaseRDFNodeType::Literal(xsd::INTEGER.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::Datetime(_, Some(_)) => {
            Ok(BaseRDFNodeType::Literal(xsd::DATE_TIME_STAMP.into_owned())
                .into_default_input_rdf_node_state())
        }
        DataType::Datetime(_, None) => Ok(BaseRDFNodeType::Literal(xsd::DATE_TIME.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::Duration(_) => Ok(BaseRDFNodeType::Literal(xsd::DURATION.into_owned())
            .into_default_input_rdf_node_state()),
        DataType::Struct(fields) => {
            let names: Vec<_> = fields.iter().map(|x| x.name.as_str()).collect();
            let mut dts = vec![];
            let mut found_lang_string_value = false;
            let mut found_lang_string_lang = false;

            let mut only_lang_string = true;
            let mut unknown_fields = HashSet::new();
            for f in &names {
                match *f {
                    LANG_STRING_VALUE_FIELD => {
                        found_lang_string_value = true;
                        dts.push(BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned()));
                    }
                    LANG_STRING_LANG_FIELD => {
                        found_lang_string_lang = true;
                    }
                    MULTI_IRI_DT => {
                        only_lang_string = false;
                        dts.push(BaseRDFNodeType::IRI);
                    }
                    MULTI_NONE_DT => {
                        only_lang_string = false;
                        dts.push(BaseRDFNodeType::None);
                    }
                    MULTI_BLANK_DT => {
                        only_lang_string = false;
                        dts.push(BaseRDFNodeType::BlankNode);
                    }
                    f => {
                        let stripped = if let Some(f_pre) = f.strip_prefix("<") {
                            f_pre.strip_suffix(">")
                        } else {
                            None
                        };
                        if let Some(stripped) = stripped {
                            if let Ok(nn) = NamedNode::new(stripped) {
                                only_lang_string = false;
                                dts.push(BaseRDFNodeType::Literal(nn));
                            } else {
                                unknown_fields.insert(f.to_string());
                            }
                        } else {
                            unknown_fields.insert(f.to_string());
                        }
                    }
                }
            }
            if found_lang_string_value ^ found_lang_string_lang {
                return Err(RepresentationError::DatatypeError(
                    "Found just one of the lang string cols".into(),
                ));
            }

            if !unknown_fields.is_empty() {
                let unknown: Vec<_> = unknown_fields.into_iter().collect();
                return Err(RepresentationError::DatatypeError(format!(
                    "Unknown fields remain, could not determine type, remaining: {}",
                    unknown.join(", ")
                )));
            }

            if only_lang_string {
                Ok(BaseRDFNodeType::Literal(rdf::LANG_STRING.into_owned())
                    .into_default_input_rdf_node_state())
            } else {
                Ok(RDFNodeState::default_from_types(dts))
            }
        }
        dt => Err(RepresentationError::DatatypeError(format!(
            "Unknown datatype {dt:?}"
        ))),
    }
}

pub fn date_column_to_strings(column: &Column) -> Column {
    column
        .date()
        .unwrap()
        .strftime(XSD_DATE_WITHOUT_TZ_FORMAT)
        .unwrap()
        .into_column()
}

pub fn datetime_column_to_strings(column: &Column, tz_opt: &Option<TimeZone>) -> Column {
    if let Some(tz) = tz_opt {
        hack_format_timestamp_with_timezone(column, &mut tz.clone())
    } else {
        column
            .datetime()
            .unwrap()
            .strftime(XSD_DATETIME_WITHOUT_TZ_FORMAT)
            .expect("Conversion OK")
            .into_column()
            .cast(&DataType::Datetime(
                TimeUnit::Nanoseconds,
                Some(TimeZone::UTC),
            ))
            .unwrap()
    }
}

pub fn hack_format_timestamp_with_timezone(column: &Column, tz: &mut TimeZone) -> Column {
    let name = column.name().to_string();
    let timezone_opt: Result<chrono_tz::Tz, _> = tz.parse();
    if let Ok(timezone) = timezone_opt {
        let datetime_strings_vec: Vec<_> = column
            .datetime()
            .unwrap()
            .as_datetime_iter()
            .map(|x| match x {
                Some(x) => AnyValue::StringOwned(
                    format!(
                        "{}",
                        timezone
                            .with_ymd_and_hms(
                                x.year(),
                                x.month(),
                                x.day(),
                                x.hour(),
                                x.minute(),
                                x.second()
                            )
                            .latest()
                            .unwrap()
                            .with_nanosecond(x.nanosecond())
                            .unwrap()
                            .format(XSD_DATETIME_WITH_TZ_FORMAT)
                    )
                    .into(),
                ),
                None => AnyValue::Null,
            })
            .collect();

        Series::from_any_values_and_dtype(
            name.into(),
            &datetime_strings_vec,
            &DataType::String,
            false,
        )
        .unwrap()
        .into_column()
    } else {
        panic!("Unknown timezone{tz}");
    }
}

// These terms must all be of the same data type
pub fn particular_opt_term_vec_to_series(
    term_vec: Vec<Option<Term>>,
    dt: BaseRDFNodeTypeRef,
    c: &str,
) -> Series {
    if dt.is_lang_string() {
        let langs = term_vec
            .par_iter()
            .map(|t| {
                if let Some(t) = t {
                    match t {
                        Term::Literal(l) => LiteralValue::Scalar(Scalar::from(
                            PlSmallStr::from_string(l.language().unwrap().to_string()),
                        )),
                        _ => panic!("Should never happen"),
                    }
                } else {
                    LiteralValue::untyped_null()
                }
            })
            .collect();
        let vals = term_vec
            .into_par_iter()
            .map(|t| {
                if let Some(t) = t {
                    match t {
                        Term::Literal(l) => {
                            let (s, _, _) = l.destruct();
                            LiteralValue::Scalar(Scalar::from(PlSmallStr::from_string(s)))
                        }
                        _ => panic!("Should never happen"),
                    }
                } else {
                    LiteralValue::untyped_null()
                }
            })
            .collect();

        let val_ser = polars_literal_values_to_series(vals, LANG_STRING_VALUE_FIELD);
        let lang_ser = polars_literal_values_to_series(langs, LANG_STRING_LANG_FIELD);
        let mut df = DataFrame::new(vec![val_ser.into(), lang_ser.into()])
            .unwrap()
            .lazy()
            .with_column(
                as_struct(vec![
                    col(LANG_STRING_VALUE_FIELD),
                    col(LANG_STRING_LANG_FIELD),
                ])
                .alias(c),
            )
            .collect()
            .unwrap();
        df.drop_in_place(c).unwrap().take_materialized_series()
    } else {
        let any_iter: Vec<_> = term_vec
            .into_par_iter()
            .map(|t| {
                if let Some(t) = t {
                    match t {
                        Term::NamedNode(nn) => rdf_owned_named_node_to_polars_literal_value(nn),
                        Term::BlankNode(bb) => rdf_owned_blank_node_to_polars_literal_value(bb),
                        Term::Literal(l) => rdf_literal_to_polars_literal_value(&l),
                        #[cfg(feature = "rdf-star")]
                        _ => unimplemented!(),
                    }
                } else {
                    LiteralValue::untyped_null()
                }
            })
            .collect();
        polars_literal_values_to_series(any_iter, c)
    }
}
