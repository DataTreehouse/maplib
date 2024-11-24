use crate::errors::RepresentationError;
use crate::multitype::{
    all_multi_main_cols, multi_has_this_type_column, MULTI_BLANK_DT, MULTI_IRI_DT, MULTI_NONE_DT,
};
use crate::{
    literal_blanknode_to_blanknode, literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType,
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, OBJECT_COL_NAME, SUBJECT_COL_NAME,
};
use chrono::TimeZone as ChronoTimeZone;
use chrono::{Datelike, Timelike};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, Subject, Triple, Variable};
use polars::export::rayon::iter::IndexedParallelIterator;
use polars::export::rayon::iter::ParallelIterator;
use polars::export::rayon::prelude::IntoParallelIterator;
use polars::prelude::{
    as_struct, col, AnyValue, Column, DataFrame, DataType, IntoColumn, IntoLazy, Series, TimeZone,
};
use spargebra::term::Term;
use std::collections::{HashMap, HashSet};
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

fn column_as_terms(column: &Column, t: &RDFNodeType) -> Vec<Option<Term>> {
    let height = column.len();
    let terms: Vec<_> = match t {
        RDFNodeType::None
        | RDFNodeType::IRI
        | RDFNodeType::BlankNode
        | RDFNodeType::Literal(..) => {
            basic_rdf_node_type_column_to_term_vec(column, &BaseRDFNodeType::from_rdf_node_type(t))
        }
        RDFNodeType::MultiType(types) => {
            let mut iters: Vec<IntoIter<Option<Term>>> = vec![];
            for (t, colname) in types.iter().zip(all_multi_main_cols(types)) {
                let v = if t.is_lang_string() {
                    let mut lf = DataFrame::new(vec![
                        column
                            .struct_()
                            .unwrap()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .unwrap()
                            .cast(&DataType::String)
                            .unwrap()
                            .clone()
                            .into_column(),
                        column
                            .struct_()
                            .unwrap()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .unwrap()
                            .cast(&DataType::String)
                            .unwrap()
                            .clone()
                            .into_column(),
                    ])
                    .unwrap()
                    .lazy();
                    lf = lf.with_column(
                        as_struct(vec![
                            col(LANG_STRING_LANG_FIELD),
                            col(LANG_STRING_VALUE_FIELD),
                        ])
                        .alias(&colname),
                    );
                    let df = lf.collect();
                    let ser = df.unwrap().drop_in_place(&colname).unwrap();
                    basic_rdf_node_type_column_to_term_vec(&ser, t)
                } else {
                    basic_rdf_node_type_column_to_term_vec(
                        &column
                            .struct_()
                            .unwrap()
                            .field_by_name(&colname)
                            .unwrap()
                            .into_column(),
                        t,
                    )
                };
                iters.push(v.into_iter())
            }
            let mut final_terms = vec![];
            for _ in 0..height {
                let mut use_term = None;
                for iter in iters.iter_mut() {
                    if let Some(term) = iter.next() {
                        if let Some(term) = term {
                            use_term = Some(term);
                        }
                    }
                }
                final_terms.push(use_term);
            }
            final_terms
        }
    };
    terms
}

pub fn df_as_result(df: DataFrame, dtypes: &HashMap<String, RDFNodeType>) -> QuerySolutions {
    if df.height() == 0 {
        let variables = dtypes.keys().map(|x| Variable::new(x).unwrap()).collect();
        return QuerySolutions {
            variables,
            solutions: vec![],
        };
    }
    let mut all_terms = vec![];
    let mut variables = vec![];
    let height = df.height();
    for (k, t) in dtypes {
        if let Ok(ser) = df.column(k) {
            //TODO: Perhaps correct this upstream?
            variables.push(Variable::new_unchecked(k));
            let terms = column_as_terms(ser, t);
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

pub fn df_as_triples(
    df: DataFrame,
    subject_type: &RDFNodeType,
    object_type: &RDFNodeType,
    verb: &NamedNode,
) -> Vec<Triple> {
    let subjects = column_as_terms(df.column(SUBJECT_COL_NAME).unwrap(), subject_type);
    let objects = column_as_terms(df.column(OBJECT_COL_NAME).unwrap(), object_type);
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
    base_rdf_node_type: &BaseRDFNodeType,
) -> Vec<Option<Term>> {
    match base_rdf_node_type {
        BaseRDFNodeType::IRI => column
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::NamedNode(literal_iri_to_namednode(x))))
            .collect(),
        BaseRDFNodeType::BlankNode => column
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::BlankNode(literal_blanknode_to_blanknode(x))))
            .collect(),
        BaseRDFNodeType::Literal(l) => match l.as_ref() {
            rdf::LANG_STRING => {
                let value_ser = column
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap();
                let value_iter = value_ser.str().unwrap().into_iter();
                let lang_ser = column
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap();
                let lang_iter = lang_ser.str().unwrap().into_iter();
                value_iter
                    .zip(lang_iter)
                    .map(|(value, lang)| {
                        if let Some(value) = value {
                            if let Some(lang) = lang {
                                Some(Term::Literal(
                                    Literal::new_language_tagged_literal_unchecked(value, lang),
                                ))
                            } else {
                                panic!()
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            xsd::STRING => column
                .cast(&DataType::String)
                .unwrap()
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
) -> Result<RDFNodeType, RepresentationError> {
    match data_type {
        DataType::Boolean => Ok(RDFNodeType::Literal(xsd::BOOLEAN.into_owned())),
        DataType::Int8 => Ok(RDFNodeType::Literal(xsd::BYTE.into_owned())),
        DataType::Int16 => Ok(RDFNodeType::Literal(xsd::SHORT.into_owned())),
        DataType::UInt8 => Ok(RDFNodeType::Literal(xsd::UNSIGNED_BYTE.into_owned())),
        DataType::UInt16 => Ok(RDFNodeType::Literal(xsd::UNSIGNED_SHORT.into_owned())),
        DataType::UInt32 => Ok(RDFNodeType::Literal(xsd::UNSIGNED_INT.into_owned())),
        DataType::UInt64 => Ok(RDFNodeType::Literal(xsd::UNSIGNED_LONG.into_owned())),
        DataType::Int32 => Ok(RDFNodeType::Literal(xsd::INT.into_owned())),
        DataType::Int64 => Ok(RDFNodeType::Literal(xsd::LONG.into_owned())),
        DataType::Float32 => Ok(RDFNodeType::Literal(xsd::FLOAT.into_owned())),
        DataType::Float64 => Ok(RDFNodeType::Literal(xsd::DOUBLE.into_owned())),
        DataType::String => Ok(RDFNodeType::Literal(xsd::STRING.into_owned())),
        DataType::Date => Ok(RDFNodeType::Literal(xsd::DATE.into_owned())),
        DataType::Decimal(_, Some(0)) => Ok(RDFNodeType::Literal(xsd::INTEGER.into_owned())),
        DataType::Datetime(_, Some(_)) => {
            Ok(RDFNodeType::Literal(xsd::DATE_TIME_STAMP.into_owned()))
        }
        DataType::Datetime(_, None) => Ok(RDFNodeType::Literal(xsd::DATE_TIME.into_owned())),
        DataType::Duration(_) => Ok(RDFNodeType::Literal(xsd::DURATION.into_owned())),
        DataType::Categorical(_, _) => Ok(RDFNodeType::Literal(xsd::STRING.into_owned())),
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

            for dt in &dts {
                let expect = multi_has_this_type_column(dt);
                if !unknown_fields.remove(&expect) {
                    return Err(RepresentationError::DatatypeError(format!(
                        "Expected indicator field {}, could not find datatype",
                        expect
                    )));
                }
            }

            if !unknown_fields.is_empty() {
                let unknown: Vec<_> = unknown_fields.into_iter().collect();
                return Err(RepresentationError::DatatypeError(format!(
                    "Unknown fields remain, could not determine type, remaining: {}",
                    unknown.join(", ")
                )));
            }

            if only_lang_string {
                Ok(RDFNodeType::Literal(rdf::LANG_STRING.into_owned()))
            } else {
                Ok(RDFNodeType::MultiType(dts))
            }
        }
        dt => Err(RepresentationError::DatatypeError(format!(
            "Unknown datatype {:?}",
            dt
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
        return column
            .datetime()
            .unwrap()
            .strftime(XSD_DATETIME_WITHOUT_TZ_FORMAT)
            .expect("Conversion OK")
            .into_column();
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
        panic!("Unknown timezone{}", tz);
    }
}
