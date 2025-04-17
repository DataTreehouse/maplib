use crate::mapping::errors::MappingError;
use log::warn;
use oxiri::Iri;
use oxrdf::vocab::{rdfs, xsd};
use oxrdf::NamedNode;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::{col, ChunkApply, Column, IntoLazy, Series};
use rayon::current_num_threads;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::{BaseRDFNodeType, RDFNodeType};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use templates::ast::{ptype_is_blank, ptype_is_iri, PType, Parameter, Template};
use templates::subtypes_ext::is_literal_subtype_ext;
use templates::MappingColumnType;

pub fn validate(
    df: Option<DataFrame>,
    mut mapping_column_types: Option<HashMap<String, MappingColumnType>>,
    template: &Template,
    validate_iris: bool,
) -> Result<(Option<DataFrame>, HashMap<String, MappingColumnType>), MappingError> {
    validate_column_existence(&df, template)?;
    let mut map = HashMap::new();
    if let Some(mut df) = df {
        for p in &template.signature.parameter_list {
            if !p.optional && p.default_value.is_none() {
                validate_non_optional_parameter_non_null(&df, p.variable.as_str())?;
            }
            df = autoconvert_datatypes(df, &mut mapping_column_types, p);
            let name = p.variable.as_str();
            let mut found_column_type = false;
            if let Some(mapping_column_types) = &mut mapping_column_types {
                if let Some((k, given_type)) = mapping_column_types.remove_entry(name) {
                    map.insert(k, given_type);
                    found_column_type = true;
                }
            }
            if !found_column_type {
                if let Ok(c) = df.column(name) {
                    let t = infer_mapping_column_type(p, c)?;
                    map.insert(name.to_string(), t);
                }
            }

            if validate_iris {
                if let Some(t) = map.get(name) {
                    if let Ok(c) = df.column(name) {
                        let ser = c.as_materialized_series();
                        match t {
                            MappingColumnType::Flat(t) => {
                                let mut offsets = vec![];
                                let threads = current_num_threads();
                                //TODO: Is this correct?
                                let stride = ser.len().div_ceil(threads);
                                let mut last_end = 0;
                                for i in 0..threads {
                                    let stride = if i == threads - 1 { ser.len() } else { stride };
                                    offsets.push((last_end, stride));
                                    last_end += stride;
                                }
                                let res: Result<Vec<_>, MappingError> = offsets
                                    .into_par_iter()
                                    .map(|(start, stride)| {
                                        let ser = ser.slice(start as i64, stride);
                                        validate_flat_iri_column(&ser, name, t)?;
                                        Ok(())
                                    })
                                    .filter(|x| x.is_err())
                                    .collect();
                                res?;
                            }
                            MappingColumnType::Nested(n) => {
                                if let MappingColumnType::Flat(t) = n.as_ref() {
                                    //To avoid a bit of unnecessary explode
                                    if t.is_iri() {
                                        let l = ser.list().unwrap();
                                        let res: Result<Vec<_>, _> = l
                                            .par_iter()
                                            .map(|ser| {
                                                if let Some(ser) = ser {
                                                    validate_flat_iri_column(&ser, name, t)
                                                } else {
                                                    Ok(())
                                                }
                                            })
                                            .filter(|x| x.is_err())
                                            .collect();
                                        res?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok((Some(df), map))
    } else {
        Ok((None, map))
    }
}

pub fn validate_flat_iri_column(
    ser: &Series,
    colname: &str,
    t: &RDFNodeType,
) -> Result<(), MappingError> {
    if t.is_iri() {
        let c = ser
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .apply(parse_iri);
        let is_err = c.is_not_null();
        if let Some(n_errs) = is_err.sum() {
            if n_errs > 0 {
                let errs_3 = ser.filter(&is_err).unwrap();
                let errs = errs_3.head(Some(3));
                let examples = errs.cast(&DataType::String).unwrap().fmt_list();
                return Err(MappingError::InvalidIRIError(
                    colname.to_string(),
                    n_errs as usize,
                    examples,
                ));
            }
        }
    }

    Ok(())
}

fn parse_iri(iri: Option<&str>) -> Option<Cow<'_, str>> {
    if let Some(iri) = iri {
        match Iri::parse(iri) {
            Ok(_) => None,
            Err(_) => Some("".into()),
        }
    } else {
        None
    }
}

fn validate_non_optional_parameter_non_null(
    df: &DataFrame,
    column_name: &str,
) -> Result<(), MappingError> {
    if df.column(column_name).unwrap().is_null().any() {
        let is_null = df.column(column_name).unwrap().is_null();
        Err(MappingError::NonOptionalColumnHasNull(
            column_name.to_string(),
            df.filter(&is_null).unwrap(),
        ))
    } else {
        Ok(())
    }
}

fn validate_column_existence(
    df: &Option<DataFrame>,
    template: &Template,
) -> Result<(), MappingError> {
    if let Some(df) = df {
        let mut df_columns = HashSet::new();
        df_columns.extend(df.get_column_names().into_iter().map(|x| x.to_string()));

        for parameter in &template.signature.parameter_list {
            let variable_name = parameter.variable.as_str();
            if df_columns.contains(variable_name) {
                df_columns.remove(variable_name);
            } else if !parameter.optional && parameter.default_value.is_none() {
                return Err(MappingError::MissingParameterColumn(
                    variable_name.to_string(),
                ));
            }
        }
        if !df_columns.is_empty() {
            return Err(MappingError::ContainsIrrelevantColumns(
                df_columns.iter().map(|x| x.to_string()).collect(),
            ));
        }
        Ok(())
    } else if !template.signature.parameter_list.is_empty() {
        // TODO: Handle case all default or optional
        return Err(MappingError::MissingDataFrameForNonEmptySignature);
    } else {
        Ok(())
    }
}

fn infer_mapping_column_type(
    p: &Parameter,
    column: &Column,
) -> Result<MappingColumnType, MappingError> {
    if let Some(ptype) = &p.ptype {
        Ok(infer_validate_mapping_column_type_from_ptype(
            column.name(),
            column.dtype(),
            ptype,
        )?)
    } else {
        infer_type_from_column(column)
    }
}

pub fn infer_type_from_column(column: &Column) -> Result<MappingColumnType, MappingError> {
    if is_iri_col(column) {
        return Ok(MappingColumnType::Flat(RDFNodeType::IRI));
    }
    let series_inferred_mapping_column_type =
        polars_datatype_to_mapping_column_datatype(column.dtype())?;
    Ok(series_inferred_mapping_column_type)
}

fn is_iri_col(column: &Column) -> bool {
    let dtype = column.dtype();
    if dtype.is_string() {
        let strchk = column.str().unwrap();
        let fnn = strchk.first_non_null();
        if let Some(fnn) = fnn {
            let s = strchk.get(fnn).unwrap();
            if NamedNode::new(s).is_ok() {
                return true;
            }
        }
    }
    false
}

fn infer_validate_mapping_column_type_from_ptype(
    column_name: &str,
    datatype: &DataType,
    ptype: &PType,
) -> Result<MappingColumnType, MappingError> {
    match ptype {
        PType::None => {
            let series_inferred_rdf_node_type = polars_type_to_literal_type(datatype)
                .map_err(MappingError::DatatypeInferenceError)?;
            Ok(MappingColumnType::Flat(series_inferred_rdf_node_type))
        }
        PType::Basic(nn) => {
            if datatype.is_null() {
                Ok(MappingColumnType::Flat(RDFNodeType::None))
            } else if ptype_is_iri(nn.as_ref()) {
                if datatype.is_string() || datatype.is_categorical() {
                    Ok(MappingColumnType::Flat(RDFNodeType::IRI))
                } else {
                    Err(MappingError::ColumnDataTypeMismatch(
                        column_name.to_string(),
                        datatype.clone(),
                        ptype.clone(),
                        Some(DataType::String),
                    ))
                }
            } else if ptype_is_blank(nn.as_ref()) {
                if datatype.is_string() || datatype.is_categorical() {
                    Ok(MappingColumnType::Flat(RDFNodeType::BlankNode))
                } else {
                    Err(MappingError::ColumnDataTypeMismatch(
                        column_name.to_string(),
                        datatype.clone(),
                        ptype.clone(),
                        Some(DataType::String),
                    ))
                }
            } else {
                //literal
                if matches!(nn.as_ref(), rdfs::LITERAL | rdfs::RESOURCE) {
                    let series_inferred_rdf_node_type = polars_type_to_literal_type(datatype)
                        .map_err(MappingError::DatatypeInferenceError)?;
                    Ok(MappingColumnType::Flat(series_inferred_rdf_node_type))
                } else {
                    let ptype_rdf_node_type = BaseRDFNodeType::Literal(nn.clone());
                    let ptype_dt = ptype_rdf_node_type.polars_data_type();
                    if ptype_dt.is_string() && (datatype.is_string() || datatype.is_categorical())
                        || &ptype_dt == datatype
                    {
                        Ok(MappingColumnType::Flat(
                            ptype_rdf_node_type.as_rdf_node_type(),
                        ))
                    } else {
                        let series_inferred_rdf_node_type =
                            polars_type_to_literal_type(datatype)
                                .map_err(MappingError::DatatypeInferenceError)?;
                        if let RDFNodeType::Literal(inferred_nn) = &series_inferred_rdf_node_type {
                            if is_literal_subtype_ext(inferred_nn.as_ref(), nn.as_ref()) {
                                Ok(MappingColumnType::Flat(series_inferred_rdf_node_type))
                            } else {
                                Err(MappingError::ColumnDataTypeMismatch(
                                    column_name.to_string(),
                                    datatype.clone(),
                                    ptype.clone(),
                                    Some(ptype_dt),
                                ))
                            }
                        } else {
                            panic!("Should not happen");
                        }
                    }
                }
            }
        }
        PType::Lub(inner) | PType::List(inner) | PType::NEList(inner) => {
            if let DataType::List(dt) = datatype {
                let res = infer_validate_mapping_column_type_from_ptype(column_name, dt, inner)?;
                Ok(MappingColumnType::Nested(Box::new(res)))
            } else {
                Err(MappingError::ColumnDataTypeMismatch(
                    column_name.to_string(),
                    datatype.clone(),
                    ptype.clone(),
                    None,
                ))
            }
        }
    }
}

pub fn polars_datatype_to_mapping_column_datatype(
    datatype: &DataType,
) -> Result<MappingColumnType, MappingError> {
    if let DataType::List(dt) = datatype {
        Ok(MappingColumnType::Nested(Box::new(
            polars_datatype_to_mapping_column_datatype(dt)?,
        )))
    } else {
        let dt =
            polars_type_to_literal_type(datatype).map_err(MappingError::DatatypeInferenceError)?;
        Ok(MappingColumnType::Flat(dt))
    }
}

fn autoconvert_datatypes(
    mut df: DataFrame,
    mapping_column_types: &mut Option<HashMap<String, MappingColumnType>>,
    p: &Parameter,
) -> DataFrame {
    if let Some(t) = &p.ptype {
        if let Ok(c) = df.column(p.variable.as_str()) {
            if let Some(dt) = cast_datetime_to_date(t, c.dtype()) {
                warn!(
                    "Automatically casting column {} to {}",
                    p.variable.as_str(),
                    dt
                );
                df = df
                    .lazy()
                    .with_column(col(p.variable.as_str()).cast(dt))
                    .collect()
                    .unwrap();
                if let Some(mapping_column_types) = mapping_column_types {
                    mapping_column_types.remove(p.variable.as_str());
                }
            }
        }
    }
    df
}

fn cast_datetime_to_date(ptype: &PType, data_type: &DataType) -> Option<DataType> {
    match ptype {
        PType::None => None,
        PType::Basic(b) => {
            if let DataType::List(data_type) = data_type {
                //Handles case where OTTR is creating lists
                cast_datetime_to_date(ptype, data_type)
            } else if b.as_ref() == xsd::DATE {
                if matches!(data_type, DataType::Datetime(..)) {
                    Some(DataType::Date)
                } else {
                    None
                }
            } else {
                None
            }
        }
        PType::Lub(l) | PType::List(l) | PType::NEList(l) => {
            if let DataType::List(data_type) = data_type {
                cast_datetime_to_date(l, data_type).map(|t| DataType::List(Box::new(t)))
            } else {
                None
            }
        }
    }
}
