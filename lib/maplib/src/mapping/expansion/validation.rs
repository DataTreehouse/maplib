use crate::mapping::errors::MappingError;
use oxrdf::vocab::{rdfs, xsd};
use oxrdf::NamedNode;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::Series;
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::{BaseRDFNodeType, RDFNodeType};
use std::collections::{HashMap, HashSet};
use templates::ast::{ptype_is_blank, ptype_is_iri, PType, Parameter, Template};
use templates::MappingColumnType;

const OWL_REAL: &str = "http://www.w3.org/2002/07/owl#real";
const OWL_RATIONAL: &str = "http://www.w3.org/2002/07/owl#rational";

pub fn validate(
    df: Option<DataFrame>,
    mut mapping_column_types: Option<HashMap<String, MappingColumnType>>,
    template: &Template,
) -> Result<(Option<DataFrame>, HashMap<String, MappingColumnType>), MappingError> {
    validate_column_existence(&df, template)?;
    let mut map = HashMap::new();
    if let Some(df) = df {
        for p in &template.signature.parameter_list {
            if !p.optional && p.default_value.is_none() {
                validate_non_optional_parameter_non_null(&df, p.variable.as_str())?;
            }
            let name = p.variable.as_str();
            let mut found_column_type = false;
            if let Some(mapping_column_types) = &mut mapping_column_types {
                if let Some((k, given_type)) = mapping_column_types.remove_entry(name) {
                    map.insert(k, given_type);
                    found_column_type = true;
                }
            }
            if !found_column_type {
                if let Ok(ser) = df.column(name) {
                    let t = infer_mapping_column_type(p, ser)?;
                    map.insert(name.to_string(), t);
                }
            }
            // Todo: check e.g. IRIs are valid..
        }
        Ok((Some(df), map))
    } else {
        Ok((None, map))
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
    series: &Series,
) -> Result<MappingColumnType, MappingError> {
    if let Some(ptype) = &p.ptype {
        Ok(infer_validate_mapping_column_type_from_ptype(
            series.name(),
            series.dtype(),
            ptype,
        )?)
    } else {
        let series_inferred_mapping_column_type =
            polars_datatype_to_mapping_column_datatype(series.dtype())?;
        Ok(series_inferred_mapping_column_type)
    }
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
            } else if ptype_is_iri(nn) {
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
            } else if ptype_is_blank(nn) {
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
                            if is_literal_subtype(inferred_nn, nn) {
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

fn is_literal_subtype(s: &NamedNode, t: &NamedNode) -> bool {
    if t.as_str() == OWL_REAL {
        owl_real_subtype(s)
    } else if t.as_str() == OWL_RATIONAL {
        owl_rational_subtype(s)
    } else {
        match t.as_ref() {
            xsd::DECIMAL => xsd_decimal_subtype(s),
            xsd::INTEGER => xsd_integer_subtype(s),
            xsd::LONG => xsd_long_subtype(s),
            xsd::INT => xsd_int_subtype(s),
            xsd::SHORT => xsd_short_subtype(s),
            xsd::NON_NEGATIVE_INTEGER => xsd_non_negative_integer_subtype(s),
            xsd::POSITIVE_INTEGER => xsd_positive_integer_subtype(s),
            xsd::UNSIGNED_LONG => xsd_unsigned_long_subtype(s),
            xsd::UNSIGNED_INT => xsd_unsigned_int_subtype(s),
            xsd::UNSIGNED_SHORT => xsd_unsigned_short_subtype(s),
            xsd::NON_POSITIVE_INTEGER => xsd_non_positive_integer_subtype(s),
            xsd::DURATION => {
                matches!(
                    s.as_ref(),
                    xsd::YEAR_MONTH_DURATION | xsd::DAY_TIME_DURATION
                )
            }
            xsd::DATE_TIME => {
                matches!(s.as_ref(), xsd::DATE_TIME_STAMP)
            }
            _ => false,
        }
    }
}

fn owl_real_subtype(s: &NamedNode) -> bool {
    matches!(s.as_str(), OWL_RATIONAL) || owl_rational_subtype(s)
}

fn owl_rational_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::DECIMAL) || xsd_decimal_subtype(s)
}

fn xsd_decimal_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::INTEGER) || xsd_integer_subtype(s)
}

fn xsd_integer_subtype(s: &NamedNode) -> bool {
    matches!(
        s.as_ref(),
        xsd::LONG | xsd::NON_NEGATIVE_INTEGER | xsd::NON_POSITIVE_INTEGER
    ) || xsd_long_subtype(s)
        || xsd_non_negative_integer_subtype(s)
        || xsd_non_positive_integer_subtype(s)
}

fn xsd_non_positive_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::NEGATIVE_INTEGER)
}

fn xsd_non_negative_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::POSITIVE_INTEGER) || xsd_positive_integer_subtype(s)
}

fn xsd_positive_integer_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_LONG) || xsd_unsigned_long_subtype(s)
}

fn xsd_unsigned_long_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_INT) || xsd_unsigned_int_subtype(s)
}

fn xsd_unsigned_int_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_SHORT) || xsd_unsigned_short_subtype(s)
}

fn xsd_unsigned_short_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::UNSIGNED_BYTE)
}

fn xsd_long_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::INT) || xsd_int_subtype(s)
}

fn xsd_int_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::SHORT) || xsd_short_subtype(s)
}

fn xsd_short_subtype(s: &NamedNode) -> bool {
    matches!(s.as_ref(), xsd::BYTE)
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
