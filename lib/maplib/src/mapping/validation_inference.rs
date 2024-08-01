use super::Mapping;
use templates::ast::{PType, Parameter, Signature};

use crate::mapping::errors::MappingError;
use polars::prelude::{DataFrame, DataType};
use representation::polars_to_rdf::polars_type_to_literal_type;
use std::collections::{HashMap, HashSet};
use templates::MappingColumnType;

impl Mapping {
    pub fn validate_infer_dataframe_columns(
        &self,
        signature: &Signature,
        df: &mut Option<DataFrame>,
    ) -> Result<HashMap<String, MappingColumnType>, MappingError> {
        let mut map = HashMap::new();
        if let Some(df) = df {
            let mut df_columns = HashSet::new();
            df_columns.extend(df.get_column_names().into_iter().map(|x| x.to_string()));

            for parameter in &signature.parameter_list {
                let variable_name = parameter.variable.as_str();
                if df_columns.contains(variable_name) {
                    df_columns.remove(variable_name);
                    if !parameter.optional && parameter.default_value.is_none() {
                        validate_non_optional_parameter(df, variable_name)?;
                    }
                    let column_data_type =
                        validate_infer_column_data_type(df, parameter, variable_name)?;

                    map.insert(variable_name.to_string(), column_data_type);
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
        } else if !signature.parameter_list.is_empty() {
            return Err(MappingError::MissingDataFrameForNonEmptySignature);
        }
        Ok(map)
    }
}

fn validate_infer_column_data_type(
    dataframe: &mut DataFrame,
    parameter: &Parameter,
    column_name: &str,
) -> Result<MappingColumnType, MappingError> {
    let dtype = dataframe.column(column_name).unwrap().dtype();
    let mapping_column_type = if let Some(ptype) = &parameter.ptype {
        if let Some(adjust_dtype) = validate_datatype(column_name, dtype, ptype)? {
            dataframe
                .with_column(
                    dataframe
                        .column(column_name)
                        .unwrap()
                        .cast(&adjust_dtype)
                        .unwrap(),
                )
                .unwrap();
        }
        infer_rdf_node_type(&ptype)
    } else {
        polars_datatype_to_mapping_column_datatype(dtype)?
    };
    Ok(mapping_column_type)
}

fn infer_rdf_node_type(ptype: &PType) -> MappingColumnType {
    match ptype {
        PType::Basic(b) => MappingColumnType::Flat(b.as_rdf_node_type()),
        PType::Lub(l) => MappingColumnType::Nested(Box::new(infer_rdf_node_type(l))),
        PType::List(l) => MappingColumnType::Nested(Box::new(infer_rdf_node_type(l))),
        PType::NEList(l) => MappingColumnType::Nested(Box::new(infer_rdf_node_type(l))),
    }
}

fn validate_non_optional_parameter(df: &DataFrame, column_name: &str) -> Result<(), MappingError> {
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

fn validate_datatype(
    column_name: &str,
    datatype: &DataType,
    target_ptype: &PType,
) -> Result<Option<DataType>, MappingError> {
    let validate_if_series_list = |inner| {
        if let DataType::List(dt) = datatype {
            if let Some(res) = validate_datatype(column_name, dt, inner)? {
                Ok(Some(DataType::List(Box::new(res))))
            } else {
                Ok(None)
            }
        } else {
            Err(MappingError::ColumnDataTypeMismatch(
                column_name.to_string(),
                datatype.clone(),
                target_ptype.clone(),
                None,
            ))
        }
    };
    match target_ptype {
        PType::Basic(b) => {
            if let DataType::List(_) = datatype {
                Err(MappingError::ColumnDataTypeMismatch(
                    column_name.to_string(),
                    datatype.clone(),
                    target_ptype.clone(),
                    None,
                ))
            } else {
                let expected = b.polars_data_type();
                if &expected != datatype {
                    if datatype == &DataType::Null {
                        Ok(Some(expected))
                    } else {
                        Err(MappingError::ColumnDataTypeMismatch(
                            column_name.to_string(),
                            datatype.clone(),
                            target_ptype.clone(),
                            Some(expected),
                        ))
                    }
                } else {
                    Ok(None)
                }
            }
        }
        PType::Lub(inner) => validate_if_series_list(inner),
        PType::List(inner) => validate_if_series_list(inner),
        PType::NEList(inner) => validate_if_series_list(inner),
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
        let dt = polars_type_to_literal_type(datatype)
            .map_err(|x| MappingError::DatatypeInferenceError(x))?;
        Ok(MappingColumnType::Flat(dt))
    }
}
