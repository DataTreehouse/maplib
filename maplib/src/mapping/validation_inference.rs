use super::Mapping;
use crate::ast::{has_iritype, PType, Parameter, Signature};

use crate::constants::OTTR_IRI;
use crate::mapping::errors::MappingError;
use crate::mapping::{PrimitiveColumn, RDFNodeType};
use oxrdf::NamedNode;
use polars::prelude::{DataFrame, DataType};
use representation::polars_to_rdf::polars_type_to_literal_type;
use std::collections::{HashMap, HashSet};

impl Mapping {
    pub fn validate_infer_dataframe_columns(
        &self,
        signature: &Signature,
        df: &Option<DataFrame>,
    ) -> Result<HashMap<String, PrimitiveColumn>, MappingError> {
        let mut map = HashMap::new();
        if let Some(df) = df {
            let mut df_columns = HashSet::new();
            df_columns.extend(df.get_column_names().into_iter().map(|x| x.to_string()));

            for parameter in &signature.parameter_list {
                let variable_name = &parameter.stottr_variable.name;
                if df_columns.contains(variable_name.as_str()) {
                    df_columns.remove(variable_name.as_str());
                    if !parameter.optional {
                        validate_non_optional_parameter(df, variable_name)?;
                    }
                    let column_data_type =
                        validate_infer_column_data_type(df, parameter, variable_name)?;

                    map.insert(variable_name.to_string(), column_data_type);
                } else if !parameter.optional {
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
    dataframe: &DataFrame,
    parameter: &Parameter,
    column_name: &str,
) -> Result<PrimitiveColumn, MappingError> {
    let series = dataframe.column(column_name).unwrap();
    let dtype = series.dtype();
    let ptype = if let Some(ptype) = &parameter.ptype {
        validate_datatype(series.name(), dtype, ptype)?;
        ptype.clone()
    } else {
        polars_datatype_to_xsd_datatype(dtype)
    };
    let rdf_node_type = infer_rdf_node_type(&ptype);

    Ok(PrimitiveColumn { rdf_node_type })
}

fn infer_rdf_node_type(ptype: &PType) -> RDFNodeType {
    match ptype {
        PType::Basic(b, _) => {
            if has_iritype(b.as_str()) {
                RDFNodeType::IRI
            } else {
                RDFNodeType::Literal(b.clone())
            }
        }
        PType::Lub(l) => infer_rdf_node_type(l),
        PType::List(l) => infer_rdf_node_type(l),
        PType::NEList(l) => infer_rdf_node_type(l),
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
) -> Result<(), MappingError> {
    let mismatch_error = || {
        Err(MappingError::ColumnDataTypeMismatch(
            column_name.to_string(),
            datatype.clone(),
            target_ptype.clone(),
        ))
    };
    let validate_if_series_list = |inner| {
        if let DataType::List(dt) = datatype {
            validate_datatype(column_name, dt, inner)
        } else {
            mismatch_error()
        }
    };
    match target_ptype {
        PType::Basic(bt, _) => {
            if let DataType::List(_) = datatype {
                mismatch_error()
            } else {
                Ok(validate_basic_datatype(column_name, datatype, bt)?)
            }
        }
        PType::Lub(inner) => validate_if_series_list(inner),
        PType::List(inner) => validate_if_series_list(inner),
        PType::NEList(inner) => validate_if_series_list(inner),
    }
}

fn validate_basic_datatype(
    _column_name: &str,
    _datatype: &DataType,
    _rdf_datatype: &NamedNode,
) -> Result<(), MappingError> {
    // match rdf_datatype.as_ref() {
    //     xsd::INT => {
    //         Ok(());
    //
    //     }
    // }
    Ok(())
}

pub fn polars_datatype_to_xsd_datatype(datatype: &DataType) -> PType {
    if let Some(rdf_node_type) = polars_type_to_literal_type(datatype) {
        let nn = match rdf_node_type {
            RDFNodeType::IRI => NamedNode::new_unchecked(OTTR_IRI),
            RDFNodeType::Literal(l) => l,
            _ => unimplemented!("Unsupported datatype:{}", datatype),
        };
        let nn_string = nn.to_string();
        return PType::Basic(nn, nn_string);
    }

    match datatype {
        DataType::List(inner) => {
            return PType::List(Box::new(polars_datatype_to_xsd_datatype(inner)));
        }
        _ => {
            unimplemented!("Unsupported datatype:{}", datatype)
        }
    };
}
