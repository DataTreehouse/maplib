use super::Mapping;
use crate::ast::{has_iritype, PType, Parameter, Signature};

use crate::mapping::errors::MappingError;
use crate::mapping::{ExpandOptions, PrimitiveColumn, RDFNodeType};
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars_core::datatypes::BooleanChunked;
use polars_core::export::rayon::prelude::ParallelIterator;
use polars_core::frame::DataFrame;
use polars_core::prelude::DataType;
use std::collections::{HashMap, HashSet};

impl Mapping {
    pub fn validate_infer_dataframe_columns(
        &self,
        signature: &Signature,
        df: &Option<DataFrame>,
        options: &ExpandOptions,
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
                    if parameter.non_blank {
                        //TODO handle blanks;
                        validate_non_blank_parameter(df, variable_name)?;
                    }
                    let column_data_type = validate_infer_column_data_type(
                        df,
                        parameter,
                        variable_name,
                        &options.language_tags,
                    )?;

                    map.insert(variable_name.to_string(), column_data_type);
                } else {
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
    language_tag_map: &Option<HashMap<String, String>>,
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
    let language_tag = if let Some(map) = language_tag_map {
        map.get(column_name).cloned()
    } else {
        None
    };
    Ok(PrimitiveColumn {
        rdf_node_type,
        language_tag,
    })
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

fn validate_non_blank_parameter(df: &DataFrame, column_name: &str) -> Result<(), MappingError> {
    let is_blank_node_mask: BooleanChunked = df
        .column(column_name)
        .unwrap()
        .utf8()
        .map(move |x| {
            x.par_iter()
                .map(move |x| x.unwrap_or("").starts_with("_:"))
                .collect()
        })
        .unwrap();
    if is_blank_node_mask.any() {
        return Err(MappingError::NonBlankColumnHasBlankNode(
            column_name.to_string(),
            df.column(column_name)
                .unwrap()
                .filter(&is_blank_node_mask)
                .unwrap(),
        ));
    }
    Ok(())
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
    let xsd_nn_ref = match datatype {
        DataType::Boolean => xsd::BOOLEAN,
        DataType::Int8 => xsd::BYTE,
        DataType::Int16 => xsd::SHORT,
        DataType::UInt8 => xsd::UNSIGNED_BYTE,
        DataType::UInt16 => xsd::UNSIGNED_SHORT,
        DataType::UInt32 => xsd::UNSIGNED_INT,
        DataType::UInt64 => xsd::UNSIGNED_LONG,
        DataType::Int32 => xsd::INT,
        DataType::Int64 => xsd::LONG,
        DataType::Float32 => xsd::FLOAT,
        DataType::Float64 => xsd::DOUBLE,
        DataType::Utf8 => xsd::STRING,
        DataType::Date => xsd::DATE,
        DataType::Datetime(_, Some(_)) => xsd::DATE_TIME_STAMP,
        DataType::Datetime(_, None) => xsd::DATE_TIME,
        DataType::Duration(_) => xsd::DURATION,
        DataType::Categorical(_) => xsd::STRING,
        DataType::List(inner) => {
            return PType::List(Box::new(polars_datatype_to_xsd_datatype(inner)))
        }
        _ => {
            panic!("Unsupported datatype:{}", datatype)
        }
    };
    PType::Basic(xsd_nn_ref.into_owned(), "".to_string())
}
