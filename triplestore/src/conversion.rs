use polars::prelude::{col, lit, IntoLazy};
use polars::prelude::{DataFrame, DataType, Series};
use representation::polars_to_sparql::{date_series_to_strings, datetime_series_to_strings};
use representation::{LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};

pub fn convert_to_string(series: &Series) -> Option<Series> {
    let series_data_type = series.dtype();

    match series_data_type {
        DataType::String => return None,
        DataType::Date => return Some(date_series_to_strings(series)),
        DataType::Datetime(_, tz_opt) => return Some(datetime_series_to_strings(series, tz_opt)),
        DataType::Duration(_) => {
            todo!()
        }
        DataType::Time => {
            todo!()
        }
        DataType::List(_) => {
            panic!("Not supported")
        }
        DataType::Categorical(..) => {
            panic!("Not supported")
        }
        DataType::Struct(_) => {
            let mut df = DataFrame::new(vec![series.clone()]).unwrap();
            df = df
                .lazy()
                .with_column(
                    (lit("\"")
                        + col(series.name())
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .cast(DataType::String)
                        + lit("\"@")
                        + col(series.name())
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD))
                    .cast(DataType::String)
                    .alias(series.name()),
                )
                .collect()
                .unwrap();
            return Some(df.drop_in_place(series.name()).unwrap());
        }
        DataType::Unknown => {
            panic!("Not supported")
        }
        _ => {}
    }
    Some(series.cast(&DataType::String).unwrap())
}
