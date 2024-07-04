use polars::prelude::{DataType, Series};
use representation::polars_to_rdf::{date_series_to_strings, datetime_series_to_strings};

pub fn convert_to_string(series: &Series) -> Series {
    match series.dtype() {
        DataType::String => series.clone(),
        DataType::Date => date_series_to_strings(&series),
        DataType::Datetime(_, tz_opt) => datetime_series_to_strings(&series, tz_opt),
        DataType::Duration(_) => {
            todo!()
        }
        DataType::Time => {
            todo!()
        }
        DataType::List(_) => {
            panic!("Not supported")
        }
        DataType::Categorical(..) => series.cast(&DataType::String).unwrap(),
        DataType::Struct(_) => panic!("Not supported"),
        _ => series.cast(&DataType::String).unwrap(),
    }
}
