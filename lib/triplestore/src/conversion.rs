use polars::prelude::{DataType, Series};
use polars_core::prelude::Column;
use representation::polars_to_rdf::{date_column_to_strings, datetime_column_to_strings};

pub fn convert_to_string(column: &Column) -> Column {
    match column.dtype() {
        DataType::String => column.clone(),
        DataType::Date => date_column_to_strings(column),
        DataType::Datetime(_, tz_opt) => datetime_column_to_strings(column, tz_opt),
        DataType::Duration(_) => {
            todo!()
        }
        DataType::Time => {
            todo!()
        }
        DataType::List(_) => {
            panic!("Not supported")
        }
        DataType::Categorical(..) => column.cast(&DataType::String).unwrap(),
        DataType::Struct(_) => panic!("Not supported"),
        _ => column.cast(&DataType::String).unwrap(),
    }
}
