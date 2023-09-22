use crate::constants::{
    XSD_DATETIME_WITHOUT_TZ_FORMAT, XSD_DATETIME_WITH_TZ_FORMAT, XSD_DATE_WITHOUT_TZ_FORMAT,
};
use chrono::TimeZone as ChronoTimeZone;
use chrono::{Datelike, Timelike};
use polars_core::datatypes::{DataType, TimeZone};
use polars_core::series::{IntoSeries, Series};

pub fn convert_to_string(series: &Series) -> Option<Series> {
    let series_data_type = series.dtype();

    match series_data_type {
        DataType::Utf8 => return None,
        DataType::Date => {
            return Some(
                series
                    .date()
                    .unwrap()
                    .strftime(XSD_DATE_WITHOUT_TZ_FORMAT)
                    .into_series(),
            )
        }
        DataType::Datetime(_, tz_opt) => {
            if let Some(tz) = tz_opt {
                return Some(hack_format_timestamp_with_timezone(series, &mut tz.clone()));
            } else {
                return Some(
                    series
                        .datetime()
                        .unwrap()
                        .strftime(XSD_DATETIME_WITHOUT_TZ_FORMAT)
                        .expect("Conversion OK")
                        .into_series(),
                );
            }
        }
        DataType::Duration(_) => {
            todo!()
        }
        DataType::Time => {
            todo!()
        }
        DataType::List(_) => {
            panic!("Not supported")
        }
        DataType::Categorical(_) => {
            panic!("Not supported")
        }
        DataType::Struct(_) => {
            panic!("Not supported")
        }
        DataType::Unknown => {
            panic!("Not supported")
        }
        _ => {}
    }
    Some(series.cast(&DataType::Utf8).unwrap())
}

fn hack_format_timestamp_with_timezone(series: &Series, tz: &mut TimeZone) -> Series {
    let timezone_opt: Result<chrono_tz::Tz, _> = tz.parse();
    if let Ok(timezone) = timezone_opt {
        let datetime_strings = Series::from_iter(
            series
                .datetime()
                .unwrap()
                .as_datetime_iter()
                .map(|x| x.unwrap())
                .map(|x| {
                    format!(
                        "{}",
                        timezone
                            .ymd(x.year(), x.month(), x.day())
                            .and_hms_nano(x.hour(), x.minute(), x.second(), x.nanosecond())
                            .format(XSD_DATETIME_WITH_TZ_FORMAT)
                    )
                }),
        );

        datetime_strings
    } else {
        panic!("Unknown timezone{}", tz);
    }
}
