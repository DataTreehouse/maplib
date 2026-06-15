use polars::error::PolarsError;
use polars::prelude::{Column, IntoColumn, Series};

pub fn str_before(c: Column, s: String) -> Result<Column, PolarsError> {
    let bef = c.str()?.iter().map(|x: Option<&str>| {
        if let Some(x) = x {
            let range_to = x.find(&s);
            if let Some(range_to) = range_to {
                Some(&x[0..range_to])
            } else {
                Some(x)
            }
        } else {
            None
        }
    });
    let mut ser = Series::from_iter(bef);
    ser.rename(c.name().clone());
    Ok(ser.into_column())
}
