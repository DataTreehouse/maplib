use polars::error::PolarsError;
use polars::prelude::{Column, IntoColumn, Series};

pub fn str_after(c: Column, s: String) -> Result<Column, PolarsError> {
    let bef = c.str()?.iter().map(|x: Option<&str>| {
        if let Some(x) = x {
            let range_to = x.find(&s);
            if let Some(range_to) = range_to {
                Some(&x[range_to + s.len()..])
            } else {
                // SPARQL 1.1 17.4.3.9/17.4.3.10: no occurrence gives the empty string
                Some("")
            }
        } else {
            None
        }
    });
    let mut ser = Series::from_iter(bef);
    ser.rename(c.name().clone());
    Ok(ser.into_column())
}
