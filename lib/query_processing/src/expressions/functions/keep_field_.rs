use polars::datatypes::Field;
use polars::error::PolarsError;
use polars::prelude::Schema;

pub fn keep_field(_s: &Schema, f: &Field) -> Result<Field, PolarsError> {
    Ok(f.clone())
}
